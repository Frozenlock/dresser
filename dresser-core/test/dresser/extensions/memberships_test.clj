(ns dresser.extensions.memberships-test
  (:require [clojure.test :as t :refer [deftest testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.memberships :as mbr]
            [dresser.impl.hashmap :as hm]
            [dresser.extensions.durable-refs :as refs]
            [dresser.test :as dt]))

(use-fixtures :once (dt/coverage-check mbr))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)))

(deftest upsert-member
  (let [dresser (test-dresser)
        p1 (refs/ref! dresser :projects :p1)
        p2 (refs/ref! dresser :projects :p2) ; test that we don't return unrelated groups
        usr1 (refs/ref! dresser :users :usr1)
        usr2 (refs/ref! dresser :users :usr2)]
    (db/tx-> dresser
      ;; No relations
      (dt/is-> (mbr/members-of-group p1) (= []))
      (dt/is-> (mbr/memberships-of-member usr1) (= []))
      (dt/is-> (mbr/memberships-of-member usr2) (= []))
      ;; Add a member with roles
      (dt/is-> (mbr/upsert-group-member! p1 usr1 [:admin :editor]) (= p1))
      (dt/is-> (mbr/members-of-group p1) (= [usr1]))
      (dt/is-> (mbr/memberships-of-member usr1) (= [p1]))
      (dt/is-> (mbr/memberships-of-member usr2) (= []))
      ;; Another admin, check that urs1 is still there.
      (dt/is-> (mbr/upsert-group-member! p1 usr2 [:admin]) (= p1))
      (dt/is-> (mbr/members-of-group p1) (= [usr1 usr2]))
      (dt/is-> (mbr/memberships-of-member usr1) (= [p1]))
      (dt/is-> (mbr/memberships-of-member usr2) (= [p1]))
      ;; Update to a single role, memberships should stay the same
      (dt/is-> (mbr/upsert-group-member! p1 usr1 [:admin]) (= p1))
      (dt/is-> (mbr/members-of-group p1) (= [usr1 usr2]))
      (dt/is-> (mbr/memberships-of-member usr1) (= [p1]))
      (dt/is-> (mbr/memberships-of-member usr2) (= [p1]))
      ;; No roles, should remove from group
      (dt/is-> (mbr/upsert-group-member! p1 usr1 []) (= p1))
      (dt/is-> (mbr/members-of-group p1) (= [usr2]))
      (dt/is-> (mbr/memberships-of-member usr1) (= []))
      (dt/is-> (mbr/memberships-of-member usr2) (= [p1])
               "usr2 is unaffected"))))

(deftest members-of-group-with-roles
  (let [dresser (test-dresser)
        p1 (refs/ref! dresser :projects :p1)
        usr1 (refs/ref! dresser :users :usr1)
        usr2 (refs/ref! dresser :users :usr2)]
    (db/tx-> dresser
      (mbr/upsert-group-member! p1 usr1 [:admin :editor])
      (mbr/upsert-group-member! p1 usr2 [:editor :visitor])
      (dt/is-> (mbr/members-of-group-with-roles p1 [:admin]) (= [usr1]))
      (dt/is-> (mbr/members-of-group-with-roles p1 [:admin :visitor]) (= [usr1 usr2]))
      (dt/is-> (mbr/members-of-group-with-roles p1 [:editor]) (= [usr1 usr2]))
      (dt/is-> (mbr/members-of-group-with-roles p1 [:clown]) (= []))
      (dt/is-> (mbr/members-of-group-with-roles p1 nil) (= [])))))

(deftest add-with-roles
  (let [dresser (test-dresser)
        grp1 (refs/ref! dresser :groups :grp1)
        doc {:name "Bob"}
        drawer :api-keys]
    (db/tx-let [tx dresser]
        [id (mbr/add-with-roles! tx
                                 grp1 [:reader] ; as reader in grp1
                                 drawer doc ; add document in drawer
                                 )]
      (-> tx
          (dt/is-> (db/fetch-by-id drawer id) ((fn [added-doc]
                                                 (= doc (select-keys added-doc (keys doc)))))
                   "Document is correctly added")
          (dt/is-> ((fn [tx]
                      (let [[tx doc-ref] (db/dr (refs/ref! tx drawer id))]
                        (mbr/memberships-of-member tx doc-ref))))
                   (= [grp1])
                   "Document has membership")))))

(defn- add-docs
  "Adds n documents in a drawer and return their DB refs."
  [dresser drawer n]
  (db/with-tx [tx (db/with-result dresser nil)] ; clean any existing result
    (reduce (fn [tx _i]
              (let [refs (db/result tx)
                    [tx id] (db/dr (db/add! tx drawer {}))
                    [tx new-ref] (db/dr (refs/ref! tx drawer id))]
                (db/with-result tx (conj refs new-ref))))
            tx (range n))))

(deftest membership-wrap
  (testing "db/delete"
    (db/tx-let [tx (mbr/keep-sync (test-dresser))]
        [[usr1] (add-docs tx :users 1)
         [grp1 grp2] (add-docs tx :groups 2)]
      (-> tx
          (mbr/upsert-group-member! grp1 usr1 [:admin :editor])
          (mbr/upsert-group-member! grp2 usr1 [:clown])
          (dt/is-> (-> (mbr/memberships-of-member usr1)
                       (db/update-result set))
                   (= #{grp1 grp2}))
          (dt/testing-> "delete group"
            (refs/delete! grp1)

            (dt/is-> (mbr/members-of-group grp1) (= []))
            (dt/is-> (mbr/members-of-group grp2) (= [usr1]))
            (dt/is-> (mbr/memberships-of-member usr1) (= [grp2])))
          (dt/testing-> "delete user"
            (refs/delete! usr1)
            (dt/is-> (mbr/memberships-of-member usr1) (= []))
            (dt/is-> (mbr/members-of-group grp2) (= []))))))
  (testing "db/drop"
    (db/tx-let [tx (mbr/keep-sync (test-dresser))]
        [[usr1] (add-docs tx :users 1)
         [grp1 grp2] (add-docs tx :groups 2)
         [project1] (add-docs tx :projects 1)]
      (-> tx
          (mbr/upsert-group-member! grp1 usr1 [:admin :editor])
          (mbr/upsert-group-member! grp2 usr1 [:clown])
          (mbr/upsert-group-member! project1 grp1 [:admin])
          (dt/is-> (-> (mbr/memberships-of-member usr1)
                       (db/update-result set))
                   (= #{grp1 grp2}))
          (dt/is-> (mbr/members-of-group project1) (= [grp1]))
          (db/drop! :groups)
          (dt/is-> (mbr/memberships-of-member usr1) (= []))
          (dt/is-> (mbr/memberships-of-member grp1) (= []))
          (dt/is-> (mbr/members-of-group grp1) (= []))
          (dt/is-> (mbr/members-of-group grp1) (= []))))))
