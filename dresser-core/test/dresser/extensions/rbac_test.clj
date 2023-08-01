(ns dresser.extensions.rbac-test
  (:require [clojure.test :as t :refer [deftest testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.memberships :as mbr]
            [dresser.extensions.rbac :as rbac]
            [dresser.impl.hashmap :as hm]
            [dresser.extensions.durable-refs :as refs]
            [dresser.test :as dt]))

(use-fixtures :once (dt/coverage-check rbac))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)
      (mbr/keep-sync)))

(defn- add-groups!
  "Adds n groups and return their DB refs."
  [dresser n]
  (db/tx-let [tx dresser]
      [_ (db/with-result tx nil)] ; clean any existing result
    (reduce (fn [tx _i]
              (let [refs (db/result tx)
                    [tx id] (db/dr (db/add! tx :grps {}))
                    [tx new-ref] (db/dr (refs/ref! tx :grps id))]
                (db/with-result tx (conj refs new-ref))))
            tx (range n))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(deftest permission-chain
  (let [dresser (test-dresser)
        dresser (db/raw-> dresser (add-groups! 5))
        [p1 usr1 usr2 grp1 grp2] (db/result dresser)]
    (db/tx-> dresser
      (dt/testing-> "Direct permission"
        (rbac/set-roles-permissions! p1 {:owner [:read :write]})

        (mbr/upsert-group-member! p1 grp1 [:owner])
        (dt/is-> (rbac/permission-chain p1 :write grp1) (= [grp1]))

        (mbr/upsert-group-member! p1 grp1 [:visitor])  ; <- undefined role
        (dt/is-> (rbac/permission-chain p1 :write grp1) (= [])))

      (dt/testing-> "Indirect permission"
        (rbac/set-roles-permissions! p1 {:owner [:read :write]})
        ;; grp1 is owner of p1
        (mbr/upsert-group-member! p1 grp1 [:owner])
        (dt/is-> (rbac/permission-chain p1 :write grp1) (= [grp1]))

        ;; grp2 is admin of grp1.
        ;; Different roles (owner vs admin) to validate we use permissions, not roles.
        (mbr/upsert-group-member! grp1 grp2 [:admin])
        (dt/is-> (rbac/permission-chain p1 :write grp2) (= [grp2 grp1]))

        ;; usr1 can READ, but not write in grp2
        (mbr/upsert-group-member! grp2 usr1 [:reader])
        (dt/is-> (rbac/permission-chain p1 :read usr1) (= [usr1 grp2 grp1]))
        (dt/is-> (rbac/permission-chain p1 :write usr1) (= []))

        ;; Let's fix that by giving usr1 the :admin role.
        (mbr/upsert-group-member! grp2 usr1 [:admin :reader])
        (dt/is-> (rbac/permission-chain p1 :write usr1) (= [usr1 grp2 grp1]))))

    (db/tx-> dresser
      (dt/testing-> "Circular dependencies"
        ;; Set roles for p1 and add grp1 as a member of p1
        (rbac/set-roles-permissions! p1 {:manager [:read :write]})
        (mbr/upsert-group-member! p1 grp1 [:manager])

        ;; Set roles for grp1 and grp2
        (rbac/set-roles-permissions! grp1 {:manager [:read :write]})
        (rbac/set-roles-permissions! grp2 {:manager [:read :write]})

        ;; Add usr1 to grp1 and usr2 to grp2
        (mbr/upsert-group-member! grp1 usr1 [:manager])
        (mbr/upsert-group-member! grp2 usr2 [:manager])

        ;; Add grp2 as a member of grp1 and test permissions for usr1
        (mbr/upsert-group-member! grp1 grp2 [:manager])
        (dt/is-> (rbac/permission-chain p1 :read usr1) (= [usr1 grp1]))

        ;; Add grp1 as a member of grp2 and test permissions for usr1
        ;; This creates a circular dependency, but it should not affect the permission chain.
        (mbr/upsert-group-member! grp2 grp1 [:manager])
        (dt/is-> (rbac/permission-chain p1 :read usr1) (= [usr1 grp1]))
        (dt/is-> (rbac/permission-chain p1 :wrong-permission usr1) (= []))))))


(deftest guest-roles
  (let [dresser (test-dresser)
        dresser (db/raw-> dresser (add-groups! 5))
        [p1 usr1 usr2 grp1 grp2] (db/result dresser)]
    (db/tx-> dresser
      (dt/testing-> "Direct permission"
        ;; Initially, nobody can write in grp1.
        (dt/is-> (rbac/permission-chain grp1 :write usr1) (= []))
        ;; But once one of the guest roles is 'admin', everybody can.
        (rbac/set-guest-roles! grp1 [:admin])
        (dt/is-> (rbac/permission-chain grp1 :write usr1) (= [usr1]))
        (dt/is-> (rbac/permission-chain grp1 :write usr2) (= [usr2]))
        (dt/is-> (rbac/permission-chain grp1 :write grp1) (= [grp1])))

      (dt/testing-> "Indirect permission"
        (rbac/set-guest-roles! grp1 nil) ; reset guest roles
        ;; p1 -> grp1 -> (grp2) -> usr1
        (mbr/upsert-group-member! p1 grp1 [:admin])
        (mbr/upsert-group-member! grp1 grp2 [:admin])
        (mbr/upsert-group-member! grp2 usr1 nil)
        (mbr/upsert-group-member! grp2 usr2 [:admin])
        ;; grp2 is the broken link for usr1
        (dt/is-> (rbac/permission-chain p1 :read usr1) (= []))
        (dt/is-> (rbac/permission-chain p1 :read usr2) (= [usr2 grp2 grp1]))

        ;; Even if usr1 was never officially set as a member of grp2,
        ;; it can act as a guest in it.
        (rbac/set-guest-roles! grp2 [:reader])
        (dt/is-> (rbac/permission-chain p1 :read usr1) (= [usr1 grp2 grp1]))
        (dt/is-> (rbac/permission-chain p1 :write usr1) (= []) ":reader guest can't write")
        (dt/is-> (rbac/permission-chain p1 :write usr2) (= [usr2 grp2 grp1]))

        ;; The permission chain is shortened if grp1 itself accepts guests.
        (rbac/set-guest-roles! grp1 [:reader])
        (dt/is-> (rbac/permission-chain p1 :read usr1) (= [usr1 grp1]))
        (dt/is-> (rbac/permission-chain p1 :write usr1) (= []) ":reader guest can't write")
        (dt/is-> (rbac/permission-chain p1 :read usr2) (= [usr2 grp1]))
        (dt/is-> (rbac/permission-chain p1 :write usr2) (= [usr2 grp2 grp1]))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;


(deftest wrapped-rbac
  (let [dresser (test-dresser)
        dresser (db/raw-> dresser (add-groups! 5))
        [p1 usr1 usr2 grp1 grp2] (db/result dresser)
        dresser (db/raw-> dresser
                  (rbac/set-roles-permissions! grp1 {:owner [:read :write]})
                  (mbr/upsert-group-member! grp1 usr1 [:owner]))]
    (db/tx-> (rbac/enforce-rbac dresser usr1) ; wrap the dresser for "usr1"
      (dt/testing-> "Basic operations"
        (dt/is-> (db/drop! :drawer)
                 (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission")
                 "Drop is never permitted")
        ;; Basic operations behave as expected.
        (dt/is-> (refs/assoc-at! grp1 [:my-test] "value")
                 (= "value"))
        (dt/is-> (refs/get-at grp1 [:my-test])
                 (= "value"))
        ;; ... And are forbidden when the user doesn't have the necessary rights.
        (dt/is-> (refs/assoc-at! grp2 [:my-test] "value")
                 (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission"))

        (dt/testing-> "Add"
          (dt/is-> (db/add! :drawer {:doc "data"})
                   (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing document adder reference"))

          (rbac/with-doc-adder grp1 [:drawer])
          (dt/is-> (db/add! :drawer {:doc "data"})
                   (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission")
                   "Add still requires the adder permission")

          (mbr/upsert-group-member! grp1 usr1 [:admin])
          (dt/is-> (db/add! :drawer {:doc "data"}) some?
                   "Can add with sufficient permission and allowed drawer")

          (dt/is-> (db/add! :other-drawer {:doc "data"})
                   (thrown-with-msg? clojure.lang.ExceptionInfo #"Drawer not allowed")
                   "Drawer must be in the allowed list")))

      (dt/testing-> "Higher order operations"
        ;; usr1 is owner and should be able to edit grp1 roles.
        (mbr/upsert-group-member! grp1 usr1 [:owner :visitor])
        ;; Usr1 however doesn't have the permission to write in grp2.
        ;; It cannot 'escalate' its rights.
        (dt/is-> (mbr/upsert-group-member! grp2 usr1 [:owner])
                 (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission"))))

    (testing "Self modification"
      (let [user-data {:id "user1", :name "New user"}]
        (let [[dresser new-user] (db/dr (db/raw-> (test-dresser) (refs/ref! :users (:id user-data))))]
          (db/tx-> dresser
            (rbac/enforce-rbac new-user)
            (dt/is-> (refs/assoc-at! new-user [:a] "some value")
                     (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission")
                     "Trying to write to 'itself' when doesn't exist should fail.")))

        (let [[dresser new-user] (db/dr (db/raw-> (test-dresser) (refs/ref! :users (:id user-data))))]
          (db/tx-> (-> (db/raw-> dresser (db/upsert! :users user-data))
                       (rbac/enforce-rbac new-user))
            (refs/assoc-at! new-user [:a] "some value")
            (dt/is-> (refs/fetch-by-ref new-user) (= (assoc user-data :a "some value")))))))))
