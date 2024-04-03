(ns dresser.extensions.rbac-test
  (:require [clojure.test :as t :refer [deftest testing use-fixtures is]]
            [dresser.base :as db]
            [dresser.extensions.durable-refs :as refs]
            [dresser.extensions.memberships :as mbr]
            [dresser.extensions.rbac :as rbac]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt]
            [dresser.protocols :as dp]))

(use-fixtures :once (dt/coverage-check rbac))

(defn sequential-id-by-drawer
  "Replaces gen-id implementation (metadata) with ones that increases sequentially.

  Ephemeral DB only; does not check for existing IDs.
  Only works at the fundamental level."
  [dresser]
  (let [*counter (atom {})]
    (->> (dp/mapify-impls [(dp/impl -gen-id
                             [dresser drawer]
                             (db/with-result dresser
                               (-> (swap! *counter update drawer (fnil inc 0))
                                   (get drawer))))])
         (vary-meta dresser merge))))

(defn- test-dresser
  []
  (-> (hm/build)
      (sequential-id-by-drawer)
      ;(dt/sequential-id)
      (dt/no-tx-reuse)
      (mbr/keep-sync)))

(defn- add-groups!
  "Adds n groups and return their DB refs."
  ([dresser n]
   (add-groups! dresser n :grps))
  ([dresser n drawer]
   (db/tx-let [tx dresser]
       [_ (db/with-result tx nil)] ; clean any existing result
     (reduce (fn [tx _i]
               (let [refs (db/result tx)
                     [tx id] (db/dr (db/add! tx drawer {}))
                     [tx new-ref] (db/dr (refs/ref! tx drawer id))]
                 (db/with-result tx (conj (or refs []) new-ref))))
             tx (range n)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest missing-permissions
  (let [request-everything {:read :?}
        request-username {:read {:username :?}}
        request-address-street {:read {:address {:street :?}}}
        request-email {:read {:email :?}}]
    (testing "All read allowed"
      (let [perms {:read true}]
        (is (empty? (rbac/missing-permissions perms request-everything)))
        (is (empty? (rbac/missing-permissions perms request-username)))
        (is (empty? (rbac/missing-permissions perms request-address-street)))
        (is (empty? (rbac/missing-permissions perms request-email)))))
    (testing "Single top field"
      (let [perms {:read {:username true}}]
        (is (not-empty (rbac/missing-permissions perms request-everything)))
        (is (empty? (rbac/missing-permissions perms request-username)))
        (is (not-empty (rbac/missing-permissions perms request-address-street)))
        (is (not-empty (rbac/missing-permissions perms request-email)))))
    (testing "Single nested field"
      (let [perms {:read {:address {:street true}}}]
        (is (not-empty (rbac/missing-permissions perms request-everything)))
        (is (not-empty (rbac/missing-permissions perms request-username)))
        (is (empty? (rbac/missing-permissions perms request-address-street)))
        (is (not-empty (rbac/missing-permissions perms request-email)))))
    (testing "Wildcard permissions"
      (let [perms {:read {:* true}}] ; Wildcard allowing read access to all top-level fields
        (is (empty? (rbac/missing-permissions perms request-everything)))
        (is (empty? (rbac/missing-permissions perms request-username)))
        (is (empty? (rbac/missing-permissions perms request-address-street)))
        (is (empty? (rbac/missing-permissions perms request-email)))))
    (testing "Wildcard nested field permissions"
      (let [perms {:read {:address {:* true}}}] ; Wildcard allowing read access to all fields under :address
        (is (not-empty (rbac/missing-permissions perms request-everything)))
        (is (not-empty (rbac/missing-permissions perms request-username)))
        (is (empty? (rbac/missing-permissions perms request-address-street))) ; Should succeed because :street is under :address
        (is (not-empty (rbac/missing-permissions perms request-email)))))
    (testing "Specific and wildcard mixed permissions"
      (let [perms {:read {:username true, :* {:street true}}}] ; Allow :username at top level and :street under any field
        (is (not-empty (rbac/missing-permissions perms request-everything)))
        (is (empty? (rbac/missing-permissions perms request-username)))
        (is (empty? (rbac/missing-permissions perms request-address-street)))
        (is (not-empty (rbac/missing-permissions perms request-email)))
        (is (not-empty (rbac/missing-permissions perms {:address :?})))))
    (testing "Cumulative specific and wildcard permissions"
      (let [request-address {:read {:address :?}}
            request-address-street-zip {:read {:address {:street :?
                                                         :zip    :?}}}
            perms {:read {:*       {:street true}
                          :address {:zip true}}}]
        (is (empty? (rbac/missing-permissions perms request-address-street-zip)))
        (is (not-empty (rbac/missing-permissions perms request-address)))))))




(deftest permitted?
  (testing "Single map, simple permissions"
    (is (rbac/permitted? [{:read true, :write false}] {:read :?}))
    (is (not (rbac/permitted? [{:read false}] {:read :?}))))

  (testing "Single map, nested permissions"
    (is (rbac/permitted? [{:data {:read true}}] {:data {:read :?}}))
    (is (not (rbac/permitted? [{:data {:read false}}] {:data {:read :?}}))))

  (testing "Multiple maps, combining permissions"
    (is (rbac/permitted? [{:read false} {:read true}] {:read :?}))
    (is (not (rbac/permitted? [{:read false} {:write true}] {:read :?}))))

  (testing "Multiple maps with specific and wildcard permissions"
    (is (rbac/permitted? [{:data {:read {:email true}}} {:data {:* true}}] {:data {:read :?}}))
    (is (rbac/permitted? [{:data {:* false}} {:data {:read true}}] {:data {:read :?}})))

  (testing "Combining permissions across maps with nested structures"
    (let [perm1 {:data {:read {:email false}, :write false}}
          perm2 {:data {:read {:email true}, :write true}}
          request {:data {:read {:email :?}, :write :?}}]
      (is (rbac/permitted? [perm1 perm2] request))
      (is (not (rbac/permitted? [perm1] request))))))




(deftest permission-chain
  (let [dresser (test-dresser)
        dresser (db/raw-> dresser (add-groups! 5))
        [p1 usr1 usr2 grp1 grp2] (db/result dresser)]
    (db/tx-> dresser
      (dt/testing-> "Direct permission"
        (rbac/set-roles-permissions! p1 {:owner {:read  true
                                                 :write true}})

        (mbr/upsert-group-member! p1 grp1 [:owner])
        (dt/is-> (rbac/permission-chain p1 {:write true} grp1) (= [grp1]))

        (mbr/upsert-group-member! p1 grp1 [:visitor])  ; <- undefined role
        (dt/is-> (rbac/permission-chain p1 {:write true} grp1) (= [])))

      (dt/testing-> "Indirect permission"
        (rbac/set-roles-permissions! p1 {:owner {:read  true
                                                 :write true}})
        ;; grp1 is owner of p1
        (mbr/upsert-group-member! p1 grp1 [:owner])
        (dt/is-> (rbac/permission-chain p1 {:write true} grp1) (= [grp1]))

        ;; grp2 is admin of grp1.
        ;; Different roles (owner vs admin) to validate we use permissions, not roles.
        (mbr/upsert-group-member! grp1 grp2 [:admin])
        (dt/is-> (rbac/permission-chain p1 {:write true} grp2) (= [grp2 grp1]))

        ;; usr1 can READ, but not write in grp2
        (mbr/upsert-group-member! grp2 usr1 [:reader])
        (dt/is-> (rbac/permission-chain p1 {:read true} usr1) (= [usr1 grp2 grp1]))
        (dt/is-> (rbac/permission-chain p1 {:write true} usr1) (= []))

        ;; Let's fix that by giving usr1 the :admin role.
        (mbr/upsert-group-member! grp2 usr1 [:admin :reader])
        (dt/is-> (rbac/permission-chain p1 {:write true} usr1) (= [usr1 grp2 grp1]))))

    (db/tx-> dresser
      (dt/testing-> "Circular dependencies"
        ;; Set roles for p1 and add grp1 as a member of p1
        (rbac/set-roles-permissions! p1 {:manager {:read  true
                                                   :write true}})
        (mbr/upsert-group-member! p1 grp1 [:manager])

        ;; Set roles for grp1 and grp2
        (rbac/set-roles-permissions! grp1 {:manager {:read  true
                                                     :write true}})
        (rbac/set-roles-permissions! grp2 {:manager {:read  true
                                                     :write true}})

        ;; Add usr1 to grp1 and usr2 to grp2
        (mbr/upsert-group-member! grp1 usr1 [:manager])
        (mbr/upsert-group-member! grp2 usr2 [:manager])

        ;; Add grp2 as a member of grp1 and test permissions for usr1
        (mbr/upsert-group-member! grp1 grp2 [:manager])
        (dt/is-> (rbac/permission-chain p1 {:read true} usr1) (= [usr1 grp1]))

        ;; Add grp1 as a member of grp2 and test permissions for usr1
        ;; This creates a circular dependency, but it should not affect the permission chain.
        (mbr/upsert-group-member! grp2 grp1 [:manager])
        (dt/is-> (rbac/permission-chain p1 {:read true} usr1) (= [usr1 grp1]))
        (dt/is-> (rbac/permission-chain p1 {:wrong-permission true} usr1) (= []))))))


(deftest guest-roles
  (let [dresser (test-dresser)
        dresser (db/raw-> dresser (add-groups! 5))
        [p1 usr1 usr2 grp1 grp2] (db/result dresser)]
    (db/tx-> dresser
      (dt/testing-> "Direct permission"
        ;; Initially, nobody can write in grp1.
        (dt/is-> (rbac/permission-chain grp1 {:write :?} usr1) (= []))
        ;; But once one of the guest roles is 'admin', everybody can.
        (rbac/set-guest-roles! grp1 [:admin])
        (dt/is-> (rbac/permission-chain grp1 {:write :?} usr1) (= [usr1]))
        (dt/is-> (rbac/permission-chain grp1 {:write :?} usr2) (= [usr2]))
        (dt/is-> (rbac/permission-chain grp1 {:write :?} grp1) (= [grp1])))

      (dt/testing-> "Indirect permission"
        (rbac/set-guest-roles! grp1 nil) ; reset guest roles
        ;; p1 -> grp1 -> (grp2) -> usr1
        (mbr/upsert-group-member! p1 grp1 [:admin])
        (mbr/upsert-group-member! grp1 grp2 [:admin])
        (mbr/upsert-group-member! grp2 usr1 nil)
        (mbr/upsert-group-member! grp2 usr2 [:admin])
        ;; grp2 is the broken link for usr1
        (dt/is-> (rbac/permission-chain p1 {:read :?} usr1) (= []))
        (dt/is-> (rbac/permission-chain p1 {:read :?} usr2) (= [usr2 grp2 grp1]))

        ;; Even if usr1 was never officially set as a member of grp2,
        ;; it can act as a guest in it.
        (rbac/set-guest-roles! grp2 [:reader])
        (dt/is-> (rbac/permission-chain p1 {:read :?} usr1) (= [usr1 grp2 grp1]))
        (dt/is-> (rbac/permission-chain p1 {:write :?} usr1) (= []) ":reader guest can't write")
        (dt/is-> (rbac/permission-chain p1 {:write :?} usr2) (= [usr2 grp2 grp1]))

        ;; The permission chain is shortened if grp1 itself accepts guests.
        (rbac/set-guest-roles! grp1 [:reader])
        (dt/is-> (rbac/permission-chain p1 {:read :?} usr1) (= [usr1 grp1]))
        (dt/is-> (rbac/permission-chain p1 {:write :?} usr1) (= []) ":reader guest can't write")
        (dt/is-> (rbac/permission-chain p1 {:read :?} usr2) (= [usr2 grp1]))
        (dt/is-> (rbac/permission-chain p1 {:write :?} usr2) (= [usr2 grp2 grp1]))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;


(deftest wrapped-rbac
  (let [dresser (test-dresser)
        [dresser [usr1 usr2]] (db/dr (db/raw-> dresser (add-groups! 2 :users)))
        [dresser [grp1 grp2]] (db/dr (db/raw-> dresser (add-groups! 2 :grps)))
        dresser (db/raw-> dresser
                  (rbac/set-roles-permissions! grp1 {:owner {:read  true
                                                             :write true}})
                  (mbr/upsert-group-member! grp1 usr1 [:owner]))]
    (db/tx-> (rbac/enforce-rbac dresser usr1 {:add :*})

      ; wrap the dresser for "usr1"
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

          (rbac/with-doc-adder grp2)
          (dt/is-> (db/add! :drawer {:doc "data"})
                   (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission")
                   "Requires write permission to the adder to set it as an admin")

          (rbac/with-doc-adder grp1)
          ((fn [tx]
             (db/tx-let [tx tx]
                 [new-doc-id (dt/is-> tx
                                      (db/add! :drawer {:doc "data"}) some?
                                      "Can add with sufficient permission and allowed drawer")
                  new-doc-ref (refs/ref tx :drawer new-doc-id)
                  admin-refs (mbr/members-of-group-with-roles tx new-doc-ref [mbr/role-admin])]
               (def aaa tx)
               (is (= admin-refs [grp1])))))))

      (dt/testing-> "Higher order operations"
        ;; usr1 is owner and should be able to edit grp1 roles.
        (mbr/upsert-group-member! grp1 usr1 [:owner :visitor])
        ;; Usr1 however doesn't have the permission to write in grp2.
        ;; It cannot 'escalate' its rights.
        (dt/is-> (mbr/upsert-group-member! grp2 usr1 [:owner])
                 (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission"))))

    (testing "Fetch"
      (let [user1-data {:id "user1", :name "user1"}
            user2-data {:id "user2", :name "user2"}
            grp1-data {:id "grp1" :name "grp1"}
            [dresser [user1-ref
                      user2-ref
                      grp1-ref]] (db/dr
                                  (db/tx-let [tx (test-dresser) {:result? false}]
                                      [user1-ref (refs/upsert! tx :users user1-data)
                                       user2-ref (refs/upsert! tx :users user2-data)
                                       grp1-ref (refs/upsert! tx :grps grp1-data)
                                       _ (mbr/upsert-group-member! tx grp1-ref user1-ref [:reader])]
                                    [user1-ref user2-ref grp1-ref]))]
        (db/tx-> dresser
          (dt/is-> (db/fetch :users {:where {:id (:id user1-data)}
                                     :only  (keys user1-data)})
                   (= [user1-data])
                   "Sanity check, no rbac"))

        ;; User1 can fetch itself and grp1
        (db/tx-> (rbac/enforce-rbac dresser user1-ref {})
          (dt/is-> (db/fetch :users {:where {:id (:id user1-data)}
                                     :only  (keys user1-data)})
                   (= [user1-data])
                   "User can fetch itself")
          (dt/is-> (db/fetch :users {:where {:id (:id user2-data)}
                                     :only  (keys user2-data)})
                   (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission")
                   "Can't fetch without permission")
          (dt/is-> (db/fetch :grps {:where {:id (:id grp1-data)}
                                    :only  (keys grp1-data)})
                   (= [grp1-data])
                   "User can fetch grp with permission"))

        ;; User2 can only fetch itself
        (db/tx-> (rbac/enforce-rbac dresser user2-ref {})
          (dt/is-> (db/fetch :users {:where {:id (:id user1-data)}
                                     :only  (keys user1-data)})
                   (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission")
                   "Can't fetch without permission")
          (dt/is-> (db/fetch :users {:where {:id (:id user2-data)}
                                     :only  (keys user2-data)})
                   (= [user2-data])
                   "User can fetch itself")
          (dt/is-> (db/fetch :grps {:where {:id (:id grp1-data)}
                                    :only  (keys grp1-data)})
                   (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission")
                   "Can't fetch without permission"))

        (db/tx-> (rbac/enforce-rbac dresser user2-ref {:read {:grps true}})
          (dt/is-> (db/fetch :grps {:where {:id (:id grp1-data)}
                                    :only  (keys grp1-data)})
                   (= [grp1-data])
                   "Provided permission"))))

    (testing "Self modification"
      (let [user-data {:id "user1", :name "New user"}]
        (let [[dresser new-user] (db/dr (db/raw-> (test-dresser) (refs/ref! :users (:id user-data))))]
          (db/tx-> dresser
            (rbac/enforce-rbac new-user {})
            (dt/is-> (refs/assoc-at! new-user [:a] "some value")
                     (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing permission")
                     "Trying to write to 'itself' when doesn't exist should fail.")))

        (let [[dresser new-user] (db/dr (db/raw-> (test-dresser) (refs/ref! :users (:id user-data))))]
          (db/tx-> (-> (db/raw-> dresser (db/upsert! :users user-data))
                       (rbac/enforce-rbac new-user {}))
            (refs/assoc-at! new-user [:a] "some value")
            (dt/is-> (refs/fetch-by-ref new-user) (= (assoc user-data :a "some value")))))))))
