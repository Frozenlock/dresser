(ns dresser.extensions.relations-test
  (:require [clojure.test :as t :refer [deftest testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.relations :as rel]
            [dresser.extensions.durable-refs :as refs]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt]))

(use-fixtures :once (dt/coverage-check rel))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)))

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

(deftest basic-relations
  (db/tx-let [tx (test-dresser)]
      [[doc1 doc2 doc3] (add-docs tx :docs 3)]
    (-> tx
        ;; No relations initially
        (dt/is-> (rel/is? doc1 :friend doc2) false?)
        (dt/is-> (rel/is? doc2 :friend doc1) false?)

        ;; Create a simple relation (uses same relation name in both directions)
        (rel/upsert-relation! doc1 doc2 :friend)
        (dt/is-> (rel/is? doc1 :friend doc2) true?)
        (dt/is-> (rel/is? doc2 :friend doc1) true?)

        ;; Debug: simple test
        (dt/is-> (rel/relation doc1 :friend doc2) some?)
        (dt/is-> (rel/relation doc1 :friend doc2) some?)
        (dt/is-> (rel/relation doc2 :friend doc1) some?)

        ;; Create a bidirectional relation with different relation names
        (rel/upsert-relation! doc1 doc3 :parent :child)
        (dt/is-> (rel/is? doc1 :parent doc3) true?)
        (dt/is-> (rel/is? doc3 :child doc1) true?)

        ;; Remove a specific relation
        (rel/remove-relation! doc1 :friend doc2)
        (dt/is-> (rel/is? doc1 :friend doc2) false?)
        (dt/is-> (rel/is? doc2 :friend doc1) false?)

        ;; Parent-child relation should still exist
        (dt/is-> (rel/is? doc1 :parent doc3) true?)
        (dt/is-> (rel/is? doc3 :child doc1) true?)

        ;; Remove all relations for doc1
        (rel/remove-all-relations! doc1)
        (dt/is-> (rel/is? doc1 :parent doc3) false?)
        (dt/is-> (rel/is? doc3 :child doc1) false?))))

(deftest relation-data
  (db/tx-let [tx (test-dresser)]
      [[company person] (add-docs tx :docs 2)]
    (-> tx
        ;; Create relation with data: company employs person, person is employed by company
        (rel/upsert-relation! company person :employer :employee
                              {:security-clearance 1}
                              {:available-weekends? true})
        (dt/is-> (rel/is? company :employer person) true?)
        (dt/is-> (rel/is? person :employee company) true?)

        ;; Check relation data
        (dt/is-> (rel/relation company :employer person)
                 (= {:security-clearance 1}))
        (dt/is-> (rel/relation person :employee company)
                 (= {:available-weekends? true}))

        ;; Update relation with new data
        (rel/upsert-relation! company person :employer :employee
                              {:security-clearance 2}
                              {:available-weekends? false})
        (dt/is-> (rel/relation company :employer person)
                 (= {:security-clearance 2}))
        (dt/is-> (rel/relation person :employee company)
                 (= {:available-weekends? false})))))

(deftest keep-sync-extension
  (testing "delete"
    (db/tx-let [tx (rel/keep-sync (test-dresser))]
        [[doc1 doc2 doc3] (add-docs tx :docs 3)]
      (-> tx
          (rel/upsert-relation! doc1 doc2 :friend)
          (rel/upsert-relation! doc1 doc3 :parent :child)
          (dt/is-> (rel/is? doc1 :friend doc2) true?)
          (dt/is-> (rel/is? doc1 :parent doc3) true?)

          ;; Delete doc1, all its relations should be removed
          (refs/delete! doc1)
          (dt/is-> (refs/fetch-by-ref doc1) nil?)
          (dt/is-> (rel/is? doc2 :friend doc1) false?)
          (dt/is-> (rel/is? doc3 :child doc1) false?))))

  (testing "delete-many"
    (db/tx-let [tx (rel/keep-sync (test-dresser))]
        [docs (add-docs tx :docs 4)
         [doc1 doc2 doc3 doc4] docs]
      (-> tx
          (rel/upsert-relation! doc1 doc2 :friend)
          (rel/upsert-relation! doc3 doc4 :friend)
          (dt/is-> (rel/is? doc1 :friend doc2) true?)
          (dt/is-> (rel/is? doc3 :friend doc4) true?)

          ;; Delete doc1 and doc3 with delete-many
          (db/delete-many! :docs {:id {::db/any [(:doc-id doc1) (:doc-id doc3)]}})

          ;; Relations to deleted docs should be gone
          (dt/is-> (rel/is? doc2 :friend doc1) false?)
          (dt/is-> (rel/is? doc4 :friend doc3) false?))))

  (testing "drop"
    (db/tx-let [tx (rel/keep-sync (test-dresser))]
        [[doc1 doc2] (add-docs tx :docs1 2)
         [doc3 doc4] (add-docs tx :docs2 2)]
      (-> tx
          (rel/upsert-relation! doc1 doc3 :related)
          (rel/upsert-relation! doc2 doc4 :related)
          (dt/is-> (rel/is? doc1 :related doc3) true?)
          (dt/is-> (rel/is? doc2 :related doc4) true?)

          ;; Drop one drawer
          (db/drop! :docs1)

          ;; Relations to docs1 should be gone
          (dt/is-> (refs/fetch-by-ref doc1) nil?)
          (dt/is-> (refs/fetch-by-ref doc2) nil?)
          (dt/is-> (rel/is? doc3 :related doc1) false?)
          (dt/is-> (rel/is? doc4 :related doc2) false?)))))

(deftest update-in-relation
  (db/tx-let [tx (test-dresser)]
      [[bob alice company person] (add-docs tx :docs 4)]
    (-> tx
        ;; Bob allows Alice to borrow his car with initial restrictions
        ;; Alice provides her insurance info
        (rel/upsert-relation! bob alice :lends-to :borrows-from
                              {:can-borrow-car? true :max-days 3}
                              {:has-insurance? true})

        ;; Test updating Bob's lending permissions (Bob controls these)
        (rel/update-in-relation! bob :lends-to alice :borrows-from
                                 #(assoc % :max-days 7)  ; Bob increases max days
                                 nil)

        ;; Check Bob's updated permissions
        (dt/is-> (rel/relation bob :lends-to alice)
                 (= {:can-borrow-car? true :max-days 7}))

        ;; Company employs person with initial security clearance
        ;; Person provides availability
        (rel/upsert-relation! company person :employer :employee
                              {:security-clearance 1}
                              {:available-weekends? true})


        ;; Test updating company's security clearance (company controls this)
        (rel/update-in-relation! company :employee person :employer
                                 #(assoc % :security-clearance 2)  ; Company increases clearance
                                 nil)

        ;; Check company's updated clearance
        (dt/is-> (rel/relation company :employee person)
                 (= {:security-clearance 2}))

        ;; Test accumulating data - Bob adds more allowed activities
        (rel/update-in-relation! bob :lends-to alice :borrows-from
                                 #(update % :allowed-activities (fnil conj []) "grocery-shopping")
                                 nil)
        (rel/update-in-relation! bob :lends-to alice :borrows-from
                                 #(update % :allowed-activities (fnil conj []) "work-commute")
                                 nil)

        ;; Check Bob's accumulated permissions
        (dt/is-> (rel/relation bob :lends-to alice)
                 (= {:can-borrow-car? true :max-days 7 :allowed-activities ["grocery-shopping" "work-commute"]}))

        ;; Test Alice updating her own info
        (rel/update-in-relation! alice :borrows-from bob :lends-to
                                 #(assoc % :emergency-contact "555-1234")
                                 nil)

        ;; Check Alice's updated info
        (dt/is-> (rel/relation alice :borrows-from bob)
                 (= {:has-insurance? true :emergency-contact "555-1234"})))))
