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

(deftest relations-query
  (db/tx-let [tx (test-dresser)]
      [[alice bob charlie diana] (add-docs tx :people 4)
       [company1 company2] (add-docs tx :companies 2)
       people-drawer-id (refs/drawer-id alice)
       company-drawer-id (refs/drawer-id company1)]
    (-> tx
        ;; Create multiple relations for alice
        (rel/upsert-relation! alice bob :friend)
        (rel/upsert-relation! alice charlie :friend)
        (rel/upsert-relation! alice company1 :friend) ; Add friend relation to company to test cross-drawer
        (rel/upsert-relation! alice diana :mentor :student {:subject "clojure"} nil)
        (rel/upsert-relation! alice company1 :employee :employer {:role "developer"} nil)
        (rel/upsert-relation! alice company2 :consultant :client {:hourly-rate 100} nil)

        ;; Create some relations for bob too
        (rel/upsert-relation! bob charlie :colleague)

        ;; Test querying all friends of alice (should include both people and company)
        (dt/is-> (rel/relations alice :friend) (= {bob {} charlie {} company1 {}}))

        ;; Test querying mentor relations (should find diana)
        (dt/is-> (rel/relations alice :mentor) (= {diana {:subject "clojure"}}))

        ;; Test querying employee relations (should find company1)
        (dt/is-> (rel/relations alice :employee) (= {company1 {:role "developer"}}))

        ;; Test querying consultant relations (should find company2)
        (dt/is-> (rel/relations alice :consultant) (= {company2 {:hourly-rate 100}}))

        ;; Test querying non-existent relation type
        (dt/is-> (rel/relations alice :enemy)
                 empty?)

        ;; Test querying relations for someone with no relations in that direction
        (dt/is-> (rel/relations diana :friend)
                 empty?)

        ;; Test drawer filtering - only get :friend relations to people (not companies)
        (dt/is-> (rel/relations alice :friend [people-drawer-id]) (= {bob {} charlie {}}))

        ;; Test drawer filtering - only get :friend relations to companies (not people)
        (dt/is-> (rel/relations alice :friend [company-drawer-id]) (= {company1 {}}))

        ;; Test multiple drawer filtering - get :friend relations from both drawers
        (dt/is-> (rel/relations alice :friend [people-drawer-id company-drawer-id])
                 (= {bob {} charlie {} company1 {}}))

        ;; Verify reverse direction works (diana should see alice as student)
        (dt/is-> (rel/relations diana :student) (= {alice {}})))))
