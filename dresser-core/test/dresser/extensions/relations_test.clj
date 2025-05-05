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
      [[doc1 doc2] (add-docs tx :docs 2)]
    (-> tx
        ;; Create relation with data
        (rel/upsert-relation! doc1 doc2 :coworker :colleague {:department "Engineering"} {:team "Frontend"})
        (dt/is-> (rel/is? doc1 :coworker doc2) true?)
        (dt/is-> (rel/is? doc2 :colleague doc1) true?)
        
        ;; Check relation data
        (dt/is-> (rel/relation doc1 :coworker doc2) 
                 #(and (= (:rel %) :coworker)
                       (= (:department %) "Engineering")))
        (dt/is-> (rel/relation doc2 :colleague doc1)
                 #(and (= (:rel %) :colleague)
                       (= (:team %) "Frontend")))
        
        ;; Update relation with new data
        (rel/upsert-relation! doc1 doc2 :coworker :colleague {:department "Product"} {:team "Design"})
        (dt/is-> (rel/relation doc1 :coworker doc2)
                 #(and (= (:rel %) :coworker)
                       (= (:department %) "Product")))
        (dt/is-> (rel/relation doc2 :colleague doc1)
                 #(and (= (:rel %) :colleague)
                       (= (:team %) "Design"))))))

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