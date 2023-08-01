(ns dresser.extensions.durable-refs-test
  (:require [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [dresser.base :as db]
            [dresser.drawer :as dd]
            [dresser.extensions.durable-refs :as refs]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt]))

(comment
  (use-fixtures :once (dt/coverage-check refs)))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)
      (refs/keep-sync)))

(deftest refs
  (db/tx-let [tx (test-dresser)]
      [doc-id (db/add! tx :docs {:a 1})
       doc-ref (refs/ref! tx :docs doc-id)
       doc-by-ref (refs/fetch-by-ref tx doc-ref)
       doc-by-id (db/fetch-by-id tx :docs doc-id)]
    (is (= doc-by-ref doc-by-id))
    (-> tx
        (dt/is-> (refs/ref :docs doc-id) some?)
        (dt/is-> (refs/ref (dd/drawer :docs) doc-id) some?))))


(defn- add-docs!
  "Adds n docs and return their DB refs."
  [dresser drawer n]
  (db/with-tx [tx (db/with-result dresser nil)] ; clean any existing result
    (reduce (fn [tx _i]
              (let [refs (db/result tx)
                    [tx id] (db/dr (db/add! tx drawer {}))
                    [tx new-ref] (db/dr (refs/ref! tx drawer id))]
                (db/with-result tx (conj refs new-ref))))
            tx (range n))))

(deftest keep-sync
  (testing "Implementation"
    (dt/test-impl #(refs/keep-sync (test-dresser))))

  (testing "Rename"
    (db/tx-let [tx (refs/keep-sync (test-dresser))]
        [[ref-d1-1 ref-d1-2] (add-docs! tx :d1 2)
         d1-doc1 (refs/fetch-by-ref tx ref-d1-1)
         d1-doc2 (refs/fetch-by-ref tx ref-d1-2)
         [ref-d2-1 ref-d2-2] (add-docs! tx :d2 2)
         d2-doc1 (refs/fetch-by-ref tx ref-d2-1)
         d2-doc2 (refs/fetch-by-ref tx ref-d2-2)]
      (-> tx
          (db/rename-drawer! (dd/drawer :d1)
                             (dd/drawer :new-d1))
          (dt/is-> (refs/fetch-by-ref ref-d1-1) (= d1-doc1)
                   "Ref for renamed drawer should still fetch the document")
          (dt/is-> (refs/fetch-by-ref ref-d2-1) (= d2-doc1)
                   "Other drawer still work as expected"))))
  (testing "Drop"
    (db/tx-> (refs/keep-sync (test-dresser))
      (add-docs! :d1 2)
      (add-docs! :d2 2)

      (dt/is-> (db/get-at refs/key->ids :d1 [:id]) some?)
      (dt/is-> (db/get-at refs/key->ids :d2 [:id]) some?)
      (dt/is-> (db/fetch refs/registry {:where {:key :d1}}) not-empty)
      (dt/is-> (db/fetch refs/registry {:where {:key :d2}}) not-empty)
      (db/drop! :d1)
      (dt/is-> (db/get-at refs/key->ids :d1 [:id]) nil?)
      (dt/is-> (db/get-at refs/key->ids :d2 [:id]) some?)
      (dt/is-> (db/fetch refs/registry {:where {:key :d1}}) empty?)
      (dt/is-> (db/fetch refs/registry {:where {:key :d2}}) not-empty))))
