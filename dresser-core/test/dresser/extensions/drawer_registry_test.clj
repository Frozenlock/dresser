(ns dresser.extensions.drawer-registry-test
  (:require [clojure.test :as t :refer [deftest testing use-fixtures]]
            [dresser.base :as db]
            [dresser.drawer :as dd]
            [dresser.extensions.drawer-registry :as d-reg]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt]))

(comment
  (use-fixtures :once (dt/coverage-check refs)))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)
      (d-reg/drawer-registry)))

(deftest keep-sync
  (testing "Implementation"
    (dt/test-impl #(d-reg/drawer-registry (test-dresser))))

  (testing "Rename"
    (db/tx-let [tx (d-reg/drawer-registry (test-dresser))]
        [d1-1 (db/add! tx :d1 {})
         d1-doc1 (db/fetch-by-id tx :d1 d1-1)
         d2-1 (db/add! tx :d2 {})
         d2-doc1 (db/fetch-by-id tx :d2 d2-1)
         ;; force drawer-id creation
         _ (d-reg/drawer-id tx :d1 :upsert)
         _ (d-reg/drawer-id tx :d2 :upsert)]
      (-> tx
          (dt/is-> (db/fetch-by-id :d1 d1-1) (= d1-doc1)
                   "Sanity check")
          (dt/is-> (db/rename-drawer! (dd/drawer :d1)
                                      (dd/drawer :d2))
                   (thrown-with-msg? clojure.lang.ExceptionInfo #"Can't rename to an existing drawer"))

          (db/rename-drawer! (dd/drawer :d1)
                             (dd/drawer :new-d1))
          (dt/is-> (db/fetch-by-id :d1 d1-1) nil?
                   "Document doesn't exist under old drawer name")
          (dt/is-> (db/fetch-by-id :new-d1 d1-1) (= d1-doc1)
                   "Document should be available under the new drawer name")
          (dt/is-> (db/fetch-by-id :d2 d2-1) (= d2-doc1)
                   "Other drawer still work as expected"))))
  (testing "Drop"
    (db/tx-let [tx (d-reg/drawer-registry (test-dresser))]
        [d1-id (-> tx
                   (dt/is-> (d-reg/drawer-id :d1 false) nil?)
                   (dt/is-> (d-reg/drawer-id :d1 :upsert) some?))
         d1-key (-> tx
                    (dt/is-> (d-reg/drawer-key d1-id) (= :d1)))
         d2-id (-> tx
                   (dt/is-> (d-reg/drawer-id :d2 false) nil?)
                   (dt/is-> (d-reg/drawer-id :d2 :upsert) some?))
         d2-key (-> tx
                    (dt/is-> (d-reg/drawer-key d2-id) (= :d2)))]
      (-> tx
          (db/drop! :d1)
          (dt/is-> (d-reg/drawer-id :d1 false) nil?)
          (dt/is-> (d-reg/drawer-key d1-id) nil?)
          (dt/is-> (d-reg/drawer-key d2-id) (= :d2))))))
