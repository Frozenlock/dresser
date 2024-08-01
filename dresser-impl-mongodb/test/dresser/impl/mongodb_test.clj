(ns dresser.impl.mongodb-test
  (:require [clojure.test :as t :refer [deftest is use-fixtures]]
            [dresser.base :as db]
            [dresser.impl.mongodb :as impl]
            [dresser.test :as dt]
            [dresser.impl.mongodb-test-utils :as tu]))

;; TODO: check if renaming/dropping drawers is having the expected
;; effect on the underlying collections.

(defn test-db []
  (impl/build {:db-name (str (gensym "dresser_test_db"))
               :host    "127.0.0.1"
               :port    27018}))

(defmacro with-test-db
  [binding & body]
  `(do
     (tu/ensure-test-db!)
     (let ~binding
       (try
         ~@body
         (finally
           (future (.drop (:db (first ~binding)))
                   (.close (:client (first ~binding)))))))))

(deftest support-dot-and-dollar-sign
  (with-test-db [db (test-db)]
    (let [drawer-key ::a.b$c
          nested-key "b$$.~~.y"
          doc-id (db/add! db drawer-key {:a 1, nested-key {:b 2}})]
      (is (= {:b 2}
             (db/get-at db drawer-key doc-id [nested-key]))))))

(deftest test-expand-ors
  (is (= (impl/expand-ors {:1 {:2 1}
                           :a {::db/any [{:b {db/lt 3 db/gt 1}}
                                         {:b 3}]}})
         {:1   {:2 1}
          :$or [{:a {:b {db/lt 3, db/gt 1}}}
                {:a {:b 3}}]}))

  (is (= (impl/expand-ors {:1 {:2 1}
                           :a {::db/any [{:b {db/lt 3 db/gt 1}}
                                         {:b 3}
                                         {:c {db/any [{:d 1}
                                                      {:d 2}]}}]}})
         {:1   {:2 1}
          :$or [{:$or [{:a {:c {:d 1}}}
                       {:a {:c {:d 2}}}]}
                {:a {:b {db/lt 3, db/gt 1}}}
                {:a {:b 3}}]})))

(deftest complex-ids
  (with-test-db [db (test-db)]
    (let [id1 "id1"
          id2 :id2
          id3 [id1 id2]
          not-id3 [id2 id1] ; not same order
          id4 {id1 id2}
          id5 #{id1 id2 id4}
          docs [{:id id1} {:id id2} {:id id3} {:id id4} {:id id5}]]
      (db/upsert-many! db :drawer docs)
      (is (dt/u= (db/fetch db :drawer {:sort [[[:id] :desc]]})
                 docs))
      (is (db/fetch db :drawer {:where {:id not-id3}}) empty?))))
