(ns dresser.impl.codax-test
  (:require [clojure.test :as t :refer [deftest is use-fixtures]]
            [codax.core :as c]
            [dresser.base :as db]
            [dresser.impl.codax :as impl]
            [dresser.test :as dt]))

(def test-db-path "test-db")

(defn with-temp-db
  [f]
  (try (f)
       (finally (c/destroy-database! test-db-path))))

(use-fixtures :each with-temp-db)

(defn deref-timeout
  [*promise]
  (let [ret (deref *promise 500 :timeout)]
    (when (= ret :timeout)
      (throw (ex-info "Timeout" {})))
    ret))

(deftest write-tx-restart
  (let [db1 (-> (impl/build test-db-path)
                (dt/no-tx-reuse))
        db2 (-> (impl/build test-db-path)
                (dt/no-tx-reuse))
        *thread1-read (promise)
        *thread2-write (promise)
        *thread1 (future
                   (db/tx-let [tx db1]
                       [_ (db/fetch tx :test {}) ;; <-- read tx
                        _ (deliver *thread1-read true)
                        ;; wait until the other tx does a write
                        _ (deref-timeout *thread2-write)
                        ;; Try a write. This will trigger a tx
                        ;; upgrade, but it should fail because
                        ;; another write occured in the meantime
                        _ (db/update-at! tx :test :doc1 [:counter] (fnil inc 0))]))]
    (deref-timeout *thread1-read) ; wait for the other tx to have started
    (db/tx-let [tx db2]
        [_ (db/assoc-at! tx :test :doc1 [:counter] 10)]
      (deliver *thread2-write true))
    (deref-timeout *thread1)
    (is (= 11 (db/get-at db2 :test :doc1 [:counter])))))

(deftest fetch-isnt-lazy
  ;; Codax transactions and lazyness are not a good match.  Better to
  ;; make sure the fetch request has completed before doing anything
  ;; else.
  (db/tx-let [tx (impl/build test-db-path)]
      [_ (reduce (fn [tx' idx]
                   (db/add! tx' :users {:name (str "user-" idx)}))
                 tx (range 50))
       users (db/fetch tx :users)
       is-realized? (realized? users)]
      (is is-realized?)
    tx))
