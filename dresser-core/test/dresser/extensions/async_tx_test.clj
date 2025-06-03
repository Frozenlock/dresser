(ns dresser.extensions.async-tx-test
  (:require [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.async-tx :as atx]
            [dresser.impl.atom :as at]
            [dresser.impl.hashmap :as hm]
            [dresser.protocols :as dp]
            [dresser.test :as dt]))

(use-fixtures :once (dt/coverage-check atx))

;; no-tx-reuse can 'silently' throw in complex-tx test.
(defn- immutable-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)))

(defn- mutable-dresser
  []
  (-> (at/build)
      (dt/sequential-id)))

(deftest impl
  (testing "Nested"
    (testing "immutable"
      (dt/test-impl #(atx/async-tx
                      (atx/async-tx
                       (immutable-dresser)))))
    (testing "mutable"
      (dt/test-impl #(atx/async-tx
                      (atx/async-tx
                       (mutable-dresser)))))))

(defn- wait-while
  "Blocks until predicate returns false, or throws if timeout."
  [predicate-fn timeout-ms]
  (let [*p (promise)]
    (future
      (while (and (not (realized? *p))
                  (try (predicate-fn)
                       (catch Exception e
                         (deliver *p e))))
        (Thread/sleep 5))
      (deliver *p true))
    (let [p (deref *p timeout-ms :timeout)]
      (cond (= p :timeout)
            (do (deliver *p :timeout)
                (throw (ex-info "Timeout" {:timeout-ms timeout-ms})))

            (instance? Exception p)
            (throw p)

            :else nil))))

(deftest transact-init-errs
  (let [dresser (vary-meta (hm/build)
                           assoc `dp/transact
                           (fn [tx f opts]
                             (throw (ex-info "Boom!" {}))))
        d1 (atx/async-tx dresser)]

    (testing "started tx are 'lazy'"
      (is (atx/start-tx! d1)));; Should not throw yet

    (testing "Start can be called multiple times"
        (atx/start-tx! d1)
        (atx/start-tx! d1))

    ;; Correctly transmit exceptions when a transaction initialization throws
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Boom!"
         (db/add! d1 :users {:name "Bob"})))))

(deftest immutability
  (let [dresser (atx/async-tx (immutable-dresser))
        _ (is (true? (db/immutable? dresser)) "Sanity check")
        tx (atx/start-tx! dresser)
        _ (is (not (db/immutable? tx)) "No longer immutable when async transaction started")
        ended-tx (atx/end-tx! tx)
        _ (is (true? (db/immutable? ended-tx)) "Back to immutable when async transaction ended")

        tx (atx/start-tx! dresser)
        _ (is (not (db/immutable? tx)) "No longer immutable when async transaction started")
        cancel-tx (atx/end-tx! tx)
        _ (is (true? (db/immutable? cancel-tx)) "Back to immutable when async transaction cancelled")]))

(deftest complex-tx
  (doseq [dresser [(immutable-dresser)
                   (mutable-dresser)]]
    (let [d1 (atx/async-tx dresser)
          d2 (atx/async-tx (atx/async-tx dresser))
          *d1-committed? (promise)]
      (testing "Starting multiple tx doesn't block"
        (atx/start-tx! d1)
        (atx/start-tx! d2))

      ;; WARNING! While only 'starting' the tx doesn't block, any
      ;; subsequent operation *might* lock the dresser to a single
      ;; tx. The tx will then have to be cancelled/committed before
      ;; releasing the lock.
      (testing "While tx is open, values are transferred, even for immutable dressers."
        (db/add! d1 :users {:name "Bob"})
        (is (= '({:name "Bob"})
               (db/fetch d1 :users {:only [:name]}))))

      ;; d2 *might* lock until d1 is ended
      (future (db/add! d2 :users {:name "Alice"}))
      (future ;(Thread/sleep 50)
              (atx/end-tx! d1)
              (deliver *d1-committed? true))
      ;; DO NOT touch d1 until d2 is ended.
      (wait-while #(empty? (db/fetch d2 :users {:where {:name "Alice"}})) 300)
      (is (= (-> (atx/end-tx! d2)
                 (db/fetch :users {:where {:name "Alice"}
                                   :only  [:name]}))
             '({:name "Alice"})))

      ;; Wait for commit to complete
      (deref *d1-committed?)
      (testing "Doesn't hang"
        ;; Now that d2 is ended, d1 should be available.
        (is (= (if (db/immutable? d1)
                 '()
                 '({:name "Bob"} {:name "Alice"}))
               (db/fetch d1 :users {:only [:name]}))))

      (testing "Cancelled tx isn't committed"
        (atx/start-tx! d1)
        (db/add! d1 :c {:name "John"})
        (is (= (db/fetch d1 :c {:only [:name]})
               '({:name "John"})))
        (atx/cancel-tx! d1)
        (is (= (db/fetch d1 :c {:only [:name]})
               '()))))))


(deftest cancel-tx
  (let [d1 (atx/async-tx (atx/async-tx (immutable-dresser)))]
    (testing "Cancelled tx isn't committed"
      (atx/start-tx! d1)
      (db/add! d1 :c {:name "John"})
      (is (= (db/fetch d1 :c {:only [:name]})
             '({:name "John"})))
      (atx/cancel-tx! d1)
      (is (= (db/fetch d1 :c {:only [:name]})
             '())))
    (testing "Still works after a cancelled transaction"
      (is (some? (db/add! d1 :c {:name "Bob"}))))

    (testing "Cancel can be called multiple times"
      (atx/start-tx! d1)
      (atx/cancel-tx! d1)
      (atx/cancel-tx! d1))))

(deftest client-tracking
  (testing "Multi-client transaction lifecycle"
    (testing "Complete flow - all clients commit"
      (let [d1 (atx/async-tx (immutable-dresser))
            tx (atx/start-tx! d1)
            ;; Register additional clients
            tx-with-clients (-> tx
                               (atx/start-tx! {:client-id :client-a})
                               (atx/start-tx! {:client-id :client-b}))]

        ;; Add test data within transaction
        (db/add! tx-with-clients :users {:name "Test data"})

        ;; First client commits - transaction should still be active
        (let [tx-after-a (atx/end-tx! tx-with-clients {:client-id :client-a})]
          ;; Verify data is visible within transaction
          (is (= '({:name "Test data"})
                 (db/fetch tx-after-a :users {:only [:name]})))

          ;; Data is not yet committed - verify in the transaction only

          ;; Second client commits - transaction should still be active
          (let [tx-after-b (atx/end-tx! tx-after-a {:client-id :client-b})]

            ;; Default client commits - should finalize the transaction
            (let [final-tx (atx/end-tx! tx-after-b)]
              ;; Data should be persisted in final transaction
              (is (= '({:name "Test data"})
                     (db/fetch final-tx :users {:only [:name]}))))))))

    (testing "Partial commit followed by cancellation"
      (let [d1 (atx/async-tx (mutable-dresser))
            tx (atx/start-tx! d1)
            tx-with-client (atx/start-tx! tx {:client-id :client-a})]

        ;; Add data to the transaction
        (db/add! tx-with-client :users {:name "Should not persist"})

        ;; First client commits - transaction should still be active
        (let [tx-after-a (atx/end-tx! tx-with-client {:client-id :client-a})]

          ;; Verify data is visible within transaction
          (is (= '({:name "Should not persist"})
                 (db/fetch tx-after-a :users {:only [:name]})))

          ;; Cancel instead of committing with default client
          (atx/cancel-tx! tx-after-a)

          ;; Data should NOT be persisted since we canceled
          (is (empty? (db/fetch d1 :users {:only [:name]}))))))

    (testing "Direct cancellation with multiple clients"
      (let [d1 (atx/async-tx (mutable-dresser))
            ;; Start transaction with multiple clients
            tx (-> (atx/start-tx! d1)
                  (atx/start-tx! {:client-id :client-1})
                  (atx/start-tx! {:client-id :client-2}))]

        ;; Add test data
        (db/add! tx :users {:name "Should be cancelled"})

        ;; Verify data is visible within transaction
        (is (= '({:name "Should be cancelled"})
               (db/fetch tx :users {:only [:name]})))

        ;; Cancel should terminate the transaction regardless of clients
        (atx/cancel-tx! tx)

        ;; Verify transaction was cancelled (data not persisted)
        (is (empty? (db/fetch d1 :users)))

        ;; We should be able to start a new transaction after cancellation
        (let [new-tx (atx/start-tx! d1)]
          (db/add! new-tx :users {:name "After cancellation"})
          (let [committed (atx/end-tx! new-tx)]
            ;; This data should be persisted
            (is (= '({:name "After cancellation"})
                   (db/fetch committed :users {:only [:name]})))
            (is (= '({:name "After cancellation"})
                   (db/fetch d1 :users {:only [:name]}))))))))

  (testing "Immutable dresser client tracking"
    (let [d1 (atx/async-tx (immutable-dresser))
          tx (atx/start-tx! d1)
          tx-with-client (atx/start-tx! tx {:client-id :client-a})]

      ;; Add data to the transaction
      (let [[tx-with-data _] (db/dr (db/raw-> tx-with-client
                                      (db/add! :users {:name "Immutable test"})))]
        ;; First client commits - transaction should still be active
        (let [tx-after-a (atx/end-tx! tx-with-data {:client-id :client-a})]
          ;; Verify data is visible within transaction
          (is (= '({:name "Immutable test"})
                 (db/fetch tx-after-a :users {:only [:name]})))

          ;; Final commit
          (let [[final-tx _] (db/dr (atx/end-tx! tx-after-a))]
            ;; Data should be persisted
            (is (= '({:name "Immutable test"})
                   (db/fetch final-tx :users {:only [:name]})))))))))

;; TODO: add tests for temp-data passed in and out

(deftest end-tx-usage
  (testing "New end-tx! API"
    (let [d1 (atx/async-tx (mutable-dresser))
          tx (atx/start-tx! d1)
          ;; Register additional clients
          tx-with-clients (-> tx
                             (atx/start-tx! {:client-id :client-a})
                             (atx/start-tx! {:client-id :client-b}))]

      ;; Add test data within transaction
      (db/add! tx-with-clients :users {:name "Using end-tx!"})

      ;; First client ends participation - transaction should still be active
      (let [tx-after-a (atx/end-tx! tx-with-clients {:client-id :client-a})]
        ;; Verify data is visible within transaction
        (is (= '({:name "Using end-tx!"})
               (db/fetch tx-after-a :users {:only [:name]})))

        ;; Second client ends - transaction should still be active
        (let [tx-after-b (atx/end-tx! tx-after-a {:client-id :client-b})]

          ;; Default client ends - should finalize the transaction
          (let [final-tx (atx/end-tx! tx-after-b)]
            ;; Data should be persisted
            (is (= '({:name "Using end-tx!"})
                   (db/fetch d1 :users {:only [:name]}))))))))

  (testing "Multiple clients ending transaction"
    (let [d1 (atx/async-tx (mutable-dresser))
          tx (atx/start-tx! d1)
          tx (atx/start-tx! tx {:client-id :client-a})
          tx (atx/start-tx! tx {:client-id :client-b})]

      ;; Add data
      (db/add! tx :users {:name "Multiple clients"})

      ;; End each client's participation
      (let [tx-after-a (atx/end-tx! tx {:client-id :client-a})
            tx-after-b (atx/end-tx! tx-after-a {:client-id :client-b})]

        ;; Default client with new API
        (let [final-tx (atx/end-tx! tx-after-b)]
          ;; Data should be persisted
          (is (= '({:name "Multiple clients"})
                 (db/fetch d1 :users {:only [:name]}))))))))

(deftest late-explicit-start-tx
  (testing "Using tx-let with client-id in immutable dresser"
    (let [dresser (atx/async-tx (hm/build))
          test-data {:id "doc-id" :value "test-value"}]
      (db/with-tx [tx dresser]
        (let [tx1 (db/assoc-at! tx :drawer1 "doc-id" [] test-data)
              tx2 (db/fetch-by-id tx :drawer1 "doc-id")
              _ (is (db/dresser? tx2))
              tx3 (atx/start-tx! tx2 {:client-id :client-a})
              tx4 (db/fetch-by-id tx3 :drawer1 "doc-id")
              _ (is (db/dresser? tx4) "If this fails then the result was extracted too soon")]
          (atx/end-tx! tx4 {:client-id :client-a}))))))

#_(deftest rollback-stress-test
    (doseq [i (range 50)]
      ;; Exceptions thrown inside a transaction should cause a rollack
      (let [doc {:id "some-id", :str "string"}
            dresser (db/raw-> (atx/async-tx (atx/async-tx (mutable-dresser)))
                      (db/assoc-at! :drawer1 "some-id" [] doc))] ; <-- return the dresser object

        (atx/start-tx! dresser)

        ; Now make a few changes inside a transaction
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"Test exception"
             (db/tx-> dresser
               (db/assoc-at! :drawer1 "some-id" [:str2] "string 2")
               (db/update-at! :drawer1 "some-id" [:str3] (fn [_] (throw (ex-info "Test exception" {}))))
               )))

        (is (= doc (db/fetch-by-id dresser :drawer1 "some-id")))
        (atx/cancel-tx! dresser))))
