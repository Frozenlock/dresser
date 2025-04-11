(ns dresser.extensions.async-tx-test
  (:require [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.async-tx :as atx]
            [dresser.impl.atom :as at]
            [dresser.impl.hashmap :as hm]
            [dresser.protocols :as dp]
            [dresser.test :as dt]))

(use-fixtures :once (dt/coverage-check atx))

(defn- immutable-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)))

(defn- mutable-dresser
  []
  (-> (at/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)))

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
                           assoc `dp/-transact
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

      ;; d2 *might* lock until d1 is closed
      (future (db/add! d2 :users {:name "Alice"}))
      (future ;(Thread/sleep 50)
              (atx/commit-tx! d1)
              (deliver *d1-committed? true))
      ;; DO NOT touch d1 until d2 is closed.
      (wait-while #(empty? (db/fetch d2 :users {:where {:name "Alice"}})) 2000)
      (is (= (-> (atx/commit-tx! d2)
                 (db/fetch :users {:where {:name "Alice"}
                                   :only  [:name]}))
             '({:name "Alice"})))

      ;; Wait for commit to complete
      (deref *d1-committed?)
      (testing "Doesn't hang"
        ;; Now that d2 is closed, d1 should be available.
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

;; TODO: add tests for temp-data passed in and out


#_(deftest rollback-stress-test
    (doseq [i (range 50)]
      ;; Exceptions thrown inside a transaction should cause a rollack
      (let [doc {:id "some-id", :str "string"}
            dresser (db/raw-> (atx/async-tx (atx/async-tx (mutable-dresser)))
                      (db/upsert! :drawer1 doc))] ; <-- return the dresser object

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
