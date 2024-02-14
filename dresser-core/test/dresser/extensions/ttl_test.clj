(ns dresser.extensions.ttl-test
  (:require [clojure.test :as t :refer [are deftest is testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.durable-refs :as refs]
            [dresser.extensions.ttl :as ttl]
            [dresser.impl.atom :as at]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt])
  (:import [java.util Date]))

(use-fixtures :once (dt/coverage-check ttl))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)
      (refs/keep-sync)))

(defn- add-docs!
  "Adds n docs and return their DB refs."
  ([dresser n] (add-docs! dresser :drawer n))
  ([dresser drawer n]
   (db/tx-let [tx dresser]
       [_ (db/with-result tx nil)] ; clean any existing result
     (reduce (fn [tx _i]
               (let [refs (db/result tx)
                     [tx new-ref] (db/dr (refs/add! tx drawer {}))]
                 (db/with-result tx (conj refs new-ref))))
             tx (range n)))))

(deftest time-utils
  (testing "Millisecs fn"
    (are [x y] (= x y)
      (ttl/secs 1) 1000
      (ttl/mins 1) (* 1000 60)
      (ttl/hours 1) (* 1000 60 60)
      (ttl/days 1) (* 1000 60 60 24)
      (ttl/weeks 1) (* 1000 60 60 24 7)))
  (testing "Now + <duration>"
    (with-redefs [ttl/now (fn
                            ([] (Date. 10))
                            ([ms-to-add] (ttl/add-ms (Date. 10) ms-to-add)))]
      (are [x y] (= x (Date. (+ y 10)))
        (ttl/now+secs 1) 1000
        (ttl/now+mins 1) (* 1000 60)
        (ttl/now+hours 1) (* 1000 60 60)
        (ttl/now+days 1) (* 1000 60 60 24)
        (ttl/now+weeks 1) (* 1000 60 60 24 7))))
  (testing "Greater/smaller comparators"
    (let [[t1 t2 t3] [(ttl/now+secs 0) (ttl/now+secs 10) (ttl/now+secs 20)]]
      (is (ttl/> t3 t2 t1))
      (is (ttl/>= t3 t2 t2 t1))
      (is (ttl/< t1 t2 t3))
      (is (ttl/<= t1 t1 t2 t2)))))

(deftest upsert-expiration
  (db/tx-let [tx (test-dresser)]
      [[d1 d2 d3 d4] (add-docs! tx 4)
       now (ttl/now)
       past (ttl/now (ttl/secs -20))
       future (ttl/now (ttl/secs 20))]
    (with-redefs [ttl/now (fn [] now)] ; Freeze 'now'
      (db/tx-> tx
        ;; Set initial expirations, test return values
        (dt/is-> (ttl/upsert-expiration! d1 past) (= d1))
        (dt/is-> (ttl/upsert-expiration! d2 past) (= d2))
        (dt/is-> (ttl/upsert-expiration! d3 past) (= d3))
        (dt/is-> (ttl/upsert-expiration! d4 past) (= d4))

        (dt/is-> (ttl/get-expiration d1) (= past))
        (dt/is-> (ttl/get-expiration d2) (= past))
        (dt/is-> (ttl/get-expiration d3) (= past))
        (dt/is-> (ttl/get-expiration d4) (= past))

        ;; Update expirations
        (ttl/upsert-expiration! d1 past)
        (ttl/upsert-expiration! d2 past)
        (ttl/upsert-expiration! d3 now)
        (ttl/upsert-expiration! d4 future)

        (dt/is-> (ttl/get-expiration d1) (= past))
        (dt/is-> (ttl/get-expiration d2) (= past))
        (dt/is-> (ttl/get-expiration d3) (= now))
        (dt/is-> (ttl/get-expiration d4) (= future))

        ;; Remove the expiration
        (ttl/upsert-expiration! d1 nil)
        (dt/is-> (ttl/get-expiration d1) nil?)

        (ttl/delete-expired!)
        (dt/is-> (refs/fetch-by-ref d1) some?)
        (dt/is-> (refs/fetch-by-ref d2) nil?)
        (dt/is-> (refs/fetch-by-ref d3) some?)
        (dt/is-> (refs/fetch-by-ref d4) some?)))))

(defmacro with-start-stop
  [binding & body]
  `(let [~(first binding) (db/start ~(second binding))]
     (try (let [ret# ~@body]
            (if (db/dresser? ret#)
              (db/stop ret#)
              (do (db/stop ~(first binding))
                  ret#)))
          (catch Exception e#
            (db/stop ~(first binding))
            (throw e#)))))

(defn- wait-while
  "Blocks until predicate returns false, or throws if timeout."
  [predicate-fn timeout-ms]
  (let [*p (promise)]
    (future
      (while (and (not (realized? *p))
                  (predicate-fn))
        (Thread/sleep 5))
      (deliver *p true))
    (when (= (deref *p timeout-ms :timeout) :timeout)
      (deliver *p :timeout)
      (throw (ex-info "Timeout" {:timeout-ms timeout-ms})))))

(deftest ttl-auto-delete
  (with-start-stop [dresser (-> (hm/build)
                                (dt/sequential-id)
                                (at/build) ; Can't use immutable map to test autodeletion.
                                (ttl/ttl 5))]
    ;; DO NOT TEST INSIDE A TRANSACTION!
    ;; The autodeleter won't be able to see the docs until committed.
    (let [[d1 d2 d3 d4] (add-docs! dresser 4)
          d5 (ttl/add-with-ttl! dresser ::any {:doc 1} (ttl/secs -20))
          now (ttl/now)
          past (ttl/now (ttl/secs -20))
          future (ttl/now (ttl/secs 20))
          dresser (db/raw-> dresser
                    (ttl/upsert-expiration! d1 past)
                    (ttl/upsert-expiration! d2 past)
                    (ttl/upsert-expiration! d3 future)
                    (ttl/upsert-expiration! d4 future))]
      (wait-while #(some? (refs/fetch-by-ref dresser d1)) 500)
      (db/tx-> dresser
        (dt/is-> (refs/fetch-by-ref d1) nil?)
        (dt/is-> (refs/fetch-by-ref d2) nil?)
        (dt/is-> (refs/fetch-by-ref d3) some?)
        (dt/is-> (refs/fetch-by-ref d4) some?)
        (dt/is-> (refs/fetch-by-ref d5) nil?)))))
