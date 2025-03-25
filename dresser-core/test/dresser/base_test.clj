(ns dresser.base-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [dresser.base :as db]
            [dresser.impl.hashmap :as hm]))

(deftest lexical-encoding
  (let [n1 3
        n2 15
        n3 200
        n4 10e10
        expected-order [n1 n2 n3 n4]
        id->n (into {} (for [n (reverse expected-order)]
                         [(db/lexical-encode n) n]))]
    (testing "Ordering"
      (is (= expected-order
             (map last (sort-by key id->n)))))
    (testing "Encode/Decode"
      (is (= n3
             (db/lexical-decode
              (db/lexical-encode n3)))))))

(deftest map-tx-test
  (let [dresser (hm/build)]

    (testing "Basic mapping functionality"
      (let [numbers [1 2 3 4 5]
            result (db/map-tx dresser (fn [tx n] (db/with-result tx (* n 2))) numbers)]
        (is (= [2 4 6 8 10] result))))

    (testing "Empty collection handling"
      (let [result (db/map-tx dresser (fn [tx n] (db/with-result tx (* n 2))) [])]
        (is (= dresser result))))

    (testing "Using dresser result as collection"
      (let [result (-> dresser
                       (db/with-result [1 2 3])
                       (db/map-tx (fn [tx n] (db/with-result tx (* n 3)))))]
        (is (= [3 6 9] result))))

    (testing "Order preservation"
      (let [result (db/map-tx dresser (fn [tx n] (db/with-result tx n)) [3 1 4 2])]
        (is (= [3 1 4 2] result))))

    (testing "Transaction context preservation"
      (let [collected-values (atom [])
            result (db/tx-> dresser
                     (db/with-result (range 1 4))
                     (db/map-tx (fn [tx n]
                                  (swap! collected-values conj n)
                                  (db/with-result tx (* n 10)))))]
        (is (= [10 20 30] result))
        (is (= [1 2 3] @collected-values))))

    (testing "Nested transaction handling"
      (let [result (db/tx-> dresser
                     (db/with-result [1 2 3])
                     (db/map-tx (fn [tx n]
                                  (db/tx-> tx
                                    (db/with-result (* n 2))))))]
        (is (= [2 4 6] result))))))
