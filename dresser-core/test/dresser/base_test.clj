(ns dresser.base-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [dresser.base :as db]))

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
