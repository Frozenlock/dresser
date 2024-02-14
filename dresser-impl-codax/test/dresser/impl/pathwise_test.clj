(ns dresser.impl.pathwise-test
  (:require [clojure.test :as t :refer [deftest is]]
            [codax.core :as c]
            [dresser.impl.pathwise :as pathwise]))

pathwise/side-effect

(deftest maps
  (let [m1 {:z 26, :a 1}
        m2 (sorted-map :a 1 :z 26)
        m3 (with-meta {:a 1, :z 26} {:some :meta})
        results (for [m [m1 m2 m3]]
                  (select-keys (c/check-path-encoding m)
                               [:equal :decoded :encoded]))]
    (doseq [r results]
      (is (= {:equal   true
              :decoded {:a 1, :z 26}
              :encoded "TlBZAFKY0iYCcWoBYWQBcWoBemQa"}
             r)))))


(deftest sets
  (let [s1 #{5 4 3 2 1}
        s2 (sorted-set 1 2 3 4 5)
        s3 (with-meta #{1 2 3 4 5} {:some :meta})
        results (for [s [s1 s2 s3]]
                  (select-keys (c/check-path-encoding s)
                               [:equal :decoded :encoded]))]
    (doseq [r results]
      (is (= {:equal   true
              :decoded #{1 4 3 2 5}
              :encoded "TlBZAFKcyyYFZANkAWQFZAJkBA=="}
             r)))))
