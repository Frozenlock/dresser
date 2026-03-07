(ns dresser.encoding-test
  (:require [clojure.test :as t :refer [deftest is]]
            [dresser.encoding :as enc]))

(deftest bytes-round-trip
  (let [b (.getBytes "test")
        actual (-> (enc/encode-bytes b)
                   (enc/restore-bytes))]
    (is (java.util.Arrays/equals b actual))))
