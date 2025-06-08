(ns dresser.encoding-test
  (:require [clojure.test :as t :refer [deftest is]]
            [dresser.encoding :as enc]))

(defrecord TestRecord [])

(deftest record-round-trip
  (let [r (->TestRecord)
        actual (-> (enc/encode-record r)
                   (enc/restore-record))]
    (is (= r actual))))


(deftest bytes-round-trip
  (let [b (.getBytes "test")
        actual (-> (enc/encode-bytes b)
                   (enc/restore-bytes))]
    (is (java.util.Arrays/equals b actual))))
