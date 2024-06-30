(ns dresser.extensions.cache-test
  (:require [clojure.test :as t :refer [deftest testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.durable-refs :as refs]
            [dresser.extensions.cache :as cache]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt]))

;; (use-fixtures :once (dt/coverage-check cache))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)))

;; (deftest cache
;;   (testing "Implementation"
;;     (dt/test--dissoc-at #(cache/cache (test-dresser) (hm/build)))))
