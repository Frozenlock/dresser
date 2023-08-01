(ns dresser.impl.mongodb-test
  (:require [clojure.test :as t :refer [deftest is use-fixtures]]
            [dresser.base :as db]
            [dresser.impl.mongodb :as impl]
            [dresser.test :as dt]
            [dresser.impl.test-utils :as tu]))

(defn with-ensure-db
  [f]
  (tu/ensure-test-db!)
  (f))

(use-fixtures :once with-ensure-db)

;; TODO: check if renaming/dropping drawers is having the expected
;; effect on the underlying collections.
