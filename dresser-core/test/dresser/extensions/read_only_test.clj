(ns dresser.extensions.read-only-test
  (:require [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.read-only :as ro]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt]))

(use-fixtures :once (dt/coverage-check ro))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)))

(defmacro is-read-only-exception?
  [& body]
  `(is (~'thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"This dresser is read-only"
        ~@body)))

(defmacro are-read-only-exceptions?
  [& body]
  `(do ~@(for [form# body]
          (list 'is-read-only-exception? form#))))

(deftest read-only
  (let [dresser (ro/read-only (test-dresser))]
    (testing "Throw read only exceptions"
      (are-read-only-exceptions?
       (db/assoc-at! dresser :drawer :id [:path] {:a 1})
       (db/upsert-all! dresser :drawer [{:a 1} {:b 1}])
       (db/upsert! dresser :drawer {:a 1})
       (db/gen-id! dresser :drawer)
       (db/delete! dresser :drawer :id)
       (db/drop! dresser :drawer)
       (db/add! dresser :drawer {:a 1})
       (db/update-at! dresser :drawer :id [:path] identity)
       (db/replace! dresser :drawer :id {:a 1})))
    (testing "Sanity check"
      ;; Quickly check that we didn't broke the other methods.
      (is (nil? (db/get-at dresser :drawer 1 [:path])))
      (is (db/dresser-id dresser) (= :my-dresser)))))
