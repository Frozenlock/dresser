(ns dresser.extensions.subdocs-test
  (:require [clojure.test :as t :refer [deftest testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.durable-refs :as refs]
            [dresser.extensions.subdocs :as subdocs]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt]))

(use-fixtures :once (dt/coverage-check subdocs))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)))

(defn- add-docs
  "Adds n docs and return their DB refs."
  ([dresser n] (add-docs dresser :drawer n))
  ([dresser drawer n]
   (db/tx-let [tx dresser]
       [_ (db/with-result tx nil)] ; clean any existing result
     (reduce (fn [tx _i]
               (let [refs (db/result tx)
                     [tx id] (db/dr (db/add! tx drawer {}))
                     [tx new-ref] (db/dr (refs/ref! tx drawer id))]
                 (db/with-result tx (conj refs new-ref))))
             tx (range n)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(deftest subdocs
  (testing "public fns"
    (db/tx-let [tx (test-dresser)]
        [[d1 d2 d3] (add-docs tx 3)]
      (-> tx
          (dt/is-> (subdocs/all-ancestors d3) nil?)
          (dt/is-> (subdocs/add-child! d1 d2) (= d1))
          (dt/is-> (subdocs/children d1) (= [d2]))
          (dt/is-> (subdocs/all-ancestors d2) (= [d1]))
          (subdocs/add-child! d2 d3)
          (dt/is-> (subdocs/all-ancestors d3) (= [d2 d1]))
          (dt/is-> (subdocs/add-child! d3 d1)
                   (thrown-with-msg? clojure.lang.ExceptionInfo #"Circular children loop detected"))
          (dt/is-> (subdocs/remove-child! d2 d3) nil?)
          (dt/is-> (subdocs/all-ancestors d3) nil?)
          (dt/is-> (subdocs/all-ancestors d2) (= [d1]))
          (dt/is-> (subdocs/remove-parent! d2) nil?)
          (dt/is-> (subdocs/all-ancestors d2) nil?))))
  (testing "extension delete"
    (db/tx-let [tx (subdocs/keep-sync (test-dresser))]
        [[d1 d2 d3 d4 d5 d6] (add-docs tx 6)]
      (-> tx
          (dt/testing-> "Delete all descendants"
            (subdocs/add-child! d1 d2)
            (subdocs/add-child! d2 d3)
            (subdocs/add-child! d3 d4)
            (refs/delete! d1)
            (dt/is-> (refs/fetch-by-ref d1) nil?)
            (dt/is-> (refs/fetch-by-ref d2) nil?)
            (dt/is-> (refs/fetch-by-ref d3) nil?)
            (dt/is-> (refs/fetch-by-ref d4) nil?)
            (dt/is-> (refs/fetch-by-ref d5) some?))
          (dt/testing-> "Remove parent relation"
            (subdocs/add-child! d5 d6)
            (dt/is-> (subdocs/children d5) (= [d6]))
            (refs/delete! d6)
            (dt/is-> (subdocs/children d5) nil?))))
    (testing "extension drop"
      (db/tx-let [tx (subdocs/keep-sync (test-dresser))]
          [[o1 o2 & orgs] (add-docs tx :orgs 52)
           [p1 p2 p3] (add-docs tx :projects 3)
           members (add-docs tx :members 50)
           _ (reduce #(subdocs/add-child! %1 (first %2) (last %2)) tx (apply map vector [orgs members]))]
        (-> tx
            (dt/is-> (db/fetch-count :members) (= 50))
            (subdocs/add-child! o1 p1)
            (subdocs/add-child! o2 p2)
            (db/drop! :orgs)
            (dt/is-> (refs/fetch-by-ref o1) nil?)
            (dt/is-> (refs/fetch-by-ref o2) nil?)
            (dt/is-> (refs/fetch-by-ref p1) nil?)
            (dt/is-> (refs/fetch-by-ref p2) nil?)
            (dt/is-> (refs/fetch-by-ref p3) some?)
            (dt/is-> (db/fetch-count :members) (= 0)))))))
