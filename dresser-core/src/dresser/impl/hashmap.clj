(ns dresser.impl.hashmap
  (:require [clojure.test :as t :refer [is]]
            [dresser.base :as db]
            [dresser.impl.optional :as opt]
            [dresser.protocols :as dp]
            [dresser.test :as dt]))

;; Implementation with simple hashmap

(t/with-test
  (defn take-from
    "Returns the query map will all its node values filled with values
  taken from source under the same path."
    ([source query]
     (take-from source query ::drop-not-found))
    ([source query not-found]
     (if (every? map? [source query])
       (->> (for [[dk dv] query
                  :let [sv (get source dk not-found)]
                  :when (not= sv ::drop-not-found)]
              [dk (take-from sv dv not-found)])
            (into {}))
       source)))

  (let [source {:a "a"
                :b "b"
                :c {:d "cd", :z "cz"}}
        query {:a :?, :c {:d :?, :i :?}, :n :?}]
    (is (= {:a "a", :c {:d "cd", :i :default}, :n :default}
           (take-from source query :default)))
    (is (= {:a "a", :c {:d "cd"}}
           (take-from source query)))
    (is (= source
           (take-from source nil)))))


(defn- path-comparator [path order]
  (let [order-int ({:asc 1, :desc -1} order)]
    (fn [m1 m2]
      (let [v1 (get-in m1 path)
            v2 (get-in m2 path)]
        (* order-int (compare v1 v2))))))

(defn- multi-path-comparator [paths]
  (fn [m1 m2]
    (loop [paths paths]
      (if (empty? paths)
        0
        (let [[path order] (first paths)
              cmp (path-comparator path order)
              result (cmp m1 m2)]
          (if (not= 0 result)
            result
            (recur (rest paths))))))))


(t/with-test
  (defn- sort-maps-by [sort-config maps]
    (if (not-empty sort-config)
      (let [comparator (multi-path-comparator sort-config)]
        (sort comparator maps))
      maps))

  (let [[m1 m2 m3 m4 :as maps] [{:id 1, :a {:b 4 :c {:d 3}}}
                                {:id 2, :a {:b 1 :c {:d 4}}}
                                {:id 3, :a {:b 2 :c {:d 1}}}
                                {:id 4, :a {:b 1 :c {:d 2}}}]
        sort-config [[[:a :b] :asc]
                     [[:a :c :d] :desc]]
        equality-sort-conflict [[[:a :b] :desc]]]
    (is (= [m2 m4 m3 m1] (sort-maps-by sort-config maps)))
    (is (= [m1 m3 m2 m4] (sort-maps-by equality-sort-conflict maps)))
    (is (= maps (sort-maps-by [] maps)))))


;; Ops
(defn- lt
  [data-entry field-key test-val]
  (some-> (get data-entry field-key)
          (compare test-val)
          (<  0)))

(defn- lte
  [data-entry field-key test-val]
  (some-> (get data-entry field-key)
          (compare test-val)
          (<=  0)))

(defn- gt
  [data-entry field-key test-val]
  (some-> (get data-entry field-key)
          (compare test-val)
          (>  0)))

(defn- gte
  [data-entry field-key test-val]
  (some-> (get data-entry field-key)
          (compare test-val)
          (>=  0)))

(defn- exists?
  [data-entry field-key bool]
  (if bool
    (contains? data-entry field-key)
    (not (contains? data-entry field-key))))

(declare where?)

(defn- any
  [data-entry field-key test-val]
  (some #(where? data-entry {field-key %}) test-val))


(def query-ops
  {;; :=       = ; implied
   ::db/exists? exists?
   ::db/gt      gt
   ::db/gte     gte
   ::db/lt      lt
   ::db/lte     lte
   ::db/any     any})


(defn where?
  "Returns `data` if all the conditions are met, nil otherwise."
  [data conditions]
  (letfn [(check-condition [data condition-key condition-val]
            (let [value (get data condition-key)]
              (cond
                (and (map? condition-val) (some query-ops (keys condition-val)))
                (every? (fn [[k v]] ((get query-ops k) data condition-key v))
                        condition-val)

                (map? condition-val)
                (where? value condition-val)

                (= condition-key ::db/any) ;; any at top level
                (some #(where? data %) condition-val)

                :else
                (= value condition-val))))]

    (if (every? (fn [[k v]]
                  (check-condition data k v))
                conditions)
      data)))


;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (comment

;;   (defn take-from
;;     "Returns the query map with all its node values filled with values
;;   taken from source under the same path. Supports wildcards (::db/*)."
;;     ([source query]
;;      (take-from source query ::drop-not-found))
;;     ([source query not-found]
;;      (if
;;          (and (map? source) (map? query))
;;        (->> (for [[dk dv] query
;;                   :let [sv (if (= dk ::db/*)
;;                              source
;;                              (get source dk not-found))]
;;                   :when (not= sv ::drop-not-found)]
;;               (if (= dk ::db/*)
;;                 (into {} (for [[k v] source]
;;                            [k (take-from v dv not-found)]))
;;                 [dk (take-from sv dv not-found)]))
;;             (into {}))
;;        source)))


;;   {:members
;;    {::db/*
;;     {::db/when {:type  :admin
;;                 :posts {::db/* {:likes {::db/gte 10}}}}
;;      :email    true}}}
;;   ;; Example supporting ::db/* (wildcard)

;;   )


;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn take-or-all
  "Exactly like `clojure.core/take`, but returns coll if n is nil."
  [n coll]
  coll
  (if n
    (take n coll)
    coll))

(defn fetch-from-docs
  [docs-coll only limit where sort-config skip]
  (->> docs-coll
       (filter (fn [doc] (where? doc where)))
       (sort-maps-by sort-config)
       (drop (or skip 0))
       (take-or-all limit)
       (map (fn [doc] (take-from doc only)))))

(dp/defimpl -fetch
  [tx drawer only limit where sort-config skip]
  (db/with-result tx
    (-> (vals (get-in tx [:db drawer]))
        (fetch-from-docs only limit where sort-config skip))))

(dp/defimpl -upsert
  [tx drawer data]
  (-> (assoc-in tx [:db drawer (:id data)] data)
      (db/with-result data)))

(dp/defimpl -transact
  [dresser f {:keys [result?]}]
  (if (:transact dresser)
    (f dresser)
    ;; When not already inside a transaction, add a marker
    (let [dresser (f (assoc dresser :transact true))]
      ; Extract the result at the end
      (if result?
        (db/result dresser)
        (dissoc dresser :transact)))))

(dp/defimpl -with-temp-data
  [dresser data]
  (assoc dresser :data data))

(dp/defimpl -temp-data
  [dresser]
  (get dresser :data))

(dp/defimpl -delete-many
  [tx drawer where]
  (db/tx-let [tx tx]
      [ids (-> (db/fetch tx drawer {:where where
                                    :only  {:id :?}})
               (db/update-result #(mapv :id %)))]
    (-> (update-in tx [:db drawer] #(apply dissoc % ids))
        (db/with-result {:deleted-count (count ids)}))))

(dp/defimpl -all-drawers
  [tx]
  (db/with-result tx (keys (:db tx))))

(def hashmap-base-impl
  (dp/mapify-impls
   [-all-drawers
    -delete-many
    ;-delete
    -fetch
    -temp-data
    -transact
    -upsert
    -with-temp-data]))

(dp/defimpl -assoc-at
  [tx drawer id ks data]
  (-> (update-in tx [:db drawer id]
                 (fn [doc]
                   (-> (if (seq ks)
                         (assoc-in doc ks data)
                         data)
                       (assoc :id id))))
      (db/with-result data)))

(dp/defimpl -fetch-by-id
  [tx drawer id only where]
  (db/with-result tx
    (some-> (get-in tx [:db drawer id])
            (where? where)
            (take-from only))))

(def hashmap-adv-impl
  (dp/mapify-impls [-assoc-at
                    -fetch-by-id]))


;; This is used to test the optional implementations
(defn- base-impl-build
  {:test #(dt/test-impl (fn [] (dt/no-tx-reuse (base-impl-build {}))))}
  [m]
  (-> (vary-meta {:db (into (sorted-map) m)}
                 merge
                 {:type ::db/immutable-dresser}
                 opt/optional-impl
                 hashmap-base-impl)
      (db/with-temp-dresser-id)))

(defn build
  {:test #(dt/test-impl (fn [] (dt/no-tx-reuse (build))))}
  ([] (build {}))
  ([m] (vary-meta (base-impl-build m)
                  merge
                  hashmap-adv-impl)))
