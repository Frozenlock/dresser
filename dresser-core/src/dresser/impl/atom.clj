(ns dresser.impl.atom
  (:require [dresser.base :as db]
            [dresser.impl.hashmap :as hm]
            [dresser.protocols :as dp]
            [dresser.test :as dt]))

;; We don't use `swap!` to avoid the automatic retries.

(dp/defimpl -transact
  [dresser f {:keys [result?]}]
  (locking (:lock dresser)
    (let [temp-data (db/temp-data dresser)
          source @(:*source dresser)
          source' (-> (vary-meta source merge
                                 (dissoc (meta dresser) `dp/-transact
                                         `dp/-start
                                         `dp/-stop))
                      (db/with-temp-data temp-data)
                      (db/transact! #(f %) {:result? false}))
          _ (when-not (compare-and-set! (:*source dresser)
                                        source
                                        (db/with-temp-data source' nil))
              ;; Should never happen because of the lock.
              (throw (ex-info "Transaction failed" {})))
          updated (db/with-temp-data dresser (db/temp-data source'))]
      (if result?
        (db/result updated)
        updated))))

(dp/defimpl -start
  [dresser]
  (update dresser :*source (fn [*a] (swap! *a dp/-start) *a)))

(dp/defimpl -stop
  [dresser]
  (update dresser :*source (fn [*a] (swap! *a dp/-stop) *a)))

(def atom-impl
  (dp/mapify-impls
   [-transact hm/-temp-data hm/-with-temp-data
    -start -stop]))

(defn build
  "Build an IDresser atom from a map or from another IDresser object."
  {:test (fn []
           (dt/test-impl #(dt/no-tx-reuse (build))))}
  ([] (build {}))
  ([map-or-dresser]
   (let [inner-dresser (cond
                         (db/dresser? map-or-dresser) map-or-dresser
                         (map? map-or-dresser) (hm/build map-or-dresser))]
     (-> {:*source (atom inner-dresser)
          :data    (db/temp-data inner-dresser)
          :lock    :lock}
         (with-meta (merge
                     (meta inner-dresser)
                     atom-impl))))))
