(ns dresser.impl.atom
  (:require [dresser.base :as db]
            [dresser.impl.hashmap :as hm]
            [dresser.protocols :as dp]
            [dresser.test :as dt]))

;; We don't use `swap!` to avoid the automatic retries.

;; Implementation functions
(defn do-transact
  [dresser f opts]
  (locking (:lock dresser)
    (let [temp-data (db/temp-data dresser)
          source @(:*source dresser)
          source' (-> (vary-meta source merge
                                 (dissoc (meta dresser)
                                         ;; Exclude non-transactional
                                         `dp/transact `dp/start `dp/stop
                                         `dp/temp-data `dp/with-temp-data
                                         `dp/immutable? `dp/tx?))
                      (db/with-temp-data temp-data)
                      (db/transact! f (assoc opts :result? false)))
          _ (when-not (compare-and-set! (:*source dresser)
                                        source
                                        (db/with-temp-data source' nil))
              ;; Should never happen because of the lock.
              (throw (ex-info "Transaction failed" {})))]
      (db/with-temp-data dresser (db/temp-data source')))))

(defn do-start
  [dresser]
  (update dresser :*source (fn [*a] (swap! *a dp/-start) *a)))

(defn do-stop
  [dresser]
  (update dresser :*source (fn [*a] (swap! *a dp/-stop) *a)))

(defn get-temp-data
  [dresser]
  (get dresser :data))

(defn set-temp-data
  [dresser data]
  (assoc dresser :data data))

;; AtomDresser record - minimal implementation.  All transactional
;; methods are delegated to the inner dresser.
(defrecord AtomDresser [*source data lock]
  dp/IsDresser
  (-dresser? [this] true)

  dp/DresserFundamental
  (-transact [this f opts]
    (do-transact this f opts))

  (-temp-data [this]
    (get-temp-data this))

  (-with-temp-data [this data]
    (set-temp-data this data))

  (-immutable? [this]
    false)

  (-tx? [this]
    (dp/-tx? @(:*source this)))

  dp/DresserLifecycle
  (-start [this]
    (do-start this))

  (-stop [this]
    (do-stop this)))

(defn build
  "Build an IDresser atom from a map or from another IDresser object."
  {:test (fn []
           (dt/test-impl #(dt/no-tx-reuse (build))))}
  ([] (build nil))
  ([map-or-dresser]
   (let [inner-dresser (cond
                         (db/dresser? map-or-dresser) map-or-dresser
                         (map? map-or-dresser) (hm/build map-or-dresser)
                         :else (hm/build))]
     (-> (->AtomDresser (atom inner-dresser)
                        (db/temp-data inner-dresser)
                        (gensym "lock-"))
         (db/with-temp-dresser-id)))))
