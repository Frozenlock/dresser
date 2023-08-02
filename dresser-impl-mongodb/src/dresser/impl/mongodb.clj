(ns dresser.impl.mongodb
  (:require [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [dresser.base :as db]
            [dresser.drawer :as dd]
            [dresser.impl.optional :as opt]
            [dresser.impl.test-utils :as tu]
            [dresser.protocols :as dp]
            [dresser.test :as dt]
            [mongo-driver-3.client :as mcl]
            [mongo-driver-3.collection :as mc]))

;; TODO: `upsert-all!` for a mass insert.  Also consider 'batching' or
;; parallelizing operations. The latter might be simpler and can be
;; applied to all methods.

(defn- qualified-ident-name
  "Returns the qualified name when possible, otherwise just the name."
  [x]
  (if-let [n (and (ident? x)
                  (namespace x))]
    (str n "/" (name x))
    (name x)))

(defn- mongo-dotted-path
  "Given a list of keywords or strings, return a single
   'mongo-dotted-path' with the dot notation."
  [keywords]
  (->> keywords (map qualified-ident-name) (str/join ".")))


(def ^:private query-ops
  {::db/exists? :$exists
   ::db/gt      :$gt
   ::db/gte     :$gte
   ::db/lt      :$lt
   ::db/lte     :$lte})

(defn- replace-query-ops
  [m]
  (w/postwalk-replace query-ops m))


(defn- flatten-keys* [acc ks m]
  (if (and (map? m)
           (not (empty? m))
           (not (some query-ops (keys m))))
    (reduce into
            (map (fn [[k v]]
                   (flatten-keys* acc (conj ks k) v))
                 m))
    (assoc acc ks m)))

(defn- flatten-keys
  "Transforms a nested map into a map where keys are paths through
  the original map and values are leafs these paths lead to.

  (flatten-keys {:a {:b {:c :x :d :y}}})
  => {[:a :b :c] :x
      [:a :b :d] :y}"
  [m]
  (if (empty? m)
    m
    (flatten-keys* {} [] m)))



;; Pretty shitty encode/decode. I'm open to suggestions.

(defn- encode
  [x]
  (cond
    (map? x) (into {} (for [[k v] x]
                        ;; Mongo doesn't support much besides strings
                        ;; as keys. Serialize if necessary.
                        [(cond
                           (coll? k) (str "drs_coll_" (pr-str k))
                           (keyword? k) (if (query-ops k)
                                          k
                                          (encode k))
                           (number? k) (str "drs_num_" (pr-str k))
                           (inst? k) (str "drs_inst_" (.getTime k))
                           :else k)
                         (encode v)]))
    (and (coll? x)
         (not (db/ops? x))) (into (empty x) (map encode x))
    (keyword? x) (str "drs_kw_" (qualified-ident-name x))
    :else x))

(defn- decode
  [x]
  x
  (cond
    (map? x) (into {} (for [[k v] x]
                        [(decode k)
                         (decode v)]))
    (coll? x) (into (empty x) (map decode x))
    (string? x) (cond
                  (str/starts-with? x "drs_kw_") (keyword (str/replace-first x "drs_kw_" ""))
                  (str/starts-with? x "drs_coll_") (edn/read-string (str/replace-first x "drs_coll_" ""))
                  (str/starts-with? x "drs_num_") (edn/read-string (str/replace-first x "drs_num_" ""))
                  (str/starts-with? x "drs_inst_") (java.util.Date.
                                                    (edn/read-string (str/replace-first x "drs_inst_" "")))
                  :else x)
    :else x))




(defn- id->mid
  ":id -> :_id"
  [m]
  (if-let [id (:id m)]
    (-> (assoc m "_id" (encode id))
        (dissoc :id))
    m))

(defn- mid->id
  "\"_id\" -> :id"
  [m]
  (if-let [id (get m "_id")]
    (-> (dissoc m :_id "_id")
        (assoc :id (decode id)))
    m))

(defn- encode-drawer
  [drawer]
  (encode (dd/key drawer)))


;;; This is infuriating.
;;; In transactions (or multi-docs transactions), MongoDB doesn't support:
;;; - Drop
;;; - Renaming a collection when it was also modified (ex: adding a document)
;;; - Listing all collections
;;;
;;; Also, transactions can only be used when replica set is active,
;;; even when a single instance is used. Sad.
;;;
;;; TokuMX was forked from MongoDB a decade ago and even in its
;;; abandonned state it's arguably better. It even has partitioned
;;; collection! It's a shame Percona no longer supports it.
;;;
;;; Anyway, to fix all those unsupported transaction operations we
;;; have to keep our own registry of drawers. It will add some
;;; overhead, but with some caching it should be negligible.


;;; --- Drawers registry ---
(def drawers-registry "drs_drawers_registry")

;; Initially the drawer and collection name should be identical.  This
;; allows Dresser to connect to an existing MongoDB and interact with
;; it. Note that this relationship will break down once the drawers
;; are renamed, as the underlying collection won't be renamed.

(defn- drawer->coll
  ([[db session] drawer]
   (drawer->coll [db session] drawer false))
  ([[db session] drawer upsert?]
   (let [encoded-drawer (encode-drawer drawer)
         {:keys [coll expired?]} (mc/find-one db
                                              drawers-registry
                                              {:drawer encoded-drawer}
                                              {:projection {:coll     1
                                                            :expired? 1
                                                            "_id"     0}
                                               :session    session})]
     (if (or (not coll) expired?)
       (let [coll-name (if expired?
                         (str (gensym encoded-drawer))
                         encoded-drawer)]
         (do (when upsert?
               (mc/insert-one db
                              drawers-registry
                              {:drawer encoded-drawer
                               :coll   coll-name}
                              {:session session}))
             coll-name))
       coll))))

(defn drop-expired-collections!
  [db]
  ;; 'Drop' cannot be used inside a transaction.  Ergo, this whole
  ;; function is done without transaction. We must be extra careful as
  ;; the state can change between operations.
  (let [expired (mc/find db drawers-registry
                         {:expired? true}
                         {:projection {:coll 1}})
        ;; The collection might be used elsewhere, for example after a
        ;; rename.
        still-used (mc/find db drawers-registry
                            {:coll     {:$in (map :coll expired)}
                             :expired? {:$ne true}})
        expired-colls (set/difference (set (map :coll expired))
                                      (set (map :coll still-used)))]
    (doseq [coll expired-colls]
      (mc/drop db coll))
    (mc/delete-many db drawers-registry {:_id {:$in (map :_id expired)}})))

(defn- rename-in-registry!
  [{::keys [db session] :as tx} drawer new-drawer]
  (let [coll (drawer->coll [db session] drawer)]
    (mc/find-one-and-update db
                            drawers-registry
                            {:drawer (encode-drawer drawer)}
                            {:$set {:expired? true}}
                            {:session session})
    (mc/insert-one db
                   drawers-registry
                   {:drawer (encode-drawer new-drawer)
                    :coll   coll}
                   {:session session}))
  (update tx :post-tx-fns conj #(drop-expired-collections! db)))

(dp/defimpl -rename-drawer
  [{::keys [db session] :as tx} drawer new-drawer]
  (rename-in-registry! tx drawer new-drawer))

(dp/defimpl -all-drawers
  [{::keys [db session] :as tx}]
  (->> (mc/find db drawers-registry {} {:projection {:drawer 1}
                                        :session    session})
       (map :drawer)
       (decode)
       (db/with-result tx)))

;;; --------------------------



(defn- prepare-where
  [where]
  (let [m (-> (id->mid where)
              (encode)
              (flatten-keys)
              (replace-query-ops))]
    (into {} (for [[k v] m]
               [(mongo-dotted-path k) v]))))


(defn- prepare-only
  [only]
  (some->> (not-empty only)
           ((fn [m]
              (if-let [id (:id m)]
                (-> (assoc m "_id" id)
                    (dissoc :id))
                m)))
           (w/postwalk #(if-not (map-entry? %)
                          %
                          (let [[k v] %]
                            (if (map? v)
                              %
                              [k (if v 1 0)]))))
           (encode)))

(defn- prepare-sort
  [sort-config]
  (some->> (not-empty sort-config)
           (mapcat (fn [[[p1 & ps :as path] order]]
                     [(if (= p1 :id)
                        (mongo-dotted-path (into ["_id"] (map encode ps)))
                        (mongo-dotted-path (map encode path)))
                      (case order
                        :asc 1
                        :desc -1)]))
           ; keep the correct sort order with array-map
           (apply array-map)))

(defn fetch
  [{::keys [db session] :as tx} drawer only limit where sort-config skip]
  (let [result (->> (mc/find db
                             (drawer->coll [db session] drawer)
                             (prepare-where where)
                             {:keywordize? false
                              :limit       limit
                              :sort (prepare-sort sort-config)
                              :projection  (prepare-only only)
                              :session     session
                              :skip        skip})
                    (map mid->id))]
    (->> (map (fn [x]
                (cond
                  (and (seq only) (:id only)) x

                  (seq only) (dissoc x :id)

                  :else x))
              result)
         (decode)
         (reverse)
         (db/with-result tx))))

(dp/defimpl -fetch
  [{::keys [db session] :as tx} drawer only limit where sort-config skip]
  (fetch tx drawer only limit where sort-config skip))

(dp/defimpl -fetch-count
  [{::keys [db session] :as dresser} drawer where]
  (->> (mc/count-documents db
                           (drawer->coll [db session] drawer)
                           (prepare-where where)
                           {:session session})
       (db/with-result dresser)))

;; MongoDB currently doesn't support '.drop()' inside a transaction.
(dp/defimpl -drop
  [{::keys [db session] :as dresser} drawer]
  (mc/find-one-and-update db
                          drawers-registry
                          {:drawer (encode-drawer drawer)}
                          {:$set {:expired? true}}
                          {:session session})
  (-> dresser
      (update :post-tx-fns conj #(drop-expired-collections! db))
      (db/with-result drawer)))


(dp/defimpl -with-temp-data
  [dresser data]
  (assoc dresser :data data))

(dp/defimpl -temp-data
  [dresser]
  (get dresser :data))

(dp/defimpl -delete
  [{::keys [db session] :as tx} drawer id]
  (mc/delete-one db
                 (drawer->coll [db session] drawer)
                 (id->mid {:id id})
                 {:session session})
  (db/with-result tx id))

(defn upsert
  [{::keys [db session] :as dresser} drawer data]
  (let [document-id (:id data)
        _ (when-not document-id
            (throw (ex-info "Missing document ID" {})))
        encoded (-> (id->mid data)
                    (encode))]
    (->> (mc/find-one-and-replace db
                                  (drawer->coll [db session] drawer :upsert)
                                  (select-keys encoded ["_id"])
                                  encoded
                                  {:keywordize? false
                                   :return-new? true
                                   :session     session
                                   :upsert?     true})
         (mid->id)
         (decode)
         (db/with-result dresser))))

(dp/defimpl -upsert
  [{::keys [db session] :as dresser} drawer data]
  (upsert dresser drawer data))

(defn upsert-many
  [{::keys [db session] :as dresser} drawer docs]
  (mc/bulk-write db
                 (drawer->coll [db session] drawer :upsert)
                 (for [doc docs
                       :let [document-id (:id doc)
                             _ (when-not document-id
                                 (throw (ex-info "Missing document ID" {})))
                             encoded (-> (id->mid doc)
                                         (encode))]]
                   [:replace-one {:filter      (select-keys encoded ["_id"])
                                  :replacement encoded
                                  :upsert?     true}])
                 {:session session})
  (db/with-result dresser docs))

(dp/defimpl -upsert-many
  [{::keys [db session] :as dresser} drawer docs]
  (upsert-many dresser drawer docs))

;; transactionLifetimeLimitSeconds <-- might be useful in the future
(dp/defimpl -transact
  [dresser f {:keys [result?]}]
  (if (:transact dresser)
    (f dresser)
    ;; When not already inside a transaction:
    (let [dresser' (with-open [session (mcl/start-session (::client dresser))]
                     ;; TRANSACTIONS ARE ONLY ALLOWED ON REPLICA SETS?!?!

                     ;; Why not use transactionBody? Because it might
                     ;; automatically retry the transaction.
                     (.startTransaction session)
                     (try
                       (let [dresser (f (assoc dresser :transact true ::session session))
                             non-lazy (db/update-result dresser #(if (seq? %) (doall %) %))]
                         (.commitTransaction session)
                         non-lazy)
                       (catch Exception e
                         (try (.abortTransaction session)
                              (catch Exception _e))
                         (throw e))))

          ;; (let [dresser (f (assoc dresser :transact true))
          ;;       non-lazy (db/update-result dresser #(if (seq? %) (doall %) %))]
          ;;   non-lazy)

          ]
      ;; Post transaction operations.
      ;; If something bad happens, must not throw in the main thread.
      (future (doseq [f (:post-tx-fns dresser')]
                (f)))
      (if result?
        (db/result dresser')
        (dissoc dresser' :transact :post-tx-fns)))))


(defn mongo-impl
  []
  (dp/mapify-impls
   [-all-drawers
    -delete
    -fetch
    -drop
    -fetch-count
    -rename-drawer
    -temp-data
    -transact
    -upsert
    -upsert-many
    -with-temp-data]))


(def ^:private *test-dressers (atom []))

(defn build
  {:test #(let [_ (tu/ensure-test-db!)
                f (fn []
                    (let [new (build {:db-name (str (gensym "dresser_test_db"))
                                      :host    "127.0.0.1"
                                      :port    27018})]
                      (swap! *test-dressers conj new)
                      new))]
            ;; Dropping the DB takes ~100ms each time.
            ;; Cleaning up in parallel at the end is significantly faster
            (try
              (dt/test-impl f)
              (finally (doall (pmap (fn [dresser]
                                      (do (.drop (::db dresser))
                                          (.close (::client dresser))
                                          (swap! *test-dressers (fn [ds]
                                                                  (remove #{dresser} ds)))))
                                    @*test-dressers)))))}
  ([db-configs] (build {} db-configs))
  ([m {:keys [db-name host port] :as db-configs}]
   (let [client (mcl/create (str "mongodb://" host ":" port))
         db (mcl/get-db client db-name)]
     (vary-meta {::client     client
                 ::db         db
                 ::db-configs db-configs}
                merge
                opt/optional-impl
                (mongo-impl)
                {:type ::db/dresser}))))




(comment
  (def aaa (build {:db-name "dresser_test_db"
                   :host    "127.0.0.1"
                   :port    27018}))
  (do (.drop (::db aaa))
      (.close (::client aaa))
      )


  (comment
    (require '[dresser.extensions.ttl :as ttl])
    (def aaa (-> (build "test-db")
                 (ttl/ttl (ttl/secs 10))
                 (db/start)
                 ))
    (time (let [add-rnd-user! (fn [aaa idx]
                                (let [username (str (gensym "user-"))
                                      email (str username "@" (gensym "email") ".com")]
                                  (ttl/add-with-ttl! aaa :users {:username username :email email
                                                                 :idx      idx}
                                                     (ttl/secs (rand-int 20)))))]
            ;; This causes a write conflict initially because the drawer is being added to the registry
            (last (pmap (fn [idxes]
                          (db/with-tx [tx aaa]
                            (reduce (fn [tx idx]
                                      (add-rnd-user! tx idx))
                                    tx
                                    idxes))) (partition 5 (range 10000))))))
    (ttl/add-with-ttl! aaa :users {:username "Will be deleted!" :email "..@.."} (ttl/secs 10)))

  )
