(ns dresser.impl.mongodb
  (:require [clojure.core.cache.wrapped :as cw]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [dresser.base :as db]
            [dresser.impl.mongodb-test-utils :as tu]
            [dresser.impl.optional :as opt]
            [dresser.protocols :as dp]
            [dresser.test :as dt]
            [hasch.core :as hashc]
            [mongo-driver-3.client :as mcl]
            [mongo-driver-3.collection :as mc]))

;; Support for '.' and '$' in collection and key names.  While docs
;; with '.' in keys can be fetched, it breaks things for `get-at` if
;; we don't escape the character.
;; https://stackoverflow.com/questions/12397118/mongodb-dot-in-key-name

(defn escape [s]
  (str/replace s #"[~.$]"
               { "~" "~~"
                "."  "~p"
                "$"  "~d"}))

(defn unescape [s]
  (str/replace s #"~~|~p|~d"
               { "~~" "~"
                 "~p" "."
                 "~d" "$"}))

(defn- qualified-ident-name
  "Returns the qualified name when possible, otherwise just the name."
  [x]
  (if-let [n (and (ident? x)
                  (namespace x))]
    (str n "/" (name x))
    (name x)))


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

(declare encode)
(declare decode)

;; Collections are appended with their type before storing in the DB.

(defn- encode-coll
  [coll]
  (let [encode-key (cond
                     (vector? coll) "_drs_vec"
                     (list? coll) "_drs_list"
                     (set? coll) "_drs_set"
                     :else "drs_vec")]
    (into [encode-key] (map encode coll))))

(defn- decode-coll
  [encoded-coll]
  (let [encode-key (first encoded-coll)
        decoder {"_drs_vec"  #(mapv decode (rest %))
                 "_drs_list" #(reverse (into '() (map decode (rest %))))
                 "_drs_set"  #(set (map decode (rest %)))}]
    (if-let [decode-fn (decoder encode-key)]
      (decode-fn encoded-coll)
      encoded-coll)))



(defn- hash-compare
  [a b]
  (compare (hashc/b64-hash a)
           (hashc/b64-hash b)))

(defn- ordered-commutative-coll
  "For collections where order doesn't matter for equality, returns a
  sorted version that supports heterogeneous elements."
  [coll]
  (let [f (fn [x]
            (cond
              (and (map? x) (not (record? x))) (into (sorted-map-by hash-compare) x)
              (set? x) (into (sorted-set-by hash-compare) x)
              :else x))]
    (w/postwalk f coll)))

(defn- encode
  [x]
  (cond
    (map? x) (into {} (for [[k v] x]
                        ;; Mongo doesn't support much besides strings
                        ;; as keys. Serialize if necessary.
                        [(cond
                           (string? k) (escape k)
                           (keyword? k) (if (query-ops k)
                                          k
                                          (encode k))
                           :else (str "drs:mdb:edn("
                                      (escape (pr-str (ordered-commutative-coll k))) ")"))
                         (encode v)]))
    (and (coll? x)
         (not (db/ops? x))) (encode-coll x)
    (keyword? x) (str "drs:mdb:kw(" (escape (qualified-ident-name x)) ")")
    :else x))

(defn- decode
  [x]
  (cond
    (map? x) (into {} (for [[k v] x]
                        [(decode k)
                         (decode v)]))
    (coll? x) (decode-coll x)
    (string? x) (if-let [[a _drs xtype data] (re-matches #"^(drs:mdb:)(.*)\((.*)\)" x)]
                  (case xtype
                    "edn" (edn/read-string (unescape data))
                    "kw" (keyword (unescape data)))
                  (unescape x))
    :else x))




(defn- id->mid
  "Updates the map by converting Dresser :id into MongoDB \"_id\"."
  [m]
  (if-let [id (:id m)]
    (-> (assoc m "_id" (encode id))
        (dissoc :id))
    m))

(defn- mid->id
  "Updates the map by converting MongoDB \"_id\" into Dresser :id."
  [m]
  (if-let [id (get m "_id")]
    (-> (dissoc m :_id "_id")
        (assoc :id (decode id)))
    m))

(defn- encode-drawer
  [drawer]
  (encode drawer))


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

(defn- raw-drawer->coll
  [[db session] drawer upsert?]
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
      coll)))

;; Keep the drawers cached, as they are required for every operation.
(defn- drawer->coll
  ([[db session *cache] drawer]
   (drawer->coll [db session *cache] drawer false))
  ([[db session *cache] drawer upsert?]
   (let [lookup #(cw/lookup-or-miss
                  *cache
                  drawer
                  (fn [_] (raw-drawer->coll [db session] drawer upsert?)))
         result (lookup)]
     (if (and (nil? result)
              upsert?)
       (do (cw/evict *cache drawer)
           (lookup))
       result))))

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
  [{:keys [db session *cache] :as tx} drawer new-drawer]
  (let [coll (drawer->coll [db session *cache] drawer)]
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
  ;; Clearing the cache can be done after because we are in a
  ;; transaction.  In fact we may still fail later on and the cache
  ;; would have been 'wrongly' cleared.
  (cw/evict *cache drawer)
  (cw/evict *cache new-drawer)
  (update tx :post-tx-fns conj #(drop-expired-collections! db)))

(dp/defimpl -rename-drawer
  [{:keys [db session] :as tx} drawer new-drawer]
  (rename-in-registry! tx drawer new-drawer))

(dp/defimpl -all-drawers
  [{:keys [db session] :as tx}]
  (->> (mc/find db drawers-registry {} {:projection {:drawer 1}
                                        :session    session})
       (map (comp decode :drawer))
       (db/with-result tx)))

;;; --------------------------

(defn- mongo-dotted-path
  "Given a list of keywords or strings, return a single
   'mongo-dotted-path' with the dot notation."
  [coll]
  (->> (map #(if (string? %) (escape %) (encode %)) coll)
       (str/join "." )))

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
           (encode)
           (w/postwalk #(if-not (map-entry? %)
                          %
                          (let [[k v] %]
                            (if (map? v)
                              %
                              [k (if v 1 0)]))))))

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
  [{:keys [db session *cache] :as tx} drawer only limit where sort-config skip]
  (let [result (->> (mc/find db
                             (drawer->coll [db session *cache] drawer)
                             (prepare-where where)
                             {:keywordize? false
                              :limit       limit
                              :sort        (prepare-sort sort-config)
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
         (map decode)
         (db/with-result tx))))

(dp/defimpl -fetch
  [{:keys [db session] :as tx} drawer only limit where sort-config skip]
  (fetch tx drawer only limit where sort-config skip))

(dp/defimpl -fetch-count
  [{:keys [db session *cache] :as dresser} drawer where]
  (->> (mc/count-documents db
                           (drawer->coll [db session *cache] drawer)
                           (prepare-where where)
                           {:session session})
       (db/with-result dresser)))

;; MongoDB currently doesn't support '.drop()' inside a transaction.
(dp/defimpl -drop
  [{:keys [db session *cache] :as dresser} drawer]
  ;; Clearing the cache must be first because we are not in a
  ;; transaction.
  (cw/evict *cache drawer)
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
  [{:keys [db session *cache] :as tx} drawer id]
  (mc/delete-one db
                 (drawer->coll [db session *cache] drawer)
                 (id->mid {:id id})
                 {:session session})
  (db/with-result tx id))

(defn upsert
  [{:keys [db session *cache] :as dresser} drawer data]
  (let [encoded (-> (id->mid data)
                    (encode))]
    (->> (mc/find-one-and-replace db
                                  (drawer->coll [db session *cache] drawer :upsert)
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
  [{:keys [db session] :as dresser} drawer data]
  (upsert dresser drawer data))

(defn upsert-many
  [{:keys [db session *cache] :as dresser} drawer docs]
  (mc/bulk-write db
                 (drawer->coll [db session *cache] drawer :upsert)
                 (for [doc docs
                       :let [encoded (-> (id->mid doc)
                                         (encode))]]
                   [:replace-one {:filter      (select-keys encoded ["_id"])
                                  :replacement encoded
                                  :upsert?     true}])
                 {:session session})
  (db/with-result dresser docs))

(dp/defimpl -upsert-many
  [{:keys [db session *cache] :as dresser} drawer docs]
  (upsert-many dresser drawer docs))

;; transactionLifetimeLimitSeconds <-- might be useful in the future
(dp/defimpl -transact
  [dresser f {:keys [result?]}]
  (if (:transact dresser)
    (f dresser)
    ;; When not already inside a transaction:
    (let [dresser' (with-open [session (mcl/start-session (:client dresser))]
                     ;; TRANSACTIONS ARE ONLY ALLOWED ON REPLICA SETS?!?!

                     ;; Why not use transactionBody? Because it might
                     ;; automatically retry the transaction.
                     (.startTransaction session)
                     (try
                       (let [dresser (f (assoc dresser :transact true :session session))
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
      ;; Post-transaction operations.  This can't throw an exception
      ;; as the mongoDB transaction is completed, but we might still
      ;; be inside other transactions that MUST complete to stay in
      ;; sync.
      (try (doseq [f (:post-tx-fns dresser')]
             (f))
           (catch Exception _))
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
                                      (do (.drop (:db dresser))
                                          (.close (:client dresser))
                                          (swap! *test-dressers (fn [ds]
                                                                  (remove #{dresser} ds)))))
                                    @*test-dressers)))))}
  ([db-configs] (build {} db-configs))
  ([m {:keys [db-name host port] :as db-configs}]
   (let [client (mcl/create (str "mongodb://" host ":" port))
         db (mcl/get-db client db-name)]
     (vary-meta {:client     client
                 :db         db
                 :db-configs db-configs
                 :*cache (cw/lru-cache-factory {})}
                merge
                opt/optional-impl
                (mongo-impl)
                {:type ::db/dresser}))))




(comment
  (def aaa (build {:db-name "dresser_test_db"
                   :host    "127.0.0.1"
                   :port    27018}))
  (do (.drop (:db aaa))
      (.close (:client aaa))
      ))

(comment
  (require '[dresser.extensions.cache :as cache])
  (require '[dresser.impl.hashmap :as hm])
  (require '[dresser.impl.atom :as at])
  (require '[dresser.extensions.ttl :as ttl])
  (let [aaa (cache/cache aaa (hm/build))]
    (time (let [add-rnd-user! (fn [aaa idx]
                                (let [username (str (gensym "user-"))
                                      email    (str username "@" (gensym "email") ".com")]
                                  (ttl/add-with-ttl! aaa :users {:username username :email email
                                                                 :idx      idx}
                                                     (ttl/secs (rand-int 20)))))]
            (last (db/with-tx [tx aaa]
                    (reduce (fn [tx idx]
                              (add-rnd-user! tx idx))
                            tx
                            (range 1000))))))))
