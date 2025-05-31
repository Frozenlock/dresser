(ns dresser.impl.codax
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [codax.core :as c]
            [dresser.base :as db]
            [dresser.encoding :as enc]
            [dresser.impl.hashmap :as hm]
            [dresser.impl.optional :as opt]
            [dresser.impl.pathwise :as pathwise]
            [dresser.protocols :as dp]
            [dresser.test :as dt]))

pathwise/side-effect


;;; There are 2 different transaction types that need to be handled in
;;; this ns: the dresser transaction (tx) and the codax transaction
;;; (codax).

(defn- encode-records
  "Walk the data structure and encode all records"
  [data]
  (walk/postwalk enc/encode-record data))

(defn- decode-records
  "Walk the data structure and decode all records"
  [data]
  (walk/postwalk enc/restore-record data))


;; Listing all keys requires loading everything if we don't keep our
;; own registry. https://github.com/dscarpetti/codax/issues/24
(def codax-drawers :drs_codax_drawers)


(defn not-lazy
  [x]
  (if (seq? x)
    (doall x)
    x))


(defn do-transact
  [dresser f opts]
  (let [;; Codax doesn't appear to provide an easy way to extract results
        ;; from a transaction AND something else. Use a promise to extract
        ;; the dresser back from inside the codax transaction.
        *ret (promise)

        ;; `open-database!` returns the same object for a given path.
        ;; Closing it would mean breaking all other transactions.
        db (c/open-database! (:path dresser))
        transact-fn (fn [codax]
                      (let [dresser (-> (f (assoc dresser :codax codax))
                                        (db/update-result not-lazy))]
                        (deliver *ret dresser)
                        (:codax dresser)))]
    ;; TODO: Automatic retries should be optional
    (try
      ;; Upgradable-tx upgrades to a write-tx when possible.
      ;; If not, it throws and we can restart our own transaction.
      (c/with-upgradable-transaction [db codax :throw-on-upgrade true]
        (transact-fn codax))
      (catch clojure.lang.ExceptionInfo e
        (if (:codax/upgraded-transaction (ex-data e))
          ;; 'upgrade-restart-required' means we need a write TX.
          (c/with-write-transaction [db codax]
            (transact-fn codax))
          (throw e))))
    (let [dresser @*ret]
      (assoc dresser :codax false))))


(defn do-all-drawers
  [tx]
  ; When reading from a codax, we don't need to return an 'updated' codax.
  (->> (map first (c/seek-at (:codax tx) [codax-drawers]))
       (db/with-result tx)))

(defn do-delete-many
  [tx drawer where]
  (let [tx (db/with-result tx {:deleted-count 0})]
    (db/fetch-reduce tx drawer
                     (fn [tx doc]
                       (-> (update tx :codax c/dissoc-at [drawer (:id doc)])
                                     (db/update-result update :deleted-count inc)))
                     {:where (encode-records where)
                      :only  {:id :?}})))

(defn do-drop
  [tx drawer]
  (let [codax (:codax tx)
        codax (c/dissoc-at codax [drawer])
        codax (c/dissoc-at codax [codax-drawers drawer])]
    (-> (assoc tx :codax codax)
        (db/with-result drawer))))


(defn- lazy-fetch
  [codax path start-key end-key chunk-size remove-first? reverse?]
  ;; codax seeks are inclusive
  (let [max-chunk-size 64
        start-key (or start-key
                      (when-not end-key
                        (ffirst (c/seek-at codax path :limit 1 :reverse reverse?))))
        data (if (and start-key end-key
                      (= start-key end-key))
               [[start-key (c/get-at codax (conj path start-key))]]
               (if end-key
                 (if (and end-key (not start-key))
                   (if reverse?
                     (c/seek-from codax path end-key
                                  :limit chunk-size
                                  :reverse reverse?)
                     (c/seek-to codax path end-key
                                :limit chunk-size
                                :reverse false))
                   (if (neg? (compare start-key end-key))
                     (c/seek-range codax path start-key end-key
                                   :limit chunk-size
                                   :reverse reverse?)
                     (c/seek-range codax path end-key start-key
                                   :limit chunk-size
                                   :reverse reverse?)))
                 (if reverse?
                   (c/seek-to codax path start-key
                              :limit chunk-size
                              :reverse true)
                   (c/seek-from codax path start-key
                                :limit chunk-size))))
        chunk-maxed? (= chunk-size (count data))
        data (if remove-first? (rest data) data)]
    (if chunk-maxed?
      (lazy-cat
       data
       (lazy-fetch codax path
                   (first (last data)) ; last key
                   end-key
                   (min max-chunk-size (* 2 chunk-size))
                   true reverse?))
      data)))


(defn- fetch* ;; EVERY ARGUMENT SHOULD ALREADY BE ENCODED
  [codax drawer only limit where sort-config]
  (or
   ;; db/any branches might fetch multiple times the same docs.
   ;; Could be optimized, if it becomes an issue.

   ;; Top db/any
   (when-let [top-any (::db/any where)]
     (when (every? :id top-any)
       (distinct (mapcat #(fetch* codax drawer only limit (:id %) sort-config) top-any))))

   ;; Top inner db/any
   (when-let [?id (:id where)]
     (when-let [id-qs (when (map? ?id)
                        (::db/any ?id))]
       (distinct (mapcat #(fetch* codax drawer only limit (assoc where :id %) sort-config) id-qs))))

   ;; Normal
   (let [;; Documents are already sorted by ID.
         ;; Leverage this ordering with seek-at/seek-from when possible.
         sort-only-id? (and (= :id (first (ffirst sort-config)))
                            (= 1 (count sort-config)))
         sort-id-reverse? (and sort-only-id? (= :desc (second (first sort-config))))
         ;; Identify if there's an :id query and isolate it
         id-queries (when-let [id-q (:id where)]
                      (when (and (map? id-q)
                                 (some db/ops? (keys id-q)))
                        id-q))
         [start-key end-key] (if id-queries
                               (let [start-key (or (::db/gte id-queries)
                                                   (::db/gt id-queries))
                                     end-key (or (::db/lte id-queries)
                                                 (::db/lt id-queries))]
                                 (if sort-id-reverse?
                                   [end-key start-key]
                                   [start-key end-key]))
                               [(:id where) (:id where)])]
     (let [to-remove (vals (select-keys id-queries [::db/lt ::db/gt]))
           inclusive-results (lazy-fetch codax [drawer] start-key end-key
                                         2 false sort-id-reverse?)]
       (for [[k data] inclusive-results
             :when (not (some #{k} to-remove))]
         data)))))

(defn do-fetch
  [tx drawer only limit where sort-config skip]
  (if (nil? (get where :id :not-found))
    (db/with-result tx '())
    (let [where (encode-records where)
          only (encode-records only)
          other-where (dissoc where :id)
          sort-only-id? (and (= :id (first (ffirst sort-config)))
                             (= 1 (count sort-config)))
          all-docs (fetch* (:codax tx) drawer only limit where sort-config)]
      (->> (hm/fetch-from-docs all-docs only limit other-where (if sort-only-id? nil sort-config) skip)
           (map decode-records)
           (doall)
           (db/with-result tx)))))







(defn do-fetch-by-id
  [tx drawer id only where]
  (let [codax (:codax tx)
        where (encode-records where)
        only (encode-records only)]
    (db/with-result tx
      (when-let [doc (c/get-at codax [drawer id])]
        (when (hm/where? doc where)
          (let [filtered (hm/take-from doc only)]
            (decode-records filtered)))))))

(defn get-temp-data
  [dresser]
  (get dresser :data))

(defn do-assoc-at
  [tx drawer id ks data]
  (let [drawer-key drawer
        codax (:codax tx)
        ?path (some-> ks seq encode-records)
        ;; At root? Ensure we have an ID
        prepared-data (encode-records (if ?path data (assoc data :id id)))
        codax (c/assoc-at codax (into [drawer-key id] ?path) prepared-data)
        ;; Might be creating a new document, ensure we have an ID
        codax (if (not (c/get-at codax [drawer-key id :id]))
                (c/assoc-at codax [drawer-key id :id] id)
                codax)
        drawer-registered? (c/get-at codax [codax-drawers drawer-key])
        codax (if-not drawer-registered?
               (c/assoc-at codax [codax-drawers drawer-key] true)
               codax)]
    (-> (assoc tx :codax codax)
        (db/with-result data))))

(defn set-temp-data
  [dresser data]
  (assoc dresser :data data))


;; Codax implementation methods provided via metadata

(defn build
  {:test (fn []
           (let [test-path "test-db"
                 destroy! #(c/destroy-database! test-path)]
             (dt/test-impl (fn []
                             (destroy!)
                             (dt/no-tx-reuse (build test-path))))
             (destroy!)))}
  [path]
  (let [impl (-> {:path path :data nil :codax nil}
                 (with-meta
                   (merge opt/default-implementations
                          {`dp/fetch do-fetch
                           `dp/all-drawers do-all-drawers
                           `dp/delete-many do-delete-many
                           `dp/assoc-at do-assoc-at
                           `dp/drop do-drop
                           `dp/transact do-transact
                           `dp/temp-data get-temp-data
                           `dp/with-temp-data set-temp-data
                           `dp/immutable? (constantly false)
                           `dp/tx? #(boolean (:codax %))
                           `dp/start identity
                           `dp/stop identity
                           ;; Specialized implementation for better performance
                           `dp/fetch-by-id do-fetch-by-id})))]
    (-> (db/make-dresser impl false)
        (db/with-temp-dresser-id))))

















;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;




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
                                                   (ttl/secs 10))))]
          (db/with-tx [tx aaa]
            (reduce (fn [tx idx]
                      (add-rnd-user! tx idx))
                    tx (range 10000)))))
  (ttl/add-with-ttl! aaa :users {:username "Will be deleted!" :email "..@.."} (ttl/secs 10)))
