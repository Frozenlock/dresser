(ns dresser.impl.codax
  (:require [codax.core :as c]
            [dresser.base :as db]
            [dresser.drawer :as dd]
            [dresser.impl.hashmap :as hm]
            [dresser.impl.optional :as opt]
            [dresser.impl.pathwise :as pathwise]
            [dresser.protocols :as dp]
            [dresser.test :as dt]))

pathwise/side-effect

;;; There are 2 different transaction types that need to be handled in
;;; this ns: the dresser transaction (tx) and the codax transaction
;;; (codax).


;; Listing all keys requires loading everything if we don't keep our
;; own registry. https://github.com/dscarpetti/codax/issues/24
(def codax-drawers :drs_codax_drawers)


(defn not-lazy
  [x]
  (if (seq? x)
    (doall x)
    x))


(dp/defimpl -transact
  [dresser f {:keys [result?]}]
  (if (:codax dresser)
    (f dresser)
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
        (if result?
          (db/result dresser)
          (dissoc dresser :codax))))))


(dp/defimpl -all-drawers
  [tx]
  ; When reading from a codax, we don't need to return an 'updated' codax.
  (->> (map first (c/seek-at (:codax tx) [codax-drawers]))
       (db/with-result tx)))

(dp/defimpl -delete
  [tx drawer id]
  (let [codax (:codax tx)
        codax (c/dissoc-at codax [(dd/key drawer) id])]
    (-> (assoc tx :codax codax)
        (db/with-result id))))

(dp/defimpl -drop
  [tx drawer]
  (let [drawer-key (dd/key drawer)
        codax (:codax tx)
        codax (c/dissoc-at codax [drawer-key])
        codax (c/dissoc-at codax [codax-drawers drawer-key])]
    (-> (assoc tx :codax codax)
        (db/with-result drawer))))


(defn- lazy-fetch
  [codax path start-key end-key chunk-size remove-first? reverse?]
  (let [max-chunk-size 64
        start-key (or start-key
                      (when-not end-key
                        (ffirst (c/seek-at codax path :limit 1 :reverse reverse?))))
        data (if end-key
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
                              :limit chunk-size)))
        data (if remove-first? (rest data) data)
        last-key (first (last data))]
    (if last-key
      (lazy-cat
       data
       (lazy-fetch codax path
                        last-key
                        end-key
                        (min max-chunk-size (* 2 chunk-size))
                        true reverse?))
      data)))


(dp/defimpl -fetch
  [tx drawer only limit where sort-config skip]
  (let [codax (:codax tx)
        ;; Documents are already sorted by ID.
        ;; Leverage this ordering with seek-at/seek-from when possible.
        sort-only-id? (and (= :id (first (ffirst sort-config)))
                           (= 1 (count sort-config)))
        sort-id-reverse? (and sort-only-id? (= :desc (second (first sort-config))))
        ;; Identify if there's an :id query and isolate it
        id-queries (:id where)
        ;; Remove the id query from the where clause
        other-where (dissoc where :id)
        ;; Depending on the operators, choose an appropriate start and end key
        start-key (or (::db/gte id-queries)
                      (::db/gt id-queries))
        end-key (or (::db/lte id-queries)
                    (::db/lt id-queries))
        [start-key end-key] (if sort-id-reverse?
                              [end-key start-key]
                              [start-key end-key])
        id-ops (set (keys id-queries))
        all-docs (let [results (map second (lazy-fetch codax [(dd/key drawer)] start-key end-key 1 false sort-id-reverse?))]
                   (if sort-id-reverse?
                     (cond->> results
                       (id-ops ::db/lt) rest
                       (id-ops ::db/gt) drop-last)
                     (cond->> results
                       (id-ops ::db/lt) drop-last
                       (id-ops ::db/gt) rest)))]
    (->> (hm/fetch-from-docs all-docs only limit other-where (if sort-only-id? nil sort-config) skip)
         (doall)
         (db/with-result tx))))







(dp/defimpl -fetch-by-id
  [tx drawer id only where]
  (let [codax (:codax tx)]
    (db/with-result tx
      (some-> (c/get-at codax [(dd/key drawer) id])
              (hm/where? where)
              (hm/take-from only)))))

(dp/defimpl -temp-data
  [dresser]
  (get dresser :data))


(dp/defimpl -upsert
  [tx drawer data]
  (let [drawer-key (dd/key drawer)
        codax (:codax tx)
        codax (c/assoc-at codax [drawer-key (:id data)] data)
        drawer-registered? (c/get-at codax [codax-drawers drawer-key])
        codax (if-not drawer-registered?
               (c/assoc-at codax [codax-drawers drawer-key] true)
               codax)]
    (-> (assoc tx :codax codax)
        (db/with-result data))))

(dp/defimpl -with-temp-data
  [dresser data]
  (assoc dresser :data data))


(defn codax-impl
  []
  (dp/mapify-impls
   [-all-drawers
    -delete
    -drop
    -fetch
    -fetch-by-id
    -temp-data
    -transact
    -upsert
    -with-temp-data]))

(defn build
  {:test (fn []
           (let [test-path "test-db"
                 destroy! #(c/destroy-database! test-path)]
             (dt/test-impl (fn []
                             (destroy!)
                             (dt/no-tx-reuse (build test-path))))
             (destroy!)))}
  [path]
  (vary-meta {:path path}
             merge
             {:type ::db/dresser}
             opt/optional-impl
             (codax-impl)))

















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
