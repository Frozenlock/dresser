(ns dresser.extensions.cache
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.protocols :as dp]))

;; -- Work in progress --
;;
;; Isn't complete and doesn't provide much speed gain for local
;; dressers.

;; Let's assume a simple nested document.
;; {:a
;;  {:b
;;   {:c1  "hello"
;;    :c2 "bonjour"}}}
;;
;; `get-at` root path [] will result in:
;; {:a {:b {:c1 "hello", :c2 "bonjour"}}}
;;
;; This can be used as a cache for :a or any deeper keys.
;; [:a] -> {:b {:c1 "hello", :c2 "bonjour"}}
;; [:a :b] -> {:c1 "hello", :c2 "bonjour"}}
;; [:a :b :c1] -> "hello"
;;
;; However, the reverse is not true.  We can't use the cache for
;; shallower path or an adjacent one.
;;
;; Also, fetch might only return a subset of a document.
;; (fetch ... :only {:a {:b {:c1 true}}}) -> {:a {:b {:c1 "hello"}}}
;; Note that :c2 is missing from the result.
;; Trying to retrieve it from the cache would return nil.
;;
;; This means that even if a field is present, there's no guarantee
;; it's complete.
;;
;; The solution attempted in this namespace is to keep the
;; 'completeness' status of a (sub)document in parallel with the
;; cached value.
;;
;;
;; The cache itself is stored in a dresser for a few reasons:
;; 1. clojure.core.cache doesn't work very well with our case of
;;    complete/incomplete documents.
;; 2. It will be easier to add a 'durable' caching.

(defn complete-doc?
  "True if `ks` is a path where the document is complete."
  [dresser drawer id ks]
  (db/tx-let [tx (db/with-result dresser false)]
      [ks (into [:complete] ks)
       only-m (when (seq ks)
                (reduce #(hash-map %2 %1) true (reverse ks)))
       paths (reverse (take-while seq (iterate butlast ks)))
       ;; Fetch the document up to a true (complete) value
       ;; Not complete: {:a {:b {}}}
       ;; Complete: {:a true}
       ret (db/fetch-by-id tx drawer id {:only only-m})]
    (or (some #(true? (get-in ret %)) paths) false)))

(comment
  (require '[dresser.impl.hashmap :as hm])
  (complete-doc? (hm/build {:drawer {:id {:complete {:a {:b {:c true}}}}}})
             :drawer :id [:a :b]) ;=> false

  (complete-doc? (hm/build {:drawer {:id {:complete {:a {:b {:c true}}}}}})
             :drawer :id [:a :b :c]) ;=> true

  (complete-doc? (hm/build {:drawer {:id {:complete {:a true}}}})
                 :drawer :id [:a :b :c])) ;=> true

(defn mark-complete!
  "Mark path as complete, if not already."
  [dresser drawer id ks]
  (db/tx-let [tx dresser]
      [already? (complete-doc? tx drawer id ks)]
    (if already?
      tx
      (db/assoc-at! tx drawer id (into [:complete] ks) true))))

(defn get-cache-at
  [dresser drawer id ks]
  (db/get-at dresser drawer id (into [:value] ks)))

(defn set-cache-at!
  [dresser drawer id ks data]
  (db/tx-> dresser
    (mark-complete! drawer id ks)
    (db/assoc-at! drawer id (into [:value] ks) data)))




(defn cache-get-at
  [cache-key get-at-method]
  (fn [tx drawer id ks only]
    ;; Start a transaction with the cache dresser
    (let [cache-db (db/tx-let [cache-tx (db/temp-data tx [cache-key]) {:result? false}]
                       [valid-cache? (complete-doc? cache-tx drawer id ks)
                        cached-value (if valid-cache?
                                       (get-cache-at cache-tx drawer id ks)
                                       ::miss)]
                     (if (not= cached-value ::miss)
                       ;; Potential operation with the non-cache tx.
                       ;; It must be returned alongside the result in
                       ;; case it was modified.
                       (db/with-result cache-tx [tx cached-value])
                       ;; The current cache-tx must be put back into 'tx' before using it
                       (let [[tx value] (-> (db/update-temp-data tx assoc cache-key cache-tx)
                                            (get-at-method drawer id ks only)
                                            (db/dr))
                             cache-tx (db/temp-data tx [cache-key])
                             cache-tx (set-cache-at! cache-tx drawer id ks value)]
                         (db/with-result cache-tx [tx value]))))
          ;; Extract the potentially updated 'tx' from the cache-db results.
          [tx value] (db/result cache-db)
          ;; Clear the result, to avoid keeping a reference to the tx.
          cache-db (db/with-result cache-db nil)]
      (-> (db/update-temp-data tx assoc cache-key cache-db)
          (db/with-result value)))))

(defn cache-upsert
  [cache-key upsert-method]
  (fn [tx drawer data]
    (-> (upsert-method tx drawer data)
        (db/update-temp-data update
                             cache-key
                             #(db/raw-> %
                                (set-cache-at! drawer (:id data) [] data))))))

(defn cache-assoc-at
  [cache-key assoc-at-method]
  (fn [tx drawer id ks data]
    (-> (assoc-at-method tx drawer id ks data)
        (db/update-temp-data update
                             cache-key
                             #(db/raw-> %
                                (set-cache-at! drawer id ks data))))))


(ext/defext cache
  [cache-dresser]
  (let [cache-key (gensym "cache-")]
    {:deps         []
     :init-fn      (fn [dresser]
                     (db/update-temp-data dresser assoc cache-key cache-dresser))
     :wrap-configs {`dp/-get-at {:wrap (partial cache-get-at cache-key)}
                    `dp/-assoc-at {:wrap (partial cache-assoc-at cache-key)}
                    `dp/-upsert {:wrap (partial cache-upsert cache-key)}}}))
