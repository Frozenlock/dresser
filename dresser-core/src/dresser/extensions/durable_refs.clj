(ns dresser.extensions.durable-refs
  "Durable references across documents"
  (:refer-clojure :exclude [ref])
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.drawer-registry :as d-reg]))

(defprotocol IDurableRef
  :extend-via-metadata true
  (drawer-id [d-ref] "Returns the drawer ID")
  (doc-id [d-ref] "Returns the document ID"))

(defrecord DurableRef [drawer-id doc-id]
  IDurableRef
  (drawer-id [_this] drawer-id)
  (doc-id [_this] doc-id))

(defn durable
  [drawer-id doc-id]
  (->DurableRef drawer-id doc-id))

(defn- ref*
  ([dresser drawer doc-id upsert?]
   (if doc-id
     (db/tx-> dresser
       (d-reg/drawer-id drawer upsert?)
       (db/update-result
        (fn [x]
          (when x (durable x doc-id)))))

     dresser)))

(defn ref
  "Returns a ref if it exists, nil otherwise."
  ;; Currently we only check if the drawer has a durable-id
  [dresser drawer doc-id]
  (ref* dresser drawer doc-id false))

(defn ref!
  "Returns a `ref`, creating it if it doesn't exists.
  Should contain all the informations to retrieve the document from a dresser."
  [dresser drawer doc-id]
  (ref* dresser drawer doc-id :upsert))

(defn drawer
  "Returns the drawer associated with this ref."
  [dresser ref]
  (d-reg/drawer-key dresser (drawer-id ref)))


;; Some convenience functions

(defn add!
  [dresser drawer data]
  (db/tx-let [tx dresser]
      [d-id (db/add! tx drawer data)
       d-ref (ref! tx drawer d-id)]
    (db/with-result tx d-ref)))

(defn upsert!
  [dresser drawer data]
  (db/tx-let [tx dresser]
      [{:keys [id]} (db/upsert! tx drawer data)
       d-ref (ref! tx drawer id)]
    (db/with-result tx d-ref)))

;; (defn- wrap-base-fn
;;   [f]
;;   (fn [dresser ref & args]
;;     (db/tx-let [tx dresser]
;;         [drawer-o (drawer tx ref)
;;          doc-id (doc-id ref)]
;;       (apply f tx drawer-o doc-id args))))

;; (defn update-at!
;;   "`dp/update-at` but for refs"
;;   {:arglists '([dresser d-ref ks f & args])}
;;   [dresser ref ks f & args]
;;   (wrap-base-fn db/update-at!))


(defn update-at!
  "`dp/update-at` but for refs"
  {:arglists '([dresser d-ref ks f & args])}
  [dresser ref ks f & args]
  (db/tx-let [tx dresser]
      [d-key (drawer tx ref)]
    (apply db/update-at! tx d-key (doc-id ref) ks f args)))

(defn dissoc-at!
  "`dp/dissoc-at` but for refs"
  {:arglists '([dresser d-ref ks & dissoc-ks])}
  [dresser ref ks & dissoc-ks]
  (db/tx-let [tx dresser]
      [d-key (drawer tx ref)]
    (apply db/dissoc-at! tx d-key (doc-id ref) ks dissoc-ks)))

(defn assoc-at!
  "`dp/assoc-at` but for refs"
  {:arglists '([dresser d-ref ks data])}
  [dresser ref ks data]
  (db/tx-let [tx dresser]
      [d-key (drawer tx ref)]
    (db/assoc-at! tx d-key (doc-id ref) ks data)))

(defn fetch-by-ref
  "`dp/fetch-by-id` but for refs"
  {:arglists '([dresser d-ref]
               [dresser d-ref {:keys [only where]}])}
  ([dresser ref] (fetch-by-ref dresser ref {}))
  ([dresser ref opts]
   (db/tx-let [tx dresser]
       [d-key (drawer tx ref)]
     (db/fetch-by-id tx d-key (doc-id ref) opts))))

(defn get-at
  "`dp/get-at` but for refs"
  ([dresser ref ks] (get-at dresser ref ks nil))
  ([dresser ref ks only]
   (db/tx-let [tx dresser]
       [d-key (drawer tx ref)]
     (when d-key
       (db/get-at tx d-key (doc-id ref) ks only)))))

(defn delete!
  "`dp/delete` but for refs"
  [dresser ref]
  (db/tx-let [tx dresser]
      [d-key (drawer tx ref)]
    (db/delete! tx d-key (doc-id ref))))



(defn expand-fn
  "Given a function expecting a reference, returns a function that will
  accept a drawer and a doc-id instead.

  (fn [dresser ref ...] ...)
    becomes
  (fn [dresser drawer doc-id ...] ...)"
  [f]
  (fn [dresser drawer id & args]
    (db/tx-let [tx dresser]
        [target-ref (ref! tx drawer id)]
      (apply f tx target-ref args))))
;;;;


;; {<drawer-id> {<doc-id ...}}
;; (defn fetch-by-refs
;;   "Query a map of references as if it was a drawer.

;;    Takes a map of drawer-ids to document-ids and executes a query across all referenced
;;    documents, returning results as if they were a single drawer.

;;    Parameters:
;;      dresser - The dresser object
;;      refs-map - A map of drawer-ids to maps of document-ids
;;                 e.g., {\"drawer1_id\" {\"doc1_id\" {}, \"doc2_id\" {}}}
;;      query - Standard dresser query map with :where, :only, :limit, :sort, :skip

;;    Returns:
;;      Documents matching the query, with an added :dref key containing the reference"
;;   [dresser refs-map {:keys [where only limit sort skip] :as query}]
;;   (db/with-tx [tx dresser]
;;     (reduce (fn [tx [drawer-id id->doc]]
;;               (db/tx-let [tx tx]
;;                   [result (db/result tx)
;;                    q (cond-> (assoc-in query [:where :id]
;;                                        {db/any (keys id->doc)})
;;                        (:only query) (assoc-in [:only :id] :?))
;;                    drawer (d-reg/drawer-key tx drawer-id)
;;                    new-result (db/fetch tx drawer q)
;;                    new-result (-> (for [d new-result]
;;                                     (assoc d :dref (durable drawer-id (:id d))))
;;                                   (hm/fetch-from-docs nil limit nil sort skip))]
;;                 (db/with-result tx (into result new-result))))
;;             (db/with-result tx [])
;;             refs-map)))


(ext/defext durable-refs
  []
  {:deps [d-reg/drawer-registry]})
