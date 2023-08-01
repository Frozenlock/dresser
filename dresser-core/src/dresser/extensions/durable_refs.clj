(ns dresser.extensions.durable-refs
  "Durable references across documents"
  (:refer-clojure :exclude [ref])
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.protocols :as dp]
            [dresser.drawer :as dd]))


;;; --- Drawer registry ---

;; Allows to keep track of drawers even after renames.

;; Why not define as a Dresser method? Because in addition to adding
;; complexity, it must also be automatically updated whenever a drawer
;; is dropped. At this point it might as well be an extension.

(def registry :drs_drawer-registry)
(def key->ids :drs_drawer-key->drawer-ids) ; drawer-key is used as doc key

;; Each drawer key can have multiple IDs, for example if we merge 2
;; dressers together that have an identical drawer key.

(defn- drawer-id
  "Returns a drawer ID.
  If `upsert?` is true, create an ID and return it if none were found.
  Drawers can be renamed, but IDs should be forever."
  [dresser drawer upsert?]
  (db/tx-let [tx dresser]
      [drawer-key (dd/key drawer)
       d-ids (db/get-at tx key->ids drawer-key [:d-ids])
       id (first d-ids)]
    (cond
      id (db/with-result tx id)
      upsert? (let [[tx new-id] (db/dr (db/gen-id! tx registry))]
                (-> (db/upsert! tx key->ids {:id drawer-key, :d-ids [new-id]})
                    (db/upsert! registry {:id new-id, :key drawer-key})
                    (db/with-result new-id)))
      :else (db/with-result tx nil))))

(defn- drawer-key
  "Returns the drawer key associated with this ID."
  [dresser drawer-id]
  (db/get-at dresser registry drawer-id [:key]))


(defn- rename-drawer-in-registry!
  [dresser drawer new-drawer]
  (db/tx-let [tx dresser]
      [drawer-key (dd/key drawer)
       new-drawer-key (dd/key new-drawer)
       exists? (drawer-id tx new-drawer false)
       _ (assert (not exists?) "Can't rename to an existing drawer")
       d-ids (db/get-at tx key->ids drawer-key [:d-ids])]
    (if (not-empty d-ids)
      (-> (reduce (fn [tx' d-id]
                    (db/assoc-at! tx registry d-id [:key] new-drawer-key))
                  tx d-ids)
          (db/assoc-at! key->ids new-drawer-key [:d-ids] d-ids)
          (db/delete! key->ids drawer-key))
      tx)))

(defn- drop-drawer!
  [dresser drawer]
  (db/tx-let [tx dresser]
      [drawer-key (dd/key drawer)
       d-ids (db/get-at tx key->ids drawer-key [:d-ids])]
    (-> (reduce (fn [tx' d-id]
                  (db/delete! tx' registry d-id))
                tx d-ids)
        (db/delete! key->ids drawer-key))))






;;; --- Refs ---

;; Consider having a references drawer.
;; ref-id {:dresser-id ... :drawer-id ... :doc-id...}

;; TODO: After some thoughts it might be preferable.  Consumers would
;; only store the ref-id and it would allow the ref to migrate to
;; another DB if needed.  Wouldn't it be slow to fetch the ref and
;; then the drawer-key? Not really, as everything except the doc-id
;; should end-up cached in memory.

;; Update: Is it really better? By storing the drawer-id inside the
;; ref object, we can quickly dispatch to the target drawer. Even more
;; worthwile if we link multiple dressers together.


(defn- ref-drawer
  "Returns a ref-drawer. Similar to the drawer object, but also contains
  the dresser-id and the drawer-id"
  [{:keys [drawer-key drawer-id dresser-id] :as args}]
  (some-> (dd/drawer drawer-key) ; TODO: test `some->`
          (merge args)))

(defn- ref*
  ([dresser drawer doc-id upsert?]
   (db/tx-let [tx dresser]
       [drawer-id (drawer-id tx drawer upsert?)
        dresser-id (when drawer-id (db/dresser-id tx))]
     (db/with-result tx
       (when drawer-id [dresser-id drawer-id doc-id])))))

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

(defn- drawer
  "Returns the drawer associated with this ref."
  [dresser ref]
  (let [[dresser-id drawer-id _doc-id] ref]
    (db/tx-let [tx dresser]
        [d-key (drawer-key tx drawer-id)]
      (db/with-result tx (ref-drawer {:drawer-id  drawer-id
                                      :drawer-key d-key
                                      :dresser-id dresser-id})))))

(defn doc-id
  "Returns the document ID associated with this ref."
  [ref]
  (let [[_dresser-id _drawer-id doc-id] ref]
    doc-id))


;; Some convenience functions

(defn add!
  [dresser drawer data]
  (db/tx-let [tx dresser]
      [d-id (db/add! tx drawer data)
       d-ref (ref! tx drawer d-id)]
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
  {:arglists '([dresser d-ref ks])}
  [dresser ref ks]
  (db/tx-let [tx dresser]
      [d-key (drawer tx ref)]
    (db/get-at tx d-key (doc-id ref) ks)))

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
  (fn [tx drawer id & args]
    (let [[tx target-ref] (db/dr (ref! tx drawer id))]
      (apply f tx target-ref args))))
;;;;


(ext/defext keep-sync
  []
  :init-fn #(db/with-system-drawers % [registry key->ids])
  :wrap-configs
  {`dp/-rename-drawer {:wrap (fn [method]
                               (fn [tx drawer new-drawer]
                                 (-> tx
                                     (rename-drawer-in-registry! drawer new-drawer)
                                     (method drawer new-drawer))))}
   `dp/-drop          {:closing (fn [tx drawer]
                                  (-> tx
                                      (drop-drawer! drawer)
                                      (db/with-result drawer)))}})
