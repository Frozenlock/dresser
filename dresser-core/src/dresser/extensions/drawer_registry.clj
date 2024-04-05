(ns dresser.extensions.drawer-registry
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.protocols :as dp]
            [dresser.drawer :as dd]))

;; The drawer registry adds the ability to attach durable metadata to
;; a drawer, such as a drawer-id, schemas, creation date, etc.

(def registry :drs_drawer-registry)
(def key->ids :drs_drawer-key->drawer-ids) ; drawer-key is used as doc key

(comment
  {:id      "uuid1"
   :key     "users"
   :schemas [:map
             [:email string?]
             ...]})

(defn drawer-id
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

(defn drawer-key
  "Returns the drawer key associated with this ID."
  [dresser drawer-id]
  (db/get-at dresser registry drawer-id [:key]))

(defn- rename-drawer-in-registry!
  [dresser drawer new-drawer]
  (db/tx-let [tx dresser]
      [drawer-key (dd/key drawer)
       new-drawer-key (dd/key new-drawer)
       exists? (drawer-id tx new-drawer false)
       _ (when exists? (throw (ex-info "Can't rename to an existing drawer" {})))
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

(ext/defext drawer-registry
  []
  {:init-fn #(db/with-system-drawers % [registry key->ids])
   :wrap-configs
   {`dp/-rename-drawer {:wrap (fn [method]
                                (fn [tx drawer new-drawer]
                                  (-> tx
                                      (rename-drawer-in-registry! drawer new-drawer)
                                      (method drawer new-drawer))))}
    `dp/-drop          {:closing (fn [tx drawer]
                                   (-> tx
                                       (drop-drawer! drawer)
                                       (db/with-result drawer)))}}})
