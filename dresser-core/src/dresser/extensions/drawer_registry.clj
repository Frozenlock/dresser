(ns dresser.extensions.drawer-registry
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.protocols :as dp]))

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

(defn- drawer-count!
  "Increments and returns the drawer count value."
  [dresser]
  ;; Should it be a random increment?  Are there situations where it
  ;; would be useful to have distributed versions that can be merged
  ;; back together?
  (db/tx-> dresser
    (db/update-at! db/drs-drawer :drs_drawer-registry [:drs_drawer-counter]
                   (fn [x]
                     ((fnil inc 0) x)))))

(defn drawer-id
  "Returns a drawer ID.
  String of the form <dresser-id>_<encoded-drawer-count>.

  If `upsert?` is true, create an ID and return it if none were found.
  Drawers can be renamed, but IDs should be forever."
  [dresser drawer upsert?]
  (db/tx-let [tx dresser]
      [d-ids (db/get-at tx key->ids drawer [:d-ids])
       id (first d-ids)]
    (cond
      id (db/with-result tx id)
      upsert? (let [[tx drawer-num] (db/dr (drawer-count! tx))
                    [tx dresser-id] (db/dr (db/dresser-id tx))
                    new-id (str dresser-id "_" (db/lexical-encode drawer-num))]
                (db/tx-> tx
                  (db/assoc-at! key->ids drawer [] {:d-ids [new-id]})
                  (db/assoc-at! registry new-id [] {:key drawer})
                  (db/with-result new-id)))
      :else (db/with-result tx nil))))

(defn drawer-ids
  "Returns all IDs associated with the drawer."
  [dresser drawer]
  (db/get-at dresser key->ids drawer [:d-ids]))

(defn drawer-key
  "Returns the drawer key associated with this ID."
  [dresser drawer-id]
  (db/get-at dresser registry drawer-id [:key]))

(defn- rename-drawer-in-registry!
  [dresser drawer new-drawer]
  (db/tx-let [tx dresser]
      [exists? (drawer-id tx new-drawer false)
       _ (when exists? (throw (ex-info "Can't rename to an existing drawer" {})))
       d-ids (db/get-at tx key->ids drawer [:d-ids])]
    (if (not-empty d-ids)
      (-> (reduce (fn [tx' d-id]
                    (db/assoc-at! tx registry d-id [:key] new-drawer))
                  tx d-ids)
          (db/assoc-at! key->ids new-drawer [:d-ids] d-ids)
          (db/delete! key->ids drawer))
      tx)))

(defn- drop-drawer!
  [dresser drawer]
  (db/tx-let [tx dresser]
      [d-ids (db/get-at tx key->ids drawer [:d-ids])]
    (-> (reduce (fn [tx' d-id]
                  (db/delete! tx' registry d-id))
                tx d-ids)
        (db/delete! key->ids drawer))))

(ext/defext drawer-registry
  []
  {:init-fn #(db/with-system-drawers % [registry key->ids])
   :wrap-configs
   {`dp/rename-drawer {:wrap (fn [method]
                               (fn [tx drawer new-drawer]
                                 (-> tx
                                     (rename-drawer-in-registry! drawer new-drawer)
                                     (method drawer new-drawer))))}
    `dp/drop          {:closing (fn [tx drawer]
                                  (-> tx
                                      (drop-drawer! drawer)
                                      (db/with-result drawer)))}
    `dp/drawer-key    {:wrap (fn [_method]
                               (fn [tx drawer-id]
                                 (drawer-key tx drawer-id)))}}})
