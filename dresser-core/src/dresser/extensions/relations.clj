(ns dresser.extensions.relations
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.durable-refs :as refs]
            [dresser.protocols :as dp]))

(def rel-drawer :drs_relations)

(defn- ref-vec
  [{:keys [drawer-id doc-id]}]
  [drawer-id doc-id])

(defn relations
  "Returns all the relations of a given name.
  Optional drawer-ids can be passed to select only relations for documents in those drawers."
  ([dresser ref1 rel-name]
   (relations dresser ref1 rel-name nil))
  ([dresser ref1 rel-name drawer-ids]
   (db/tx-let [tx dresser]
       [rels (db/get-at tx rel-drawer (ref-vec ref1) [:rels rel-name])]
     (into {}
           (for [[drawer-id doc-id->rel] rels
                 :when (if (not-empty drawer-ids)
                         (some #{drawer-id} drawer-ids)
                         true)
                 [doc-id rel] doc-id->rel]
             [(refs/durable drawer-id doc-id) (:data rel {})])))))

(defn relation
  "Returns the user data part of a relation, if it exists."
  ([dresser ref1 rel-name ref2]
   (relation dresser ref1 rel-name ref2 nil))
  ([dresser ref1 rel-name ref2 only]
   (let [path (into [:rels rel-name] (ref-vec ref2))]
     (-> (db/get-at dresser rel-drawer (ref-vec ref1) path)
         (db/update-result #(:data % {}))))))

(defn is?
  "True if the relation exists.
  Ex: r1 is a parent of r2
      (is? r1 :parent r2) => true"
  [dresser ref1 rel-name ref2]
  (let [path (into [:rels rel-name] (ref-vec ref2))]
    (-> (db/get-at dresser rel-drawer (ref-vec ref1) (conj path :_inv-rel))
        (db/update-result some?))))

(defn upsert-relation!
  "To avoid clashes, consider using a namespaced or vector `rel-name`.
  Ex: [:drs :memberships]"
  ([dresser ref1 ref2 rel-name]
   (upsert-relation! dresser ref1 ref2 rel-name rel-name))
  ([dresser ref1 ref2 rname1->2 rname2->1]
   (upsert-relation! dresser ref1 ref2 rname1->2 rname2->1 nil nil))
  ([dresser ref1 ref2 rname1->2 rname2->1 data1->2 data2->1]
   (let [p1->2 (into [:rels rname1->2] (ref-vec ref2))
         p2->1 (into [:rels rname2->1] (ref-vec ref1))
         d1->2 (merge {:_inv-rel rname2->1} (when data1->2 {:data data1->2}))
         d2->1 (merge {:_inv-rel rname1->2} (when data2->1 {:data data2->1}))]
     (db/tx-> dresser
       (db/assoc-at! rel-drawer (ref-vec ref1) p1->2 d1->2)
       (db/assoc-at! rel-drawer (ref-vec ref2) p2->1 d2->1)))))

(defn update-in-relation!
  "Update relation data using functions, like update-in for maps.
  Ensures bidirectional relations exist. Pass nil for f1->2 or f2->1 if no update needed.
  Optimized: checks if relation exists before creating it."
  [dresser ref1 rname1->2 ref2 rname2->1 f1->2 f2->1]
  (let [p1->2 (into [:rels rname1->2] (ref-vec ref2))
        p2->1 (into [:rels rname2->1] (ref-vec ref1))
        make-update-fn (fn [f]
                         (when f
                           (fn [current]
                             (let [current-data (get current :data)
                                   updated-data (f current-data)]
                               (if updated-data
                                 (assoc current :data updated-data)
                                 (dissoc current :data))))))]
    (db/tx-let [tx dresser]
        [relation-exists? (db/get-at tx rel-drawer (ref-vec ref1) (conj p1->2 :_inv-rel))]
      (if relation-exists?
        ;; Fast path: relation exists, just apply updates
        (db/tx-> tx
          (#(if f1->2
              (db/update-at! % rel-drawer (ref-vec ref1) p1->2 (make-update-fn f1->2))
              %))
          (#(if f2->1
              (db/update-at! % rel-drawer (ref-vec ref2) p2->1 (make-update-fn f2->1))
              %)))
        ;; Slow path: relation doesn't exist, create it with initial data
        (let [initial-data-1->2 (when f1->2 (f1->2 nil))
              initial-data-2->1 (when f2->1 (f2->1 nil))]
          (upsert-relation! tx ref1 ref2 rname1->2 rname2->1 initial-data-1->2 initial-data-2->1))))))

(defn- clean-dissoc-at!
  "Dissoc at path. If resulting map is empty, dissoc it from parent map.
  If document ends up empty, delete it."
  [dresser drawer id ks & dissoc-ks]
  (db/tx-let [tx dresser]
      [ret (apply db/update-at! tx drawer id ks dissoc dissoc-ks)
       _ (if (empty? ret)
           (clean-dissoc-at! tx drawer id (butlast ks) (last ks))
           (if (and (empty? ks)
                    (empty? (dissoc ret :id)))
             (db/delete! tx drawer id)))]))

(defn remove-relation!
  [dresser ref1 rname1->2 ref2]
  (let [id1 (ref-vec ref1)
        id2 (ref-vec ref2)
        p1->2 (concat [:rels rname1->2] id2)]
    (db/tx-let [tx dresser]
        [rel-data (db/get-at tx rel-drawer id1 p1->2)
         rname2->1 (:_inv-rel rel-data)]
      (-> tx
          (clean-dissoc-at! rel-drawer id1 [:rels rname1->2 (first id2)] (last id2))
          (clean-dissoc-at! rel-drawer id2 [:rels rname2->1 (first id1)] (last id1))))))


(defn remove-all-relations!
  [dresser ref1]
  (let [id1 (ref-vec ref1)]
    (db/tx-let [tx dresser]
        [rels (db/get-at tx rel-drawer id1 [:rels])
         id->rel (for [[_rel-name entry] rels
                       [drawer-id doc-id->rel] entry
                       [doc-id {rel :_inv-rel}] doc-id->rel]
                   [[drawer-id doc-id] rel])]
      (-> (reduce (fn [tx [id2 rel]]
                    (clean-dissoc-at! tx rel-drawer id2 [:rels rel (first id1)] (last id1)))
                  tx
                  id->rel)
          (db/delete! rel-drawer id1)))))

(defn- wipe!
  [tx drawer where]
  (let [f (refs/expand-fn remove-all-relations!)]
    (db/fetch-reduce
     tx drawer
     #(f %1 drawer (:id %2))
     {:where where
      :only  {:id :?}})))

(ext/defext keep-sync
  "Automatically removes relations when deleting a document or
  dropping a drawer."
  []
  {:deps [refs/durable-refs]
   :wrap-configs
   {`dp/-delete-many {:wrap (fn [delete-method]
                              (fn [tx drawer where]
                                (-> (wipe! tx drawer where)
                                    (delete-method drawer where))))}
    `dp/-drop        {:wrap (fn [drop-method]
                              (fn [tx drawer]
                                (-> (wipe! tx drawer {})
                                    (drop-method drawer))))}}})


(comment
  (require '[dresser.impl.hashmap :as hm])
  (let [bob (refs/durable :Bob "Bob")
        alice (refs/durable :Alice "Alice")
        mike (refs/durable :Mike "Mike")]
    (db/raw-> (hm/build)
      (upsert-relation! bob alice :parent :children {:can-borrow-car? true} nil)
      (upsert-relation! bob mike :friend)
      (upsert-relation! alice mike :friend)
      (relations bob :children)
      (remove-all-relations! bob)
      (is? bob :friend mike)
      )))
