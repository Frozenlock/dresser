(ns dresser.extensions.relations
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.durable-refs :as refs]
            [dresser.protocols :as dp]
            [dresser.extensions.drawer-registry :as d-reg]))

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
             [{:drawer-id drawer-id
               :doc-id    doc-id} rel])))))

(defn relation
  "Returns the relation data, if it exists."
  ([dresser ref1 rel-name ref2]
   (relation dresser ref1 rel-name ref2 nil))
  ([dresser ref1 rel-name ref2 only]
   (let [path (into [:rels rel-name] (ref-vec ref1))]
     (db/get-at dresser rel-drawer (ref-vec ref2) path only))))

(defn is?
  "True if the relation exists.
  Ex: (is? r1 :parent r2)"
  [dresser ref1 rel-name ref2]
  (-> (relation dresser ref1 rel-name ref2 {:_rel :?})
      (db/update-result some?)))

(defn upsert-relation!
  "To avoid clashes, consider using a namespaced or vector `rel-name`.
  Ex: [:drs :memberships]

  Optional additional data must be a map. (:_rel key is reserved)"
  ([dresser ref1 ref2 rel-name]
   (upsert-relation! dresser ref1 ref2 rel-name rel-name))
  ([dresser ref1 ref2 rname1->2 rname2->1]
   (upsert-relation! dresser ref1 ref2 rname1->2 rname2->1 nil nil))
  ([dresser ref1 ref2 rname1->2 rname2->1 data1->2 data2->1]
   (let [p1->2 (into [:rels rname2->1] (ref-vec ref2))
         p2->1 (into [:rels rname1->2] (ref-vec ref1))
         d1->2 (assoc data1->2 :_rel rname1->2)
         d2->1 (assoc data2->1 :_rel rname2->1)]
     (db/tx-> dresser
       (db/assoc-at! rel-drawer (ref-vec ref1) p1->2 d1->2)
       (db/assoc-at! rel-drawer (ref-vec ref2) p2->1 d2->1)))))

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
        p1->2 (concat [:rels rname1->2] id2 [:_rel])]
    (db/tx-let [tx dresser]
        [rname2->1 (db/get-at tx rel-drawer id1 p1->2)]
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
                       [doc-id {rel :_rel}] doc-id->rel]
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
  (let [r1 {:drawer-id 1, :doc-id 1}
        r2 {:drawer-id 2, :doc-id 2}
        r3 {:drawer-id 3, :doc-id 3}]
    (db/raw-> (hm/build)
      (upsert-relation! r1 r2 :parent :children)
      (upsert-relation! r1 r3 :friend)
      (upsert-relation! r2 r3 :friend)
      (relations r1 :children)
      ;(remove-all-relations! r1)
      ;(is? r1 :friend r2)
      )))
