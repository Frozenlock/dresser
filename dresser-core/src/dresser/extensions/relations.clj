(ns dresser.extensions.relations
  (:require [clojure.string :as str]
            [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.durable-refs :as refs]
            [dresser.protocols :as dp]))

(def rel-drawer :drs_relations)

(defn- ref-vec
  [{:keys [drawer-id doc-id]}]
  [drawer-id doc-id])

(defn- string->bytes-int
  "Convert string to bytes then to big integer"
  [s]
  (let [bytes (.getBytes s "UTF-8")]
    (if (empty? bytes)
      0N
      (bigint (BigInteger. 1 bytes)))))

(defn- encode [x]
  (-> (pr-str x)
      (string->bytes-int)
      (db/lexical-encode)))

(defn- bytes-int->string
  "Convert big integer back to string via bytes"
  [n]
  (if (zero? n)
    ""
    (let [big-int (BigInteger. (str n))
          bytes (.toByteArray big-int)
          ;; Remove leading zero byte if present (from sign bit)
          bytes (if (and (> (count bytes) 1) (zero? (first bytes)))
                  (byte-array (rest bytes))
                  bytes)]
      (String. bytes "UTF-8"))))

(defn decode [encoded]
  (-> encoded
      (db/lexical-decode)
      (bytes-int->string)
      (read-string)))

(def separator "-")

(defn rel-id
  [ref1 rel-name ref2]
  (->> (map encode (concat (ref-vec ref1)
                           [rel-name]
                           (ref-vec ref2)))
       (str/join separator)))

(defn fetch-relations
  ([dresser ref1 rel-name]
   (fetch-relations dresser ref1 rel-name nil))
  ([dresser ref1 rel-name query]
   ;; Optimize query by using the 'index' ID if possible
   (let [;; Prefix is the encoded ref1 and the relation name.
         [drawer-id doc-id] (ref-vec ref1)
         prefix (str (encode drawer-id) separator
                     (encode doc-id) separator
                     (encode rel-name) separator)

         ;; suffix is potentially some target ID
         d-id-query (get-in query [:where :target-ref :drawer-id])
         d-ids (when d-id-query
                 (or (if (map? d-id-query)
                       (or (get d-id-query db/any)
                           (when-not (db/ops? (keys d-id-query))
                             [d-id-query]))
                       [d-id-query])))
         id-query {:id (if (coll? d-ids)
                         (let [q (vec (for [d-id d-ids
                                            :let [prefix (str prefix (encode d-id))]]
                                        {db/gte prefix
                                         ;; upper-bound
                                         db/lt  (str prefix db/lexical-max)}))]
                           (if (rest q)
                             {db/any q}
                             q))

                         {db/gte prefix
                          db/lt  (str prefix db/lexical-max)})}]
     (db/tx-let [tx dresser]
         [query (update query :where
                        (fn [w]
                          (-> (or (when (and d-id-query
                                             (not (next (keys (:target-ref w)))))
                                    (dissoc w :target-ref))
                                  w)
                              (merge id-query))))
          rels (db/fetch tx rel-drawer query)]
       rels))))

(defn relation
  "Returns the user data part of a relation, if it exists."
  ([dresser ref1 rel-name ref2]
   (relation dresser ref1 rel-name ref2 nil))
  ([dresser ref1 rel-name ref2 only]
   (let [id (rel-id ref1 rel-name ref2)]
     (-> (db/fetch-by-id dresser rel-drawer id {:only only})
         (db/update-result #(:data % {}))))))

(defn is?
  "True if the relation exists.
  Ex: r1 is a parent of r2
      (is? r1 :parent r2) => true"
  [dresser ref1 rel-name ref2]
  (let [id (rel-id ref1 rel-name ref2)]
    (-> (db/fetch-by-id dresser rel-drawer id {:only {:inv-rel :?}})
        (db/update-result some?))))

(defn upsert-relation!
  "To avoid clashes, consider using a namespaced or vector `rel-name`.
  Ex: [:drs :memberships]"
  ([dresser ref1 ref2 rel-name]
   (upsert-relation! dresser ref1 ref2 rel-name rel-name))
  ([dresser ref1 ref2 rname1->2 rname2->1]
   (upsert-relation! dresser ref1 ref2 rname1->2 rname2->1 nil nil))
  ([dresser ref1 ref2 rname1->2 rname2->1 data1->2 data2->1]
   (let [id1->2 (rel-id ref1 rname1->2 ref2)
         id2->1 (rel-id ref2 rname2->1 ref1)
         d1->2 (cond-> {:inv-rel rname2->1
                        :target-ref ref2}
                       data1->2 (assoc :data data1->2))
         d2->1 (cond-> {:inv-rel rname1->2
                        :target-ref ref1}
                       data2->1 (assoc :data data2->1))]
     ;; Use assoc-at instead of upsert because it's simpler with RBAC checks.
     (db/tx-> dresser
       (db/assoc-at! rel-drawer id1->2 [] d1->2)
       (db/assoc-at! rel-drawer id2->1 [] d2->1)))))

(defn update-in-relation!
  "Update relation data using functions, like update-in for maps.
  Ensures bidirectional relations exist. Pass nil for f1->2 or f2->1 if no update needed.
  Optimized: checks if relation exists before creating it."
  [dresser ref1 rname1->2 ref2 rname2->1 f1->2 f2->1]
  (let [id1->2 (rel-id ref1 rname1->2 ref2)
        id2->1 (rel-id ref2 rname2->1 ref1)
        make-update-fn (fn [f]
                         (when f
                           (fn [current]
                             (let [current-data (get current :data)
                                   updated-data (f current-data)]
                               (if updated-data
                                 (assoc current :data updated-data)
                                 (dissoc current :data))))))]
    (db/tx-let [tx dresser]
        [relation-exists? (db/fetch-by-id tx rel-drawer id1->2 {:only {:inv-rel :?}})]
      (if relation-exists?
        ;; Fast path: relation exists, just apply updates
        (db/tx-> tx
          (#(if f1->2
              (db/update-at! % rel-drawer id1->2 [] (make-update-fn f1->2))
              %))
          (#(if f2->1
              (db/update-at! % rel-drawer id2->1 [] (make-update-fn f2->1))
              %)))
        ;; Slow path: relation doesn't exist, create it with initial data
        (let [initial-data-1->2 (when f1->2 (f1->2 nil))
              initial-data-2->1 (when f2->1 (f2->1 nil))]
          (upsert-relation! tx ref1 ref2 rname1->2 rname2->1 initial-data-1->2 initial-data-2->1))))))


(defn remove-relation!
  [dresser ref1 rname1->2 ref2]
  (let [id1->2 (rel-id ref1 rname1->2 ref2)]
    (db/tx-let [tx dresser]
        [rname2->1 (db/get-at tx rel-drawer id1->2 [:inv-rel])
         id2->1 (rel-id ref2 rname2->1 ref1)]
      (db/tx-> tx
        (db/delete! rel-drawer id1->2)
        (db/delete! rel-drawer id2->1)))))


(defn remove-all-relations!
  [dresser ref1]
  ;; Find all relations where ref1 is involved by searching for IDs that start with ref1's encoding
  (let [[drawer-id doc-id] (ref-vec ref1)
        prefix (str (encode drawer-id) separator (encode doc-id) separator)
        upper-bound (str prefix db/lexical-max)]
    (db/tx-let [tx dresser]
        [rels (db/fetch tx rel-drawer {:where {:id {db/gte prefix
                                                    db/lt  upper-bound}}
                                       :only  [:inv-rel :id]})]
      ;; For each relation, also delete its inverse
      (reduce (fn [tx rel]
                (let [inverse-rel-name (:inv-rel rel)
                      ;; Parse the ID to extract ref2 and rel-name
                      id-parts (str/split (:id rel) (re-pattern separator))
                      ref2-drawer-id (decode (nth id-parts 3))
                      ref2-doc-id (decode (nth id-parts 4))
                      ref2 (refs/durable ref2-drawer-id ref2-doc-id)
                      inverse-id (rel-id ref2 inverse-rel-name ref1)]
                  (db/tx-> tx
                    (db/delete! rel-drawer (:id rel))
                    (db/delete! rel-drawer inverse-id))))
              tx
              rels))))

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
                                (cond-> tx
                                  (not= drawer rel-drawer) (wipe! drawer where)
                                  true (delete-method drawer where))))}
    `dp/-drop        {:wrap (fn [drop-method]
                              (fn [tx drawer]
                                (cond-> tx
                                  (not= drawer rel-drawer) (wipe! drawer {})
                                  true (drop-method drawer))))}}})


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
