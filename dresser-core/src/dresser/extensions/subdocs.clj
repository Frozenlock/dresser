(ns dresser.extensions.subdocs
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.durable-refs :as refs]
            [dresser.protocols :as dp]))

(def ^:private field :drs_children)

(defn- parent
  [dresser child-ref]
  (refs/get-at dresser child-ref [field :parent]))

(defn children
  [dresser parent-ref]
  (refs/get-at dresser parent-ref [field :children]))

(defn all-ancestors
  [dresser child-ref]
  (db/tx-let [tx dresser]
      [parent-ref (parent tx child-ref)]
    (if parent-ref
      (-> (all-ancestors tx parent-ref)
          (db/update-result conj parent-ref))
      (db/with-result tx nil))))

(defn add-child!
  "Set child as a children of parent and returns parent.
  Documents can only have one parent at a time."
  [dresser parent-ref child-ref]
  (db/tx-let [tx dresser]
      [ancestors (all-ancestors tx parent-ref)]
    (when (some #{child-ref} ancestors)
      (throw (ex-info "Circular children loop detected"
                      {:child            child-ref
                       :parent           parent-ref
                       :parent-ancestors ancestors})))
    (-> tx
        (refs/update-at! parent-ref [field :children] #(distinct (conj % child-ref)))
        (refs/assoc-at! child-ref [field :parent] parent-ref))))

(defn remove-child!
  "Removes the parent/child relation. Returns nil."
  [dresser parent-ref child-ref]
  (db/tx-let [tx dresser]
      [other-children (refs/update-at! tx parent-ref [field :children]
                                       #(remove #{child-ref} %))
       _ (when (empty? other-children)
           (refs/dissoc-at! tx parent-ref [field] :children))]
    (refs/dissoc-at! tx child-ref [field] :parent)))

(defn remove-parent!
  "Removes the parent, if any. Returns nil."
  [dresser child-ref]
  (db/tx-let [tx dresser]
      [parent-ref (parent tx child-ref)]
    (if parent-ref
      (remove-child! tx parent-ref child-ref)
      (db/with-result tx nil))))

(defn- delete-children!
  "Recursively delete children"
  [dresser parent-ref]
  (db/tx-let [tx dresser]
      [children-refs (children tx parent-ref)
       ;; Delete children of children recursively
       _ (reduce #(delete-children! %1 %2) tx children-refs)]
    (reduce #(refs/delete! %1 %2) tx children-refs)))

(ext/defext keep-sync
  "Children documents are automatically deleted"
  []
  {:deps [refs/durable-refs]
   :wrap-configs
   (let [clean! (fn [tx drawer id]
                  (db/tx-let [tx tx]
                             [doc-ref (refs/ref tx drawer id)
                              _ (when doc-ref (delete-children! tx doc-ref))
                              _ (when doc-ref (remove-parent! tx doc-ref))]
                             tx))]
     {`dp/-delete {:wrap (fn [method]
                           (fn [tx drawer id]
                             (-> tx
                                 (clean! drawer id)
                                 (method drawer id))))}
      `dp/-drop   {:wrap (fn [method]
                           (fn [tx drawer]
                             (-> (let [[tx ids] (db/dr (db/all-ids tx drawer))]
                                   (reduce (fn [tx' id] (clean! tx' drawer id))
                                           tx ids))
                                 (method drawer))))}})})
