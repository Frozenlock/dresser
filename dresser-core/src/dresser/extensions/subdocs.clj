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


(defn- clean!
  [dresser drawer where]
  (db/fetch-reduce
   dresser drawer
   (fn [tx doc]
     (db/tx-let [tx tx]
         [id (:id doc)
          this-ref (refs/ref tx drawer id)
          {:keys [children parent]} (get doc field)
          _ (if (seq children) (reduce #(refs/delete! %1 %2) tx children))]
       (when (some? parent)
         (remove-parent! tx this-ref))))
   {:where (merge where {field {db/exists? true}})
    :only  {:id   :?
            field :?}}))

(ext/defext keep-sync
  "Children documents are automatically deleted"
  []
  {:deps [refs/durable-refs]
   :wrap-configs
   {`dp/-delete-many {:wrap (fn [method]
                              (fn [tx drawer where]
                                (-> tx
                                    (clean! drawer where)
                                    (method drawer where))))}
    `dp/-drop        {:wrap (fn [method]
                              (fn [tx drawer]
                                (-> tx
                                    (clean! drawer {}) ;; Match all documents
                                    (method drawer))))}}})
