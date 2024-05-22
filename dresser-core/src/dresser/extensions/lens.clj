(ns dresser.extensions.lens
  "Extension for creating 'lenses' that provide a flexible
  and efficient way to reference, manipulate, and navigate nested data
  structures within a dresser. Lenses combine the capabilities of
  database references and objects, supporting nested paths and dynamic
  updates.

  Advantages:
  - Simplifies accessing and updating deeply nested data.
  - Integrates seamlessly with the existing dresser library.
  - Supports transactional operations for consistent data manipulation.
  - Allows for intuitive path-based data navigation and modification.
  - Can be invoked as functions to fetch or modify data directly.

  Example:

  (let [dresser (lens/lenses (hm/build))]
    (db/tx-> dresser
      ;; Add a document and set the lens reference
      (lens/add! :users {:name \"Alice\", :address {:street \"Main St\"}})
      ;; Focus on the address field
      (lens/focus [:address])
      ;; Update the street field
      (lens/assoc-at! [:street] \"Elm St\")
      ;; Retrieve the updated address
      (lens/get-at)))"
  (:refer-clojure :exclude [ref])
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.protocols :as dp]
            [dresser.extensions.lens :as lens]
            [dresser.impl.hashmap :as hm]))

(defprotocol ILens
  :extend-via-metadata true
  (-ref [dresser] "Returns the current reference")
  (-set-ref [dresser ref] "Sets the current reference"))

(defn ref
  [dresser]
  (-ref dresser))

(defn set-ref!
  [dresser ref]
  (-set-ref dresser ref))

;;;;

(defn- get-drawer
  [lens]
  (let [ref (ref lens)]
    (if-some [drawer (:drawer ref)]
      drawer
      (throw (ex-info "Undefined drawer ref" {:ref ref})))))

(defn add!
  "Similar to `db/add!`, but also sets the lens to the newly added document."
  ([lens doc]
   (add! lens (get-drawer lens) doc))
  ([dresser drawer doc]
   (db/tx-let [tx dresser]
       [doc-id (db/add! tx drawer doc)]
     (set-ref! tx {:drawer drawer
                   :doc-id doc-id}))))

(defn get-at
  ([lens] (get-at lens []))
  ([lens ks]
   (let [{:keys [drawer doc-id path]} (ref lens)]
     (db/get-at lens drawer doc-id (into (or path []) ks)))))

(defn update-at!
  [lens ks f & args]
  (let [{:keys [drawer doc-id path]} (ref lens)]
    (apply db/update-at! lens drawer doc-id (into (or path []) ks) f args)))

(defn assoc-at!
  ([lens data] (assoc-at! lens [] data))
  ([lens ks data]
   (let [{:keys [drawer doc-id path]} (ref lens)]
     (db/assoc-at! lens drawer doc-id (into (or path []) ks) data))))

(defn dissoc-at!
  ([lens]
   (let [{:keys [drawer doc-id path]} (ref lens)
         _ (when-not (seq path)
             (throw (ex-info "Can't dissoc a lens if its path is empty")))
         [path dissoc-k] [(butlast path) (last path)]]
     (db/dissoc-at! lens drawer doc-id path dissoc-k)))
  ([lens ks & dissoc-ks]
   (let [{:keys [drawer doc-id path]} (ref lens)]
     (apply db/dissoc-at! lens drawer doc-id (into (or path []) ks) dissoc-ks))))


(declare lenses)

(defn focus
  "Appends the path to the current reference path, or sets the same
reference as `ref-lens`."
  {:arglists '[[lens path]
               [dresser-or-lens ref-lens]]}
  [dresser-or-lens path-or-lens]
  (let [;; Make sure dresser supports ILens
        lens (lenses dresser-or-lens)
        ?ref-from-lens (if (db/dresser? path-or-lens)
                         (ref path-or-lens))
        ?path (when-not ?ref-from-lens path-or-lens)
        ?ref (ref lens)]
    (cond
      ?ref-from-lens
      (lens/set-ref! lens ?ref-from-lens)

      (and ?ref ?path)
      (->> (update ?ref :path (fn [p]
                                (into (or p []) ?path)))
           (set-ref! lens))

      (and (not ?ref) ?path)
      (throw
       (ex-info
        (str "Can't apply path to dresser without reference. "
             "Make sure to use `set-ref!`, or provide a reference instead of a path.")
        {}))

      :else (throw (ex-info "Unexpected focus arguments"
                            {:dresser-or-lens dresser-or-lens
                             :path-or-lens    path-or-lens})))))

(defn lens?
  [dresser]
  (db/dresser? dresser))

(defn invoke
  ([dresser]
   (get-at dresser))
  ([dresser ks]
   (get-at dresser ks))
  ([dresser ks only]
   (let [{:keys [drawer doc-id path]} (ref dresser)
         only (if (seq path)
                (assoc-in {} path only)
                only)]
     (db/tx-> dresser
       (db/fetch-by-id drawer doc-id {:only only})
       (db/update-result get-in path)))))


(ext/defext lenses
  "A dresser variation which can contain a reference to a document or
subdocument. When called as a function, it returns the data
located at the reference, as if called with `get-at`."
  []
  {:deps    []
   :init-fn #(vary-meta % merge {`dp/-invoke invoke
                                 `-ref       (fn [dresser]
                                               (db/temp-data dresser [::ref]))
                                 `-set-ref   (fn [dresser ref]
                                               (db/assoc-temp-data dresser ::ref ref))})})

(comment

  (require '[dresser.impl.hashmap :as hm])

  (db/raw-> (hm/build)
    (lenses)
    (add! :users {:name "Bob", :address {:street 100}})
    (focus [:address]))

  (let [l-address (db/raw-> (hm/build)
                    (lenses)
                    (add! :users {:name "Bob", :address {:street 100}})
                    (focus [:address]))
        l-address2 (db/raw-> l-address
                     (assoc-at! [:street2] "second street"))]
    (l-address2)))
