(ns dresser.protocols
  #?@
   (:cljs [(:require-macros [dresser.base :refer [defpro impl]])]))

#?(:clj
   (defmacro defpro
     "Same as defprotocol, also defines a map of methods (qualified
  symbols) and configs under the name <protocol-name>-methods."
     {:arglists '[name & opts+sigs]}
     [protocol-name & body]
     (let [method->cfgs (into {} (for [x body
                                       :when (list? x)
                                       :let [args (second x)]]
                                   [(first x) (merge (select-keys (meta args) [:tx :w])
                                                     {:args args})]))]
       `(do (defprotocol ~protocol-name ~@body)
            (def ~(symbol (str protocol-name "-methods"))
              (into {} [~@(for [[m args] method->cfgs]
                            `[(symbol (resolve '~m)) '~args])]))))))

(defpro DresserFundamental
  :extend-via-metadata true
  ;; --- Fundamental methods ---
  (-fetch ^:tx [dresser drawer only limit where sort skip]
          "Fetches documents from drawer.")
  (-all-drawers ^:tx [dresser] "Returns a sequence of all drawers keys")
  (-delete ^:tx ^:w [dresser drawer id] "Deletes the document if it exists. Returns id.")
  (-upsert ^:tx ^:w [dresser drawer data] "Upserts a document with the provided ID and returns it.")
  (-transact [dresser f result?]
             "Evaluates the provided function inside a transaction.
  If 'result?' is true (default), automatically returns the result
  when exiting the transaction. The function should accept dresser as
  an argument.")
  (-temp-data [dresser] "Returns the dresser temporary data (a map).")
  (-with-temp-data [dresser data] "Sets the dresser temporary data (a map).

  The data should be immutable. Assume that any process can call this
  method at any time."))

(defpro DresserOptional
  "Those method can be implemented directly for an improved performance,
or they can be constructed using the `DresserFundamental` methods. See
`optional-impl`."
  :extend-via-metadata true
  (-fetch-by-id ^:tx [dresser drawer id only where]
                "Fetches documents from drawer. See also `fetch`.")
  (-fetch-count ^:tx [dresser drawer where])
  (-update-at ^:tx ^:w [dresser drawer id ks f args]
              "Similar to `clojure.core/update-in`. Returns the value that was updated-in.")
  (-add ^:tx ^:w [dresser drawer data] "Adds a document (map) and returns its ID.")
  (-all-ids ^:tx [dresser drawer] "Returns a sequence of all document IDs from this drawer.")
  (-assoc-at ^:tx ^:w [dresser drawer id ks data] "Similar to `clojure.core/assoc-in`, but for a drawer. Returns data.")
  (-dissoc-at ^:tx ^:w [dresser drawer id ks dissoc-ks]
              "Dissoc the dissoc-keys from the value at located at ks. Returns nil.")
  (-drop ^:tx ^:w [dresser drawer] "Removes the drawer.")
  (-gen-id ^:tx ^:w [dresser drawer] "Generates an ID for a document in the given drawer.")
  (-get-at ^:tx [dresser drawer id ks] "Similar to `clojure.core/get-in`, but for a drawer.")
  (-replace ^:tx ^:w [dresser drawer id data] "Replaces the document data. Returns the new document.")
  (-upsert-all ^:tx ^:w [dresser drawer docs] "Insert all the documents. Returns documents")
  (-dresser-id ^:tx [dresser] "Returns the dresser ID")
  (-rename-drawer ^:tx ^:w [dresser drawer new-drawer] "Returns new-drawer.")
  (-has-drawer? ^:tx [dresser drawer] "Returns true if the dresser has the drawer."))


(defpro DresserLifecycle
  :extend-via-metadata true
  (-start [dresser] "Returns dresser")
  (-stop [dresser] "Returns dresser"))

;; Default implementation is no-op.
(extend-protocol DresserLifecycle
  #?(:clj java.lang.Object :cljs default)
  (-start [this]
    this)
  (-stop [this]
    this))

(def dresser-methods
  (merge DresserFundamental-methods
         DresserOptional-methods
         DresserLifecycle-methods))

(def dresser-symbols
  "All the dresser methods as symbols."
  (keys dresser-methods))

;; Those macros are there to attach the ns metadata for easier
;; debugging and provide a few sanity checks when defining a method
;; implementation.

#?(:clj
   (do
     (defn vali-method!
       [qualified-symbol args]
       (let [expected-args (get-in dresser-methods [qualified-symbol :args])]
         (when-not expected-args
           (throw (ex-info "Method not found" {:method    qualified-symbol
                                               :potential (sort (keys dresser-methods))})))
         (when-not (= (count args) (count expected-args))
           (throw (ex-info "Arguments mismatch" {:method        qualified-symbol
                                                 :provided-args args
                                                 :expected-args expected-args})))))

     (defmacro with-source
       "Attaches the :line, :column and :file metadata."
       [obj &form]
       (let [f *file*
             form-meta (meta &form)]
         `(vary-meta ~obj merge ~form-meta {:file ~f :ns (ns-name *ns*)})))

     (let [current-ns *ns*]
       (defmacro impl
         "Creates an inline IDresser implementation."
         {:style/indent 1}
         [sym args & body]
         (let [qualified-symbol (symbol (ns-resolve current-ns sym))
               _ (vali-method! qualified-symbol args)
               f `(fn ~(symbol (name sym))
                    ~args
                    ~@body)]
           `(-> (with-source ~f ~&form) ; &form won't work if called from another macro...
                (vary-meta merge (meta '~sym)) ; ...so also use the symbol's metadata
                (vary-meta merge {:method-symbol '~qualified-symbol}))))

       (defmacro defimpl
         "Defines an idresser method implementation."
         {:style/indent 1}
         [sym args & body]
         (let [qualified-symbol (symbol (ns-resolve current-ns sym))
               _ (vali-method! qualified-symbol args)
               arglists (list (get-in dresser-methods [qualified-symbol :args]))]
           `(let [f# (with-source (impl ~sym ~args ~@body) ~&form)
                  fvar# (def ~sym f#)]
              (alter-meta! fvar# merge (meta f#) {:arglists      '~arglists
                                                  :omit-coverage true})
              fvar#))))))

(defn mapify-impls
  "Turns implementations into a usable (metadata) protocol methods map.

  Works with `impl` and `defimpl`."
  [impls]
  (->> (for [impl impls
             :let [sym (:method-symbol (meta impl))
                   _ (assert sym)]]
         [sym impl])
       (into {})))
