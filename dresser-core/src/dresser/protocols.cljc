(ns dresser.protocols
  "Core Dresser protocols with extension support.

  All dressers use a unified base.Dresser record with metadata-based method
  dispatch. This enables consistent extension/wrapping behavior across all
  implementations.

  ## Protocols

  - DresserFundamental: Core methods that implementations must provide
  - DresserOptional: Convenience methods with default implementations

  ## Implementation Guidelines

  Provide method implementations via metadata for extension compatibility:
  ```clojure
  (with-meta {:db data}
    {`dp/fetch my-fetch-fn
     `dp/transact my-transact-fn})
  ```"
  (:refer-clojure :exclude [drop]))

;;; Extendable / wrappable protocols

(defprotocol DresserFundamental
  :extend-via-metadata true
  ;; --- Fundamental methods ---
  (-fetch [tx drawer only limit where sort skip])
  (-all-drawers [tx])
  (-delete-many [tx drawer where])
  (-assoc-at [tx drawer id ks data])
  (-drop [tx drawer])
  (-transact [dresser f {:keys [result?] :as opts}])
  (-immutable? [dresser])
  (-tx? [dresser]))

;; DresserOptional: Optional methods that can be implemented directly for
;; improved performance, or they fall back to default implementations using
;; the `DresserFundamental` methods. See `dresser.impl.optional`.
;;
;; IMPORTANT: DresserOptional cannot be partially implemented on
;; defrecords. Either implement ALL methods or implement NONE and use
;; metadata to override specific methods. See hashmap.clj for examples
;; of the metadata approach:
;; (vary-meta dresser assoc `dp/-method-name my-impl-fn)

(defprotocol DresserOptional
  :extend-via-metadata true
  (-fetch-by-id [tx drawer id only where])
  (-fetch-count [tx drawer where])
  (-update-at [tx drawer id ks f args])
  (-add [tx drawer data])
  (-all-ids [tx drawer])
  (-dissoc-at [tx drawer id ks dissoc-ks])
  (-gen-id [tx drawer])
  (-get-at [tx drawer id ks only])
  (-upsert-many [tx drawer docs])
  (-dresser-id [tx])
  (-drawer-key [tx drawer-id])
  (-rename-drawer [tx drawer new-drawer])
  (-has-drawer? [tx drawer])
  (-temp-data [dresser])
  (-with-temp-data [dresser data]))

(defprotocol DresserLifecycle
  :extend-via-metadata true
  (-start [dresser])
  (-stop [dresser]))

;; Default no-op implementation for DresserLifecycle
(extend-protocol DresserLifecycle
  #?(:clj Object :cljs default)
  (-start [this] this)
  (-stop [this] this))

(defmacro defmethod-fn
  "Defines a metadata-dispatched function for a protocol method.

  Usage:
    (defmethod-fn add -add [tx drawer data])

  Creates a function that checks metadata for a wrapped version,
  falling back to the specified protocol method resolved at call time."
  [name protocol-method args & body]
  (let [ext-sym (symbol (str (ns-name *ns*)) (str name))]
    `(defn ~name {::p-method (symbol (var ~protocol-method))} ~args
       (let [f# (or (get (meta ~(first args)) '~ext-sym)
                    ~protocol-method)]
         (f# ~@args)))))

(defmacro named-fn
  [sym-name args & body]
  `(fn ~sym-name ~args ~@body))

(defn fundamental
  [sym]
  (if-let [protocol-method-sym (::p-method (meta (resolve sym)))]
    (let [method (resolve protocol-method-sym)]
      (fn [& args]
        (apply method args)))
    (throw (ex-info "Symbol isn't a method wrapper" {:symbol sym}))))

(defn wrap-method
  "Wraps an extension method on a dresser, handling metadata-first dispatch.
  Creates a wrapper that resolves the protocol method at call time.

  Usage:
    (wrap-method dresser `ext-add wrapper-fn arg1 arg2 ...)

  The extension function's ::p-method metadata contains the protocol method symbol."
  [dresser ext-sym wrapper-fn & wrapper-args]
  (vary-meta dresser
             (fn [m]
               (update m ext-sym #(apply wrapper-fn
                                         (or % (fundamental ext-sym))
                                         wrapper-args)))))

(defn get-or-fundamental
  "Gets the metadata fn stored at 'sym', or fallbacks on the fundamental
method."
  [m sym]
  (or (get m sym)
      (fundamental sym)))

;; Extension functions for all wrappable methods

;; DresserFundamental extensions
(defmethod-fn fetch -fetch [tx drawer only limit where sort skip])
(defmethod-fn all-drawers -all-drawers [tx])
(defmethod-fn delete-many -delete-many [tx drawer where])
(defmethod-fn assoc-at -assoc-at [tx drawer id ks data])
(defmethod-fn drop -drop [tx drawer])
(defmethod-fn transact -transact [dresser f opts])
(defmethod-fn immutable? -immutable? [dresser])
(defmethod-fn tx? -tx? [dresser])

;; DresserOptional extensions
(defmethod-fn fetch-by-id -fetch-by-id [tx drawer id only where])
(defmethod-fn fetch-count -fetch-count [tx drawer where])
(defmethod-fn update-at -update-at [tx drawer id ks f args])
(defmethod-fn add -add [tx drawer data])
(defmethod-fn all-ids -all-ids [tx drawer])
(defmethod-fn dissoc-at -dissoc-at [tx drawer id ks dissoc-ks])
(defmethod-fn gen-id -gen-id [tx drawer])
(defmethod-fn get-at -get-at [tx drawer id ks only])
(defmethod-fn upsert-many -upsert-many [tx drawer docs])
(defmethod-fn dresser-id -dresser-id [tx])
(defmethod-fn drawer-key -drawer-key [tx drawer-id])
(defmethod-fn rename-drawer -rename-drawer [tx drawer new-drawer])
(defmethod-fn has-drawer? -has-drawer? [tx drawer])
(defmethod-fn temp-data -temp-data [dresser])
(defmethod-fn with-temp-data -with-temp-data [dresser data])

;; DresserLifecycle extensions
(defmethod-fn start -start [dresser])
(defmethod-fn stop -stop [dresser])
