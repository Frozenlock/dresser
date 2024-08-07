(ns dresser.base
  "Universal storage abstraction layer"
  (:require [clojure.string :as str]
            [dresser.protocols :as dp]))

;; TODO: When ID is nil, should methods be no-op and return nil?


(defn dresser?
  "Returns true if x is of type ::dresser (checks via metadata)."
  [x]
  ;; Currently (clojure 1.11.1) `satisfies?` doesn't work with methods
  ;; provided via metadata. Fallback on types instead.
  (isa? (type x) ::dresser))

(derive ::immutable-dresser ::dresser)

(defn immutable?
  [x]
  (= (type x) ::immutable-dresser))

(defn temp-data
  {:doc (:doc (meta #'dp/-temp-data))}
  ([dresser] (dp/-temp-data dresser))
  ([dresser ks]
   (get-in (dp/-temp-data dresser) ks)))

(defn with-temp-data
  {:doc (:doc (meta #'dp/-with-temp-data))}
  [dresser data]
  (dp/-with-temp-data dresser data))

(defn update-temp-data
  [dresser f & args]
  (->> (apply f (temp-data dresser) args)
       (with-temp-data dresser)))

(defn assoc-temp-data
  ([dresser key val]
   (update-temp-data dresser assoc key val))
  ([dresser key val & kvs]
   (let [ret (assoc-temp-data dresser key val)]
     (if kvs
       (if (next kvs)
         (recur ret (first kvs) (second kvs) (nnext kvs))
         (throw (ex-info "uneven number of arguments for keys/vals")))
       ret))))


(defn start
  {:doc (:doc (meta #'dp/-start))}
  [dresser]
  (dp/-start dresser))

(defn stop
  {:doc (:doc (meta #'dp/-stop))}
  [dresser]
  (dp/-stop dresser))


(defn result
  "Returns the result currently stored in the transaction."
  [dresser]
  (:result (temp-data dresser)))

(defn with-result
  "Inserts a result into the transaction."
  [dresser result]
  (update-temp-data dresser assoc :result result))

(defn temp-dresser-id
  "Temporary (in-memory) dresser ID. Doesn't require a transaction."
  [dresser]
  (temp-data dresser [:temp-id]))

(defn with-temp-dresser-id
  ([dresser] (with-temp-dresser-id dresser (str (gensym "dresser-"))))
  ([dresser id]
   (update-temp-data dresser assoc :temp-id id)))

(defn transact!
  {:doc (:doc (meta #'dp/-transact))}
  ([dresser f] (dp/-transact dresser f {:result? true}))
  ([dresser f {:keys [result?] :as opts}]
   (assert (dresser? dresser) "Transact first argument should be a dresser")
   (dp/-transact dresser f opts)

   ;; (if (started? dresser)
   ;;   (-transact dresser f result?)
   ;;   ;; Too complex? Should we just assume that the dresser is
   ;;   ;; always started?
   ;;   (let [started (start dresser)
   ;;         return (-> (try (-transact started f false)
   ;;                         (catch Exception e
   ;;                           (stop started)
   ;;                           (throw e)))
   ;;                    (stop))]
   ;;     (if result? (result return) return)))
   ))

(def drs-drawer
  "Drawer to store dresser configuration."
  :drs_config)

(def drs-doc-id
  "drs-config")


(defn system-drawers
  "Drawers that are important for 'basic' system operations.
  Most of the time they should be ignored by extensions."
  [dresser]
  (-> (or (:drs-system-drawers (temp-data dresser))
          #{})
      (conj drs-drawer)))

(defn with-system-drawers
  [dresser drawers]
  (update-temp-data dresser update :drs-system-drawers #(into (or % #{}) drawers)))


(defmacro with-tx
  "Opens a transaction and binds it to tx-name before evaluating the body.
  The value returned by body MUST be a dresser.

  Defaults to extracting the result when leaving the transaction.
  To return the updated dresser instead, supply `:result?` as false."
  {:arglists '([[tx-name dresser & [{:keys [result?] :as opts}]] & body])}
  [[tx-name dresser & [{:keys [result?] :as opts}]] & body]
  `(transact! ~dresser
              (fn [tx#]
                (let [~tx-name tx#
                      ret# (do ~@body)]
                  (if (dresser? ret#)
                    ret#
                    (throw (ex-info "Value returned by body must be a dresser"
                                    {:body     (quote ~body)
                                     :returned ret#})))))
              ~(if (some? result?)
                 opts
                 (assoc opts :result? true))))

(defmacro tx-let
  "Similar to `let`, but the first binding is wrapped inside a transaction.
  The value returned by body MUST be a dresser.

  If the value on the right side is a dresser, binds left symbol to its result.
  [result (func tx ...))
    becomes
  [tx result] (dr (func tx ...))

  If the value returned by body is not a dresser, insert the value as
  a result of the dresser.

  Defaults to extracting the result when leaving the transaction.
  To return the updated dresser instead, supply `:result?` as false."
  {:style/indent 2
   :arglists     '([[tx-name dresser & [{:keys [result?]}]] bindings & body])}
  [[tx-name dresser & [opts]] bindings & body]
  ;; This will only handle 1 dresser/tx at a time. Should add support
  ;; for many? Or perhaps throw an exception if many are detected?
  `(with-tx [~tx-name ~dresser ~opts]
     ;; When applicable, convert the bindings in the following:
     ;;
     ;; [[tx result] (dr (func tx...))
     (let [~@(mapcat (fn [[l# r#]]
                       ; Avoid accidental tx binding to the extracted result
                       (when (= l# tx-name)
                         (throw (ex-info "Can't rebind transaction to a result"
                                         {:tx-symbol tx-name
                                          :expr      r#})))
                       [[tx-name l#]
                        `(let [ret# ~r#]
                           (if (dresser? ret#)
                             (dr ret#)
                             [~tx-name ret#]))]) (partition 2 bindings))
           ret# (do ~@body)]
       (if (dresser? ret#)
         ret#
         (with-result ~tx-name ret#)))))


(defmacro tx->
  {:style/indent 1}
  [dresser & body]
  `(transact! ~dresser
              (fn [tx#]
                (let [ret# (-> tx#
                               ~@body)]
                  (if (dresser? ret#)
                    ret#
                    (throw (ex-info "Value returned by body must be a dresser"
                                    {:body     (quote ~body)
                                     :returned ret#})))))))

(defmacro raw->
  "Same as `tx->`, but doesn't return the result when exiting the transaction.
  Similar to (transact! dresser (fn [tx] body) {:result? false})"
  {:style/indent 1}
  [dresser & body]
  `(transact! ~dresser (fn [tx#] (tx-> tx# ~@body)) {:result? false}))


(defmacro txr->
  "Threads the result of each expression through the rest of the forms.
   The transaction (tx) is passed as the first argument to each form.
   Returns the final transaction with the last result."
  {:style/indent 1}
  [dresser & forms]
  (let [tx (gensym "tx-")
        result (gensym "result-")
        bindings (into [result `(~(ffirst forms) ~tx ~@(next (first forms)))]
                       (mapcat #(list result `(~(first %) ~tx ~result ~@(next %)))
                               (rest forms)))]
    `(tx-let [~tx ~dresser]
         ~bindings
       ~result)))



;; Wrap all the methods into functions. We need to intercept a few
;; methods anyway for varargs and some syntactic sugar.

(def ^:private tx-note
  (str "\n\n"
       "  Note: If inside a transaction, the return value is stored in the result\n"
       "        field and returns the updated dresser object."))

(defmacro wrap-dresser-tx-methods
  []
  (cons 'do
        (for [[method-symbol {:keys [tx w]}] dp/dresser-methods
              :when tx]
          (let [fn-name (symbol (-> (name (symbol method-symbol))
                                    (str/replace #"^-" "")
                                    (str (when w "!"))))
                method-var (resolve method-symbol)
                method-meta (meta method-var)]
            `(defn ~fn-name
               ~(str (:doc method-meta)
                     tx-note)
               {:arglists '~(:arglists method-meta)}
               [~'dresser & ~'args]
               (transact! ~'dresser
                          (fn [~'tx]
                            (apply ~method-symbol ~'tx ~'args))))))))


(wrap-dresser-tx-methods)



;; Need to wrap those in a function because protocols don't support varargs

(defn update-at!
  {:doc (str (:doc (meta #'dp/-update-at)) tx-note)}
  [dresser drawer id ks f & args]
  (tx-> dresser
    (dp/-update-at drawer id ks f args)))

(defn dissoc-at!
  {:doc (str (:doc (meta #'dp/-dissoc-at)) tx-note)}
  [dresser drawer id ks & dissoc-ks]
  (tx-> dresser
    (dp/-dissoc-at drawer id ks dissoc-ks)))

(defn only-sugar
  "If provided with a collection, converts it into a simple only map.
  (only-sugar [:a :b]) => {:a true, :b true}

  Also converts any node collection:
  (only-sugar {:z {[:a :b] [:c]}}) => {:z {[:a :b] {:c true}}}"
  [only]
  (letfn [(expand [only]
            (cond
              (nil? only) nil
              (map? only) (if (::only-expanded? (meta only))
                            only
                            (reduce-kv (fn [m k v]
                                         (assoc m k (if (coll? v) (expand v) v)))
                                       {} only))
              (coll? only) (reduce #(assoc %1 %2 true) {} only)
              :else (throw (ex-info "Expects a map or a coll" {}))))]
    (some-> (expand only)
            (vary-meta assoc ::only-expanded? true))))


;; {:sort :a}
;; {:sort {:a :desc}}
;; {:sort [{:a :desc}
;;         {:b :asc}]}
;; {:sort [[[:a :a2] :asc]
;;         [[:a :v2] :desc]]} ;<-- final form
;; (defn- sort-sugar
;;   [sort-config]
;;   (cond
;;     (map? sort-config) (if (key))
;;     (keyword? sort-config) {[sort-config] :asc}
;;     (map? sort-config) (first )))

(defn fetch
  {:doc (str (:doc (meta #'dp/-fetch)) tx-note)}
  ([dresser drawer]
   (fetch dresser drawer {}))
  ([dresser drawer {:keys [only limit where sort skip]}]
   (tx-> dresser
     (dp/-fetch drawer (only-sugar only) limit where sort skip))))

(defn get-at
  {:doc (str (:doc (meta #'dp/-get-at)) tx-note)}
  ([dresser drawer id ks]
   (get-at dresser drawer id ks nil))
  ([dresser drawer id ks only]
   (tx-> dresser
     (dp/-get-at drawer id ks (only-sugar only)))))


;; EXPERIMENTAL
;; Cannot be used inside a transaction.
;; Will probably remove.
(defn lfetch
  "Lazy version of `fetch`."
  ([dresser drawer]
   (lfetch dresser drawer {}))
  ([dresser drawer {:keys [only limit where sort skip] :as opts}]
   (lfetch dresser drawer opts 1 nil))
  ([dresser drawer {:keys [only limit where sort skip] :as opts} chunk-size previous-last]
   ;; TODO: 'only' can remove necessary keys
   (let [max-chunk-size 512
         ;; Must provide a sort as there's no guarantee of ordering
         ;; and and we could end up with duplicates or skip values.
         sort (or (not-empty sort) [[[:id] :asc]])
         [main-sort-path main-sort-dir] (first sort)
         ;; Sort is a killer.  Unless there's an index on the sorted
         ;; field, lazyness will make it much more slower as it will
         ;; force re-sorting multiple times.
         new-opts (assoc (dissoc opts :only)
                         :sort sort
                         :limit (if limit (min limit chunk-size)
                                    chunk-size))
         ;_ (clojure.pprint/pprint opts)
         results (fetch dresser drawer new-opts)]
     ;(clojure.pprint/pprint results)
     (when (not-empty results)
       (let [last-result (last results)
             main-sort-last-value (get-in last-result main-sort-path)
             main-sort-previous-last (get-in previous-last main-sort-path)
             main-sort-dup (count (filter #(= main-sort-last-value
                                              (get-in % main-sort-path))
                                          results))
             skip (if (= main-sort-previous-last main-sort-last-value)
                    (+ (or skip 0) main-sort-dup)
                    main-sort-dup)
             next-where (assoc-in where
                                  (conj main-sort-path (if (= main-sort-dir :desc)
                                                         ::lte
                                                         ::gte))
                                  (get-in last-result main-sort-path))

             next-limit (some-> limit (- chunk-size))
             next-chunk-size (min max-chunk-size (* 2 chunk-size) (if limit next-limit max-chunk-size))]
         (lazy-cat results
                   (lfetch dresser drawer (assoc opts
                                                 :skip skip
                                                 :where next-where
                                                 :limit next-limit)
                           next-chunk-size last-result)))))))


(defn fetch-by-id
  {:doc (str (:doc (meta #'dp/-fetch-by-id)) tx-note)}
  ([dresser drawer id] (fetch-by-id dresser drawer id {}))
  ([dresser drawer id {:keys [only where]}]
   (tx-> dresser
     (dp/-fetch-by-id  drawer id (only-sugar only) where))))

(defn fetch-count
  ([dresser drawer]
   (fetch-count dresser drawer nil))
  ([dresser drawer {:keys [where]}]
   (tx-> dresser
     (dp/-fetch-count drawer where))))

(defn delete!
  {:doc (str "Deletes the document if it exists. Returns id." tx-note)}
  [dresser drawer id]
  (tx-> dresser
    (delete-many! drawer {:id id})
    (with-result id)))

(defn upsert!
  {:doc (str (:doc (meta #'dp/-upsert)) tx-note)}
  [dresser drawer data]
  (when-not (:id data)
    (throw (ex-info "Missing document ID" {:doc data})))
  (tx-> dresser
    (dp/-upsert drawer data)))

(defn upsert-many!
  {:doc (str (:doc (meta #'dp/-upsert-many)) tx-note)}
  [dresser drawer docs]
  (doseq [doc docs]
    (when-not (:id doc)
      (throw (ex-info "Missing document ID" {:doc doc}))))
  (tx-> dresser
    (dp/-upsert-many drawer docs)))

;;;;;;




;;; query ops

(def lt ::lt)
(def lte ::lte)
(def gt ::gt)
(def gte ::gte)
(def exists? ::exists?)
(def any ::any)

(defn ops?
  "True if key is a query operation"
  [k]
  (boolean (some #{k} [exists? gte gt lte lt])))

(defn unsupported-ops-err
  [op form]
  (throw (ex-info "Unsupported query operation" {:op   op
                                                 :form form})))




;;;



(defn dr
  "(Dresser and result) Returns a tuple of [dresser result]."
  [dresser]
  [dresser (result dresser)])


(defn update-result
  "Apply f to the current dresser result,
  upserting the resulting value as the new result"
  [dresser f & args]
  (with-result dresser (apply f (result dresser) args)))


(defn fetch-one
  ([dresser drawer]
   (fetch-one dresser drawer nil))
  ([dresser drawer {:keys [only where sort]}]
   (tx-> dresser
     (fetch drawer {:limit 1
                    :only  only
                    :sort  sort
                    :where where})
     (update-result first))))

(defn fetch-reduce
  "Reduces f with the fetched documents.

  f should be a function of 2 arguments: [dresser doc]


  :chunk-size sets how many documents can be fetched at once."
  ([dresser drawer f]
   (fetch-reduce dresser drawer f nil))
  ([dresser drawer f {:keys [only where sort chunk-size]
                      :as   query
                      :or   {chunk-size 50}}]
   (let [?fetch-id (get-in query [:where :id])
         query (dissoc query :chunk-size)]
     (with-tx [tx dresser]
       (loop [tx tx
              ?last-id nil]
         (let [query (if ?last-id
                       (assoc-in query [:where :id] {::gt ?last-id})
                       query)
               ?only (when-let [only (some-> (:only query)
                                             (only-sugar))]
                       (assoc only :id :?))
               r (result tx)
               [tx' docs] (dr (fetch tx drawer (merge query {:sort  [[[:id] :asc]]
                                                             :limit chunk-size}
                                                      (when ?only {:only ?only}))))
               last-id (:id (last docs))
               docs (if (get-in query [:only :id])
                      docs
                      (map #(dissoc % :id) docs))
               tx' (with-result tx' r)
               tx' (reduce f tx' docs)]
           (if (< (count docs) chunk-size)
             tx'
             (recur tx' last-id))))))))



(defn tx-failure-ex
  "Use this to generate a transaction error."
  ([data] (tx-failure-ex data nil))
  ([data cause]
   (ex-info "Transaction failure" (assoc data ::tx-error true) cause)))

(defn last-tx-failure
  "Returns the last transaction failure data, if any."
  [dresser]
  (temp-data dresser [:last-tx-failure]))

(defn with-last-tx-failure
  [dresser data]
  (update-temp-data dresser assoc :last-tx-failure data))



(defn to-edn
  "Returns a simple hashmap (EDN) version of the dresser."
  [dresser]
  (tx-let [tx dresser]
      [drawers (all-drawers tx)
       _ (reduce (fn [tx' drawer-key]
                   (let [accu (result tx')
                         [tx' docs] (dr (fetch tx' drawer-key))
                         m (into {} (for [d docs]
                                      [(:id d) d]))]
                     (with-result tx' (assoc accu drawer-key m))))
                 (with-result tx {})
                 drawers)]
    tx))


(def ^:private lexical-chars
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

(def ^:private lexical-chars-enc
  "Characters available for normal encoding."
  (vec (butlast lexical-chars)))

(def ^:private lexical-chars-qty (count lexical-chars-enc))

;; Reserve the last character as a separator
(def ^:private lexical-separator
  "Character which comes after the lexical-chars-enc."
  (str (last lexical-chars)))


(def lexical-max
  "Similar in purpose to Integer/MAX_VALUE, but for sorting strings."
  ;; Sorts after all the lexical chars.
  ;; While still being URL-safe, it should never used inside the encoding.
  "~")

(defn- int->enc [n]
  (if (zero? n)
    "0"
    (loop [num n
           result []]
      (if (zero? num)
        (apply str (reverse result))
        (recur (quot num lexical-chars-qty)
               (conj result (nth lexical-chars (rem num lexical-chars-qty))))))))

(defn- enc->int [s]
  (reduce (fn [acc c]
            (+ (*' acc lexical-chars-qty)
               (str/index-of lexical-chars (str c))))
          0
          s))

(defn lexical-encode [n]
  "Encodes an integer of arbitrary size into a lexically sortable string.
`lexical-max` is guaranteed to sort after."
  (let [encoded (int->enc (bigint n))
        encoded-length (count encoded)
        length-prefix (int->enc encoded-length)
        prefix-prefix (apply str (repeat (dec (count length-prefix)) lexical-separator))]
    (str prefix-prefix length-prefix lexical-separator encoded)))

(defn lexical-decode [s]
  (some-> (re-find (re-pattern (str "[^" lexical-separator "]*$")) s)
          (enc->int)))
