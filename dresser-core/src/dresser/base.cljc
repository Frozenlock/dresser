(ns dresser.base
  "Universal storage abstraction layer"
  (:require [clojure.string :as str]
            [dresser.protocols :as dp]))

;; TODO: When ID is nil, should methods be no-op and return nil?

(defrecord Dresser [in-tx? temp-data immutable?])

(defn dresser?
  "Returns true if x is a dresser."
  [x]
  (= (type x) Dresser))

(defn make-dresser
  [fundamental-impl immutable?]
  (-> (->Dresser false {} immutable?)
      (into fundamental-impl)
      (with-meta (meta fundamental-impl))))


(defn immutable?
  "Returns true if the dresser is immutable."
  [dresser]
  (dp/immutable? dresser))

(defn tx?
  "Returns true if the dresser is currently in a transaction."
  [dresser]
  (dp/tx? dresser))

(defn temp-data
  {:doc (:doc (meta #'dp/-temp-data))}
  ([dresser]
   (dp/temp-data dresser))
  ([dresser ks]
   (get-in (temp-data dresser) ks)))

(defn with-temp-data
  {:doc (:doc (meta #'dp/-with-temp-data))}
  [dresser data]
  (dp/with-temp-data dresser data))

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
  "Starts the dresser. Returns the started dresser."
  [dresser]
  (dp/start dresser))

(defn stop
  "Stops the dresser. Returns the stopped dresser."
  [dresser]
  (dp/stop dresser))


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
  "Evaluates the provided function inside a transaction.

  If already in a transaction, simply calls the function.
  Otherwise starts a new transaction.

  Returns the result if :result? is true (default), otherwise returns the updated dresser."
  ([dresser f] (transact! dresser f {:result? true}))
  ([dresser f {:keys [result?] :as opts}]
   (assert (dresser? dresser) "Transact first argument should be a dresser")
   (if (tx? dresser)
     (f dresser)  ; Already in transaction
     (let [tx (dp/transact dresser f opts)]
       (if result?
         (result tx)    ; Extract and return result
         tx)))))

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


(defn all-drawers
  {:doc (str "Returns a sequence of all drawers keys." tx-note)}
  [dresser]
  (tx-> dresser dp/all-drawers))

(defn delete-many!
  {:doc (str "Delete all documents matching `where`. Returns {:deleted-count <qty>}." tx-note)}
  [dresser drawer where]
  (tx-> dresser (dp/delete-many drawer where)))

(defn drop!
  {:doc (str "Removes the drawer. Returns the provided drawer." tx-note)}
  [dresser drawer]
  (tx-> dresser (dp/drop drawer)))

(defn add!
  {:doc (str "Adds a document (map) and returns its ID." tx-note)}
  [dresser drawer data]
  (tx-> dresser (dp/add drawer data)))

(defn all-ids
  {:doc (str "Returns a sequence of all document IDs from this drawer." tx-note)}
  [dresser drawer]
  (tx-> dresser (dp/all-ids drawer)))

(defn gen-id!
  {:doc (str "Generates an ID for a document in the given drawer." tx-note)}
  [dresser drawer]
  (tx-> dresser (dp/gen-id drawer)))

(defn dresser-id
  {:doc (str "Returns the dresser ID." tx-note)}
  [dresser]
  (tx-> dresser dp/dresser-id))

(defn drawer-key
  {:doc (str "Returns the drawer key. Mostly for implementing the drawer registry." tx-note)}
  [dresser drawer-id]
  (tx-> dresser (dp/drawer-key drawer-id)))

(defn rename-drawer!
  {:doc (str "Returns new-drawer." tx-note)}
  [dresser drawer new-drawer]
  (tx-> dresser (dp/rename-drawer drawer new-drawer)))

(defn has-drawer?
  {:doc (str "Returns true if the dresser has the drawer." tx-note)}
  [dresser drawer]
  (tx-> dresser (dp/has-drawer? drawer)))

;; Custom wrapper for assoc-at! to add ID validation
(defn assoc-at!
  {:doc (str "Similar to `clojure.core/assoc-in`, but for a drawer. Returns data." tx-note)}
  [dresser drawer id ks data]
  (when (nil? id)
    (throw (ex-info "Missing document ID" {:drawer drawer :id id :ks ks :data data})))
  (tx-> dresser (dp/assoc-at drawer id ks data)))

;; Need to wrap those in a function because protocols don't support varargs

(defn update-at!
  {:doc (str "Similar to `clojure.core/update-in`. Returns the value that was updated-in." tx-note)}
  [dresser drawer id ks f & args]
  (tx-> dresser (dp/update-at drawer id ks f args)))

(defn dissoc-at!
  {:doc (str "Dissoc the dissoc-keys from the value located at ks. Returns nil." tx-note)}
  [dresser drawer id ks & dissoc-ks]
  (tx-> dresser (dp/dissoc-at drawer id ks dissoc-ks)))

(defn only-sugar
  "If provided with a collection, converts it into a simple only map.
  (only-sugar [:a :b]) => {:a true, :b true}

  Also converts any node collection:
  (only-sugar {:z {[:a :b] [:c]}}) => {:z {[:a :b] {:c true}}}

  Avoids re-processing already expanded maps using metadata."
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

(defn fetch
  {:doc (str "Fetches documents from drawer.

  Options:
  - :only   - Map specifying which fields to return
  - :limit  - Maximum number of documents to return
  - :where  - Query conditions map
  - :sort   - Sort configuration [[path order] ...]
  - :skip   - Number of documents to skip

  Returns documents matching the criteria." tx-note)}
  ([dresser drawer]
   (fetch dresser drawer {}))
  ([dresser drawer {:keys [only limit where sort skip]}]
   (tx-> dresser (dp/fetch drawer (only-sugar only) limit where sort skip))))

(defn get-at
  {:doc (str "Similar to `clojure.core/get-in`, but for a drawer.

  Returns the value at the specified path within a document." tx-note)}
  ([dresser drawer id ks]
   (get-at dresser drawer id ks nil))
  ([dresser drawer id ks only]
   (tx-> dresser (dp/get-at drawer id ks (only-sugar only)))))

(defn fetch-by-id
  {:doc (str "Fetches a document from drawer by its ID.

  Options:
  - :only   - Map specifying which fields to return
  - :where  - Additional query conditions" tx-note)}
  ([dresser drawer id] (fetch-by-id dresser drawer id {}))
  ([dresser drawer id {:keys [only where]}]
   (tx-> dresser (dp/fetch-by-id drawer id (only-sugar only) where))))

(defn fetch-count
  {:doc (str "Returns the count of documents matching the criteria.

  Options:
  - :where  - Query conditions" tx-note)}
  ([dresser drawer]
   (fetch-count dresser drawer {}))
  ([dresser drawer {:keys [where]}]
   (tx-> dresser (dp/fetch-count drawer where))))

(defn delete!
  {:doc (str "Deletes the document if it exists. Returns id." tx-note)}
  [dresser drawer id]
  (tx-> dresser
    (delete-many! drawer {:id id})
    (with-result id)))

(defn upsert!
  "Inserts the document document in the drawer. Must have :id."
  [dresser drawer data]
  (assoc-at! dresser drawer (:id data) [] data))

(defn upsert-many!
  {:doc (str "Insert many documents, each containing an `:id`. Returns documents." tx-note)}
  [dresser drawer docs]
  (doseq [doc docs]
    (when-not (:id doc)
      (throw (ex-info "Missing document ID" {:doc doc}))))
  (tx-> dresser (dp/upsert-many drawer docs)))

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
                       (update-in query [:where :id] merge {::gt ?last-id})
                       query)
               ?expanded-only (some-> (:only query)
                                      (only-sugar))
               r (result tx)
               [tx' docs] (dr (fetch tx drawer (merge query {:sort  [[[:id] :asc]]
                                                             :limit chunk-size}
                                                      (when ?expanded-only
                                                        {:only (assoc ?expanded-only :id :?)}))))
               last-id (:id (last docs))
               docs (if (or (:id ?expanded-only)
                            (nil? ?expanded-only))
                      docs
                      (map #(dissoc % :id) docs))
               tx' (with-result tx' r)
               tx' (reduce f tx' docs)]
           (if (< (count docs) chunk-size)
             tx'
             (recur tx' last-id))))))))

(defn reduce-tx
  "Reduces a function over elements of a collection within a transaction context.
  If called with only dresser and f, uses the dresser's result as the collection.
  The reducing function should take (tx element) and return an updated tx.
  Returns the final dresser state.

  Arguments:
    dresser - The dresser object
    f       - A function that takes (tx element) and returns an updated tx
    coll    - (Optional) The collection to process (defaults to current result)

  Example:
    (db/tx-> my-db
      (db/fetch :users)
      (db/reduce-tx (fn [tx user]
                      (db/update! tx :users (:id user) [:last-seen] (java.util.Date.)))))"
  ([dresser f]
   (let [ret (result dresser)]
     (assert (or (nil? ret)
                 (coll? ret)) "Previous dresser result must be a collection")
     (reduce-tx dresser f ret)))
  ([dresser f coll]
   (if (not-empty coll)
     (with-tx [tx dresser]
       (loop [tx tx, items coll]
         (let [tx' (f tx (first items))]
           (if-let [more-items (next items)]
             (recur tx' more-items)
             tx'))))
     dresser)))

(defn map-tx
  "Maps a function over elements of a collection within a transaction context.
  If called with only dresser and f, uses the dresser's result as the collection.
  Returns a new dresser with the results as a sequence in the same order as the input.

  Arguments:
    dresser - The dresser object
    f       - A function that takes (tx element) and returns an updated tx with a result
    coll    - (Optional) The collection to process (defaults to current result)

  Example:
    (db/tx-> my-db
      (db/fetch :users)
      (db/map-tx (fn [tx user]
                   (db/get-at tx :orders (:id user) [] {:only [:total]}))))"
  ([dresser f]
   (let [ret (result dresser)]
     (assert (or (nil? ret)
                 (coll? ret)) "Previous dresser result must be a collection")
     (map-tx dresser f ret)))
  ([dresser f coll]
   (if (not-empty coll)
     (let [dresser-with-results (with-result dresser [])]
       (-> dresser-with-results
           (reduce-tx (fn [tx item]
                        (let [acc (result tx)
                              [tx' item-result] (dr (f tx item))]
                          (with-result tx' (conj acc item-result))))
                      coll)))
     dresser)))


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


;; LEXICAL ENCODING
;;
;; A custom base-61 URL-safe encoding that preserves lexicographical
;; sort order for arbitrary-sized integers. This is crucial for
;; dresser's range queries.
;;
;; Design principles:
;; 1. Length-prefixed: Numbers with more digits sort after shorter ones
;;    (e.g., "99" < "100" in our encoding, unlike strings where "99" > "100")
;; 2. URL-safe: Uses only alphanumeric characters
;; 3. Reserved characters:
;;    - 'z' is reserved as a separator (not used in encoding)
;;    - '~' is lexical-max (sorts after all encoded values)
;;
;; The encoding works by:
;; 1. Converting number to base-61 string
;; 2. Prefixing with the encoded length
;; 3. Prefixing the length itself with separators if needed
;;
;; Example: Why length-prefixing matters
;; Without length prefix:
;;   99  -> "1c" (2 chars in base-61)
;;   100 -> "1D" (2 chars in base-61)
;;   But "1c" > "1D" lexicographically! (wrong order)
;;
;; The actual algorithm with length prefix:
;;   5       -> "1z5"    (length=1, separator, value="5")
;;   99      -> "2z1c"   (length=2, separator, value="1c")
;;   100     -> "2z1d"   (length=2, separator, value="1d")
;;   10000   -> "3z2fv"  (length=3, separator, value="2fv")
;;   1000000 -> "4z4OjR" (length=4, separator, value="4OjR")
;;
;; The 'z' separator is crucial - it sorts after all encoding chars (0-9A-Za-y),
;; ensuring that longer numbers always sort after shorter ones.
;;
;; But wait - what if the length itself needs multiple characters?
;; Same problem! A length of "13" would sort between "1" and "2".
;; Solution: prefix the length with 'z' separators to push it forward:
;;   Length 1-60:     "1z..." to "yz..."        (single char length)
;;   Length 61-3720:  "z1z..." to "zyz..."      (one z prefix)
;;   Length 3721+:    "zz1z..." to "zzyz..."    (two z prefixes)
;;   And so on recursively...


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
