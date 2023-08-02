(ns dresser.wrap
  (:require [dresser.base :as db]
            [dresser.protocols :as dp]
            [clojure.string :as str]))

;; It's possible to wrap an Idresser implementation and intercept its
;; methods before and after they are applied.

;; One crucial step is to open the transaction of the wrapped dresser
;; before the transaction of the wrapper itself.

;; Example for 'W' wrapping dresser 'D':
;;
;; ┌─── open transaction D
;; |
;; | ┌─ open transaction W
;; | |
;; | |─── (pre) function W
;; | |
;; | |─── function D
;; | |
;; | |─── (post) function W
;; | |
;; | └─ close transaction W
;; |
;; └─── close transaction D


(defn- f--transact
  "Generates `dp/-transact` implementation with a private tx-data."
  [tx-data-key]
  (let [dissoc-tx-data (fn [dresser]
                             (db/with-temp-data dresser
                               (-> (db/temp-data dresser)
                                   (dissoc tx-data-key))))]
    (dp/impl -transact
      [dresser f result?]
      (if (:transact dresser)
        (f dresser)

        (let [f' (fn [tx]
                   ;; The last action in the transaction will be to
                   ;; remove the data stored at tx-data-key.
                   (-> (f tx)
                       (dissoc-tx-data)))

              ;; Evaluate everything inside the source transaction.
              updated-source (db/transact!
                              (:source dresser)
                              (fn [src-tx]
                                ;; Inside the source transaction (src-tx), do the wrapped transaction.
                                (:source (f' (assoc dresser :transact true :source src-tx))))
                              false)
              return (assoc dresser :source updated-source)]
          (if result?
            (db/result return)
            (dissoc return :transact)))))))

(defn- wrap-temp-data
  "The source temp-data is restored after applying pre or post, except
  for tx-data-key."
  [pre-or-post tx-data-key]
  (fn [tx & args]
    (let [src-temp-data (db/temp-data tx)
          tx' (apply pre-or-post tx args)
          tx-data (get (db/temp-data tx') tx-data-key)]
      (db/with-temp-data tx' (if tx-data
                               (assoc src-temp-data tx-data-key tx-data)
                               src-temp-data)))))

(defn- wrap-method
  "`:wrap` receives a dresser method and returns a wrapped
  method. Similar to web handler middleware.

  (fn [method]
    (fn [tx & args]
      (apply method tx args)))

  `:pre` and `:post` receives all the arguments this particular method
  would also receive, but the tx contains the pre-wrapped methods. In
  other words, `:pre` and `:post` capture the current dresser's state
  before any additional wrapper.

  (fn [tx & args]
    (do-something tx))


  All temp data is immediately restored after the `:pre` and `:post`
  steps as to not break consumers.

  --------

  A more advanced version of the wrappers is available with the '+'
  suffix.  This will add `wrap-cfgs` as the first argument.

  For example, `:wrap+` would expect the following:

  (fn [wrap-cfgs method] ...))

  And `:pre+`/`:post+`:
  (fn [wrap-cfgs tx ...] ...)

    wrap-cfgs contains:
  - :tx-data-key

  The data stored under this key is the only one that will survive
  across the `:pre` and `:post` steps.  It will also be destroyed at
  the end of the transaction.
  "
  [method-sym {:keys [post post+ pre pre+ wrap wrap+]} tx-data-key]
  (assert (not (or (and post post+)
                   (and pre pre+)
                   (and wrap wrap+))))
  (let [method (resolve method-sym)
        wrap-cfgs {:tx-data-key tx-data-key}
        wrap' (or (some-> wrap+ (partial wrap-cfgs))
                  wrap)
        pre' (some-> (or (some-> pre+ (partial wrap-cfgs))
                         pre)
                     (wrap-temp-data tx-data-key))
        post' (some-> (or (some-> post+ (partial wrap-cfgs))
                          post)
                      (wrap-temp-data tx-data-key))
        wrapped-method (or (when wrap' (wrap' method))
                           method)]
    (-> (fn [dresser & method-args]
          (cond-> (:source dresser)
            pre' ((fn [tx] (apply pre' tx method-args)))
            true ((fn [tx] (apply wrapped-method tx method-args)))
            post' ((fn [tx] (apply post' tx method-args)))
            true ((fn [tx] (assoc dresser :source tx)))))
        (vary-meta merge (meta wrapped-method)))))

(dp/defimpl -temp-data
  [dresser]
  (db/temp-data (:source dresser)))


;; Note that wrapping cannot modify implementations. For example,
;; `gen-id!` will have no effect on how `add!` generates IDs for new
;; documents.


(defn wrap-closing-fn
  [method method-sym closing-fn]
  (fn [tx & args]
    (let [path [::closing-fns method-sym]
          ;; add the closing-fn to the stack
          tx (db/update-temp-data tx update-in path #(conj % closing-fn))
          ;; apply the method
          tx (apply method tx args)
          ;; retrieve the closing-fns stack
          [cf1 cf2 & cfs] (db/temp-data tx path)]
      ;; Collapse the closing-fns one at a time.  When only when is
      ;; remaining, we are leaving the last layer and can apply it.
      (if cf2
        (let [new-cf1 (fn [tx & args] (apply cf1 (apply cf2 tx args) args))]
          (db/update-temp-data tx assoc-in path (conj cfs new-cf1)))
        (apply cf1 tx args)))))


(defn build
  {:doc
   (str "Expects a map of methods with :pre/:wrap/:post.
  Ex:
  {`dp/-add {:pre do-this-before-add}}"
        "\n\n  "
        (:doc (meta #'wrap-method)))}
  [dresser method->pre-post-wrap]
  (assert (db/dresser? dresser))
  (let [unexpected-symbols (seq (remove (set dp/dresser-symbols)
                                        (keys method->pre-post-wrap)))
        _ (when unexpected-symbols (throw (ex-info "Unexpected method symbols"
                                                   {:symbols unexpected-symbols})))
        tx-data-key (gensym "tx-data-")]

    (vary-meta (assoc {} :source dresser)
               merge
               ;; Preserve the original dresser metadata
               (meta dresser)
               ;; By default all methods are wrapped in order to
               ;; correctly handle the wrapped dresser object.
               (into {} (for [sym dp/dresser-symbols]
                          [sym (wrap-method sym {} tx-data-key)]))

               ;; User-provided wrapped method
               (into {} (for [[sym cfgs] method->pre-post-wrap
                              :let [closing (or (:closing cfgs)
                                                (fn closing-default [tx & _args] tx))]]
                          [sym (-> (wrap-method sym cfgs tx-data-key)
                                   (wrap-closing-fn sym closing))]))

               ;; Those methods require careful attention.
               ;; Better to not let users redefine them.
               (dp/mapify-impls [(f--transact tx-data-key)
                                 -temp-data]))))




;;;;;;;;;;;;;;;


(comment
  ;; An extension could be written with this principle for all dresser
  ;; methods inside a transaction. The cache would need to be
  ;; invalidated for a given document whenever a 'write' method is used.
  (defn tx-memoize
    "Returns a function that stores its results in the tx temporary data
  store. All subsequent calls will use the stored results instead of
  querying the DB again.

  Only use when you are certain the result cannot change in the middle
  of the transaction."
    [f tx-data-key]
    (fn [tx & args]
      (let [cache-key [tx-data-key ::memoize f args]
            cached (get-in (db/temp-data tx) cache-key :not-found)]
        (if-not (= cached :not-found)
          (db/with-result tx cached)
          (let [[tx result] (db/dr (apply f tx args))]
            (-> (db/update-temp-data tx assoc-in cache-key result)
                (db/with-result result))))))))
