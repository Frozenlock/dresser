(ns dresser.wrap
  (:require [dresser.base :as db]
            [dresser.protocols :as dp]))

;; It's possible to wrap a Dresser implementation and intercept its
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


(dp/defimpl -transact
  [dresser f {:keys [result?] :as opts}]
  (if (:transact dresser)
    (f dresser)

    (let [;; Evaluate everything inside the source transaction.
          updated-source (db/transact!
                          (:source dresser)
                          (fn [src-tx]
                            ;; Inside the source transaction (src-tx), do the wrapped transaction.
                            (:source (f (assoc dresser :transact true :source src-tx))))
                          (assoc opts :result? false))
          return (assoc dresser :source updated-source)]
      (if result?
        (db/result return)
        (dissoc return :transact)))))

(defn- wrap-method
  "`:wrap` receives a dresser method and returns a wrapped
  method. Similar to web handler middleware.

  (fn [method]
    (fn [tx & args]
      (apply method tx args)))"
  [method-sym wrap]
  (let [method (resolve method-sym)
        wrapped-method (cond-> method
                         wrap wrap)]
    (fn [dresser & method-args]
      (update dresser :source #(apply wrapped-method % method-args)))))

(dp/defimpl -temp-data
  [dresser]
  (db/temp-data (:source dresser)))


;; Note that wrapping cannot modify implementations. For example,
;; `gen-id!` will have no effect on how `add!` generates IDs for new
;; documents.


(def ^:dynamic ^:private *stack-level* 0)

(defn wrap-closing-fn
  "`:closing` is a function receiving the same arguments as the wrapped
  method, but it is evaluated after the wrapped method.

  (fn [tx & args] ...)

  If multiple wrap layers exist, the closing functions across all the
  layers are composed in such a way that the FIRST layer closing
  function is evaluated LAST.

  (-> dresser
    (wrapper-1)
    (wrapper-2)
    (closing-2)
    (closing-1))"
  [method method-sym closing-fn]
  (let [path [::closing-fns method-sym]]
    (fn [tx & args]
      ;; Fetching/updating inside a nested map is relatively
      ;; expensive, especially considering that `wrap-closing-fn` is
      ;; used on all methods. Using a dynamic variable is MUCH faster.
      (binding [*stack-level* (inc *stack-level*)]
        (let [;; add the closing-fn to the stack
              tx (cond-> tx
                   closing-fn (db/update-temp-data update-in path #((fnil conj []) % closing-fn)))
              ;; apply the method
              tx (apply method tx args)
              ;; retrieve the closing-fns stack
              cfs (when (= 1 *stack-level*) ; back to the first outer layer
                    (db/temp-data tx path))]
          (if (not-empty cfs)
            (-> (reduce (fn [tx' cf] (apply cf tx' args)) tx cfs)
                (db/update-temp-data update ::closing-fns dissoc method-sym))
            tx))))))

(defn build
  {:doc
   (str "Expects a map of methods with :wrap and/or :closing
  Ex:
  {`dp/-add {:wrap add-wrapper}}"
        "\n\n  "
        (:doc (meta #'wrap-method))
        "\n\n  "
        (:doc (meta #'wrap-closing-fn)))}
  [dresser method->wrap]
  (assert (db/dresser? dresser))
  (let [unexpected-symbols (seq (remove (set dp/dresser-symbols)
                                        (keys method->wrap)))
        _ (when unexpected-symbols (throw (ex-info "Unexpected method symbols"
                                                   {:symbols unexpected-symbols})))]

    (vary-meta (assoc {} :source dresser)
               merge
               ;; Preserve the original dresser metadata
               (meta dresser)
               ;; By default all methods are wrapped in order to
               ;; correctly handle the wrapped dresser object.
               (into {} (for [sym dp/dresser-symbols]
                          [sym (wrap-method sym nil)]))

               ;; User-provided wrapped method
               (into {} (for [[sym cfgs] method->wrap
                              :let [{:keys [wrap closing]} cfgs]]
                          [sym (-> (wrap-method sym wrap)
                                   (wrap-closing-fn sym closing))]))

               ;; Those methods require careful attention.
               ;; Better to not let users redefine them.
               (dp/mapify-impls [-transact
                                 -temp-data]))))

