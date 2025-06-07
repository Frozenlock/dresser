(ns dresser.wrap
  (:require [dresser.base :as db]
            [dresser.protocols :as dp]))

(def ^:dynamic ^:private *stack-level* nil)

(defn- wrap-closing-fn
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
      (binding [*stack-level* (update *stack-level* method-sym (fnil inc 0))]
        (let [;; add the closing-fn to the stack
              tx (cond-> tx
                   closing-fn (db/temp-update-in path #((fnil conj []) % closing-fn)))
              ;; apply the method
              tx (apply method tx args)
              ;; retrieve the closing-fns stack
              cfs (when (= 1 (get *stack-level* method-sym)) ; back to the first outer layer
                    (db/temp-get-in tx path))]
          (if (not-empty cfs)
            (let [tx (db/temp-update tx ::closing-fns dissoc method-sym)]
              (reduce (fn [tx' cf] (apply cf tx' args)) tx cfs))
            tx))))))

(defn build
  "Expects a map of method symbols with a configuration map
  Ex: {`dp/add {:wrap ..., :closing ...}}

  -----
  `:wrap` is a function receiving a dresser method and returns a
  wrapped method. Similar to web handler middleware.

  (fn [method]
    (fn [tx & args]
      (apply method tx args)))

  `:closing` is a function receiving the same arguments as the wrapped
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
  [dresser method->wrap]
  (assert (db/dresser? dresser))
  (let [wrapper-id (gensym "wrapper-")]
    (vary-meta dresser
               (fn [m]
                 (reduce (fn [m [sym {:keys [wrap closing]}]]
                           (update m sym
                                   #(-> ((or wrap identity) (or % (dp/fundamental sym)))
                                        (wrap-closing-fn sym closing))))
                         m
                         method->wrap)))))
