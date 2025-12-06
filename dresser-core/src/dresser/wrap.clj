(ns dresser.wrap
  (:require [dresser.base :as db]
            [dresser.protocols :as dp]))

;; Dynamic var for depth tracking per [dresser-id method-sym].
;; Closing fns are accumulated in temp-data because they must persist
;; across binding frames (inner wrappers add fns that outer wrappers execute).
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
  [method method-sym closing-fn dresser-id]
  (let [k [dresser-id method-sym]
        path [::closing-fns k]]
    (fn [tx & args]
      (binding [*stack-level* (update *stack-level* k (fnil inc 0))]
        (let [tx (cond-> tx
                   closing-fn (db/temp-update-in path #((fnil conj []) % closing-fn)))
              tx (apply method tx args)
              cfs (when (= 1 (get *stack-level* k))
                    (db/temp-get-in tx path))]
          (if (seq cfs)
            (let [tx (db/temp-update-in tx [::closing-fns] dissoc k)]
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
  (let [dresser-id (db/temp-dresser-id dresser)]
    (vary-meta dresser
               (fn [m]
                 (reduce (fn [m [sym {:keys [wrap closing]}]]
                           (update m sym
                                   #(-> ((or wrap identity) (or % (dp/fundamental sym)))
                                        (wrap-closing-fn sym closing dresser-id))))
                         m
                         method->wrap)))))
