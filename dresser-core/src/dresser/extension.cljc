(ns dresser.extension
  "When an extension stores data, the convention is to use the 'drs_'
  prefix in the fields or the drawers to avoid clashes with data from
  the users."
  (:require [dresser.base :as db]
            [dresser.wrap :as wrap])
  #?(:cljs (:require-macros [dresser.extension :refer (defext)])))

(defn assert-ext
  [dresser ext]
  (let [{::keys [extensions]} (db/temp-data dresser)]
    (assert (some #{ext} extensions)
            (str "Missing extension " ext))))

(defn build-ext
  "Returns a function to wrap a dresser."
  [dresser ext-key {:keys [deps wrap-configs init-fn throw-on-reuse?]}]
  ;; Extensions should not apply more than once.
  (let [init-fn' (or init-fn identity)
        already-used? (some #{ext-key} (:extensions (db/temp-data dresser)))]
    (cond (and already-used? throw-on-reuse?)
          (throw (ex-info "This extension can only be used once" {:extension ext-key}))

          already-used? dresser ;(init-fn' dresser)

          :else (-> (reduce #(%2 %1) dresser deps) ; apply deps
                    (wrap/build wrap-configs)
                    (db/update-temp-data update :extensions #((fnil conj #{}) % ext-key))
                    (init-fn')))))

(defmacro defext
  {:arglists     '([name doc-string? attr-map? {:deps [], :wrap-configs {}, :init-fn f, :throw-on-reuse? bool}])
   :doc          (str "Defines a wrapper, similar to `db/wrap`."
                      "\n  "
                      "Extensions in :deps will be added first."
                      "\n  "
                      "The wrapper will only apply itself once, but `init-fn` will always be called"
                      "\n\n  " (:doc (meta #'wrap/build)))
   :style/indent [:defn]}
  [name & fdecl]
  ;; Complex, but it's a copy/past from 'defn'
  (let [doc (if (string? (first fdecl))
              (first fdecl)
              nil)
        fdecl (if (string? (first fdecl))
                (next fdecl)
                fdecl)
        m (if (map? (first fdecl))
            (first fdecl)
            {})
        fdecl (if (map? (first fdecl))
                (next fdecl)
                fdecl)
        args (first fdecl)
        body (rest fdecl)]
    ;; Leverage 'defn'
    `(defn ~name
       ~(assoc m :doc doc)
       [~'dresser ~@args]
       (build-ext ~'dresser ~(keyword (str *ns*) (str name)) ~@body))))

(comment
  (defext my-ext
      "Some amazing extension"
    [abc]
    {:deps         []
     :wrap-configs {}})

  (defext my-ext2 []
    {:deps         [my-ext]
     :wrap-configs {}})

  (defext my-ext3 []
    {:deps         []
     :wrap-configs {}}))

