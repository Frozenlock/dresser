(ns dresser.impl.pathwise
  (:require [clojure.walk :as w]
            [codax.core :as c]
            [taoensso.nippy :as nippy]))

(defn- repeatable-unordered-coll
  "For collections where order doesn't matter for equality, constructs a
  new empty coll of the same type and inserts the sorted elements."
  [coll]
  (letfn [(new-coll [x]
            (if (or (map? x)
                    (set? x))
              (into (empty x) (sort x))
              x))]
    (w/postwalk new-coll coll)))

(c/defpathtype [0x71
                clojure.lang.PersistentHashMap
                clojure.lang.PersistentArrayMap
                clojure.lang.PersistentTreeMap]
  (fn map-encoder [m]
    (nippy/freeze-to-string (repeatable-unordered-coll m)))

  (fn map-decoder [s]
    (nippy/thaw-from-string s)))

(def side-effect true)
