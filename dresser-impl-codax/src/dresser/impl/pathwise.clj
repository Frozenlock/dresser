(ns dresser.impl.pathwise
  (:require [clojure.walk :as w]
            [codax.core :as c]
            [hasch.core :as hashc]
            [taoensso.nippy :as nippy]))

;;; To respect equality, the maps and sets are stripped of any
;;; particular type (sorted-map, sorted-set, ...).
;;; (= (sorted-map {:a 1}) {:a 1})
;;; => true
;;;
;;; Similarly, the metadata is removed as it would make it impossible
;;; to have a valid equality before deserializing.




;; Store the map/set into a simple sequence.
;; The sequence will conserve its ordering.

(defrecord FreezableMap [m])

(nippy/extend-freeze FreezableMap ::map
    [x data-output]
  (nippy/freeze-to-out! data-output (sort-by hashc/b64-hash (:m x))))

(nippy/extend-thaw ::map
  [data-input]
  (into {} (nippy/thaw-from-in! data-input)))

(defrecord FreezableSet [m])

(nippy/extend-freeze FreezableSet ::set
    [x data-output]
  (nippy/freeze-to-out! data-output (sort-by hashc/b64-hash (:m x))))

(nippy/extend-thaw ::set
  [data-input]
  (into #{} (nippy/thaw-from-in! data-input)))


;;;;;;;;;;;;;;;


(defn freezable-commutative-coll
  [coll]
  (let [f (fn [x]
            (cond
              (and (map? x) (not (record? x))) (->FreezableMap x)
              (set? x) (->FreezableSet x)
              :else x))]
    (w/postwalk f coll)))

(c/defpathtype [0x71
                ; maps
                clojure.lang.PersistentArrayMap
                clojure.lang.PersistentHashMap
                clojure.lang.PersistentTreeMap]
  (fn commutative-coll-encoder [m]
    (nippy/freeze-to-string (freezable-commutative-coll m)
                            {:incl-metadata? false}))

  (fn commutative-coll-decoder [s]
    (nippy/thaw-from-string s)))

(def side-effect true)
