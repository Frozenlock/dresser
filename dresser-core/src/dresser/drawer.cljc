(ns dresser.drawer
  (:require [clojure.test :as t :refer [is]])
  (:refer-clojure :exclude [key]))

(defprotocol IDrawer
  (get-key [this]))

(defrecord Basic-drawer [drawer-key]
  IDrawer
  (get-key [_] drawer-key))

(def drawer-type ::drawer)

(defn drawer
  "If x is a drawer, returns it.
  Else, if x is a potential drawer key, returns a new drawer object.
  If all fails, returns nil."
  {:test #(do (is (= (drawer :x)
                     (drawer (drawer :x))))
              (is (= (get-key (drawer :x))
                     :x)))}
  [x]
  (cond
    (= (type x) drawer-type) x
    (not (nil? x)) (-> (->Basic-drawer x)
                       (with-meta {:type drawer-type}))
    :else nil))


(defn key
  "Returns the drawer key if x is a drawer, x otherwise."
  [x]
  (if (= (type x) drawer-type)
    (get-key (drawer x))
    x))

(defn key=
  "True if the two drawers have the same key."
  [a b]
  (= (key a) (key b)))

