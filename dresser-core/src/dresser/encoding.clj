(ns dresser.encoding
  "Shared encoding/decoding utilities for dresser implementations.

  CLJS support plan:
  - This file would become encoding.cljc.
  - `resolve` doesn't exist in CLJS, and advanced compilation mangles
    constructor names. Records would need explicit registration via a
    registry (atom mapping stable name -> map-> constructor):
      (register-record! \"my.app/MyRecord\" map->MyRecord)
  - The stored format already uses Clojure ns/name (\"my.app/MyRecord\").
    Decode also handles legacy Java class name format for backward compat.
  - Base64: replace java.util.Base64 with goog.crypt.base64 or js/btoa.
  - bytes?: needs a CLJS equivalent (js/Uint8Array check)."
  (:require [clojure.string :as str])
  (:import (java.util Base64)))

;; Record encoding/decoding - shared across implementations

(defn- class->clj-name
  "Converts a Java class name to a Clojure ns/name string.
  e.g. \"my_app.records.MyRecord\" -> \"my-app.records/MyRecord\""
  [cls-name]
  (let [last-dot (str/last-index-of cls-name ".")
        ns-part (-> (subs cls-name 0 last-dot)
                    (str/replace "_" "-"))
        name-part (subs cls-name (inc last-dot))]
    (str ns-part "/" name-part)))

(defn encode-record
  "Encode a record by converting it to a map with type information.
  Stores the Clojure ns/name (e.g. \"my.app/MyRecord\") for cross-platform portability.
  Non-records are returned unchanged."
  [m]
  (if (record? m)
    (let [cls-name (str/replace (str (type m)) "class " "")]
      (-> (into {} m)
          (assoc "_drs-record" (class->clj-name cls-name))))
    m))

(defn restore-record
  "If the map is an encoded record, restores it.
  Handles both Clojure ns/name format (\"my.app/MyRecord\") and
  legacy Java class name format (\"my_app.MyRecord\").
  If the record namespace is not loaded, returns the normal map.
  Non-maps are returned unchanged."
  [m]
  (or (when-let [record-name (and (map? m)
                                  ;; Could throw on a sorted map if
                                  ;; keys are of different type.
                                  (not (sorted? m))
                                  (get m "_drs-record"))]
        (let [clean-map (dissoc m "_drs-record")
              [namespace simple-name]
              (if (str/includes? record-name "/")
                ;; Clojure ns/name format: "my.app/MyRecord"
                (str/split record-name #"/" 2)
                ;; Legacy Java class format: "my_app.MyRecord"
                (let [last-dot (str/last-index-of record-name ".")]
                  [(-> (subs record-name 0 last-dot)
                       (str/replace "_" "-"))
                   (subs record-name (inc last-dot))]))
              constructor-name (str namespace "/map->" simple-name)]
          (if-let [map->record (resolve (symbol constructor-name))]
            (map->record clean-map)
            clean-map)))
      m))

;; Byte array encoding/decoding

(defn encode-bytes
  "Encode byte arrays as Base64 strings for serialization.
  Non-byte-arrays are returned unchanged."
  [x]
  (cond
    (bytes? x) {:_drs-bytes (.encodeToString (Base64/getEncoder) x)}
    :else x))

(defn restore-bytes
  "Restore Base64-encoded byte arrays.
  Non-encoded values are returned unchanged."
  [x]
  (if (and (map? x) (:_drs-bytes x))
    (.decode (Base64/getDecoder) (:_drs-bytes x))
    x))
