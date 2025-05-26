(ns dresser.encoding
  "Shared encoding/decoding utilities for dresser implementations"
  (:require [clojure.string :as str]
            [clojure.walk :as walk])
  (:import (java.util Base64)))

;; Record encoding/decoding - shared across implementations

(defn encode-record
  "Encode a record by converting it to a map with type information.
  Non-records are returned unchanged."
  [m]
  (if (record? m)
    (-> (into {} m)
        (assoc "_drs-record" (str/replace (str (type m)) "class " "")))
    m))

(defn restore-record
  "If the map is an encoded record, restores it.
  If the record namespace is not loaded, returns the normal map.
  Non-maps are returned unchanged."
  [m]
  (or (when-let [record-name (and (map? m) (get m "_drs-record"))]
        (let [clean-map (dissoc m "_drs-record")
              last-dot (str/last-index-of record-name ".")
              namespace (-> (subs record-name 0 last-dot)
                            (str/replace "_" "-"))
              simple-name (subs record-name (inc last-dot))
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

