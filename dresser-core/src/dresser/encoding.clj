(ns dresser.encoding
  "Shared encoding/decoding utilities for dresser implementations."
  (:import (java.util Base64)))

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
