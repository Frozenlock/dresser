(ns dresser.impl.file
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [dresser.base :as db]
            [dresser.impl.atom :as at]
            [dresser.protocols :as dp]
            [dresser.test :as dt])
  (:import (java.util Base64)))

;; Simple file-based dresser implementation
;; Wraps atom implementation and persists to file

;; Encoding/decoding for proper serialization

(defn- encode-record
  [m]
  (if (record? m)
    (-> (into {} m)
        (assoc "_drs-record" (str/replace (str (type m)) "class " "")))
    m))

(defn- restore-record
  "If the map is an encoded record, restores it.
  If the record namespace is not loaded, returns the normal map."
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

(defn- encode-bytes
  [x]
  (cond
    (bytes? x) {:_drs-bytes (.encodeToString (Base64/getEncoder) x)}
    :else x))

(defn- restore-bytes
  [x]
  (if (and (map? x) (:_drs-bytes x))
    (.decode (Base64/getDecoder) (:_drs-bytes x))
    x))

(defn- encode
  "Encode data for EDN serialization"
  [data]
  (walk/postwalk (fn [x]
                   (-> x
                       encode-record
                       encode-bytes))
                 data))

(defn- decode
  "Decode data from EDN serialization"
  [data]
  (walk/postwalk (fn [x]
                   (-> x
                       restore-record
                       restore-bytes))
                 data))

(defn- load-from-file
  "Load data from file, return empty map if file doesn't exist or is invalid"
  [filename]
  (if (.exists (io/file filename))
    (decode (edn/read-string (slurp filename)))
    {}))

(defn- save-to-file!
  "Save data to file atomically using a temp file"
  [filename data]
  (let [temp-file (str filename ".tmp")]
    (spit temp-file (pr-str (encode data)))
    (.renameTo (io/file temp-file) (io/file filename))))

(defn- reload!
  "Deletes every document in the dresser, then populates it back using
data from the file."
  [tx filename]
  (db/tx-let [tx tx]
      [drawers (db/all-drawers tx)
       ;; delete all docs
       _ (reduce #(db/delete-many! %1 %2 {:id {db/exists? true}})
                 tx
                 drawers)
       ;; add back read data
       data (load-from-file filename)
       _ (reduce (fn [tx [drawer id->docs]]
                   (db/upsert-many! tx drawer (vals id->docs)))
                 tx
                 data)]
    tx))

(defn wrap-transact
  [t filename force-reload?]
  (fn [dresser f opts]
    (let [new-f (fn [dresser & args]
                  (if (db/temp-data dresser [::transact?])
                    ;; Already in transaction
                    (apply f dresser args)

                    ;; Starting a new transaction
                    (let [tx (apply f (db/assoc-temp-data dresser ::transact? true) args)
                          result (db/result tx)
                          [tx data] (db/dr (db/to-edn tx))]
                      (save-to-file! filename data)
                      (cond-> (db/update-temp-data tx dissoc ::transact?)
                        force-reload? (reload! filename)
                        true (db/with-result result)))))]
      (t dresser new-f opts))))

(defn build
  "Builds a file-backed dresser from a filename.
  `init-data` has priority over data from file.
  `force-reload?` reloads from file at each transaction (mostly for tests)."
  {:test (fn []
           (let [temp-file (str (gensym "test") ".edn")]
             (try
               (dt/test-impl
                #(dt/no-tx-reuse (build temp-file {} :force-reload)))
               (finally
                 (when (.exists (io/file temp-file))
                   (.delete (io/file temp-file)))))))}
  ([filename]
   (build filename nil))

  ([filename init-data]
   (build filename init-data false))

  ([filename init-data force-reload?]
   (let [data (or init-data (load-from-file filename))
         atom-dresser (at/build data)]
     (vary-meta atom-dresser
                (fn [m]
                  (update m `dp/-transact #(wrap-transact % filename force-reload?)))))))
