(ns dresser.impl.file
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [dresser.base :as db]
            [dresser.encoding :as enc]
            [dresser.impl.atom :as at]
            [dresser.protocols :as dp]
            [dresser.test :as dt]))

;; Simple file-based dresser implementation
;; Wraps atom implementation and persists to file

;;; Modification Tracking Extension (private)
;;; Tracks whether a dresser has been modified during a transaction
;;; to avoid expensive serialization/comparison on read-only transactions

(defn- modified?
  "Returns true if the dresser has been modified in the current transaction."
  [dresser]
  (db/temp-get-in dresser [::modified?]))

(defn- wrap-mutating-method
  "Wraps a mutating method to mark the dresser as modified."
  [method]
  (fn [dresser & args]
    (-> (apply method dresser args)
        (db/temp-assoc ::modified? true))))

(defn- wrap-transact-for-tracking
  "Wraps transact to reset the modified flag at transaction start."
  [transact-method]
  (fn [dresser f opts]
    (transact-method dresser
                     (fn [tx]
                       ;; Reset modified flag at transaction start
                       (f (db/temp-assoc tx ::modified? false)))
                     opts)))

(defn- with-modification-tracking
  "Adds modification tracking to a dresser by wrapping mutating protocol methods."
  [dresser]
  (-> dresser
      ;; Wrap fundamental mutating methods
      (dp/wrap-method `dp/delete-many wrap-mutating-method)
      (dp/wrap-method `dp/assoc-at wrap-mutating-method)
      (dp/wrap-method `dp/drop wrap-mutating-method)
      ;; Wrap optional mutating methods
      (dp/wrap-method `dp/update-at wrap-mutating-method)
      (dp/wrap-method `dp/add wrap-mutating-method)
      (dp/wrap-method `dp/dissoc-at wrap-mutating-method)
      (dp/wrap-method `dp/gen-id wrap-mutating-method)
      (dp/wrap-method `dp/upsert-many wrap-mutating-method)
      (dp/wrap-method `dp/rename-drawer wrap-mutating-method)
      ;; Wrap transact to reset flag
      (dp/wrap-method `dp/transact wrap-transact-for-tracking)))

(defn encode-all
  "Encode both records and byte arrays in data structure"
  [data]
  (walk/postwalk (fn [x]
                   (-> x
                       enc/encode-record
                       enc/encode-bytes))
                 data))

(defn decode-all
  "Decode both records and byte arrays in data structure"
  [data]
  (walk/postwalk (fn [x]
                   (-> x
                       enc/restore-record
                       enc/restore-bytes))
                 data))

(defn- load-from-file
  "Load data from file if it exists, nil otherwise"
  [filename deserializer]
  (when (.exists (io/file filename))
    (deserializer (slurp filename))))

(defn- save-to-file!
  "Save data to file atomically using a temp file"
  [filename data serializer]
  (let [temp-file (str filename ".tmp")]
    (spit temp-file (serializer data))
    (.renameTo (io/file temp-file) (io/file filename))))

(defn- reload!
  "Deletes every document in the dresser, then populates it back using
  data from the file."
  [tx filename deserializer]
  (db/tx-let [tx tx]
             [_ (-> (db/all-drawers tx)
                    (db/reduce-tx db/drop!))
              data (load-from-file filename deserializer)
              load-drawer! (fn [tx [drawer id->docs]]
                             (db/upsert-many! tx drawer (vals id->docs)))]
             (db/reduce-tx tx load-drawer! data)))

(defn wrap-transact
  [transact filename {:keys [force-reload? serializer deserializer]}]
  (fn [dresser f opts]
    (let [wrap-f (fn [tx]
                   (let [tx (f tx)
                         result (db/result tx)
                         modified? (modified? tx)
                         tx (if modified?
                              (let [[tx data] (db/dr (db/to-edn tx))]
                                (save-to-file! filename data serializer)
                                (cond-> tx
                                  force-reload? (reload! filename deserializer)))
                              tx)]
                     (db/with-result tx result)))]
      (transact dresser wrap-f opts))))

(declare build)

(defn test-serialization
  "Helper function to test custom serializer/deserializer functions.

  Usage:
    (test-serialization my-serializer my-deserializer)

  This runs the full dresser test suite to ensure your serialization
  functions handle all data types correctly (records, bytes, etc.)."
  [serializer deserializer]
  (let [temp-file (str (gensym "serialization-test") ".tmp")
        delete-file! #(when (.exists (io/file temp-file))
                        (.delete (io/file temp-file)))]
    (try
      (dt/test-impl
       #(do
          (delete-file!) ; Delete before each test creation
          (dt/no-tx-reuse
           (build temp-file (cond-> {:force-reload? true}
                              serializer (assoc :serializer serializer)
                              deserializer (assoc :deserializer deserializer))))))
      (finally
        (delete-file!)))))

(defn build
  "Builds a file-backed dresser from a filename.

  Options:
  - :init-data - Initial data if file doesn't exist (default: nil)
  - :force-reload? - Reload from file after each transaction (default: false, mostly for tests)
  - :serializer - Function to serialize data to string (default: pr-str with encoding)
  - :deserializer - Function to deserialize string to data (default: edn/read-string with decoding)

  Examples:

    ;; Default EDN serialization
    (build \"my-data.edn\")

    ;; Using Transit JSON for faster serialization (1.5-2.5x for typical data, 5-6x for binary-heavy data)
    ;; Requires cognitect/transit-clj dependency
    (require '[cognitect.transit :as transit])
    (build \"my-data.json\"
           {:serializer   #(let [out (java.io.ByteArrayOutputStream.)
                                 writer (transit/writer out :json {})]
                             (transit/write writer (encode-all %))
                             (.toString out))
            :deserializer #(let [in (java.io.ByteArrayInputStream. (.getBytes %))
                                 reader (transit/reader in :json {})]
                             (decode-all (transit/read reader)))})

  Note: When using custom serialization, test your serializer/deserializer with
  the test-serialization helper to ensure all dresser functionality works correctly:

    (test-serialization my-serializer my-deserializer)"
  {:test (fn []
           ;; Test default EDN serialization
           (test-serialization nil nil))}
  ([filename]
   (build filename {}))

  ([filename opts]
   (let [opts (merge {:init-data     nil
                      :force-reload? false
                      :serializer    #(pr-str (encode-all %))
                      :deserializer  #(decode-all (edn/read-string %))}
                     opts)
         {:keys [init-data force-reload? serializer deserializer]} opts
         data (or (load-from-file filename deserializer) init-data)
         atom-dresser (at/build data)]
     (-> atom-dresser
         with-modification-tracking
         (dp/wrap-method `dp/transact wrap-transact filename opts)))))
