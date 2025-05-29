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

(defn- encode-all
  "Encode both records and byte arrays in data structure"
  [data]
  (walk/postwalk (fn [x]
                   (-> x
                       enc/encode-record
                       enc/encode-bytes))
                 data))

(defn- decode-all
  "Decode both records and byte arrays in data structure"
  [data]
  (walk/postwalk (fn [x]
                   (-> x
                       enc/restore-record
                       enc/restore-bytes))
                 data))

(defn- load-from-file
  "Load data from file if it exists, nil otherwise"
  [filename]
  (when (.exists (io/file filename))
    (decode-all (edn/read-string (slurp filename)))))

(defn- save-to-file!
  "Save data to file atomically using a temp file"
  [filename data]
  (let [temp-file (str filename ".tmp")]
    (spit temp-file (pr-str (encode-all data)))
    (.renameTo (io/file temp-file) (io/file filename))))

(defn- reload!
  "Deletes every document in the dresser, then populates it back using
  data from the file."
  [tx filename]
  (db/tx-let [tx tx]
      [_ (-> (db/all-drawers tx)
             (db/reduce-tx db/drop!))
       data (load-from-file filename)
       load-drawer! (fn [tx [drawer id->docs]]
                      (db/upsert-many! tx drawer (vals id->docs)))]
    (db/reduce-tx tx load-drawer! data)))

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
  Data from file has priority over `init-data` (which is only used if file doesn't exist).
  `force-reload?` reloads from file at each transaction (mostly for tests)."
  {:test (fn []
           (let [temp-file (str (gensym "test") ".edn")
                 delete-file! #(when (.exists (io/file temp-file))
                                 (.delete (io/file temp-file)))]
             (try
               (dt/test-impl
                (fn []
                  (delete-file!)
                  (dt/no-tx-reuse (build temp-file nil :force-reload))))
               (finally
                 (when (.exists (io/file temp-file))
                   (.delete (io/file temp-file)))))))}
  ([filename]
   (build filename nil))

  ([filename init-data]
   (build filename init-data false))

  ([filename init-data force-reload?]
   (let [data (or (load-from-file filename) init-data)
         atom-dresser (at/build data)]
     (vary-meta atom-dresser
                (fn [m]
                  (update m `dp/-transact #(wrap-transact % filename force-reload?)))))))
