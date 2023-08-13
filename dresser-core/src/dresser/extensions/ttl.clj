(ns dresser.extensions.ttl
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.durable-refs :as refs]
            [dresser.protocols :as dp])
  (:refer-clojure :exclude [> >= < <=])
  (:import [java.util Date]))

;; Rather than storing the expiration as a field in the target
;; document, we store the document reference and the expiration in a
;; different drawer (ttl-drawer). This allows for a much easier
;; retrieval as we fetch on a single drawer rather than searching all
;; documents everywhere.
;;
;; To further increase speed, the expiration time is encoded in the
;; ttl-drawer document ID.  Searching for expired documents is thus
;; searching for IDs 'smaller than'. This takes advantage of the fact
;; that the ID (the primary key) is the most likely field to be
;; optimized. With the Codax implementation, this optimization lowers
;; the time taken to search 20k documents from ~500ms to <5ms.

(def ttl-drawer :drs_ttl)
(def ttl-field :drs_ttl)

(defn ms [inst]
  (.getTime inst))

(defn add-ms
  [inst ms-to-add]
  (let [millis (ms inst)]
    (Date. (+ millis ms-to-add))))

(defn now
  ([] (java.util.Date.))
  ([ms-to-add]
   (add-ms (now) ms-to-add)))

(defn secs [x] (* 1000 x))
(defn mins [x] (* (secs 60) x))
(defn hours [x] (* (mins 60) x))
(defn days [x] (* (hours 24) x))
(defn weeks [x] (* (days 7) x))

(defn now+secs [x] (now (secs x)))
(defn now+mins [x] (now (mins x)))
(defn now+hours [x] (now (hours x)))
(defn now+days [x] (now (days x)))
(defn now+weeks [x] (now (weeks x)))

(defn >
  "True if this is bigger than that."
  [this that]
  (pos? (compare this that)))

(defn <
  "True if this is smaller than that."
  [this that]
  (neg? (compare this that)))

(defn >=
  "True if this is bigger or equal to that."
  [this that]
  (let [ret (compare this that)]
    (or (zero? ret)
        (pos? ret))))

(defn <=
  "True if this is smaller or equal to that."
  [this that]
  (let [ret (compare this that)]
    (or (zero? ret)
        (pos? ret))))

(defn- ttl-id
  "Generates an ID prefixed with the expiration time in ms.
  This makes it easy to sort and leverage the fact that the ID
  field (primary key) is the most likely to be optimized/fast."
  [dresser inst]
  (db/tx-let [tx dresser]
      [id (db/gen-id! tx ttl-drawer)]
      (str (ms inst) ":" id)))

(defn upsert-expiration!
  "Sets the document expiration and returns the ref.
  nil removes any existing expiration.  The expiration is the time
  after which the document is *eventually* deleted."
  [dresser doc-ref inst]
  (assert (or (nil? inst)
              (inst? inst)))
  (db/tx-let [tx dresser]
      [new-ttl-id (when inst (ttl-id tx inst))
       ?existing-ttl-id (refs/get-at tx doc-ref [ttl-field])
       _ (db/delete! tx ttl-drawer ?existing-ttl-id)]
    (-> (if (nil? inst)
          (refs/dissoc-at! tx doc-ref [] ttl-field)
          (-> tx
              (db/upsert! ttl-drawer {:id new-ttl-id :target doc-ref})
              (refs/assoc-at! doc-ref [ttl-field] new-ttl-id)))
        (db/with-result doc-ref))))

;; TODO: convert into multiple transactions.
;; Perhaps a batched lazy-fetch, fetching 500 items at a time?
(defn delete-expired!
  [dresser]
  (db/tx-let [tx dresser]
      [ttl-docs (db/fetch tx ttl-drawer {:where {:id {db/lt (str (ms (now)))}}
                                         :only  [:target :id]})]
    ;; This reduce is kind of a pain.
    ;; Perhaps a macro to make this easier?
    (reduce (fn [tx' {:keys [target id]}]
              (-> (refs/delete! tx' target)
                  (db/delete! ttl-drawer id)))
            tx ttl-docs)))

(defn add-with-ttl!
  "Adds a document with a time to live and returns its ref.
  The time to live sets the time after which the document is
  *eventually* deleted."
  [dresser drawer data ttl-ms]
  (db/tx-let [tx dresser]
      [new-ttl-id (ttl-id tx (now ttl-ms))
       doc-ref (refs/add! tx drawer (assoc data ttl-field new-ttl-id))
       _ (db/upsert! tx ttl-drawer {:id new-ttl-id :target doc-ref})]
    doc-ref))

(ext/defext ttl
  [delay-between-delete-ms]
  {:deps         [refs/keep-sync]
   :init-fn      #(db/with-system-drawers % [ttl-drawer])
   :wrap-configs (let [*dresser (atom nil)]
                   {`dp/-start {:wrap (fn [start-method]
                                        (fn [dresser]
                                          (let [started-drs (start-method dresser)]
                                            ;; TODO: replace by a proper scheduler?
                                            (reset! *dresser started-drs)
                                            (future (while @*dresser
                                                      (try (delete-expired! @*dresser)
                                                           (catch Exception _e))
                                                      (Thread/sleep delay-between-delete-ms)))
                                            started-drs)))}
                    `dp/-stop  {:wrap (fn [stop-method]
                                        (fn [dresser]
                                          (reset! *dresser nil)
                                          (stop-method dresser)))}})})
