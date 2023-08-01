(ns dresser.extensions.ttl
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.durable-refs :as refs]
            [dresser.protocols :as dp])
  (:refer-clojure :exclude [> >= < <=])
  (:import [java.util Date]))

;; Rather than storing the expiration as a field in the target
;; document, we store the document reference and the expiration in a
;; different drawer. This allows for a much easier retrieval. (Fetch
;; on a single drawer rather than searching all documents everywhere.)

;; This is still a bad design IMO, fetch/where is slow. Consider
;; adding 'partitioned drawers' for much faster resolution.

(def ttl-drawer :drs_ttl)

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

(defn upsert-expiration!
  "Sets the document expiration and returns the ref.
  nil removes any existing expiration."
  [dresser doc-ref inst]
  (assert (or (nil? inst)
              (inst? inst)))
  (db/tx-let [tx dresser]
      [ttl-id (str "drs_ttl-" (refs/doc-id doc-ref))] ; deterministic ID from doc-ref
    (-> (if (nil? inst)
          (db/delete! tx ttl-drawer ttl-id)
          (db/upsert! tx ttl-drawer {:id ttl-id, :exp inst, :target doc-ref}))
        (db/with-result doc-ref))))

;; TODO: convert into multiple transactions.
;; Perhaps a batched lazy-fetch, fetching 500 items at a time?
(defn delete-expired!
  [dresser]
  (db/tx-let [tx dresser]
      [ttl-docs (db/fetch tx ttl-drawer {:where {:exp {db/lt (now)}}
                                         :only  [:target :id]})]
    ;; This reduce is kind of a pain.
    ;; Perhaps a macro to make this easier?
    (reduce (fn [tx' {:keys [target id]}]
              (-> (refs/delete! tx' target)
                  (db/delete! ttl-drawer id)))
            tx ttl-docs)))

(defn add-with-ttl!
  [dresser drawer data ttl-ms]
  (db/tx-let [tx dresser]
      [d-ref (refs/add! tx drawer data)]
    (-> (upsert-expiration! tx d-ref (now ttl-ms))
        (db/with-result d-ref))))

(ext/defext ttl
  [delay-between-delete-ms]
  :deps  [refs/keep-sync]
  :init-fn #(db/with-system-drawers % [ttl-drawer])
  :wrap-configs (let [*dresser (atom nil)]
                  {`dp/-start {:post (fn [dresser]
                                       ;; TODO: replace by a proper scheduler?
                                       (reset! *dresser dresser)
                                       (future (while @*dresser
                                                 (try (delete-expired! @*dresser)
                                                      (catch Exception _e))
                                                 (Thread/sleep delay-between-delete-ms)))
                                       dresser)}
                   `dp/-stop  {:pre (fn [dresser]
                                      (reset! *dresser nil)
                                      dresser)}}))
