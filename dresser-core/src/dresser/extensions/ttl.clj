(ns dresser.extensions.ttl
  (:refer-clojure :exclude [> >= < <=])
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.durable-refs :as refs]
            [dresser.protocols :as dp])
  (:import (java.lang.ref WeakReference)
           (java.util Date WeakHashMap)
           (java.util.concurrent Executors TimeUnit)))

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
  "True if each argument is bigger than the succeeding one."
  ([x] true)
  ([x y] (pos? (compare x y)))
  ([x y & more]
   (if (> x y)
     (if (next more)
       (recur y (first more) (next more))
       (> y (first more)))
     false)))

(defn <
  "True if each argument is smaller than the succeeding one."
  ([x] true)
  ([x y] (neg? (compare x y)))
  ([x y & more]
   (if (< x y)
     (if (next more)
       (recur y (first more) (next more))
       (< y (first more)))
     false)))


(defn >=
  "True if each argument is bigger than or equal to the succeeding one."
  ([x] true)
  ([x y] (let [ret (compare x y)]
           (or (zero? ret)
               (pos? ret))))
  ([x y & more]
   (if (>= x y)
     (if (next more)
       (recur y (first more) (next more))
       (>= y (first more)))
     false)))


(defn <=
  "True if each argument is smaller than or equal to the succeeding one."
  ([x] true)
  ([x y] (let [ret (compare x y)]
           (or (zero? ret)
               (neg? ret))))
  ([x y & more]
   (if (<= x y)
     (if (next more)
       (recur y (first more) (next more))
       (<= y (first more)))
     false)))


(defn- ttl-id
  "Generates an ID prefixed with the expiration time in ms.
  This makes it easy to sort and leverage the fact that the ID
  field (primary key) is the most likely to be optimized/fast."
  [dresser inst]
  (db/tx-let [tx dresser]
      [id (db/gen-id! tx ttl-drawer)]
    (str (ms inst) ":" id)))

(defn- ttl-id->inst
  "Extracts the instant from the ttl-id."
  [ttl-id]
  (-> (re-find #"\d+" ttl-id)
      read-string
      (Date.)))

(defn upsert-expiration!
  "Sets the document expiration and returns the ref.
  nil removes any existing expiration.  The expiration is the time
  after which the document is *eventually* deleted.
  Returns the provided doc-ref."
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

(defn get-expiration
  "Returns the expiration #inst associated with doc-ref, or nil if it
doesn't have any."
  [dresser doc-ref]
  (db/tx-let [tx dresser]
      [?existing-ttl-id (refs/get-at tx doc-ref [ttl-field])]
    (some-> ?existing-ttl-id ttl-id->inst)))

(defn delete-expired!
  [dresser]
  ;; Delete targets
  (db/tx-> dresser
    (db/fetch-reduce ttl-drawer
                     (fn [tx {:keys [target id]}]
                       (-> (refs/delete! tx target)
                           (db/delete! ttl-drawer id)))
                     {:where {:id {db/lt (str (ms (now)))}}
                      :only  [:target :id]})))

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


(defn- create-scheduled-task
  [dresser delay-ms]
  (let [executor (Executors/newSingleThreadScheduledExecutor)
        weak-dresser (WeakReference. dresser)
        task (fn []
               (if-let [current-dresser (.get weak-dresser)]
                 (try
                   (delete-expired! current-dresser)
                   (catch Exception e
                     (println "Error in TTL cleanup task:" (.getMessage e))))
                 ;; If the dresser has been garbage collected, shut down the executor
                 (.shutdown executor)))
        future (.scheduleWithFixedDelay executor task 0 delay-ms TimeUnit/MILLISECONDS)]
    {:executor executor
     :future future}))

(defn- stop-scheduled-task
  [{:keys [executor future]}]
  (when future
    (.cancel future false))
  (when executor
    (.shutdown executor)))

(ext/defext ttl
  [delay-between-delete-ms]
  {:deps         [refs/durable-refs]
   :init-fn      #(db/with-system-drawers % [ttl-drawer])
   :wrap-configs (let [tasks (WeakHashMap.)]
                   {`dp/-start {:wrap (fn [start-method]
                                        (fn [dresser]
                                          (let [started-drs (start-method dresser)
                                                task (create-scheduled-task started-drs delay-between-delete-ms)]
                                            (.put tasks started-drs task)
                                            started-drs)))}
                    `dp/-stop  {:wrap (fn [stop-method]
                                        (fn [dresser]
                                          (when-let [task (.get tasks dresser)]
                                            (stop-scheduled-task task)
                                            (.remove tasks dresser))
                                          (stop-method dresser)))}})})
