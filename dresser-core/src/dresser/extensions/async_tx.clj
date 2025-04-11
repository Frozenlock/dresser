(ns dresser.extensions.async-tx
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.protocols :as dp])
  (:import (java.util.concurrent LinkedBlockingQueue TimeUnit)))

(defprotocol IAsyncTx
  :extend-via-metadata true
  (-start-tx [dresser opts] "Starts an async transaction.")
  (-cancel-tx [dresser opts] "Cancels an async transaction.")
  (-commit-tx [dresser opts] "Commits an async transaction."))

(def ^:dynamic *tx-states* {})

(defn nested-tx?
  "True if already inside an evaluation of tx-id"
  [tx-id]
  (pos? (get *tx-states* tx-id 0)))

(defn start-tx!
  ([dresser] (start-tx! dresser {:result? false}))
  ([dresser opts]
   (-start-tx dresser opts)))

(defn cancel-tx!
  ([dresser] (cancel-tx! dresser {:result? false}))
  ([dresser opts]
   (-cancel-tx dresser opts)))

(defn commit-tx!
  ([dresser] (commit-tx! dresser {:result? false}))
  ([dresser opts]
   (-commit-tx dresser opts)))

(defn- process-transaction
  [tx tx-id queue opts *tx]
  (loop [tx tx]
    (let [timeout-ms (:timeout-ms opts 1000)
          {:keys [tx-fn step]} (.poll queue timeout-ms TimeUnit/MILLISECONDS)]
      (cond
        (nil? tx-fn)
        (throw (ex-info "TX timeout" {:err ::timeout :tx-id tx-id}))

        (= :end step)
        (do (reset! *tx nil)
            (db/update-result
             tx
             (fn [r]
               {:result r
                :end-fn tx-fn})))

        (fn? tx-fn)
        (let [new-tx (tx-fn tx)]
          (recur new-tx))

        :else
        (throw (ex-info "Invalid queue item" {:item tx-fn :tx-id tx-id}))))))

(defn- execute-transaction
  [dresser tx-method tx-id queue opts *tx]
  (let [result (binding [*tx-states* (update *tx-states* tx-id (fnil inc 0))]
                 (tx-method
                  dresser
                  (fn [tx]
                    (process-transaction tx tx-id queue opts *tx))
                  opts))]
    result))

(defn- handle-transaction-result
  [ended-dresser]
  (let [{:keys [result end-fn]} (db/result ended-dresser)]
    (if end-fn
      (end-fn (db/with-result ended-dresser result))
      ended-dresser)))

(defn- handle-transaction-error
  [e *tx queue dresser tx-id]
  (let [{:keys [tx-fn step]} (.poll queue)]
    (reset! *tx nil)

    (when tx-fn
      (tx-fn e))

    (let [{:keys [cancel-fn cancel-id]} (ex-data e)]
      (if (= cancel-id tx-id)
        (cancel-fn dresser)
        (throw e)))))

(defn- initialize-transaction
  [dresser *tx queue tx-method opts tx-id]
  (if (:initialized? @*tx)
    dresser
    (do
      (swap! *tx assoc :initialized? true)
      (future
        (try
          (when-let [{:keys [step] :as q} (.poll queue 30000 TimeUnit/MILLISECONDS)]
            (.put queue q)
            (let [ended-dresser (execute-transaction dresser tx-method tx-id queue opts *tx)]
              (handle-transaction-result ended-dresser)))
          (catch Exception e
            (handle-transaction-error e *tx queue dresser tx-id))))
      dresser)))

(defn- execute-transaction-step
  [tx f tx-id *result *tx queue]
  (if (instance? Exception tx)
    (deliver *result {::err tx})
    (try
      (let [tx' (f tx)]
        (deliver *result tx')
        tx')
      (catch Exception e
        (.put queue {:tx-fn (fn [e] (deliver *result {::err e}))})
        (throw e)))))

(defn- long-lived-tx
  [dresser f {:keys [queue tx-id *tx step]} opts]
  (let [timeout-ms (:timeout-ms opts 1000)
        step (or step :continue)]
    (cond

      ;; Would probably be more efficient to do a non-async
      ;; transaction when it isn't required.
      (not (:initialized? @*tx))
      (-> (start-tx! dresser)
          (db/transact! f opts)
          (commit-tx! opts))

      (nested-tx? tx-id)
      (f dresser)

      :else
      (let [f (bound-fn [tx] (f (db/with-temp-data tx (db/temp-data dresser))))
            *result (promise)]
        (.put queue {:tx-fn (fn [tx]
                              (execute-transaction-step tx f tx-id *result *tx queue))
                     :step step})
        (let [new-tx (deref *result timeout-ms {::err (ex-info "Result timeout" {:tx-id tx-id})})]
          (if-let [err (::err new-tx)]
            (if-let [*dresser (::*dresser (ex-data err))]
              @*dresser
              (throw err))
            (if (db/immutable? dresser)
              new-tx
              (-> dresser
                  (db/with-temp-data (db/temp-data new-tx))
                  (db/with-result (db/result new-tx))))))))))

(ext/defext async-tx
  "Creates an async transaction wrapper around a dresser.
  Enables the use of
  - start-tx!
  - cancel-tx!
  - commit-tx!"
  []
  {;:throw-on-reuse? true
   :init-fn
   (fn [dresser]
     (let [*tx-state (atom {})
           tx-id (keyword (gensym "tx-"))
           queue (LinkedBlockingQueue.)]
       (vary-meta
        dresser
        (fn [m]
          (let [tx-method (get m `dp/-transact)

                ?commit-method (get m `-commit-tx)
                commit (fn [dresser opts]
                         (cond
                           (not (:initialized? @*tx-state))
                           (throw (ex-info "Can't commit unstarted transaction" {}))

                           (nested-tx? tx-id)
                           (throw (ex-info "Commit cannot occur from within a transaction" {}))

                           :else
                           (let [*return (promise)
                                 f (fn [tx]
                                     (->> (if ?commit-method
                                            (let [r (?commit-method tx opts)]
                                              (db/update-result r :result))
                                            tx)
                                          (deliver *return)))]
                             (.put queue {:tx-fn f
                                          :step  :end})
                             (let [r (deref *return (:timeout-ms opts 1000) ::timeout)]
                               (when (= r ::timeout)
                                 (throw (ex-info "Timeout while committing" {})))
                               r))))

                ?cancel-method (get m `-cancel-tx)
                cancel (fn [dresser opts]
                         (if-not (:initialized? @*tx-state)
                           dresser
                           (let [*return (promise)
                                 f (fn [_tx]
                                     (throw (ex-info "Transaction cancelled" {:cancel-fn #(deliver *return %)
                                                                              :cancel-id tx-id})))
                                 _ (.put queue {:tx-fn f
                                                :step  :cancel})
                                 ret (deref *return (:timeout-ms opts 1000) ::timeout)]
                             (.clear queue)
                             (if (= ret ::timeout)
                               (throw (ex-info (str "Timeout while cancelling " tx-id) {}))
                               ret))))

                ?start-method (get m `-start-tx)
                start (fn [dresser opts]
                        (cond-> dresser
                          ?start-method (?start-method opts)
                          true (initialize-transaction *tx-state queue tx-method opts tx-id)))]

            (merge
             m
             {`-start-tx start
              `-commit-tx commit
              `-cancel-tx cancel
              :started? *tx-state
              `dp/-transact
              (fn [tx f opts]
                (if-not (:initialized? @*tx-state)
                  (tx-method tx f opts)
                  (let [tx' (long-lived-tx tx
                                           f
                                           {:*tx   *tx-state
                                            :tx-id tx-id
                                            :queue queue}
                                           (assoc opts :result? false))]
                    (if (and (zero? (get *tx-states* tx-id 0))
                             (:result? opts))
                      (db/result tx')
                      tx'))))}))))))})
