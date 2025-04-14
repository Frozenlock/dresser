(ns dresser.extensions.async-tx
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.protocols :as dp])
  (:import (java.util.concurrent LinkedBlockingQueue TimeUnit)))

(defprotocol IAsyncTx
  :extend-via-metadata true
  (-start-tx [dresser opts] "Starts an async transaction.")
  (-cancel-tx [dresser opts] "Cancels an async transaction.")
  (-end-tx [dresser opts] "Ends client participation in an async transaction."))

; TODO: When manually starting a tx from inside an active TX, the
; active TX should block (not end/commit) until the manually started
; one is ended

(def ^:dynamic *tx-states* {})

(defn nested-tx?
  "True if already inside an evaluation of tx-id"
  [tx-id]
  (pos? (get *tx-states* tx-id 0)))

(defn start-tx!
  "Start an async transaction. Returns dresser with transaction context.

   With :client-id in opts, registers that client as using the transaction.
   Without :client-id, uses the default client.

   Multiple clients can use the same transaction concurrently. Each client
   must eventually call end-tx! or cancel-tx!. The transaction only
   finalizes when all clients have deregistered via end-tx!."
  ([dresser] (start-tx! dresser {}))
  ([dresser opts]
   (-start-tx dresser (update opts :result? #(or % false)))))

(defn cancel-tx!
  "Cancel an async transaction.

   Cancels the entire transaction regardless of active clients."
  ([dresser] (cancel-tx! dresser {}))
  ([dresser opts]
   (-cancel-tx dresser (update opts :result? #(or % false)))))

(defn end-tx!
  "End a client's participation in an async transaction.

   With :client-id in opts, deregisters that specific client from the transaction.
   Without :client-id, deregisters the default client.

   The transaction only truly commits when all registered clients have
   deregistered by calling end-tx!. Until then, the transaction remains
   open and active.

   This behavior enables safe sharing of transaction context across multiple
   components that shouldn't be responsible for managing each other's
   transaction lifecycle."
  ([dresser] (end-tx! dresser {}))
  ([dresser opts]
   (-end-tx dresser (update opts :result? #(or % false)))))



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
  [dresser *tx-state queue tx-method opts tx-id]
  (let [[old _new] (swap-vals! *tx-state
                               (fn [tx-state]
                                 (if (:initialized? tx-state)
                                   tx-state
                                   (let [default-client-id (keyword (gensym "default-client-"))]
                                     (merge tx-state
                                            {:initialized?      true
                                             :default-client-id default-client-id
                                             :clients           #{default-client-id}})))))]
    (when-not (:initialized? old)
      (future
        (try
          (when-let [{:keys [init-dresser] :as q} (.poll queue 30000 TimeUnit/MILLISECONDS)]
            (.put queue q)
            (let [ended-dresser (execute-transaction init-dresser tx-method tx-id queue opts *tx-state)]
              (handle-transaction-result ended-dresser)))
          (catch Exception e
            (handle-transaction-error e *tx-state queue dresser tx-id)))))
       dresser))

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

      (not (:initialized? @*tx))
      (-> (start-tx! dresser)
          (db/transact! f opts)
          (end-tx! opts))

      (nested-tx? tx-id)
      (f dresser)

      :else
      (let [f (bound-fn [tx] (f (db/with-temp-data tx (db/temp-data dresser))))
            *result (promise)]
        (.put queue {:tx-fn (fn [tx]
                              (execute-transaction-step tx f tx-id *result *tx queue))
                     :init-dresser dresser
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

   Provides transaction management with client tracking for safer shared
   transaction contexts. Multiple components can share a transaction context
   where the transaction only finalizes when all participants have completed.

   Enables the use of:
   - start-tx!  - Begin transaction and optionally register a client
   - cancel-tx! - Immediately cancel transaction (all clients)
   - end-tx!    - Deregister a client; commits when all clients done

   Example usage with multiple clients:
   ```
   (def dresser (async-tx (atom-dresser)))

   ;; Start transaction with default client
   (def tx (start-tx! dresser))

   ;; Register additional client
   (def tx (start-tx! tx {:client-id :file-reader}))

   ;; Work with transaction
   (add! tx :users {:id 1 :name \"Alice\"})

   ;; First client done
   (def tx (end-tx! tx {:client-id :file-reader}))

   ;; Default client done - only NOW does transaction actually commit
   (end-tx! tx)
   ```"
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

                ?end-method (get m `-end-tx)
                end-tx (fn [dresser opts]
                         (cond
                           (not (:initialized? @*tx-state))
                           (throw (ex-info "Can't end unstarted transaction" {}))

                           ;; (nested-tx? tx-id)
                           ;; (throw (ex-info "End-tx cannot occur from within a transaction" {}))

                           :else
                           (let [client-id (:client-id opts)
                                 ;; If no client-id provided, use the default
                                 client-id (or client-id (:default-client-id @*tx-state))
                                 clients (:clients @*tx-state)
                                 ;; Remove this client from the set of active clients
                                 remaining-clients (disj clients client-id)
                                 last-client? (empty? remaining-clients)]

                             ;; Update clients set
                             (swap! *tx-state assoc
                                    :clients remaining-clients
                                    :initialized? (if last-client? false true))

                             ;; Only actually commit if this was the last client
                             (if last-client?
                               (let [*return (promise)
                                     f (fn [tx]
                                         (->> (if ?end-method
                                                (let [r (?end-method tx opts)]
                                                  (db/update-result r :result))
                                                tx)
                                              (deliver *return)))]
                                 (.put queue {:tx-fn f
                                              :init-dresser dresser
                                              :step  :end})
                                 (let [r (deref *return (:timeout-ms opts 1000) ::timeout)]
                                   (when (= r ::timeout)
                                     (throw (ex-info "Timeout while committing" {})))
                                   r))
                               ;; Not the last client, just return the dresser
                               dresser))))

                ?cancel-method (get m `-cancel-tx)
                cancel (fn [dresser opts]
                         (if-not (:initialized? @*tx-state)
                           dresser
                           (do
                             ;; Clear all clients - ensure a clean cancellation
                             (swap! *tx-state assoc :clients #{} :initialized? false)
                             (let [*return (promise)
                                   f (fn [_tx]
                                       (throw (ex-info "Transaction cancelled" {:cancel-fn #(deliver *return %)
                                                                                :cancel-id tx-id})))
                                   _ (.put queue {:tx-fn f
                                                  :init-dresser dresser
                                                  :step  :cancel})
                                   ret (deref *return (:timeout-ms opts 1000) ::timeout)]
                               (.clear queue)
                               (if (= ret ::timeout)
                                 (throw (ex-info (str "Timeout while cancelling " tx-id) {}))
                                 ret)))))

                ?start-method (get m `-start-tx)
                start (fn [dresser opts]
                        (let [tx (cond-> dresser
                                   ?start-method (?start-method opts)
                                   true (initialize-transaction *tx-state queue tx-method opts tx-id))
                              ;; Handle client registration if tx is initialized
                              client-id (:client-id opts)]
                          ;; If a specific client ID was provided, register it
                          (when (and client-id (:initialized? @*tx-state))
                            (swap! *tx-state update :clients conj client-id))
                          tx))]

            (merge
             m
             {`-start-tx start
              `-end-tx end-tx
              `-cancel-tx cancel
              `dp/-transact
              (fn [tx f opts]
                (let [tx' (long-lived-tx tx
                                         f
                                         {:*tx   *tx-state
                                          :tx-id tx-id
                                          :queue queue}
                                         (assoc opts :result? false))]
                  (if (and (zero? (get *tx-states* tx-id 0))
                           (:result? opts))
                    (db/result tx')
                    tx')))}))))))})
