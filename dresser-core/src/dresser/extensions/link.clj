(ns dresser.extensions.link
  (:require [dresser.base :as db]
            [dresser.protocols :as dp]
            [dresser.impl.hashmap :as hm]
            [dresser.impl.atom :as at]
            [dresser.test :as dt]
            [dresser.extension :as ext]))


;; TODO: should we use locks? Or add a `transact` option to NOT retry
;; a transaction such that any failure could bubble up the chain to avoid
;; duplicate actions.

{'dresser1 ^:closed []
 'dresser2 [:invoices
            #(clojure.string/starts-with? (name %) "acme-")]
 'dresser3 [:users
            :memberships]}

;;; base/drawer-key could attach a path to the drawer. This path would
;;; then be used by the dispatcher to know which sub-dresser is
;;; needed.


;; (defn link
;;   [registry]
;;   (assert (every? db/dresser? (map first registry))
;;           "All keys in the registry should be dressers.")
;;   (assert (every? #(or (fn? %)
;;                        (keyword? %)) (mapcat second registry))
;;           "Drawer tests should be a function or a keyword.")
;;   {:registry registry})


(let [*counter (atom nil)
      patch nil
      ;; (dp/mapify-impls [(dp/impl -gen-id
      ;;                     [dresser drawer]
      ;;                     (db/with-result dresser
      ;;                       (swap! *counter (fnil inc 0))))])
      dresser1 (vary-meta (hm/build) merge patch)
      dresser2 (vary-meta (hm/build) merge patch)
      dresser3 (vary-meta (hm/build) merge patch)]
  (def registry [[dresser1 [:invoices]]
                 [dresser2 [:users]]
                 [dresser3 [:emails]]]))




;; (defn- get-src [dresser src-id]
;;   (get-in dresser [:srcs src-id :src]))

;; (defn- assoc-src [dresser src-id src]
;;   (assoc-in dresser [:srcs src-id :src] src))



;; ;; We need to pass the dresser state up the chain of child transactions.
;; ;; This can be done by storing the dresser state inside the result of the transaction.
;; (defn- nested-transact
;;   [dresser f src-keys]
;;   (let [src1-key (first src-keys)
;;         remaining-keys (rest src-keys)]
;;     (db/with-tx [src-tx (get-src dresser src1-key) {:result? false}]
;;       (if (empty? remaining-keys)
;;         (let [dresser' (f (assoc-src dresser src1-key src-tx))]
;;           (db/with-result (get-src dresser' src1-key)
;;             {:dresser dresser'}))
;;         (let [src2' (nested-transact (assoc-src dresser src1-key src-tx) f remaining-keys)
;;               src2-key (first remaining-keys)
;;               {:keys [dresser]} (db/result src2')]
;;           (db/with-result (get-src dresser src1-key)
;;             {:dresser (assoc-src dresser src2-key (db/with-result src2' nil))}))))))

;; (dp/defimpl -transact
;;   [dresser f {:keys [result?]}]
;;   (if (:transact dresser)
;;     (f dresser)
;;     (let [src-keys (keys (:srcs dresser))
;;           src1-key (first src-keys)
;;           src1' (nested-transact (assoc dresser :transact true) f src-keys)
;;           {:keys [dresser]} (db/result src1')
;;           return (assoc-src dresser src1-key (db/with-result src1' nil))]
;;       (if result?
;;         (db/result return)
;;         (dissoc return :transact)))))





;; (defn dispatch
;;   "Returns the src-dresser for the target-drawer
;;   In order of precedence:
;;   1. Matching the drawer tests
;;   2. Already exists in a dresser
;;   3. Fallback on the first dresser."
;;   [dresser target-drawer]
;;   (db/tx-let [tx dresser]
;;       ;; First try with the registry
;;       [src-id (some (fn [[src-id {:keys [dispatch]}]]
;;                       (when (some #(if (fn? %)
;;                                      (% target-drawer)
;;                                      (= % target-drawer)) dispatch)
;;                         src-id))
;;                     (:srcs dresser))
;;        ;; Try to find a dresser that already has the target-drawer.
;;        src-id (or src-id
;;                   (let [{:keys [srcs]} tx]
;;                     (reduce (fn [tx [src-id {:keys [src]}]]
;;                               (let [[src drawer?] (db/dr (db/has-drawer? src target-drawer))]
;;                                 (cond-> (assoc-src tx src-id src)
;;                                   drawer? (db/with-result src-id)
;;                                   drawer? reduced)))
;;                             tx srcs)))
;;        ;; If this fails, just pick the first one in the registry.
;;        src-id (or src-id (ffirst (:srcs dresser)))]
;;     (db/with-result tx src-id)))


;; (defn drawer-dispatch-wrapper
;;   "Warning: only applies to methods with 'drawer' argument."
;;   [method]
;;   (fn [tx drawer & args]
;;     ;; Assume we are inside a transaction
;;     (let [[tx dispatch-key] (db/dr (dispatch tx drawer))
;;           child-tx (apply method (get-src tx dispatch-key) drawer args)
;;           result (db/result child-tx)]
;;       (-> (assoc-src tx dispatch-key child-tx)
;;           (db/with-result result)))))



;; (defn append-dresser
;;   [link-dresser child-dresser dispatch-test]
;;   (let [counter (inc (:counter link-dresser 0))]
;;     (-> (assoc link-dresser :counter counter)
;;         (assoc-in [:srcs counter]
;;                   {:src      child-dresser
;;                    :dispatch dispatch-test})
;;         (db/with-result counter))))

;; (defn- methods-with-drawer
;;   "Returns all the symbols for which the associated method has 'drawer'
;;   as the 2nd argument."
;;   []
;;   (for [[sym m] dp/dresser-methods
;;         :when (= (second (:args m)) 'drawer)]
;;     sym))


;; ;; Can't use 'ext/defext' because we overwrite `-transact` and `-temp-data`.
;; (defn build
;;   [registry]
;;   ;; Note: the order in the registry has some importance.  The last
;;   ;; dresser will be the innermost, meaning its transaction will be
;;   ;; the first to commit.
;;   (let [dresser (vary-meta {}
;;                            (fn [m]
;;                              (merge (assoc m :type ::db/dresser)
;;                                     (into {}
;;                                           (for [sym (methods-with-drawer)
;;                                                 :let [method (resolve sym)]]
;;                                             [sym (drawer-dispatch-wrapper method)]))
;;                                     {`dp/-transact -transact
;;                                      `dp/-with-temp-data hm/-with-temp-data
;;                                      `dp/-temp-data hm/-temp-data})))]
;;     (reduce (fn [m [src dispatch-test]]
;;               (append-dresser m src dispatch-test))
;;             dresser
;;             registry)))


;; (comment
;;   (db/add! dresser2 :hello {:doc 1})

;;   (db/tx-let [tx (build registry) {:result? false}]
;;       [user-id (db/add! tx :users {:username "Bob"})
;;        invoice-id (db/add! tx :invoices {:amount 10})
;;        hello-id (db/add! tx :hello {:msg "bonjour!"})]
;;     {:hello-id   hello-id
;;      :invoice-id invoice-id
;;      :user-id    user-id}))

;; (comment
;;   (let [dresser1 (hm/build)
;;         dresser2 (hm/build)]
;;     (db/tx-let [tx (build [[dresser1 [:invoices]]
;;                            [dresser2 [:users]]])
;;                 {:result? false}]
;;         [user1 (db/add! tx :users {:doc 1})
;;          invoice1 (db/add! tx :invoices {:doc 2})]
;;       (db/delete! tx :invoices invoice1)
;;       ))


;;   (let [dresser1 (at/build)
;;         dresser2 (at/build)]
;;     (db/tx-let [tx (build [[dresser1 [:invoices]]
;;                            [dresser2 [:users]]])
;;                 {:result? false}]
;;         [user1 (db/add! tx :users {:doc 1})
;;          invoice1 (db/add! tx :invoices {:doc 2})]
;;       (db/delete! tx :invoices invoice1)
;;       ))
;;   )

;; (comment
;;   (def invoices-db
;;     (let [*db (at/build (dt/no-tx-reuse (hm/build)))]
;;       (db/add! *db :invoices {:invoice 1})
;;       *db))
;;   (def users-db
;;     (let [*db (dt/no-tx-reuse (at/build))]
;;       (db/add! *db :users {:name "Bob"})
;;       *db))

;;   (def merged-db
;;     (build [[invoices-db []]
;;             [users-db []]])))
