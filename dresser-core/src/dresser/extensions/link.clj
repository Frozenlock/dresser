(ns dresser.extensions.link
  (:require [dresser.base :as db]
            [dresser.protocols :as dp]
            [dresser.impl.hashmap :as hm]
            [dresser.impl.atom :as at]
            [dresser.drawer :as dd]
            [dresser.test :as dt]))


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




(defn- get-src [dresser src-id]
  (get-in dresser [:srcs src-id :src]))

(defn- assoc-src [dresser src-id src]
  (assoc-in dresser [:srcs src-id :src] src))



;; We need to pass the dresser state up the chain of child transactions.
;; This can be done by storing the dresser state inside the result of the transaction.
(defn- nested-transact
  [dresser f src-keys]
  (let [src1-key (first src-keys)
        remaining-keys (rest src-keys)]
    (db/with-tx [src-tx (get-src dresser src1-key) {:result? false}]
      (if (empty? remaining-keys)
        (let [dresser' (f (assoc-src dresser src1-key src-tx))]
          (db/with-result (get-src dresser' src1-key)
            {:dresser dresser'}))
        (let [src2' (nested-transact (assoc-src dresser src1-key src-tx) f remaining-keys)
              src2-key (first remaining-keys)
              {:keys [dresser]} (db/result src2')]
          (db/with-result (get-src dresser src1-key)
            {:dresser (assoc-src dresser src2-key (db/with-result src2' nil))}))))))

(dp/defimpl -transact
  [dresser f {:keys [result?]}]
  (if (:transact dresser)
    (f dresser)
    (let [src-keys (keys (:srcs dresser))
          src1-key (first src-keys)
          src1' (nested-transact (assoc dresser :transact true) f src-keys)
          {:keys [dresser]} (db/result src1')
          return (assoc-src dresser src1-key (db/with-result src1' nil))]
      (if result?
        (db/result return)
        (dissoc return :transact)))))





(defn dispatch
  "Returns the src-dresser for the target-drawer
  In order of precedence:
  1. Matching the drawer tests
  2. Already exists in a dresser
  3. Fallback on the first dresser."
  [dresser target-drawer]
  (db/tx-let [tx dresser]
      ;; First try with the registry
      [src-id (some (fn [[src-id {:keys [dispatch]}]]
                      (when (some #(if (fn? %)
                                     (% target-drawer)
                                     (= % target-drawer)) dispatch)
                        src-id))
                    (:srcs dresser))
       ;; Try to find a dresser that already has the target-drawer.
       ;; src-id (or src-id
       ;;            (let [{:keys [src1 src2]} tx
       ;;                  [src1' src1-has?] (db/dr (db/has-drawer? src1 target-drawer))
       ;;                  [src2' src2-has?] (if src1-has?
       ;;                                      [src2 :dont-care]
       ;;                                      (db/dr (db/has-drawer? src2 target-drawer)))]
       ;;              (db/with-result (assoc tx :src1 src1 :src2 src2)
       ;;                (cond
       ;;                  src1-has? :src1
       ;;                  src2-has? :src2
       ;;                  :else nil))))
       ;; If this fails, just pick the first one in the registry.
       src-id (or src-id (ffirst (:srcs dresser)))]
    (db/with-result tx src-id)))

(defn wrapped-add
  [dresser drawer data]
  (let [[dresser dispatch-key] (db/dr (dispatch dresser drawer))
        _ (def bbb dresser)
        target-dresser (get-src dresser dispatch-key)
        child-tx (dp/-add target-dresser drawer data)
        result (db/result child-tx)]
    (-> (assoc-src dresser dispatch-key child-tx)
        (db/with-result result))))



(defn append-dresser
  [link-dresser child-dresser dispatch-test]
  (let [counter (inc (:counter link-dresser 0))]
    (-> (assoc link-dresser :counter counter)
        (assoc-in [:srcs counter]
                  {:src      child-dresser
                   :dispatch dispatch-test})
        (db/with-result counter))))

(defn build
  [registry]
  (let [dresser (vary-meta {}
                           (fn [m]
                             (-> (assoc m :type ::db/dresser)
                                 (assoc `dp/-transact -transact)
                                 (assoc `dp/-add wrapped-add)
                                 (assoc `dp/-with-temp-data hm/-with-temp-data)
                                 (assoc `dp/-temp-data hm/-temp-data))))]
    (reduce (fn [m [src dispatch-test]]
              (append-dresser m src dispatch-test))
            dresser
            registry)))


(comment
  (db/add! dresser2 :hello {:doc 1})

  (db/tx-let [tx (build registry) {:result? false}]
      [user-id (db/add! tx :users {:username "Bob"})
       invoice-id (db/add! tx :invoices {:amount 10})
       hello-id (db/add! tx :hello {:msg "bonjour!"})]
    (db/with-result tx
      {:hello-id   hello-id
       :invoice-id invoice-id
       :user-id    user-id})))

(comment
  (db/raw-> (build {dresser1 [:invoices]
                    dresser2 [:users]}) (db/add! :users {:doc 1}) (db/add! :invoices {:doc 2}))
  )
