(ns dresser.wrap-test
  (:require [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [dresser.base :as db]
            [dresser.impl.atom :as at]
            [dresser.impl.hashmap :as hm]
            [dresser.protocols :as dp]
            [dresser.test :as dt]
            [dresser.wrap :as wrap]))

(use-fixtures :once (dt/coverage-check wrap))

(deftest wrap

  (testing "Implementation"
    (dt/test-impl #(wrap/build (dt/no-tx-reuse (hm/build)) {})))

  (testing "Pre and post method"
    (let [make-impl (fn []
                      (-> (hm/build)
                          (dt/sequential-id)
                          (dt/no-tx-reuse)
                          ;; Add 2 layers of atom implementation to be
                          ;; sure nested implementations are correctly
                          ;; handled.
                          (at/build)
                          (at/build)))
          wrap-fn (fn [impl pre-k post-k]
                    ;; The wrappper adds pre-k and post-k document
                    ;; whenever the 'add' method is called
                    (wrap/build impl {`dp/-add {:wrap (fn [add-method]
                                                        (fn [tx drawer data]
                                                          (db/tx-let [tx tx]
                                                              [_ (add-method tx drawer {:v pre-k})
                                                               id (add-method tx drawer data)
                                                               _ (add-method tx drawer {:v post-k})]
                                                            id)))}}))]
      (let [result (db/tx-> (wrap-fn (make-impl) :pre1 :post1)
                     (dt/is-> (db/add! :users {:v "User1"}) (= 2))
                     (dt/is-> (db/tx-> (db/fetch :users {})
                                (db/update-result #(mapv :v (sort-by :id %))))
                              (= [:pre1 "User1" :post1])))]
        ;; Validate that the result outside the transaction is also correct.
        (is (= [:pre1 "User1" :post1]
               result)))

      (testing "Nested wraps"
        (let [wrap2 (-> (wrap-fn (make-impl) :pre1 :post1)
                        (wrap-fn :pre2 :post2))]
          ;; This might look weird at first, but it makes sense:
          ;; Wrap2 calls db/add! from wrap1, which itself has a pre/post action.
          (let [expected [:pre1 :pre2 :post1 ; wrap2
                          :pre1 ; wrap1
                          "User1" ; original
                          :post1 ; wrap1
                          :pre1 :post2 :post1] ;wrap2
                result (db/tx-> wrap2
                         ;; The expected ID is '5', because 4 documents should be added before
                         (dt/is-> (db/add! :users {:v "User1"}) (= 5))
                         (dt/is-> (db/tx-> (db/fetch :users {})
                                    (db/update-result #(mapv :v (sort-by :id %))))
                                  (= expected)))]
            ;; Validate that the result outside the transaction is also correct.
            (is (= expected result)))))))
  (testing "closing"
    (let [impl (dt/no-tx-reuse (dt/sequential-id (hm/build)))
          dresser (-> impl
                      (wrap/build {`dp/-add {:wrap    (fn [_m]
                                                        (fn [tx & args]
                                                          (db/update-result tx conj :wrap1)))
                                             :closing (fn close1 [tx & args]
                                                        (db/update-result tx conj :closing1))}})
                      ;; Layer without closing fn
                      (wrap/build {`dp/-add {:wrap (fn [m]
                                                     (fn [tx & args]
                                                       (-> (apply m tx args)
                                                           (db/update-result conj :wrap2))))}})
                      (wrap/build {`dp/-add {:wrap    (fn [m]
                                                        (fn [tx & args]
                                                          (-> (apply m tx args)
                                                              (db/update-result conj :wrap3))))
                                             :closing (fn close2 [tx & args]
                                                        (db/update-result tx conj :closing3))}})
                      ;; Layer without closing fn
                      (wrap/build {`dp/-add {:wrap (fn [m]
                                                     (fn [tx & args]
                                                       (-> (apply m tx args)
                                                           (db/update-result conj :wrap4))))}}))]
      (db/tx-> dresser
        (dt/is-> (db/add! :test {}) (= [:closing1 :closing3 :wrap4 :wrap3 :wrap2 :wrap1]))
        (dt/testing-> "Closing fns are dropped once used"
          (db/with-result nil)
          (dt/is-> (db/add! :test {}) (= [:closing1 :closing3 :wrap4 :wrap3 :wrap2 :wrap1]))))))

  (deftest mixed-wrapped-methods
    (let [impl (dt/no-tx-reuse (dt/sequential-id (hm/build)))
          tx-wrapper (let [tx-id (gensym "tx-")]
                       {:wrap (fn [m]
                                (fn [tx f opts]
                                  (let [tx-lvl (or (db/temp-data tx [tx-id]) 1)]
                                    (-> tx (db/update-temp-data update tx-id (fnil inc 0))
                                        ; Disable returning result
                                        (db/update-result conj (str "tx-enter-" tx-lvl))
                                        (m f (assoc opts :result? false))))))
                        :closing
                        (fn [tx f opts]
                          (let [tx-lvl (or (db/temp-data tx [tx-id]) 1)
                                tx (-> (db/assoc-temp-data tx tx-id (dec tx-lvl))
                                       (db/update-result conj (str "tx-leave-" tx-lvl)))]
                            (if (= tx-lvl 1)
                              (cond-> (db/update-temp-data tx dissoc tx-id)
                                ; Enabled returning result if at top level
                                (:result? opts) db/result)
                              tx)))})
          add-wraper-fn (fn [id apply-method?]
                          {:wrap    (fn [m]
                                      (fn [tx & args]
                                        ;; Don't really add the document
                                        (-> (if apply-method?
                                              (apply m tx args)
                                              tx)
                                            (db/update-result conj (str "wrap-" id)))))
                           :closing (fn close1 [tx & args]
                                      (db/update-result tx conj (str "close-" id)))})
          dresser (-> impl
                      (wrap/build
                       {`dp/-add      (add-wraper-fn 1 false)
                        `dp/-transact tx-wrapper})
                      (wrap/build
                       {`dp/-add (add-wraper-fn 2 true)}))]
      (is (= (db/add! dresser :test {})
             ["tx-leave-1" "close-1" "close-2" "wrap-2" "wrap-1" "tx-enter-1"]))))


  (testing "Transaction reverts changes on exeption"
    (let [layer1 (-> (hm/build)
                     (dt/sequential-id)
                     (dt/no-tx-reuse)
                     (at/build))
          layer2 (-> (wrap/build layer1 {`dp/-add
                                         {:wrap
                                          (fn [m]
                                            (fn [tx & args]
                                              ; Do the action first
                                              (apply m tx args)
                                              (throw (ex-info "Boom!" {}))))}})
                     (at/build))]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Boom!"
           (db/add! layer2 :drawer {:doc "hello"})))
      (is (= []
             (db/all-ids layer1 :drawer)
             (db/all-ids layer2 :drawer))))))
