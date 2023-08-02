(ns dresser.wrap-test
  (:require [clojure.string :as str]
            [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [dresser.base :as db]
            [dresser.impl.atom :as at]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt]
            [dresser.wrap :as wrap]
            [dresser.protocols :as dp]))

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
          add-in-source-drawer (fn [i]
                                 (fn [tx drawer data]
                                   (db/add! tx drawer {:v i})))
          wrap-fn (fn [impl pre-k post-k]
                    ;; The wrappper adds pre-k and post-k document
                    ;; whenever the 'add' method is called
                    (wrap/build impl {`dp/-add {:pre  (add-in-source-drawer pre-k)
                                                :post (add-in-source-drawer post-k)}}))]
      (let [result (db/tx-> (wrap-fn (make-impl) :pre1 :post1)
                     ;; The expected ID is '2', because pre-k document is added before
                     (dt/is-> (db/add! :users {:v "User1"}) (= 2)
                              "Pre/post shouldn't accidentally modify result")
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
                         (dt/is-> (db/add! :users {:v "User1"}) (= 5)
                                  "Pre/post shouldn't modify result")
                         (dt/is-> (db/tx-> (db/fetch :users {})
                                    (db/update-result #(mapv :v (sort-by :id %))))
                                  (= expected)))]
            ;; Validate that the result outside the transaction is also correct.
            (is (= expected result)))))))

  (testing "pre+, post+, wrap+ and tx-data-key"
    (let [inc-in-tx-data (fn [tx tx-data-key]
                           (db/update-temp-data tx update tx-data-key (fnil inc 0)))
          count-steps (fn [{:keys [tx-data-key]} tx & _args]
                        (inc-in-tx-data tx tx-data-key))
          wrap-fn (fn [impl]
                    (wrap/build
                     impl
                     ;; increment the tx-data-key field in pre, wrap and post
                     {`dp/-add {:pre+  count-steps
                                :wrap+ (fn [{:keys [tx-data-key]} method]
                                         (fn [tx & args]
                                           (let [tx (inc-in-tx-data tx tx-data-key)]
                                             (-> (apply method tx args)
                                                 ;; Make the key available outside the wrapper layer
                                                 (db/update-temp-data assoc :tx-data-key tx-data-key)))))
                                :post+ count-steps}}))]
      (let [return (db/transact! (wrap-fn (dt/no-tx-reuse (dt/sequential-id (hm/build))))
                                 (fn [tx]
                                   (let [tx (db/add! tx :docs {:v "doc1"})
                                         [tx id] (db/dr (db/add! tx :docs {:v "doc1"}))
                                         _ (is (= 2 id))
                                         {:keys [tx-data-key]} (db/temp-data tx)]
                                     (is (= 6 (get (db/temp-data tx) tx-data-key))
                                         "tx-data-field should survive across all steps")
                                     tx))
                                 false)
            {:keys [tx-data-key]} (db/temp-data return)]
        (assert tx-data-key)
        (is (nil? (get (db/temp-data return) tx-data-key))
            "tx-data-key field doesn't exist outside a transaction"))))

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
                      (wrap/build {`dp/-add {:wrap    (fn [m]
                                                        (fn [tx & args]
                                                          (-> (apply m tx args)
                                                              (db/update-result conj :wrap4))))}}))]
      (is (= [:closing1 :closing3 :wrap4 :wrap3 :wrap2 :wrap1]
             (db/add! dresser :test {})))))

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
