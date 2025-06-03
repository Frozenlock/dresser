(ns dresser.test
  (:require [clojure.set :as set]
            [clojure.test :as t :refer [is testing]]
            [dresser.base :as db]
            [dresser.protocols :as dp])
  (:import (java.util Arrays Date)))

;; Redefine via leiningen injection to activate the fixture.
(def test-coverage? false)

(defn with-coverage-check
  "Fixture to record all the calls to public functions in the target namespace.
  Prints all uncalled functions."
  [target-namespace f]
  (if-not test-coverage?
    (f)
    (let [*calls (atom {})
          public-sym->vars (->> (for [[sym v] (ns-interns target-namespace)
                                      :when (and (fn? (var-get v))
                                                 (not (or (:private (meta v))
                                                          (:omit-coverage (meta v)))))]
                                  [sym v])
                                (into {}))
          wrapped-fns (->> (for [[sym v] public-sym->vars
                                 :let [f (var-get v)]]
                             [v (fn [& args]
                                  (swap! *calls update v (fnil inc 0))
                                  (apply f args))])
                           (into {}))]
      (with-redefs-fn wrapped-fns f)
      (let [called (keys @*calls)
            uncalled (remove (set called) (vals public-sym->vars))]
        (if (seq uncalled)
          (do (println (str "\nThe following " (count uncalled) " public functions were not called:"))
              (println "-----")
              (doseq [f uncalled]
                (println f)))
          (println (str "All " (count @*calls) " public functions called.")))))))

(defmacro coverage-check
  [alias-sym]
  (let [target-ns (get (ns-aliases *ns*) alias-sym)
        _ (assert target-ns "Alias not found")]
    `(partial with-coverage-check ~target-ns)))

;;;;;;;;;;;;

(def ^:dynamic *disable-tx-counter* false)

;; (defmethod t/assert-expr 'thrown-with-cause-msg?
;;   [msg form]
;;   (let [re (nth form 1)
;;         body (nthnext form 2)]
;;     `(try ~@body
;;           (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
;;           (catch clojure.lang.ExceptionInfo tx-e#
;;             (let [e# (ex-cause tx-e#)
;;                   m# (.getMessage e#)]
;;               (if (re-find ~re m#)
;;                 (t/do-report {:type     :pass,  :message ~msg,
;;                               :expected '~form, :actual  e#})
;;                 (t/do-report {:type     :fail,  :message ~msg,
;;                               :expected '~form, :actual  e#})))
;;             tx-e#))))

(defmacro is->
  "Similar to `clojure.test/is`.

  Pass `dresser` to `form` as per the `->` macro.

  WARNING: the result of `(form dresser)` is passed to the `test-form` as per
  the `->>` macro to be consistent with the expected/actual of `is`.

  Returns the dresser returned by the form.
  Useful to thread tests and operations.

   (tx-> dresser
     (is-> (add :users {}) :id
       \"New user should have an id\")
     ...)

  Also supports the `thrown-with-msg?` and `thrown?` special forms."
  {:style/indent 0}
  ([dresser form test-form] `(is-> ~dresser ~form ~test-form nil))
  ([dresser form test-form msg]
   (let [test-form (if (and (coll? test-form)
                            (seq test-form))
                     test-form
                     (list test-form))]
     (if (some #{(first test-form)} ['thrown-with-msg? 'thrown?])
       ;; If we are expected to fail, try the form, but return the
       ;; dresser object from before the form.
       `(do
          (binding [*disable-tx-counter* true]
            (is ~`(~@test-form (-> ~dresser ~form)) ~msg))
          ~dresser)
       ;; Normal test without exception check
       `(binding [*disable-tx-counter* true]
          (let [[dresser# ~'actual] (db/dr (-> ~dresser ~form))]
            (is ~(if (seq? test-form)
                   `(~@test-form ~'actual)
                   (list test-form ~'actual))
                ~msg)
            dresser#))))))

(defmacro testing->
  "Similar to `clojure.test/testing`.
  Equivalent of
  (-> dresser body)"
  {:style/indent 1}
  [dresser string & body]
  `((fn [dresser#]
      (testing ~string
        (-> dresser# ~@body))) ~dresser))

(defn sequential-id
  "Replaces gen-id implementation (metadata) with one that increases sequentially.

  Ephemeral DB only; does not check for existing IDs."
  [dresser]
  (let [*counter (atom 0)
        f (fn [dresser _drawer]
            (db/with-result dresser
              (swap! *counter inc)))]
    (vary-meta dresser merge {`dp/gen-id f})))

(defn no-tx-reuse
  "Throws if a transaction state is used more than once."
  [dresser]
  (let [*tx-count (atom 0)
        tx-count-field (gensym "tx-reuse-check")

        track-usage (fn [method]
                      (fn [tx & args]
                        (if *disable-tx-counter*
                          (apply method tx args)
                          (let [tx-count (db/temp-data tx [tx-count-field])]
                            (when (some->> tx-count (not= @*tx-count))
                              (throw (ex-info "Transaction state reused"
                                              {:method         method
                                               :args           args
                                               :counter        @*tx-count
                                               :expected-count tx-count})))
                            (let [new-count (swap! *tx-count inc)
                                  tx (db/assoc-temp-data tx tx-count-field new-count)]
                              (apply method tx args))))))

        wrap-transact (fn [transact-method]
                        (fn [dresser f opts]
                          (reset! *tx-count 0)
                          (let [dresser (db/assoc-temp-data dresser tx-count-field 0)]
                            (transact-method dresser f opts))))]

    ;; Use vary-meta to wrap protocol methods directly in metadata
    (-> dresser
        ;; DresserFundamental methods
        (dp/wrap-method `dp/fetch track-usage)
        (dp/wrap-method `dp/all-drawers track-usage)
        (dp/wrap-method `dp/delete-many track-usage)
        (dp/wrap-method `dp/assoc-at track-usage)
        (dp/wrap-method `dp/drop track-usage)
        (dp/wrap-method `dp/transact wrap-transact)
        ;; DresserOptional methods
        (dp/wrap-method `dp/fetch-by-id track-usage)
        (dp/wrap-method `dp/fetch-count track-usage)
        (dp/wrap-method `dp/update-at track-usage)
        (dp/wrap-method `dp/add track-usage)
        (dp/wrap-method `dp/all-ids track-usage)
        (dp/wrap-method `dp/dissoc-at track-usage)
        (dp/wrap-method `dp/gen-id track-usage)
        (dp/wrap-method `dp/get-at track-usage)
        (dp/wrap-method `dp/upsert-many track-usage)
        (dp/wrap-method `dp/dresser-id track-usage)
        (dp/wrap-method `dp/drawer-key track-usage)
        (dp/wrap-method `dp/rename-drawer track-usage)
        (dp/wrap-method `dp/has-drawer? track-usage))))

(comment
  (require 'dresser.impl.hashmap)
  (-> (no-tx-reuse (dresser.impl.hashmap/build))
      (db/transact!
       (fn [tx]
         (let [tx1 (db/add! tx :users {:v1 1})
               tx2 (db/add! tx :users {:v2 2})] ;<- tx reuse!
           tx2))))

  (-> (no-tx-reuse (dresser.impl.hashmap/build))
      (db/transact!
       (fn [tx]
         (let [tx1 (db/add! tx :users {:v1 1})
               tx2 (db/add! tx1 :users {:v2 2})]
           tx2)))))

(defn u= "unordered-equal"
  [x y] (= (set x) (set y)))

;; TODO: merge the 2 'assoc-at' tests?
(defn test--assoc-at-&-fetch
  [impl-f]
  (testing "Assoc-at & Fetch"
    (let [impl (impl-f)]
      ;; Throws if ID is nil
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Missing document ID"
           (db/assoc-at! impl :drawer1 nil [] {:a 1})))

      (let [test-doc {:id :id1,
                      :a [{:b "string"
                           :c :keyword}]
                      [:vec :key] 12}]
        (db/tx-> impl
                 (is-> (db/assoc-at! :drawer1 :id1 [] test-doc) (= test-doc)
                       "Expected return")
                 (is-> (db/fetch-by-id :drawer1 :id1) (= test-doc)
                       "Document correctly added via assoc-at")
                 (is-> (db/assoc-at! :drawer1 :id1 [] {:a 2}) (= {:a 2})
                       "Document overwritten")))
      (is (= {:id :some-id}
             (db/tx-> (impl-f)
                      (db/assoc-at! :drawer1 :some-id [] {})
                      (db/fetch-by-id :drawer1 :some-id)))))

    (testing "Maps as keys"
      (testing "only keywords as keys"
        ;; A naive serialization might miss the different key ordering
        (let [impl (impl-f)
              m1 {{:a 1, :b 2, :c #{"a" "b"}} "map1"}
              m2 {{:b 2, :a 1, :c #{"b" "a"}} "map2"}]
          (db/tx-> impl
                   (db/add! :drawer m1)
                   (db/add! :drawer m2)
                   (is-> (db/fetch :drawer {:only {{:a 1, :b 2, :c #{"a" "b"}} :?}})
                         (u= [m1 m2])))))
      (testing "heterogeneous keys"
        (let [impl (impl-f)
              m1 {{[:a] 1, "b" 2, #{:c 3} 3, ::d 4} "map1"}
              m2 {{::d 4, #{3 :c} 3, [:a] 1, "b" 2} "map2"}]
          (db/tx-> impl
                   (db/add! :drawer m1)
                   (db/add! :drawer m2)
                   (is-> (db/fetch :drawer {:only {{[:a] 1, "b" 2, #{:c 3} 3, ::d 4} :?}})
                         (u= [m1 m2])))))
      (testing "different map types"
        (let [impl (impl-f)
              m1 {{:a 1} "map1"}
              m2 {(sorted-map :a 1) "map2"}]
          (db/tx-> impl
                   (db/add! :drawer m1)
                   (db/add! :drawer m2)
                   (is-> (db/fetch :drawer {:only {{:a 1} :?}})
                         (u= [m1 m2])))))
      (testing "metadata doesn't mess equality"
        (binding [*print-meta* true]
          (let [impl (impl-f)
                m1 {{:a 1} "map1"}
                m2 {(with-meta {:a 1} {:some :meta}) "map2"}]
            (db/tx-> impl
                     (db/add! :drawer m1)
                     (db/add! :drawer m2)
                     (is-> (db/fetch :drawer {:only {{:a 1} :?}})
                           (u= [m1 m2])))))))

    (testing "bytes"
      (let [bytes (.getBytes "test string")
            impl (impl-f)
            b= (fn [result]
                 (Arrays/equals (:bytes (first result)) bytes))]
        (db/tx-> impl
                 (db/add! :drawer {:bytes bytes})
                 (is-> (db/fetch :drawer {:only {:bytes :?}})
                       (b=)))))

    (testing "Sets as keys"
      (binding [*print-meta* true]
        (let [impl (impl-f)
              m1 {#{"a" :b {:a 1} [10]} "map1"}
              m2 {(with-meta #{{:a 1}, :b, [10], "a"} {:some :meta}) "map2"}]
          (db/tx-> impl
                   (db/add! :drawer m1)
                   (db/add! :drawer m2)
                   (is-> (db/fetch :drawer {:only {#{"a" :b {:a 1} [10]} :?}})
                         (u= [m1 m2]))))))

    (testing "Different set types"
      (let [impl (impl-f)
            m1 {#{3 2 1} "map1"}
            m2 {(sorted-set 3 2 1) "map2"}]
        (db/tx-> impl
                 (db/add! :drawer m1)
                 (db/add! :drawer m2)
                 (is-> (db/fetch :drawer {:only {#{1 2 3} :?}})
                       (u= [m1 m2])))))))

(defn test--upsert-many
  [impl-f]
  (testing "Upsert all"
    (db/tx-let [tx (impl-f)]
               [[doc1 doc2 doc3 :as docs] [{:id 1, :a 1}, {:id 2, :b 2}, {:id "a", :c 3}]
                _ (-> tx
                      (is-> (db/upsert-many! :drawer1 docs) (= docs)
                            "Returns coll.")
                      (is-> (db/fetch-by-id :drawer1 1) (= doc1))
                      (is-> (db/fetch-by-id :drawer1 2) (= doc2))
                      (is-> (db/fetch-by-id :drawer1 "a") (= doc3))
                      (testing-> "Overwrites docs"
                                 (db/upsert-many! :drawer1 [{:id 1, :new "value"}])
                                 (is-> (db/fetch-by-id :drawer1 1) (= {:id 1, :new "value"}) "Overwritten")
                                 (is-> (db/fetch-by-id :drawer1 2) (= doc2) "Untouched")))]
               (is (thrown-with-msg?
                    clojure.lang.ExceptionInfo
                    #"Missing document ID"
                    (db/upsert-many! tx :drawer1 {:a 1 :b "String"})))
               tx)))

(defn test--fetch
  [impl-f]
  (testing "Fetch"
    (let [;; Introduce randomness in docs order with :r
          [d1 d2 d3 d4] [{:id 1, :sort {:a 1, :b 1}, :1 1, :r (rand), :type 1},
                         {:id 2, :sort {:a 1, :b 2}, :1 1, :2 2, :r (rand), :type "a"},
                         {:id 3, :sort {:a 2, :b 2}, :1 1, :2 2, :3 3,
                          :c {:d :nested, :n 4}, :r (rand), :type #{1}}
                         {:id 4, :sort {:a 3} :r (rand)}]
          docs [d1 d2 d3 d4]]
      (db/tx-> (impl-f)
               (db/upsert-many! :drawer1 (sort-by :r docs))
        ;tx
               (testing-> "- Where"
                          (is-> (db/fetch :drawer1 {:where {:id nil}}) empty?
                                "`nil` is not a wildcard")
                          (is-> (db/fetch :drawer1 {:where {:1 1}}) (u= [d1 d2 d3]))
                          (is-> (db/fetch :drawer1 {:where {:2 2}}) (u= [d2 d3]))
                          (is-> (db/fetch :drawer1 {:where {:c {:d :nested}}}) (u= [d3]))

                          (testing-> " - query op"
                                     (testing-> " - exists?"
                                                (is-> (db/fetch :drawer1 {:where {:c {db/exists? true}}}) (u= [d3]))
                                                (is-> (db/fetch :drawer1 {:where {:d {db/exists? true}}}) (u= []))
                                                (is-> (db/fetch :drawer1 {:where {:1 {db/exists? false}}}) (u= [d4])))

                                     (testing-> " - gt(e) (bigger)"
                                                (is-> (db/fetch :drawer1 {:where {:1 {db/gt 1}}}) (u= []))
                                                (is-> (db/fetch :drawer1 {:where {:2 {db/gt 1}}}) (u= [d2 d3]))
                                                (is-> (db/fetch :drawer1 {:where {:1 {db/gte 1}}}) (u= [d1 d2 d3]))
                                                (is-> (db/fetch :drawer1 {:where {:2 {db/gte 1}}}) (u= [d2 d3])))

                                     (testing-> " - lt(e) (smaller)"
                                                (is-> (db/fetch :drawer1 {:where {:1 {db/lt 2}}}) (u= [d1 d2 d3]))
                                                (is-> (db/fetch :drawer1 {:where {:2 {db/lt 2}}}) (u= []))
                                                (is-> (db/fetch :drawer1 {:where {:1 {db/lte 2}}}) (u= [d1 d2 d3]))
                                                (is-> (db/fetch :drawer1 {:where {:2 {db/lte 2}}}) (u= [d2 d3])))

                                     (testing-> " types comparison"
                                                (is-> (db/fetch-count :drawer1 {:where {:type {db/lt 2}}}) (not= 0)
                                                      "Doesn't throw"))

                                     (testing-> " - any -"
                                                (testing-> "outer"
                                                           (testing-> "top level"
                                                                      (is-> (db/fetch :drawer1 {:where {db/any
                                                                                                        [{:id {db/lte 2}}]}}) (u= [d1 d2]))
                                                                      (is-> (db/fetch :drawer1 {:where {db/any
                                                                                                        [{:id {db/lte 2}}
                                                                                                         {:id {db/gt 3}}]}}) (u= [d1 d2 d4])))
                                                           (testing-> "nested"
                                                                      (is-> (db/fetch :drawer1
                                                                                      {:where
                                                                                       {;; matches d1, d2 and d3
                                                                                        :1 1
                                    ;; matches d3 and d4
                                                                                        db/any [{:sort {db/any [{:a {db/lt 3
                                                                                                                     db/gt 1}}
                                                                                                                {:a 3}]}}]}})
                                                                            (u= [d3]))))

                                                (testing-> "inner"
                                                           (testing-> "top level"
                                                                      (is-> (db/fetch :drawer1 {:where {:id {db/any [{db/lte 2}]}}}) (u= [d1 d2]))
                                                                      (is-> (db/fetch :drawer1 {:where {:id {db/any
                                                                                                             [{db/lte 2}
                                                                                                              {db/gt 3}]}}}) (u= [d1 d2 d4])))
                                                           (testing-> "nested"
                                                                      (is-> (db/fetch :drawer1
                                                                                      {:where
                                                                                       {;; matches d1, d2 and d3
                                                                                        :1 1
                                    ;; matches d3 and d4
                                                                                        db/any [{:sort {:a {db/any [{db/lt 3
                                                                                                                     db/gt 1}
                                                                                                                    3]}}}]}})
                                                                            (u= [d3])))))))

               (testing-> "- Only"
                          (is-> (db/fetch :drawer1 {:only {:id :?}}) (u= (for [{:keys [id]} docs]
                                                                           {:id id})))
                          (testing-> " - Nested"
                                     (is-> (db/fetch :drawer1 {:only {:c {:d :?}}}) (u= [{} {} {:c {:d :nested}}]))))

               (testing-> "- Limit"
                          (is-> (-> (db/fetch :drawer1 {:limit 2
                                                        :where {:1 1}})
                                    (db/update-result count))
                                (= 2))))

      (testing " - Sort"
        (db/tx-> (impl-f)
          ;; No randomness, insert by ID order
                 (db/upsert-many! :drawer1 (sort-by :id docs))
          ;; Simple sort
                 (is-> (db/fetch :drawer1 {:sort [[[:id] :asc]]})
                       (= [d1 d2 d3 d4]))
          ;; Equality in the sort
                 (is-> (db/fetch :drawer1 {:sort [[[:sort :a] :desc]]})
                       (or (= [d4 d3 d1 d2])
                           (= [d4 d3 d2 d1])))
          ;; Sort by multiple values
                 (is-> (db/fetch :drawer1 {:sort [[[:sort :a] :asc]
                                                  [[:sort :b] :desc]]})
                       (= [d2 d1 d3 d4]))
          ;; Make sure the sorting occurs BEFORE the limit
                 (testing-> " sort/limit eval order"
                            (is-> (db/fetch :drawer1 {:limit 1
                                                      :sort [[[:id] :desc]]})
                                  (= [d4]))
                            (is-> (db/fetch :drawer1 {:limit 1
                                                      :sort [[[:id] :asc]]})
                                  (= [d1])))))

      (testing " - Skip"
        (db/tx-let [tx (impl-f)]
                   [_ (db/upsert-many! tx :drawer1 docs)
                    f1 (db/fetch tx :drawer1 {:limit 2})
                    ids1 (set (map :id f1))
                    f2 (db/fetch tx :drawer1 {:limit 2, :skip 2})
                    ids2 (set (map :id f2))]
                   (is (empty? (set/intersection ids1 ids2)))
                   tx)))))

(defn test--add-&-fetch-by-id
  [impl-f]
  (testing "Add & fetch-by-id"
    (db/tx-let [tx (impl-f)]
               [[doc1 doc2] [{:a 1 :b "String"}, {:b 2 :c {:nested "data"}}]
                id1 (db/add! tx :drawer1 doc1)
                id2 (db/add! tx :drawer1 doc2)
                id->doc {id1 (assoc doc1 :id id1)
                         id2 (assoc doc2 :id id2)}]
               (-> tx
                   (is-> (-> (db/fetch-by-id :drawer1 id1)
                             (db/update-result #(dissoc % :id)))
                         (= doc1))
                   (is-> (-> (db/fetch-by-id :drawer1 id2)
                             (db/update-result #(dissoc % :id)))
                         (= doc2))))))

(defn test--transact
  [impl-f]
  (testing "Transact"
    (let [doc {:id :some-id}]
      ;; Test that we get the expected value OUTSIDE the transaction
      (is (= doc (db/transact! (impl-f) #(db/assoc-at! % :drawer1 :some-id [] doc))))
      ;; ... Except when we ask to not extract the result
      (is (db/dresser? (db/transact! (impl-f) #(db/assoc-at! % :drawer1 :some-id [] doc) {:result? false}))))

    (testing "Rollback on exception"
      ;; Exceptions thrown inside a transaction should cause a rollack
      (let [doc {:id "some-id", :str "string"}
            dresser (db/transact! (impl-f)
                                  (fn [dresser] (db/assoc-at! dresser :drawer1 "some-id" [] doc))
                                  {:result? false})] ; <-- return the dresser object

        ; Now make a few changes inside a transaction
        (try
          (db/tx-> dresser
                   (db/assoc-at! :drawer1 "some-id" [:str2] "string 2")
                   (db/update-at! :drawer1 "some-id" [:str] (fn [_] (throw (ex-info "Test exception" {})))))
          (catch Exception _e))

        (is (= doc (db/fetch-by-id dresser :drawer1 "some-id")))))))

(defn test--assoc-at
  [impl-f]
  (testing "Assoc-at existing document"
    (db/tx-let [tx (impl-f)]
               [id (db/add! tx :drawer1 {:a {:b {:c 1}}, :z 1})]
               (-> tx
                   (is-> (db/assoc-at! :drawer1 id [:a :b :c] {:d "hello"})
                         (= {:d "hello"})
                         "Returns the value that was assoc'd-in.")
                   (is-> (db/fetch-by-id :drawer1 id {:only [:id]})
                         (#(some? (:id %))))
                   (is-> (db/fetch-by-id :drawer1 id {:only [:a :z]})
                         (= {:a {:b {:c {:d "hello"}}}, :z 1}))
                   (testing-> "nil isn't special"
                              (db/assoc-at! :drawer1 id [:a :b :c] nil)
                              (is-> (db/fetch-by-id :drawer1 id) (= {:id id, :z 1, :a {:b {:c nil}}})))
                   (testing-> "empty path"
                              (db/assoc-at! :drawer1 id [] {:a "hello"})
                              (is-> (db/fetch-by-id :drawer1 id) (= {:id id :a "hello"}))))))

  (testing "Assoc-at creates new document"
    (db/tx-let [tx (impl-f)]
               [id "my-id"]
               (-> tx
                   (is-> (db/assoc-at! :drawer1 id [:a :b :c] {:d "hello"})
                         (= {:d "hello"})
                         "Returns the value that was assoc'd-in.")

                   (is-> (db/fetch-by-id :drawer1 id)
                         (= {:a {:b {:c {:d "hello"}}} :id id}))))))

(defn test--get-at
  [impl-f]
  (testing "Get-at"
    (db/tx-let [tx (impl-f)]
               [doc {:a {:b {:c 1}}, :z 1}
                id (db/add! tx :drawer1 doc)]
               (-> tx
                   (is-> (db/get-at :drawer1 id [:a :b :c]) (= 1))
                   (is-> (db/get-at :drawer1 id [:a :b :c :d]) nil?)
                   (is-> (db/get-at :drawer1 id []) (= (assoc doc :id id)))
                   (testing-> " - Only"
                              (is-> (db/get-at :drawer1 id [] {:z :?}) (= {:z 1}))
                              (is-> (db/get-at :drawer1 id [:a :b] {:c :?}) (= {:c 1})))))))

(defn test--update-at
  [impl-f]
  (testing "Update-at"
    (db/tx-let [tx (impl-f)]
               [id (db/add! tx :drawer1 {:a {:b {:c 1}}, :z 1})]
               (-> tx
                   (is-> (db/update-at! :drawer1 id [:a :b :c] + 1 1 1) (= 4)
                         "Returns the value that was updated-in.")
                   (is-> (db/get-at :drawer1 id [:a :b :c]) (= 4))
                   (db/update-at! :drawer1 id [:a :b] dissoc :c)
                   (is-> (db/get-at :drawer1 id [:a :b :c]) nil?)
                   (testing-> "Can't remove document-id"
                              (db/update-at! :drawer1 id [:a :b] dissoc :id)
                              (is-> (db/get-at :drawer1 id [:id]) (= id)))
                   (testing-> "nil result"
                              (db/update-at! :drawer1 id [:a :b :c] (constantly nil))
                              (is-> (db/fetch-by-id :drawer1 id) (= {:id id, :z 1, :a {:b {:c nil}}})))
          ;; (testing-> "nil doc ID"
          ;;   (db/update-at! :drawer1 nil [:a :b :c] (constantly nil))
          ;;   (is-> (db/fetch-by-id :drawer1 id) (= {:id id, :z 1, :a {:b {:c nil}}})))
                   (testing-> "Empty path"
                              (is-> (db/update-at! :drawer1 id [] merge {:a 1})
                                    (= {:id id, :a 1, :z 1})))))))

(defn test--dissoc-at
  [impl-f]
  (testing "dissoc-at"
    (db/tx-let [tx (impl-f)]
               [id (db/add! tx :drawer1 {:a {:b {:c1 1, :c2 2}}, :z 1})]
               (-> tx
                   (is-> (db/dissoc-at! :drawer1 id [:a :b] :c1) nil?)
                   (is-> (db/get-at :drawer1 id [:a :b]) (= {:c2 2}))
                   (testing-> "empty path"
                              (db/dissoc-at! :drawer1 id [] :a)
                              (is-> (db/get-at :drawer1 id []) (= {:id id :z 1})))
                   (testing-> "Can't remove document-id"
                              (db/dissoc-at! :drawer1 id [] :id)
                              (is-> (db/get-at :drawer1 id []) :id))))))

(defn test--drop
  [impl-f]
  (testing "Drop"
    (let [d1 :drawer1]
      (db/tx-> (impl-f)
               (db/add! :drawer1 {:a 1})
               (db/add! :drawer1 {:b 2})
               (db/add! :drawer2 {:c 3})
               (is-> (db/drop! d1) (= d1))
               (is-> (db/all-ids :drawer1) empty?)
               (is-> (db/all-ids :drawer2) seq)))))

(defn test--all-ids
  [impl-f]
  (testing "All-ids"
    (let [docs [{:id 1} {:id 2} {:id 3}]]
      (db/tx-> (impl-f)
               (db/upsert-many! :drawer1 docs)
               (is-> (db/all-ids :drawer1) (= (map :id docs)))))))

(defn test--delete
  [impl-f]
  (testing "Delete"
    (let [[doc1 :as docs] [{:id 1} {:id 2} {:id 3}]]
      (db/tx-> (impl-f)
               (db/upsert-many! :drawer1 docs)
               (is-> (db/delete! :drawer1 nil) nil?)
               (is-> (db/delete! :drawer1 :doesnt-exist) (= :doesnt-exist)
                     "Doesn't throw when deleting a non-existing document.")
               (is-> (db/delete! :drawer1 1) (= 1) "Returns id.")
               (is-> (db/delete! :drawer1 2) (= 2) "Returns id.")
               (is-> (db/fetch-by-id :drawer1 1) nil?)
               (is-> (db/fetch-by-id :drawer1 2) nil?)
               (is-> (db/fetch-by-id :drawer1 3) some?)
               (is-> (db/assoc-at! :drawer1 1 [] doc1) some?)))))

(defn test--delete-many
  [impl-f]
  (testing "Delete-many"
    (let [[doc1 :as docs] [{:id 1, :a 1}
                           {:id 2, :a 1}
                           {:id 3, :a 2}]]
      (db/tx-> (impl-f)
               (db/upsert-many! :drawer1 docs)
               (is-> (db/delete-many! :drawer1 {:id nil}) (= {:deleted-count 0}))
               (is-> (db/delete-many! :drawer1 {:id :doesnt-exist}) (= {:deleted-count 0})
                     "Doesn't throw when nothing matches")
               (is-> (db/delete-many! :drawer1 {:a 1}) (= {:deleted-count 2}))
               (is-> (db/fetch-by-id :drawer1 1) nil?)
               (is-> (db/fetch-by-id :drawer1 2) nil?)
               (is-> (db/fetch-by-id :drawer1 3) some?)
               (is-> (db/delete-many! :drawer1 {:id 3}) (= {:deleted-count 1}))
               (is-> (db/fetch-by-id :drawer1 3) nil?)
               (is-> (db/assoc-at! :drawer1 1 [] doc1) some?)))))

(defn test--all-drawers
  [impl-f]
  (testing "All-drawers"
    (db/tx-> (impl-f)
             (is-> (db/all-drawers) empty?)

             (db/add! :drawer1 {:id 1})
             (db/add! :drawer1 {:id 1})
             (is-> (db/all-drawers) (= [:drawer1])
                   "No duplicate drawers")

             (db/add! :drawer2 {:id 1})
             (is-> (db/all-drawers) (u= [:drawer1 :drawer2])))))

(defn test--has-drawer
  [impl-f]
  (testing "has-drawer"
    (db/tx-> (impl-f)
             (is-> (db/has-drawer? :drawer1) not)

             (db/add! :drawer1 {:id 1})
             (is-> (db/has-drawer? :drawer1) true?))))

(defn test--dresser?
  [impl-f]
  (is (db/dresser? (impl-f))
      "Implementation should have :dresser.db/dresser? in its metadata"))

;; TODO: should it throw an exception if the new-name already exists?
(defn test--rename-drawer
  [impl-f]
  (testing "Rename drawer"
    (db/tx-> (impl-f)
             (db/assoc-at! :d1 1 [] {:name "Bob"})
             (db/assoc-at! :d2 1 [] {:name "John"})
             (db/rename-drawer! :d1 :d1')
             (is-> (db/has-drawer? :d1) false?
                   "Drawer was renamed and should no longer exists")
             (is-> (db/fetch-by-id :d1 1) nil?
                   "Drawer was renamed and document should no longer be there")
             (is-> (db/fetch-by-id :d1' 1) (= {:id 1, :name "Bob"}))
             (is-> (db/fetch-by-id :d2 1) (= {:id 1, :name "John"})
                   "Unrelated drawer remains intact"))))

(defn test--immutable-temp-data
  [impl-f]
  (testing "Immutable temp data"
    (let [dresser (impl-f)
          initial-temp-data (db/temp-data dresser)]
      (is (= {:data 1234}
             (db/temp-data (db/with-temp-data dresser {:data 1234}))))
      ;; Use the initial dresser. Its temp-data should not have been
      ;; modified by the previous action.
      (is (= initial-temp-data (db/temp-data dresser))))))

(defn test--lazyness
  [impl-f]
  (testing "Lazyness is realized before closing transaction"
    (let [qty 200
          docs (for [i (range qty)]
                 {:id i})
          dresser (db/raw-> (impl-f)
                            (db/upsert-many! :drawer1 docs))]
      ;; If the transaction was closed before the seq is realized,
      ;; this should throw an exception.
      (is (= qty (count (db/fetch dresser :drawer1 {})))))))

(defn test--types
  [impl-f]
  (testing "Types are correctly stored and retrieved"
    (let [inst (Date.)
          coll [1 "s" :k inst]
          data {:vector (vec coll)
                :set (set coll)
                :list (into '() coll)
                :string "abc"
                :int 12
                :float 12.2
                :keyword :keyword
                :inst inst}]
      (db/tx-let [tx (impl-f)]
                 [id (db/add! tx :drawer1 {:data data})
                  ret (db/fetch-by-id tx :drawer1 id)]
                 (is (= data (:data ret)))
                 tx))))

(defn test--dont-blow-up-stack
  [impl-f]
  (time
   ;; Takes ~6.5s for Codax and ~24s(!!!) for MongoDB
   (testing "Transactions don't blow up the stack"
     (let [add-doc! (fn [dresser idx]
                      (db/with-tx [tx dresser]
                        (db/assoc-at! tx :drawer1 idx [] {})))]
       (db/with-tx [tx (impl-f)]
         (reduce (fn [tx' idx]
                   (add-doc! tx' idx))
                 tx (range 100000)))))))

(defn test--temp-dresser-id
  [impl-f]
  (is (db/temp-dresser-id (impl-f)) "Missing temp-dresser-id"))

(defn test--lexical-encoding
  [impl-f]
  (let [n1 3
        n2 15
        n3 200
        n4 10e10
        expected-order [n1 n2 n3 n4]
        docs (for [n (reverse expected-order)]
               {:id (str "prefix2" (db/lexical-encode n)) :int n})
        dresser (db/raw-> (impl-f)
                          (db/upsert-many! :drawer1 docs)
                          (db/upsert-many! :drawer1 [{:id "prefix1"}
                                                     {:id "prefixa"}]))]
    (is (= expected-order
           (map :int (db/fetch dresser :drawer1 {:where {:id {db/gte "prefix2"
                                                              db/lt (str "prefix2" db/lexical-max)}}
                                                 :sort [[[:id] :asc]]}))))))

(defrecord DresserTestRecord [field1 field2])
(defrecord NestedRecord [name data])
(defn test--records
  [impl-f]
  (let [value1 {:some {:nested "value"}}
        value2 {:number 1}
        record (->DresserTestRecord value1 value2)
        nested-rec (->NestedRecord "nested" {:x 1 :y 2})
        record-with-nested (->DresserTestRecord nested-rec value2)
        dresser (impl-f)]
    (db/tx-> dresser
             (testing-> "store and retrieve records"
                        (db/assoc-at! :drawer "id" [:record] record)
                        (is-> (db/get-at :drawer "id" [:record])
                              (= record))
                        (db/add! :drawer {:id "nested" :data record-with-nested})
                        (is-> (db/fetch-by-id :drawer "nested")
                              (fn [result]
                                (and (instance? DresserTestRecord (:data result))
                                     (instance? NestedRecord (:field1 (:data result)))))))
             (testing-> "records in :where conditions"
        ;; Test filtering on nested values inside a record
                        (is-> (db/fetch :drawer {:where {:record {:field1 {:some {:nested "wrong value"}}}}})
                              (= '()))
                        (is-> (db/fetch :drawer {:where {:record {:field1 {:some {:nested "value"}}}}})
                              (= (list {:id "id" :record record})))
        ;; Test that records are compared by value, not by reference
                        (db/add! :drawer2 {:name "test1" :data record})
                        (db/add! :drawer2 {:name "test2" :data {:field1 value1 :field2 value2}}) ; same data as map
                        (is-> (db/fetch :drawer2 {:where {:data record}})
                              (fn [result]
                                (and (= 1 (count result))
                                     (= "test1" (:name (first result)))))))
             (testing-> ":only converts record to simple map"
                        (is-> (db/get-at :drawer "id" [:record] {:field2 {:number :?}})
                              (= {:field2 value2})))
             (testing-> "record as key in path"
                        (db/assoc-at! :drawer "id" [record] {:a {:b 1}})
                        (is-> (db/get-at :drawer "id" [record])
                              (= {:a {:b 1}})))
             (testing-> "record preservation through updates"
                        (db/add! :drawer {:id "update-test" :rec record})
                        (db/update-at! :drawer "update-test" [:rec :field1 :some :nested] (constantly "updated"))
                        (is-> (db/fetch-by-id :drawer "update-test")
                              (fn [result]
                                (and (instance? DresserTestRecord (:rec result))
                                     (= "updated" (get-in result [:rec :field1 :some :nested]))))))
             (testing-> "records in batch operations"
                        (db/upsert-many! :drawer [{:id "batch1" :rec record}
                                                  {:id "batch2" :rec (->DresserTestRecord value2 value1)}])
                        (is-> (db/fetch :drawer {:where {:id {db/any ["batch1" "batch2"]}}})
                              (fn [results]
                                (every? #(instance? DresserTestRecord (:rec %)) results))))
             (testing-> "records as map keys"
                        (db/add! :drawer3 {record "value1"})
                        (db/add! :drawer3 {(->DresserTestRecord value1 value2) "value2"}) ; same content as record
                        (is-> (db/fetch :drawer3 {:only {record :?}})
                              (u= [{record "value1"} {record "value2"}])
                              "Records with same content are treated as equal keys")))))

(defn test--in-tx
  [impl-f]
  (let [dresser (impl-f)]
    (is (not (db/tx? dresser)))
    (db/with-tx [tx dresser]
      (is (db/tx? tx))
      tx)))

(defn test-impl
  [impl-f]
  (test--in-tx impl-f)
  (test--records impl-f)
  (test--lazyness impl-f)
  (test--types impl-f)
  ;; (test--dont-blow-up-stack impl-f)
  (test--immutable-temp-data impl-f)
  (test--rename-drawer impl-f)
  (test--dresser? impl-f)
  (test--assoc-at-&-fetch impl-f)
  (test--transact impl-f)
  (test--upsert-many impl-f)
  (test--fetch impl-f)
  (test--add-&-fetch-by-id impl-f)
  (test--assoc-at impl-f)
  (test--get-at impl-f)
  (test--update-at impl-f)
  (test--drop impl-f)
  (test--all-ids impl-f)
  (test--delete impl-f)
  (test--delete-many impl-f)
  (test--all-drawers impl-f)
  (test--dissoc-at impl-f)
  (test--has-drawer impl-f)
  (test--temp-dresser-id impl-f)
  (test--lexical-encoding impl-f))
