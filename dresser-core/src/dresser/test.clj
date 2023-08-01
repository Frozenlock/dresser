(ns dresser.test
  (:require [dresser.base :as db]
            [dresser.protocols :as dp]
            [clojure.test :as t :refer [is testing]]
            [dresser.test :as dt]
            [clojure.set :as set]
            [dresser.drawer :as dd]))

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


(defn test-drawer [x]
  "Returns a derefable drawer which returns a drawer-key half to the
time and a drawer-object the other half."
  (let [*return-drawer-key? (atom false)
        drawer (dd/drawer x)]
    (reify clojure.lang.IDeref
      (deref [_]
        (if-let [dk? (swap! *return-drawer-key? not)]
          (dd/key drawer)
          drawer)))))

(def drawer1 (test-drawer :drawer1))
(def drawer2 (test-drawer :drawer2))


(def ^:dynamic *disable-tx-counter* false)


(defmethod t/assert-expr 'thrown-with-cause-msg?
  [msg form]
  (let [re (nth form 1)
        body (nthnext form 2)]
    `(try ~@body
          (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
          (catch clojure.lang.ExceptionInfo tx-e#
            (let [e# (ex-cause tx-e#)
                  m# (.getMessage e#)]
              (if (re-find ~re m#)
                (t/do-report {:type :pass, :message ~msg,
                         :expected '~form, :actual e#})
                (t/do-report {:type :fail, :message ~msg,
                         :expected '~form, :actual e#})))
            tx-e#))))

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
       `(let [[dresser# ~'actual] (db/dr (-> ~dresser ~form))]
          (is ~(if (seq? test-form)
                 `(~@test-form ~'actual)
                 (list test-form ~'actual))
              ~msg)
          dresser#)))))

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
  "Replaces gen-id implementation (metadata) with ones that increases sequentially.

  Ephemeral DB only; does not check for existing IDs.
  Only works at the fundamental level."
  [dresser]
  (let [*counter (atom 0)]
    (->> (dp/mapify-impls [(dp/impl -gen-id
                             [dresser _drawer]
                             (db/with-result dresser
                               (swap! *counter inc)))])
         (vary-meta dresser merge))))

(let [ns *ns*]
  (defn no-tx-reuse
    "Throws if a transaction state is used more than once.

  Warning: cannot validate that the provided dresser doesn't re-use
  transaction states in its own implementations."
    [dresser]
    (let [*step-count (atom 0)
          s->m (merge (into {} (for [[sym {tx :tx}] dp/dresser-methods
                                     :when tx
                                     :let [m (resolve sym)]]
                                 [sym (-> (fn [dresser & args]
                                          (db/transact
                                           dresser
                                           (fn [wrapper-tx]
                                             (update wrapper-tx :source
                                                     #(apply m % args)))))
                                        (with-meta {:ns ns}))]))
                      (dp/mapify-impls
                       [(dp/impl -transact
                          [dresser f result?]
                          (if (:transact dresser)
                            ;; Transaction is already open
                            (do
                              (when-not *disable-tx-counter*
                                (when-not (= @*step-count (:step dresser 0))
                                  (throw
                                   (ex-info "Transaction state reused"
                                            {:outside-count @*step-count
                                             :inner-count   (:step dresser 0)})))
                                (swap! *step-count inc))
                              (try
                                (f (if *disable-tx-counter*
                                     dresser
                                     (update dresser :step (fnil inc 0))))
                                ; Restore previous count if we encounter an exception
                                (catch Exception e (reset! *step-count (:step dresser)) (throw e))))

                            ;; Transaction is not yet open
                            (let [updated-source (db/transact
                                                  (:source dresser)
                                                  (fn [src-tx]
                                                    (try
                                                      (:source (f (assoc dresser :transact true :source src-tx)))
                                                      (catch Exception e (reset! *step-count 0) (throw e))))
                                                  false)
                                  return (assoc dresser :source updated-source)]
                              (reset! *step-count 0)
                              (if result?
                                (db/result return)
                                (dissoc return :transact)))))
                        (dp/impl -start
                          [dresser]
                          (update dresser :source db/start))
                        (dp/impl -stop
                          [dresser]
                          (update dresser :source db/stop))
                        (dp/impl -temp-data
                          [dresser]
                          (db/temp-data (:source dresser)))
                        (dp/impl -with-temp-data
                          [dresser data]
                          (update dresser :source db/with-temp-data data))]))]
      (-> (assoc {} :source dresser)
          (vary-meta merge s->m {:type ::db/dresser})))))

(defn u= "unordered-equal"
  [x y] (= (set x) (set y)))


(defn test--upsert-&-fetch
  [impl-f]
  (testing "Upsert & Fetch"
    (let [impl (impl-f)]
      ;; Throws if ID is missing
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Missing document ID"
           (db/upsert! impl @drawer1 {:a 1})))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Missing document ID"
           (db/upsert! impl @drawer1 {:a 1, :id nil})))

      (let [test-doc {:id         :id1,
                      :a          [{:b "string"
                                    :c :keyword}]
                      [:vec :key] 12}]
        (db/tx-> impl
          (is-> (db/upsert! @drawer1 test-doc) (= test-doc)
                "Expected return")
          (is-> (db/fetch-by-id @drawer1 :id1) (= test-doc)
                "Document correctly upserted")
          (is-> (db/upsert! @drawer1 {:id :id1, :a 2}) (= {:id :id1, :a 2})
                "Document overwritten")))
      (is (= {:id :some-id}
             (db/tx-> impl
               (db/upsert! @drawer1 {:id :some-id})
               (db/fetch-by-id @drawer1 :some-id)))))

    ;; TODO: should we accept map as keys?
    ;; (testing "Maps as keys"
    ;;   ;; Maps ordered differently. A naive serialization might miss
    ;;   ;; this one and consider them 'not equal'.
    ;;   (let [impl (impl-f)
    ;;         m1 {{:a 1, :b 2} "map1"}
    ;;         m2 {{:b 2, :a 1} "map2"}]
    ;;     (db/tx-> impl
    ;;       (db/add! :drawer m1)
    ;;       (db/add! :drawer m2)
    ;;       (is-> (db/fetch :drawer {:only {{:a 1, :b 2} :?}})
    ;;             (= [m1 m2])))))
    ))

(defn test--upsert-all
  [impl-f]
  (testing "Upsert all"
    (db/tx-let [tx (impl-f)]
        [[doc1 doc2 doc3 :as docs] [{:id 1, :a 1}, {:id 2, :b 2}, {:id "a", :c 3}]
         _ (-> tx
               (is-> (db/upsert-all! @drawer1 docs) (= docs)
                     "Returns coll.")
               (is-> (db/fetch-by-id @drawer1 1) (= doc1))
               (is-> (db/fetch-by-id @drawer1 2) (= doc2))
               (is-> (db/fetch-by-id @drawer1 "a") (= doc3)))]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Missing document ID"
           (db/upsert-all! tx @drawer1 {:a 1 :b "String"})))
      tx)))


(defn test--fetch
  [impl-f]
  (testing "Fetch"
    (let [;; Introduce randomness in docs order with :r
          [d1 d2 d3 d4] [{:id 1, :sort {:a 1, :b 1}, :1 1, :r (rand)},
                         {:id 2, :sort {:a 1, :b 2}, :1 1, :2 2, :r (rand)},
                         {:id 3, :sort {:a 2, :b 2}, :1 1, :2 2, :3 3, :c {:d :nested, :n 4}, :r (rand)}
                         {:id 4, :sort {:a 3} :r (rand)}]
          docs [d1 d2 d3 d4]]
      (db/tx-> (impl-f)
        (db/upsert-all! @drawer1 (sort-by :r docs))
        ;tx
        (testing-> "- Where"
          (is-> (db/fetch @drawer1 {:where {:1 1}}) (u= [d1 d2 d3]))
          (is-> (db/fetch @drawer1 {:where {:2 2}}) (u= [d2 d3]))
          (is-> (db/fetch @drawer1 {:where {:c {:d :nested}}}) (u= [d3]))

          (testing-> " - query op"
            (testing-> " - exists?"
              (is-> (db/fetch @drawer1 {:where {:c {db/exists? true}}}) (u= [d3]))
              (is-> (db/fetch @drawer1 {:where {:d {db/exists? true}}}) (u= []))
              (is-> (db/fetch @drawer1 {:where {:1 {db/exists? false}}}) (u= [d4])))

            (testing-> " - gt(e) (bigger)"
              (is-> (db/fetch @drawer1 {:where {:1 {db/gt 1}}}) (u= []))
              (is-> (db/fetch @drawer1 {:where {:2 {db/gt 1}}}) (u= [d2 d3]))
              (is-> (db/fetch @drawer1 {:where {:1 {db/gte 1}}}) (u= [d1 d2 d3]))
              (is-> (db/fetch @drawer1 {:where {:2 {db/gte 1}}}) (u= [d2 d3])))

            (testing-> " - lt(e) (smaller)"
              (is-> (db/fetch @drawer1 {:where {:1 {db/lt 2}}}) (u= [d1 d2 d3]))
              (is-> (db/fetch @drawer1 {:where {:2 {db/lt 2}}}) (u= []))
              (is-> (db/fetch @drawer1 {:where {:1 {db/lte 2}}}) (u= [d1 d2 d3]))
              (is-> (db/fetch @drawer1 {:where {:2 {db/lte 2}}}) (u= [d2 d3])))))

        (testing-> "- Only"
          (is-> (db/fetch @drawer1 {:only {:id :?}}) (u= (for [{:keys [id]} docs]
                                                           {:id id})))
          (testing-> " - Nested"
            (is-> (db/fetch @drawer1 {:only {:c {:d :?}}}) (u= [{} {} {:c {:d :nested}}]))))

        (testing-> "- Limit"
          (is-> (-> (db/fetch @drawer1 {:limit 2
                                       :where {:1 1}})
                    (db/update-result count))
                (= 2))))

      (testing " - Sort"
        (db/tx-> (impl-f)
          ;; No randomness, insert by ID order
          (db/upsert-all! @drawer1 (sort-by :id docs))
          ;; Simple sort
          (is-> (db/fetch @drawer1 {:sort [[[:id] :asc]]})
                (= [d1 d2 d3 d4]))
          ;; Equality in the sort
          (is-> (db/fetch @drawer1 {:sort [[[:sort :a] :desc]]})
                (or (= [d4 d3 d1 d2])
                    (= [d4 d3 d2 d1])))
          ;; Sort by multiple values
          (is-> (db/fetch @drawer1 {:sort [[[:sort :a] :asc]
                                          [[:sort :b] :desc]]})
                (= [d2 d1 d3 d4]))
          ;; Make sure the sorting occurs BEFORE the limit
          (testing-> " sort/limit eval order"
            (is-> (db/fetch @drawer1 {:limit 1
                                     :sort  [[[:id] :desc]]})
                  (= [d4]))
            (is-> (db/fetch @drawer1 {:limit 1
                                     :sort  [[[:id] :asc]]})
                  (= [d1])))))

      (testing " - Skip"
        (db/tx-let [tx (impl-f)]
            [_ (db/upsert-all! tx @drawer1 docs)
             f1 (db/fetch tx @drawer1 {:limit 2})
             ids1 (set (map :id f1))
             f2 (db/fetch tx @drawer1 {:limit 2, :skip 2})
             ids2 (set (map :id f2))]
          (is (empty? (set/intersection ids1 ids2)))
          tx)))))

(defn test--add-&-fetch-by-id
  [impl-f]
  (testing "Add & fetch-by-id"
    (db/tx-let [tx (impl-f)]
        [[doc1 doc2] [{:a 1 :b "String"}, {:b 2 :c {:nested "data"}}]
         id1 (db/add! tx @drawer1 doc1)
         id2 (db/add! tx @drawer1 doc2)
         id->doc {id1 (assoc doc1 :id id1)
                  id2 (assoc doc2 :id id2)}]
      (-> tx
          (is-> (-> (db/fetch-by-id @drawer1 id1)
                    (db/update-result #(dissoc % :id)))
                (= doc1))
          (is-> (-> (db/fetch-by-id @drawer1 id2)
                    (db/update-result #(dissoc % :id)))
                (= doc2))))))

(defn test--transact
  [impl-f]
  (testing "Transact"
    (let [doc {:id :some-id}]
      ;; Test that we get the expected value OUTSIDE the transaction
      (is (= doc (db/transact (impl-f) #(db/upsert! % @drawer1 doc))))
      ;; ... Except when we ask to not extract the result
      (is (db/dresser? (db/transact (impl-f) #(db/upsert! % @drawer1 doc) false))))

    (testing "Rollback on exception"
      ;; Exceptions thrown inside a transaction should cause a rollack
      (let [doc {:id "some-id", :str "string"}
            dresser (db/transact (impl-f)
                                 (fn [dresser] (db/upsert! dresser @drawer1 doc))
                                 false)] ; <-- standalone operation

        ; Now make a few changes inside a transaction
        (try
          (db/tx-> dresser
            (db/assoc-at! @drawer1 "some-id" [:str2] "string 2")
            (db/update-at! @drawer1 [:str] #(throw (ex-info "Test exception" {}))))
          (catch Exception _e))

        (is (= doc (db/fetch-by-id dresser @drawer1 "some-id")))))))

(defn test--assoc-at
  [impl-f]
  (testing "Assoc-at existing document"
    (db/tx-let [tx (impl-f)]
        [id (db/add! tx @drawer1 {:a {:b {:c 1}}, :z 1})]
      (-> tx
          (is-> (db/assoc-at! @drawer1 id [:a :b :c] {:d "hello"})
                (= {:d "hello"})
                "Returns the value that was assoc'd-in.")
          (is-> (db/fetch-by-id @drawer1 id {:only [:id]})
                (#(some? (:id %))))
          (is-> (db/fetch-by-id @drawer1 id {:only [:a :z]})
                (= {:a {:b {:c {:d "hello"}}}, :z 1}))
          (testing-> "nil isn't special"
            (db/assoc-at! @drawer1 id [:a :b :c] nil)
            (is-> (db/fetch-by-id @drawer1 id) (= {:id id, :z 1, :a {:b {:c nil}}})))
          (testing-> "empty path"
            (db/assoc-at! @drawer1 id [] {:a "hello"})
            (is-> (db/fetch-by-id @drawer1 id) (= {:id id :a "hello"}))))))

  (testing "Assoc-at creates new document"
    (db/tx-let [tx (impl-f)]
        [id "my-id"]
      (-> tx
          (is-> (db/assoc-at! @drawer1 id [:a :b :c] {:d "hello"})
                (= {:d "hello"})
                "Returns the value that was assoc'd-in.")

          (is-> (db/fetch-by-id @drawer1 id)
                (= {:a {:b {:c {:d "hello"}}} :id id}))))))

(defn test--get-at
  [impl-f]
  (testing "Get-at"
    (db/tx-let [tx (impl-f)]
        [doc {:a {:b {:c 1}}, :z 1}
         id (db/add! tx @drawer1 doc)]
      (-> tx
          (is-> (db/get-at @drawer1 id [:a :b :c]) (= 1))
          (is-> (db/get-at @drawer1 id [:a :b :c :d]) nil?)
          (is-> (db/get-at @drawer1 id []) (= (assoc doc :id id)))))))

(defn test--update-at
  [impl-f]
  (testing "Update-at"
    (db/tx-let [tx (impl-f)]
        [id (db/add! tx @drawer1 {:a {:b {:c 1}}, :z 1})]
      (-> tx
          (is-> (db/update-at! @drawer1 id [:a :b :c] + 1 1 1) (= 4)
                "Returns the value that was updated-in.")
          (is-> (db/get-at @drawer1 id [:a :b :c]) (= 4))
          (db/update-at! @drawer1 id [:a :b] dissoc :c)
          (is-> (db/get-at @drawer1 id [:a :b :c]) nil?)
          (testing-> "Can't remove document-id"
            (db/update-at! @drawer1 id [:a :b] dissoc :id)
            (is-> (db/get-at @drawer1 id [:id]) (= id)))
          (testing-> "nil result"
            (db/update-at! @drawer1 id [:a :b :c] (constantly nil))
            (is-> (db/fetch-by-id @drawer1 id) (= {:id id, :z 1, :a {:b {:c nil}}})))
          ;; (testing-> "nil doc ID"
          ;;   (db/update-at! @drawer1 nil [:a :b :c] (constantly nil))
          ;;   (is-> (db/fetch-by-id @drawer1 id) (= {:id id, :z 1, :a {:b {:c nil}}})))
          (testing-> "Empty path"
            (is-> (db/update-at! @drawer1 id [] merge {:a 1})
                  (= {:id id, :a 1, :z 1})))))))

(defn test--dissoc-at
  [impl-f]
  (testing "dissoc-at"
    (db/tx-let [tx (impl-f)]
        [id (db/add! tx @drawer1 {:a {:b {:c1 1, :c2 2}}, :z 1})]
      (-> tx
          (is-> (db/dissoc-at! @drawer1 id [:a :b] :c1) nil?)
          (is-> (db/get-at @drawer1 id [:a :b]) (= {:c2 2}))
          (testing-> "empty path"
            (db/dissoc-at! @drawer1 id [] :a)
            (is-> (db/get-at @drawer1 id []) (= {:id id :z 1})))
          (testing-> "Can't remove document-id"
            (db/dissoc-at! @drawer1 id [] :id)
            (is-> (db/get-at @drawer1 id []) :id))))))

(defn test--replace
  [impl-f]
  (testing "Replace"
    (db/tx-let [tx (impl-f)]
        [doc1 {:a 1 :b "String"}
         new-data {:data [1 2 3]}
         id (db/add! tx @drawer1 doc1)]
      (-> tx
          (is-> (db/replace! @drawer1 id new-data) (= (assoc new-data :id id)))
          (is-> (db/fetch-by-id @drawer1 id) (= (assoc new-data :id id)))

          (testing-> "Can't overwrite document-id"
            (db/replace! @drawer1 id {:id "new-id"})
            (is-> (db/fetch-by-id @drawer1 id) (= {:id id}))
            (is-> (db/fetch-by-id @drawer1 "new-id") nil?))))))

(defn test--drop
  [impl-f]
  (testing "Drop"
    (let [d1 @drawer1]
      (db/tx-> (impl-f)
        (db/add! @drawer1 {:a 1})
        (db/add! @drawer1 {:b 2})
        (db/add! @drawer2 {:c 3})
        (is-> (db/drop! d1) (= d1))
        (is-> (db/all-ids @drawer1) empty?)
        (is-> (db/all-ids @drawer2) seq)))))

(defn test--all-ids
  [impl-f]
  (testing "All-ids"
    (let [docs [{:id 1} {:id 2} {:id 3}]]
      (db/tx-> (impl-f)
        (db/upsert-all! @drawer1 docs)
        (is-> (db/all-ids @drawer1) (= (map :id docs)))))))

(defn test--delete
  [impl-f]
  (testing "Delete"
    (let [[doc1 :as docs] [{:id 1} {:id 2} {:id 3}]]
      (db/tx-> (impl-f)
        (db/upsert-all! (dd/drawer @drawer1) docs)
        (is-> (db/delete! @drawer1 nil) nil?)
        (is-> (db/delete! @drawer1 1) (= 1) "Returns id.")
        (is-> (db/delete! (dd/drawer @drawer1) 2) (= 2) "Returns id.")
        (is-> (db/fetch-by-id @drawer1 1) nil?)
        (is-> (db/fetch-by-id @drawer1 2) nil?)
        (is-> (db/fetch-by-id @drawer1 3) some?)
        (is-> (db/upsert! @drawer1 doc1) some?)))))

(defn test--all-drawers
  [impl-f]
  (testing "All-drawers"
    (db/tx-> (impl-f)
      (is-> (db/all-drawers) empty?)

      (db/add! @drawer1 {:id 1})
      (db/add! @drawer1 {:id 1})
      (is-> (db/all-drawers) (= [(dd/key @drawer1)])
            "No duplicate drawers")

      (db/add! @drawer2 {:id 1})
      (is-> (db/all-drawers) (u= [(dd/key @drawer1) (dd/key @drawer2)])))))

(defn test--has-drawer
  [impl-f]
  (testing "has-drawer"
    (db/tx-> (impl-f)
      (is-> (db/has-drawer? @drawer1) not)

      (db/add! @drawer1 {:id 1})
      (is-> (db/has-drawer? @drawer1) true?))))

(defn test--dresser?
  [impl-f]
  (is (db/dresser? (impl-f))
      "Implementation should have :dresser.db/dresser? in its metadata"))

;; TODO: should it throw an exception if the new-name already exists?
(defn test--rename-drawer
  [impl-f]
  (testing "Rename drawer"
    (db/tx-> (impl-f)
      (db/upsert! :d1 {:id 1, :name "Bob"})
      (db/upsert! :d2 {:id 1, :name "John"})
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
    (let  [dresser (impl-f)
           initial-temp-data (db/temp-data dresser)]
      ;; Complete transaction, exit by returning result.
      (is (= {:data 1234}
             (db/temp-data (db/with-temp-data dresser {:data 1234}))))
      ;; Use the initial dresser. Its temp-data should not have been
      ;; modified by the previous transaction.
      (is (= initial-temp-data (db/temp-data dresser))))))

(defn test--lazyness
  [impl-f]
  (testing "Lazyness is realized before closing transaction"
    (let [qty 1000
          docs (for [i (range 1000)]
                 {:id i})
          dresser (db/raw-> (impl-f)
                    (db/upsert-all! @drawer1 docs))]
      ;; If the transaction was closed before the seq is realized,
      ;; this should throw an exception.
      (is (= qty (count (db/fetch dresser @drawer1 {})))))))

(defn test--dont-blow-up-stack
  [impl-f]
  (time
    (testing "Transactions don't blow up the stack"
      (let [add-doc! (fn [dresser idx]
                       (db/with-tx [tx dresser]
                         (db/upsert! tx @drawer1 {:id idx})))]
        (db/with-tx [tx (impl-f)]
          (reduce (fn [tx' idx]
                    (add-doc! tx' idx))
                  tx (range 100000)))))))

(defn test-impl
  [impl-f]
  (test--lazyness impl-f)
  ;; (test--dont-blow-up-stack impl-f)
  (test--immutable-temp-data impl-f)
  (test--rename-drawer impl-f)
  (test--dresser? impl-f)
  (test--upsert-&-fetch impl-f)
  (test--transact impl-f)
  (test--replace impl-f)
  (test--upsert-all impl-f)
  (test--fetch impl-f)
  (test--add-&-fetch-by-id impl-f)
  (test--replace impl-f)
  (test--assoc-at impl-f)
  (test--get-at impl-f)
  (test--update-at impl-f)
  (test--replace impl-f)
  (test--drop impl-f)
  (test--all-ids impl-f)
  (test--delete impl-f)
  (test--all-drawers impl-f)
  (test--dissoc-at impl-f)
  (test--has-drawer impl-f))
