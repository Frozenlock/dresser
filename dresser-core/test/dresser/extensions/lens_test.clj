(ns dresser.extensions.lens-test
  (:require [clojure.test :as t :refer [deftest is testing use-fixtures]]
            [dresser.base :as db]
            [dresser.extensions.lens :as lens]
            [dresser.impl.hashmap :as hm]
            [dresser.test :as dt]))

(use-fixtures :once (dt/coverage-check lens))

(defn- test-dresser
  []
  (-> (hm/build)
      (dt/sequential-id)
      (dt/no-tx-reuse)))

(deftest lenses
  (testing "Implementation"
    (dt/test-impl #(lens/lenses (test-dresser))))

  (testing "ILens ('ref' and 'set-ref!')"
    (let [ref {:drawer :drawer
               :doc-id 1}
          lens (-> (lens/lenses (test-dresser))
                   (lens/set-ref! ref))]
      (is (= (lens/ref lens) ref))))

  (testing "add! & get-at"
    (let [doc1 {:title "test1"}
          doc2 {:title "test2"}]
      (db/tx-> (lens/lenses (test-dresser))
        (dt/is-> (lens/add! :docs doc1) (= 1) "Returns doc-id")
        (dt/is-> (lens/get-at) (= (assoc doc1 :id 1)))
        (dt/is-> (lens/get-at [:title]) (:title doc1))

        (dt/testing-> "Add! can use the lens' drawer for new doc"
          (dt/is-> (lens/add! doc2) (= 2) "Returns doc-id")
          (dt/is-> (lens/get-at) (= (assoc doc2 :id 2)))
          (dt/is-> (lens/get-at [:title]) (:title doc2))))))

  (testing "lens?"
    (is (not (lens/lens? {})))
    (is (lens/lens? (lens/lenses (test-dresser)))))

  (testing "lens as function"
    (let [doc1 {:title "test1", :a {:b {:c "node"}}}
          lens (db/raw-> (lens/lenses (test-dresser))
                 (lens/add! :docs doc1))]
      (is (lens) (= (assoc doc1 :id 1)))
      (is (lens [:a :b :c]) (= "node"))))

  (testing "update-at!"
    (db/tx-> (lens/lenses (test-dresser))
      (lens/set-ref! {:drawer :docs, :doc-id 1, :path [:a :b]})
      (lens/update-at! [] str "a" "b")
      (dt/is-> (lens/get-at) (= "ab"))
      (dt/is-> (db/fetch-by-id :docs 1)
               (= {:id 1
                   :a  {:b "ab"}}))))

  (testing "assoc-at!"
    (db/tx-> (lens/lenses (test-dresser))
      (lens/set-ref! {:drawer :docs, :doc-id 1, :path [:a :b]})
      (lens/assoc-at! 123)
      (dt/is-> (lens/get-at) (= 123))
      (dt/is-> (db/fetch-by-id :docs 1)
               (= {:id 1
                   :a  {:b 123}}))))

  (testing "dissoc-at!"
    (db/tx-> (lens/lenses (test-dresser))
      (lens/set-ref! {:drawer :docs, :doc-id 1, :path [:a :b]})
      (lens/assoc-at! {:b.1 {:b.1.1 "val1"
                             :b.1.2 "val2"}})
      (lens/dissoc-at! [:b.1] :b.1.1)
      (dt/is-> (lens/get-at) (= {:b.1 {:b.1.2 "val2"}}))
      (lens/dissoc-at!)
      (dt/is-> (lens/get-at) nil?)
      (dt/is-> (db/get-at :docs 1 []) (= {:a {}, :id 1}))))

  (testing "focus - "
    (let [doc1 {:title "test1", :a {:b {:c "node"}}}]
      (db/tx-> (lens/lenses (test-dresser))
        (lens/add! :docs doc1)
        (lens/focus [:a :b])
        (dt/testing-> "read"
          (dt/is-> (lens/get-at) (= (get-in doc1 [:a :b]))))
        (dt/testing-> "write"
          (dt/is-> (lens/update-at! [:c] str "-2") (= "node-2"))
          (dt/is-> (lens/get-at) (= {:c "node-2"})))))))
