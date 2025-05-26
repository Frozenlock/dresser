(ns dresser.base-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [dresser.base :as db]
            [dresser.impl.hashmap :as hm]))

(deftest lexical-encoding
  (let [n1 3
        n2 15
        n3 200
        n4 10e10
        expected-order [n1 n2 n3 n4]
        id->n (into {} (for [n (reverse expected-order)]
                         [(db/lexical-encode n) n]))]
    (testing "Ordering"
      (is (= expected-order
             (map last (sort-by key id->n)))))
    (testing "Encode/Decode"
      (is (= n3
             (db/lexical-decode
              (db/lexical-encode n3)))))))

(deftest map-tx-test
  (let [dresser (hm/build)]

    (testing "Basic mapping functionality"
      (let [numbers [1 2 3 4 5]
            result (db/map-tx dresser (fn [tx n] (db/with-result tx (* n 2))) numbers)]
        (is (= [2 4 6 8 10] result))))

    (testing "Empty collection handling"
      (let [result (db/map-tx dresser (fn [tx n] (db/with-result tx (* n 2))) [])]
        (is (= dresser result))))

    (testing "Using dresser result as collection"
      (let [result (-> dresser
                       (db/with-result [1 2 3])
                       (db/map-tx (fn [tx n] (db/with-result tx (* n 3)))))]
        (is (= [3 6 9] result))))

    (testing "Order preservation"
      (let [result (db/map-tx dresser (fn [tx n] (db/with-result tx n)) [3 1 4 2])]
        (is (= [3 1 4 2] result))))

    (testing "Transaction context preservation"
      (let [collected-values (atom [])
            result (db/tx-> dresser
                     (db/with-result (range 1 4))
                     (db/map-tx (fn [tx n]
                                  (swap! collected-values conj n)
                                  (db/with-result tx (* n 10)))))]
        (is (= [10 20 30] result))
        (is (= [1 2 3] @collected-values))))

    (testing "Nested transaction handling"
      (let [result (db/tx-> dresser
                     (db/with-result [1 2 3])
                     (db/map-tx (fn [tx n]
                                  (db/tx-> tx
                                    (db/with-result (* n 2))))))]
        (is (= [2 4 6] result))))))

(deftest reduce-tx-test
  (let [dresser (hm/build)]

    (testing "Basic reduction functionality"
      (let [result (-> dresser
                       (db/with-result 0)
                       (db/reduce-tx (fn [tx n]
                                       (db/with-result tx (+ (db/result tx) n)))
                                     [1 2 3 4 5]))]
        (is (= 15 result))))

    (testing "Transaction state propagation"
      (db/tx-let [tx dresser]
          [drawers (-> tx
                       (db/add! :drawer1 {:id "doc1"})
                       (db/add! :drawer2 {:id "doc2"})
                       (db/all-drawers))
           _ (is (= drawers [:drawer1 :drawer2]))
           _ (db/reduce-tx tx db/drop! drawers)
           drawers (db/all-drawers tx)]
        (is (empty? drawers))))))

(deftest fetch-reduce-test
  (let [dresser (hm/build)]

    (testing "Basic reduction functionality"
      (let [documents [{:id "1", :value 10}
                       {:id "2", :value 20}
                       {:id "3", :value 30}]
            dresser (db/raw-> dresser (db/upsert-many! :test-drawer documents))
            result (-> (db/with-result dresser 0)
                       (db/fetch-reduce :test-drawer
                                        (fn [tx doc]
                                          (db/with-result tx (+ (db/result tx) (:value doc))))))]
        (is (= 60 result))))

    (testing "Empty results handling"
      (let [result (db/fetch-reduce dresser :empty-drawer
                                   (fn [tx doc]
                                     (db/with-result tx (+ (db/result tx) 1))))]
        (is (= nil result))))

    (testing "Using query options"
      (let [documents [{:id "1", :value 10, :type "a"}
                       {:id "2", :value 20, :type "a"}
                       {:id "3", :value 30, :type "b"}]
            dresser (db/raw-> dresser (db/upsert-many! :test-drawer-2 documents))
            result (-> (db/with-result dresser 0)
                       (db/fetch-reduce :test-drawer-2
                                        (fn [tx doc]
                                          (db/with-result tx (+ (db/result tx) (:value doc))))
                                        {:where {:type "a"}
                                         :only  {:value :?}}))]
        (is (= 30 result))))

    (testing "Only handling with ID field"
      (let [documents [{:id "1", :value 10, :nested {:data 1}}
                       {:id "2", :value 20, :nested {:data 2}}
                       {:id "3", :value 30, :nested {:data 3}}]
            dresser (db/raw-> dresser (db/upsert-many! :test-drawer-only documents))
            collected-data-with-id (atom [])
            _ (db/fetch-reduce dresser :test-drawer-only
                              (fn [tx doc]
                                (swap! collected-data-with-id conj doc)
                                tx)
                              {:only {:id :?, :value :?}})
            collected-data-without-id (atom [])
            _ (db/fetch-reduce dresser :test-drawer-only
                              (fn [tx doc]
                                (swap! collected-data-without-id conj doc)
                                tx)
                              {:only {:value :?}})]
        (is (= #{{:id "1", :value 10} {:id "2", :value 20} {:id "3", :value 30}}
               (set @collected-data-with-id))
            "Documents should include ID when specified in :only")
        (is (= #{{:value 10} {:value 20} {:value 30}}
               (set @collected-data-without-id))
            "Documents should not include ID when not specified in :only")))

    (testing "Document order processing"
      (let [document-count 120
            documents (for [i (range document-count)]
                       {:id (format "%03d" i), :value i})
            dresser (db/raw-> dresser (db/upsert-many! :test-drawer-order documents))
            processed-ids (atom [])
            _ (db/fetch-reduce dresser :test-drawer-order
                              (fn [tx doc]
                                (swap! processed-ids conj (:id doc))
                                tx)
                              {:chunk-size 25})]
        (is (= (mapv #(format "%03d" %) (range document-count))
               @processed-ids)
            "Documents should be processed in ascending ID order across chunks")))

    (testing "Preserving transaction context"
      (let [documents [{:id "1", :value 10}
                       {:id "2", :value 20}]
            dresser (db/raw-> dresser (db/upsert-many! :test-drawer-5 documents))
            modified-ids (atom [])
            dresser (db/raw-> dresser
                      (db/fetch-reduce :test-drawer-5
                                       (fn [tx doc]
                                         (swap! modified-ids conj (:id doc))
                                         (db/assoc-at! tx :test-drawer-5 (:id doc) [:processed] true))))]
        (is (= #{"1" "2"} (set @modified-ids)))
        (is (= [{:id "1", :value 10, :processed true}
                {:id "2", :value 20, :processed true}]
               (db/fetch dresser :test-drawer-5 {:sort [[[:id] :asc]]})))))))
