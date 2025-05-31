(ns dresser.impl.optional
  (:require [dresser.base :as db]
            [dresser.protocols :as dp]))

;; Default implementations for DresserOptional protocol methods
;; These delegate to fundamental methods and can be used in extend-protocol

(defn gen-id
  [tx _drawer]
  ;; This should probably be UUIDv7, but it isn't available yet.
  (db/with-result tx (str (random-uuid))))

(defn add
  [tx drawer data]
  (let [[tx document-id] (db/dr (db/gen-id! tx drawer))]
    (-> (db/assoc-at! tx drawer document-id [] data)
        (db/with-result document-id))))

(defn all-ids
  [tx drawer]
  (-> tx
      (db/fetch drawer {:only [:id]})
      (db/update-result #(mapv :id %))))

(defn update-at
  [tx drawer id ks f args]
  (let [[tx old-data] (db/dr (db/get-at tx drawer id ks))
        new-data (apply f old-data args)]
    (-> (db/assoc-at! tx drawer id ks new-data)
        (db/with-result new-data))))

(defn dissoc-at
  [tx drawer id ks dissoc-ks]
  (-> (db/update-at! tx drawer id ks #(apply dissoc % dissoc-ks))
      (db/with-result nil)))

(defn get-at
  [tx drawer id ks only]
  (let [only-m (if (seq ks)
                 (reduce #(hash-map %2 %1) (or only :?)
                         (reverse ks))
                 only)]
    (-> tx
        (db/fetch-by-id drawer id {:only only-m})
        (db/update-result #(get-in % ks)))))

(defn fetch-by-id
  [tx drawer id only where]
  (-> tx
      (db/fetch drawer {:only  only
                        :where (assoc where :id id)})
      (db/update-result first)))

(defn fetch-count
  [tx drawer where]
  (-> tx
      (db/fetch drawer {:only  [::fake-key]
                        :where where})
      (db/update-result count)))

(defn upsert-many
  [tx drawer docs]
  (-> (reduce (fn [tx data]
                (db/assoc-at! tx drawer (:id data) [] data))
              tx docs)
      (db/with-result docs)))

(defn has-drawer?
  [tx drawer]
  (db/tx-let [tx tx]
      [ret (db/fetch tx drawer {:limit 1
                                :only  [::fake-key]})]
    (> (count ret) 0)))

(defn dresser-id
  [tx]
  (let [[tx d-id] (db/dr (db/get-at tx db/drs-drawer db/drs-doc-id [:dresser-id]))]
    (if d-id
      tx
      (let [[tx d-id] (db/dr (db/gen-id! tx db/drs-drawer))]
        (db/assoc-at! tx db/drs-drawer db/drs-doc-id [:dresser-id] d-id)))))

(defn drawer-key
  [tx drawer-id]
  (db/with-result tx drawer-id))

;; Drawer stuff. Might migrate later.

(def drawer-registry :drs_drawer-registry)

(defn rename-drawer
  [dresser drawer new-drawer]
  (db/tx-let [tx dresser]
      [;; fetch all the documents
       docs (db/fetch tx drawer {})
       _ (db/upsert-many! tx new-drawer docs)
       _ (db/drop! tx drawer)]
    (db/with-result tx new-drawer)))


;; Map of default optional implementations for metadata
(def default-implementations
  {`dp/fetch-by-id    fetch-by-id
   `dp/fetch-count    fetch-count
   `dp/update-at      update-at
   `dp/add            add
   `dp/all-ids        all-ids
   `dp/dissoc-at      dissoc-at
   `dp/gen-id         gen-id
   `dp/get-at         get-at
   `dp/upsert-many    upsert-many
   `dp/dresser-id     dresser-id
   `dp/drawer-key     drawer-key
   `dp/rename-drawer  rename-drawer
   `dp/has-drawer?    has-drawer?})

