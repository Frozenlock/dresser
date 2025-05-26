(ns dresser.impl.optional
  (:require [dresser.base :as db]
            [dresser.protocols :as dp]))

(dp/defimpl -gen-id
  [tx _drawer]
  ;; This should probably be UUIDv7, but it isn't available yet.
  (db/with-result tx (str (random-uuid))))

(dp/defimpl -add
  [tx drawer data]
  (let [[tx document-id] (db/dr (db/gen-id! tx drawer))]
    (-> (db/assoc-at! tx drawer document-id [] data)
        (db/with-result document-id))))

(dp/defimpl -all-ids
  [tx drawer]
  (-> tx
      (db/fetch drawer {:only [:id]})
      (db/update-result #(mapv :id %))))

(dp/defimpl -update-at
  [tx drawer id ks f args]
  (let [[tx old-data] (db/dr (db/get-at tx drawer id ks))
        new-data (apply f old-data args)]
    (-> (db/assoc-at! tx drawer id ks new-data)
        (db/with-result new-data))))


(dp/defimpl -dissoc-at
  [tx drawer id ks dissoc-ks]
  (-> (db/update-at! tx drawer id ks #(apply dissoc % dissoc-ks))
      (db/with-result nil)))

(dp/defimpl -get-at
  [tx drawer id ks only]
  (let [only-m (if (seq ks)
                 (reduce #(hash-map %2 %1) (or only :?)
                         (reverse ks))
                 only)]
    (-> tx
        (db/fetch-by-id drawer id {:only only-m})
        (db/update-result #(get-in % ks)))))

(dp/defimpl -fetch-by-id
  [tx drawer id only where]
  (-> tx
      (db/fetch drawer {:only  only
                        :where (assoc where :id id)})
      (db/update-result first)))

(dp/defimpl -fetch-count
  [tx drawer where]
  (-> tx
      (db/fetch drawer {:only  [::fake-key]
                        :where where})
      (db/update-result count)))

(dp/defimpl -upsert-many
  [tx drawer docs]
  (-> (reduce (fn [tx data]
                (db/assoc-at! tx drawer (:id data) [] data))
              tx docs)
      (db/with-result docs)))

(dp/defimpl -drop
  [tx drawer]
  (let [[tx all-ids] (db/dr (db/all-ids tx drawer))]
    (-> (reduce (fn [tx id]
                  (db/delete! tx drawer id))
                tx
                all-ids)
        (db/with-result drawer))))

(dp/defimpl -has-drawer?
  [tx drawer]
  (db/tx-let [tx tx]
      [ret (db/fetch tx drawer {:limit 1
                                :only  [::fake-key]})]
    true
    (> (count ret) 0)))

(dp/defimpl -dresser-id
  [tx]
  (let [[tx d-id] (db/dr (db/get-at tx db/drs-drawer db/drs-doc-id [:dresser-id]))]
    (if d-id
      tx
      (let [[tx d-id] (db/dr (db/gen-id! tx db/drs-drawer))]
        (db/assoc-at! tx db/drs-drawer db/drs-doc-id [:dresser-id] d-id)))))

(dp/defimpl -drawer-key
  [tx drawer-id]
  (db/with-result tx drawer-id))

;; Drawer stuff. Might migrate later.

(def drawer-registry :drs_drawer-registry)

(dp/defimpl -rename-drawer
  [dresser drawer new-drawer]
  (db/tx-let [tx dresser]
      [;; fetch all the documents
       docs (db/fetch tx drawer {})
       _ (db/upsert-many! tx new-drawer docs)
       _ (db/drop! tx drawer)]
    (db/with-result tx new-drawer)))




(def optional-impl
  "Naive/slow implementation of higher order dresser method."
  (dp/mapify-impls
   [-add
    -all-ids
    -dissoc-at
    -dresser-id
    -drawer-key
    -drop
    -fetch-by-id
    -fetch-count
    -gen-id
    -get-at
    -has-drawer?
    -rename-drawer
    -update-at
    -upsert-many]))
