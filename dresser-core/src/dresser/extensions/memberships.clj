(ns dresser.extensions.memberships
  "Provides groups/memberships relations between documents."
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.durable-refs :as refs]
            [dresser.protocols :as dp]))

;; Some suggested roles
(def role-admin :admin)
(def role-editor :editor)
(def role-reader :reader)

;; The user/member

(defn- add-group-to-member! ; Can break relation if used alone
  [dresser member-ref grp-ref]
  (db/tx-> dresser
    (refs/assoc-at! member-ref [:drs_memberships :member-of grp-ref]
                    true)))

(defn- remove-group-from-member! ; Can break relation if used alone
  [dresser member-ref grp-ref]
  (db/tx-> dresser
    (refs/dissoc-at! member-ref [:drs_memberships :member-of]  grp-ref)))

(defn memberships-of-member
  "Returns all refs for which target is a member."
  [dresser member-ref]
  (db/tx-> dresser
    (refs/get-at member-ref [:drs_memberships :member-of])
    (db/update-result keys)))

;; The group

(defn roles-map
  "Ensure the roles are in a map format."
  [roles]
  (if (map? roles) roles
      (reduce #(assoc %1 %2 true) {} roles)))

(defn upsert-group-member!
  "Sets the roles of member inside group.
  If no roles are provided, the member is removed.

  'roles' can be a map of roles or a collection of those keys.

  Returns the grp-ref."
  [dresser grp-ref member-ref roles]
  (let [roles (roles-map roles)
        roles? (not (empty? roles))]
    (db/tx-> dresser
      (cond->
          (not roles?) (-> (refs/update-at! grp-ref [:drs_memberships :member->roles] dissoc member-ref)
                           (remove-group-from-member! member-ref grp-ref))
          roles? (-> (refs/assoc-at! grp-ref [:drs_memberships :member->roles member-ref] roles)
                     (add-group-to-member! member-ref grp-ref))
          true (db/with-result grp-ref)))))

(defn members-of-group
  "Returns all the refs of members."
  [dresser grp-ref]
  (db/tx-> dresser
    (refs/get-at grp-ref [:drs_memberships :member->roles])
    (db/update-result (comp vec keys))))

(defn remove-member-from-group!
  "Removes the member from a group.
  Returns the group."
  [dresser grp-ref member-ref]
  (db/tx-> dresser
    (upsert-group-member! grp-ref member-ref nil)))

(defn remove-all-members-from-group!
  [dresser grp-ref]
  (db/tx-let [tx dresser]
      [members-refs (members-of-group tx grp-ref)]
    (reduce (fn [tx' member-ref] (remove-member-from-group! tx' grp-ref member-ref))
            tx members-refs)))

(defn members-of-group-with-roles
  "Returns the refs for members with any of the given roles.

  'roles' can be a map of roles or a collection of those keys."
  [dresser grp-ref roles]
  (let [roles-m (roles-map roles)]
    (db/tx-> dresser
      (refs/get-at grp-ref [:drs_memberships :member->roles])
      (db/update-result (fn [member->roles]
                          (->> (for [[member stored-roles] member->roles
                                     :when (some roles-m (keys stored-roles))]
                                 member)
                               (remove nil?)
                               (vec)))))))


(defn leave-all-groups!
  "Removes member from all of its groups."
  [dresser member-ref]
  (db/tx-let [tx dresser]
      [grp-refs (memberships-of-member tx member-ref)]
    (reduce (fn [tx' grp-ref]
              (remove-member-from-group! tx' grp-ref member-ref))
            tx grp-refs)))

(defn remove-all-member-and-group-references!
  "Leave all groups and remove all members."
  [dresser target-ref]
  (db/tx-> dresser
    (remove-all-members-from-group! target-ref)
    (leave-all-groups! target-ref)))


(defn add-with-roles!
  "Same as `db/add!`, but add the newly-added document to the group with
  the provided roles.

  'roles' can be a map of roles or a collection of those keys.

  Ex: add an API key, set the API key as a reader of group.
  Returns the new document ID."
  [dresser grp-ref roles drawer data]
  (db/tx-let [tx dresser]
      [id (db/add! tx drawer data)
       member-ref (refs/ref! tx drawer id)]
    (-> (upsert-group-member! tx grp-ref member-ref roles)
        (db/with-result id))))


;; Extension

(defn- wipe!
  [tx drawer where]
  (let [f (refs/expand-fn remove-all-member-and-group-references!)]
    (db/fetch-reduce
     tx drawer
     #(f %1 drawer (:id %2))
     {:where (assoc-in where
                       [:drs_memberships db/exists?] true)
      :only  {:id :?}})))


(ext/defext keep-sync
  "Automatically maintains membership relations when deleting a document or
  dropping a drawer."
  []
  {:deps [refs/durable-refs]
   :wrap-configs

   ;; TODO: only remove if the drawer IDs no longer exist


   {`dp/-delete-many {:wrap (fn [delete-method]
                              (fn [tx drawer where]
                                (-> (wipe! tx drawer where)
                                    (delete-method drawer where))))}
    `dp/-drop   {:wrap (fn [drop-method]
                         (fn [tx drawer]
                           (-> (wipe! tx drawer {})
                               (drop-method drawer))))}}})
