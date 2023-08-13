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
    (refs/update-at! member-ref [:drs_memberships :member-of]
                    #(distinct (conj % grp-ref)))))

(defn- remove-group-from-member! ; Can break relation if used alone
  [dresser member-ref grp-ref]
  (db/tx-> dresser
    (refs/update-at! member-ref [:drs_memberships :member-of]
                    #(remove #{grp-ref} %))))

(defn memberships-of-member
  "Returns all refs for which target is a member."
  [dresser grp-ref]
  (db/tx-> dresser
    (refs/get-at grp-ref [:drs_memberships :member-of])
    (db/update-result #(or % []))))

;; The group

(defn upsert-group-member!
  "Sets the roles of member inside group.
  If no roles are provided, the member is removed.

  Returns the grp-ref."
  [dresser grp-ref member-ref roles]
  (assert (or (nil? roles) (seq? roles) (vector? roles)) "Roles should be nil, a seq or a vector")
  (let [roles? (seq roles)]
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
  "Returns the refs for members with any of the given roles."
  [dresser grp-ref roles]
  (assert (or (nil? roles) (seq? roles) (vector? roles)) "Roles should be nil, a seq or a vector")
  (db/tx-> dresser
    (refs/get-at grp-ref [:drs_memberships :member->roles])
    (db/update-result #(->> (for [[member stored-roles] %
                                  :when (some (set roles) stored-roles)]
                              member)
                            (remove nil?)
                            (vec)))))

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

  Ex: add an API key, set the API key as a reader of group.
  Returns the new document ID."
  [dresser grp-ref roles drawer data]
  (db/tx-let [tx dresser]
      [id (db/add! tx drawer data)
       member-ref (refs/ref! tx drawer id)]
    (-> (upsert-group-member! tx grp-ref member-ref roles)
        (db/with-result id))))


;; Extension


(ext/defext keep-sync
  "Automatically maintains membership relations when deleting a document or
  dropping a drawer."
  []
  {:deps [refs/keep-sync]
   :wrap-configs

   ;; TODO: only remove if the drawer IDs no longer exist


   (let [wipe! (refs/expand-fn remove-all-member-and-group-references!)]
     {`dp/-delete {:wrap (fn [delete-method]
                           (fn [tx drawer id]
                             (-> (wipe! tx drawer id)
                                 (delete-method drawer id))))}
      `dp/-drop   {:wrap (fn [drop-method]
                           (fn [tx drawer]
                             ;; Might not need to be immediate.
                             ;; Consider converting to a job in future optimization.
                             (-> (let [[tx ids] (db/dr (db/all-ids tx drawer))]
                                   (reduce (fn [tx' id] (wipe! tx' drawer id))
                                           tx ids))
                                 (drop-method drawer))))}})})
