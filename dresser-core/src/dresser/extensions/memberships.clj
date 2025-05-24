(ns dresser.extensions.memberships
  "Provides groups/memberships relations between documents."
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.drawer-registry :as d-reg]
            [dresser.extensions.durable-refs :as refs]
            [dresser.extensions.relations :as rel]
            [dresser.protocols :as dp]))

;; Some suggested roles
(def role-admin :admin)
(def role-editor :editor)
(def role-reader :reader)

;; Relation constants
(def ^:private member-of-rel [:drs_memberships :member-of])
(def ^:private has-member-rel [:drs_memberships :members])

;; The user/member

(defn- add-group-to-member! ; Used only within this namespace
  [dresser member-ref grp-ref roles]
  (rel/upsert-relation! dresser grp-ref member-ref has-member-rel member-of-rel roles nil))

(defn- remove-group-from-member! ; Used only within this namespace
  [dresser member-ref grp-ref]
  (rel/remove-relation! dresser member-ref member-of-rel grp-ref))

(defn memberships-of-member
  "Returns all refs for which target is a member.
  An optional drawer can be passed to select only members from this drawer."
  ([dresser member-ref]
   (memberships-of-member dresser member-ref nil))
  ([dresser member-ref drawer]
   (db/tx-let [tx dresser]
       [drawer-ids (when drawer (d-reg/drawer-ids tx drawer))
        rels (rel/relations tx member-ref member-of-rel drawer-ids)]
     (keys rels))))



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
  (let [roles-map (roles-map roles)
        roles? (not (empty? roles-map))]
    (db/tx-> dresser
      (cond->
          (not roles?) (remove-group-from-member! member-ref grp-ref)
          roles? (add-group-to-member! member-ref grp-ref roles-map)
          true (db/with-result grp-ref)))))

(defn members-of-group
  "Returns all the refs of members."
  ([dresser group-ref] (members-of-group dresser group-ref nil))
  ([dresser group-ref drawer]
   (db/tx-let [tx dresser]
       [drawer-ids (when drawer (d-reg/drawer-ids tx drawer))
        d-set (set drawer-ids)
        rels (rel/relations tx group-ref has-member-rel)
        refs (keys rels)]
     (if (not-empty d-set)
       ;; Filter by drawer if specified
       (filter #(some d-set (:drawer-id %)) refs))
     refs)))

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
    (reduce (fn [tx' member-ref]
              (remove-member-from-group! tx' grp-ref member-ref))
            tx members-refs)))

(defn members-of-group-with-roles
  "Returns the refs for members with any of the given roles.

  'roles' can be a map of roles or a collection of those keys."
  [dresser grp-ref roles]
  (let [role-keys (keys (roles-map roles))]
    (db/tx-let [tx dresser]
        [members->data (rel/relations tx grp-ref has-member-rel)]
      (for [[member-ref rel-data] members->data
            :when (some-> rel-data
                          (select-keys role-keys)
                          not-empty)]
        member-ref))))

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

;; Use the relations keep-sync extension which will handle
;; cleanup of memberships automatically
(ext/defext keep-sync
  "Automatically maintains membership relations when deleting a document or
  dropping a drawer."
  []
  {:deps [rel/keep-sync]})
