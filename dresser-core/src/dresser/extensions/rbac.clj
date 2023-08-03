(ns dresser.extensions.rbac
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.durable-refs :as refs]
            [dresser.extensions.memberships :as mbr]
            [dresser.protocols :as dp]))

;; For maximum compatibility, permissions should always be the same.
(def allowed-permissions #{:add :delete :read :write})

;; Roles on the other hand can be defined locally in each group. This
;; means that group1 could have the role 'janitor' (:delete) while
;; group2 would have 'deleter' (:delete), effectively providing the
;; same permission.

;; Nevertheless, a few roles are always present to reduce boilerplate.
(def default-roles {mbr/role-admin  [:add :delete :read :write]
                    mbr/role-editor [:read :write]
                    mbr/role-reader [:read]})


;; ----------------
;; This section allows for each document to define its own roles. Is
;; it really necessary?
(defn- assert-role->permissions
  "Throws on unexpected permission"
  [role->permissions]
  (assert (map? role->permissions))
  (doseq [[role permissions] role->permissions]
    (doseq [permission permissions]
      (when (not (some #{permission} allowed-permissions))
        (throw (ex-info "Unkown permission"
                        {:permission permission}))))))

(defn set-roles-permissions!
  "Sets roles inside the group.
  Returns the roles."
  [dresser grp-ref role->permissions]
  (assert-role->permissions role->permissions)
  (db/tx-> dresser
    (refs/assoc-at! grp-ref [:drs_rbac :role->perms]
                    (dissoc role->permissions
                            (keys default-roles)))))
;; ----------------



(defn set-guest-roles!
  "Sets the roles for non-member of the document."
  [dresser grp-ref roles]
  (if (empty? roles)
    (refs/dissoc-at! dresser grp-ref [:drs_rbac] :guest-roles)
    (refs/assoc-at! dresser grp-ref [:drs_rbac :guest-roles] roles)))

(defn- members-with-permission
  "Returns a collection of member refs, or, if the permission is matched
  with a `guest-role` stored in the grp document, returns [::any]. "
  [dresser grp-ref permission]
  (db/tx-let [tx dresser]
      [{:keys [drs_rbac]} (refs/fetch-by-ref tx grp-ref
                                             {:only
                                              {:drs_rbac [:guest-roles
                                                          :role->perms]}})
       {:keys [guest-roles role->perms]} drs_rbac
       roles-w-perm (for [[role permissions] (merge role->perms default-roles)
                          :when (some #{permission} permissions)]
                      role)]
    (if (some (set guest-roles) roles-w-perm)
      (db/with-result tx [::any])
      (mbr/members-of-group-with-roles tx grp-ref roles-w-perm))))

(defn permission-chain
  "Checks if member has the requested permission for the group. If not,
  recursively checks if he's a member of the valid member groups.
  Returns a collection of member references.

  The first item in the collection is the provided ref and the last is
  the ref that has the permission for grp-ref.
  Ex: usr1 :write for Acme -> [urs1 shell-company acme-owners]"
  [dresser grp-ref permission member-ref]
  (db/tx-let [tx dresser]
      [valid-members (members-with-permission tx grp-ref permission)]
    (if (or (some #{member-ref ::any} valid-members)) ; Found member?
      (db/with-result tx [member-ref])
      ;; If the member isn't found directly, recursively check the groups
      (loop [tx tx, refs (remove #{member-ref} valid-members)]
        (if (empty? refs)
          (db/with-result tx [])
          (let [ref (first refs)
                [tx chain] (db/dr (permission-chain tx ref permission member-ref))]
            (if (seq chain)
              (db/with-result tx (conj chain ref))
              (recur tx (next refs)))))))))



;;;;;;;;;;;;;;;;;;;

;; Pseudo permissions
(def -always ::always)
(def -never ::never)

(def method->permission
  (let [m->p {`dp/-fetch       :read
              `dp/-all-drawers -never
              `dp/-delete      :delete
              `dp/-upsert      -never

              `dp/-temp-data      -always
              `dp/-with-temp-data -always
              `dp/-transact       -always

              ;; optional implementations
              `dp/-fetch-by-id        :read
              `dp/-update-at          :write
              `dp/-add                :add
              `dp/-all-ids            -never
              `dp/-assoc-at           :write
              `dp/-dissoc-at          :write
              `dp/-drop               -never
              `dp/-gen-id             -never
              `dp/-get-at             :read
              `dp/-has-drawer?        -never
              `dp/-replace            :write
              `dp/-upsert-many        -never
              `dp/-dresser-id         -always
              `dp/-rename-drawer      -never
              `dp/-drawer-id          -always
              `dp/-upsert-drawer-id   -never
              `dp/-register-drawer-id -never
              `dp/-drawer-key         -always
              `dp/-start              -always
              `dp/-stop               -always
              `dp/-started?           -always
              `dp/-fetch-count        :read}
        methods-without-permission (seq (remove (set (keys m->p))
                                                dp/dresser-symbols))]
    (when methods-without-permission
      (throw (ex-info "Missing permissions definition"
                      {:methods methods-without-permission})))
    m->p))



;; Contrary to the other methods, `add` is extremely context dependent
;; and needs to be customizable.

;; 1. For adding a new document, knowing the current user is
;; insufficient, it's also necessary to know the future owner of the
;; new document. For example, a user could be a member of 2 orgs. In
;; this scenario, in which one should a new project be added?

;; 2. ALL drawers are probably not a valid destination for a new
;; document. Furthermore, storing a list of acceptable drawers in the
;; role itself would be inconvenient, as there's a high likelyhood
;; that this will need to be updated regularly on an entire drawer
;; basis rather than one document at a time. For example, adding the
;; ability for :users to create in :api-keys shouldn't require a huge
;; migration.

(defn with-doc-adder
  "`adder-ref` will become the admin of any added document. Documents
  can only be added in `valid-drawers`."
  [dresser adder-ref valid-drawers]
  (db/update-temp-data dresser assoc ::doc-adder
                       {:adder-ref     adder-ref
                        :valid-drawers valid-drawers}))


(defn- post-add
  [tx drawer]
  ;; Do not give members/roles to system drawers
  (if (some #{drawer} (db/system-drawers tx))
    tx
    (let [new-doc-id (db/result tx)
          [tx new-doc-ref] (db/dr (refs/ref! tx drawer new-doc-id))
          doc-adder (::doc-adder (db/temp-data tx))
          {:keys [adder-ref]} doc-adder]
      (when-not adder-ref (throw (ex-info "Missing adder reference " doc-adder)))
      (mbr/upsert-group-member! tx new-doc-ref adder-ref [mbr/role-admin]))))


(def ^:dynamic *skip-rbac* false)

(defn- add-wrapper
  [member-ref method-sym]
  (fn [method]
    (fn [tx target-drawer data]
      (let [tx (if (or *skip-rbac*
                       (some #{target-drawer} (db/system-drawers tx)))
                 tx
                 (let [{:keys [adder-ref valid-drawers]} (::doc-adder (db/temp-data tx))
                       permission :add
                       error-fn (fn [message]
                                  (ex-info message
                                           {:by            member-ref
                                            :method        (name method-sym)
                                            :permission    permission
                                            :target-drawer target-drawer
                                            :type          ::permission}))]
                   (cond
                     (not adder-ref) (throw (error-fn "Missing document adder reference"))
                     (not (some #{target-drawer} valid-drawers)) (throw (error-fn "Drawer not allowed"))
                     :else (let [[tx pchain] (binding [*skip-rbac* true] ; for other wrappers
                                               (db/dr (permission-chain tx adder-ref permission member-ref)))]
                             (if (empty? pchain)
                               (throw (error-fn "Missing permission"))
                               tx)))))]
        (-> (method tx target-drawer data)
            (post-add target-drawer))))))

(defn- permission-wrapper
  [member-ref method-sym]
  (let [permission (method->permission method-sym)]
    (condp = permission
      -always nil
      :add (add-wrapper member-ref method-sym)
      (fn [method]
        (fn [tx drawer & args]
          (let [tx (if (or *skip-rbac*
                           (some #{drawer} (db/system-drawers tx)))
                     tx
                     (binding [*skip-rbac* true]
                       (let [id (first args)
                             [tx grp-ref] (db/dr (refs/ref tx drawer id))
                             error (ex-info "Missing permission"
                                            {:by         member-ref
                                             :method     (name method-sym)
                                             :permission permission
                                             :target     grp-ref
                                             :type       ::permission})]
                         (cond
                           (= permission -never) (throw error)

                           ;; Member can update itself if it exists
                           (= member-ref grp-ref)
                           (let [[tx id] (db/dr (db/get-at tx drawer id [:id]))]
                             (if-not id
                               (throw error)
                               tx))

                           ;; Normal behavior for every other drawer
                           :else
                           (let [[tx pchain] (db/dr (permission-chain tx grp-ref permission member-ref))]
                             (if (empty? pchain)
                               (throw error)
                               tx))))))]
            (apply method tx drawer args)))))))


(ext/defext enforce-rbac
  "Dresser methods touching a document other than `member-ref` will
  check for the necessary permission and will throw an exception if it
  fails.

  To update the roles: `mbr/upsert-group-member!`.

  To update the permissions: `set-roles-permissions!`.

  Keep in mind that a member cannot escalate his permissions. Those
  functions should probably be used on a dresser that is not yet
  wrapped in this extension."
  [member-ref]
  {:deps [mbr/keep-sync]
   ;:throw-on-reuse? true
   :wrap-configs
   (into {} (for [sym dp/dresser-symbols
                  :let [wrap (permission-wrapper member-ref sym)]
                  :when wrap]
              [sym {:wrap wrap}]))})
