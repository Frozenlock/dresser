(ns dresser.extensions.rbac
  (:require [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.drawer :as dd]
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

(def default-roles {mbr/role-admin {:add    true
                                    :delete true
                                    :read   true
                                    :write  true}

                    mbr/role-editor {:read  true
                                     :write true}

                    mbr/role-reader {:read true}})


(defn- deep-merge [& maps]
  (let [merge-fn (fn this-merge [& args]
                   (if (every? map? args)
                     (apply merge-with this-merge args)
                     (last args)))]
    (apply merge-with merge-fn maps)))

(defn missing-permissions
  [perm-map request]
  (reduce-kv (fn [acc k v]
               (let [wildcard-perm (get perm-map :*)
                     specific-perm (get perm-map k)
                     perm (cond
                            (and (map? wildcard-perm)
                                 (map? specific-perm)) (deep-merge wildcard-perm specific-perm)

                            (map? wildcard-perm) (or specific-perm wildcard-perm)

                            (map? specific-perm) (or wildcard-perm specific-perm)

                            :else (or specific-perm wildcard-perm))]
                 (cond
                   ;; Both perm and request values are maps, recurse.
                   (and (map? perm) (map? v))
                   (if-let [missing (not-empty (missing-permissions perm v))]
                     (assoc acc k missing)
                     acc)

                   (and perm (not (map? perm))) acc

                   ;; If perm is true, or it's a map with a true wildcard, grant permission.
                   (and (map? perm)
                        (when-let [wildcard (:* perm)]
                          (not (map? wildcard)))) acc

                   ;; No specific or wildcard permission found.
                   :else (assoc acc k v))))
             {} request))



(defn permitted?
  "Determines if a request is collectively permitted by combining permissions
  from a sequence of permission maps. Permissions are considered cumulatively
  across maps to assess if the request is fully permitted. The function checks
  each map against the request, aggregating permissions until the request is
  either fully permitted or all maps have been evaluated.

  Parameters:
  - perm-maps: Sequence of maps specifying permissions, with actions or
               resources as keys.
  - request: Map representing the requested actions or resources.

  Returns:
  True if permissions from all maps collectively cover the request, false
  otherwise."
  [perm-maps request]
  (not (reduce (fn [request perm-map]
                 (let [m (missing-permissions perm-map request)]
                   (if (empty? m)
                     (reduced nil)
                     m)))
               request perm-maps)))


;; ----------------
;; This section allows for each document to define its own roles. Is
;; it really necessary?
;; (defn- assert-role->permissions
;;   "Throws on unexpected permission"
;;   [role->permissions]
;;   (assert (map? role->permissions))
;;   (doseq [[role permissions] role->permissions]
;;     (doseq [permission permissions]
;;       (when (not (some #{permission} allowed-permissions))
;;         (throw (ex-info "Unkown permission"
;;                         {:permission permission}))))))

(defn set-roles-permissions!
  "Sets roles inside the group.
  Returns the roles."
  [dresser grp-ref role->permissions]
  ;(assert-role->permissions role->permissions)
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
  [dresser grp-ref request-map]
  (db/tx-let [tx dresser]
      [{:keys [drs_rbac]} (refs/fetch-by-ref tx grp-ref
                                             {:only
                                              {:drs_rbac [:guest-roles
                                                          :role->perms]}})
       {:keys [guest-roles role->perms]} drs_rbac
       roles-w-perm (for [[role permissions] (merge default-roles role->perms)
                          :when (permitted? [permissions] request-map)]
                      role)]
    (if (some (set guest-roles) roles-w-perm)
      (db/with-result tx [::guest])
      (mbr/members-of-group-with-roles tx grp-ref roles-w-perm))))

(defn permission-chain
  "Checks if member has the requested permission for the group. If not,
  recursively checks if he's a member of the valid member groups.
  Returns a collection of member references.

  The first item in the collection is the provided ref and the last is
  the ref that has the permission for grp-ref.
  Ex: usr1 :write for Acme -> [urs1 shell-company acme-owners]"
  [dresser grp-ref request member-ref]
  (db/tx-let [tx dresser]
      [valid-members (members-with-permission tx grp-ref request)]
    (if (or (some #{member-ref ::guest} valid-members)) ; Found member?
      (db/with-result tx [member-ref])
      ;; If the member isn't found directly, recursively check the groups
      (loop [tx tx, refs (remove #{member-ref} valid-members)]
        (if (empty? refs)
          (db/with-result tx [])
          (let [ref (first refs)
                [tx chain] (db/dr (permission-chain tx ref request member-ref))]
            (if (seq chain)
              (db/with-result tx (conj chain ref))
              (recur tx (next refs)))))))))


;;;;;;;;;;;;;;;;;;;

;; Pseudo permissions
(def -always nil)
(def -never {::never true})


;; TODO: add another permission for using `where`?  There is a
;; potential for leaking information by repeatedly using a the `where`
;; query when a partial read is permitted.  For example:
;;
;; :where {:password "123"}
;; :only {:id :?}
;; => {}

;; :where {:password "222"}
;; :only {:id :?}
;; => {:id "id1"}

(def ^:dynamic *doc-id* `unbound)

(def method->request-fn
  (let [never-f (fn [& _] -never)
        m->p {`dp/-fetch       (fn [drawer only _limit _where _sort _skip]
                                 {:read (assoc-in {} [drawer *doc-id*] only)})
              `dp/-all-drawers never-f
              `dp/-delete      (fn [drawer id]
                                 {:delete {drawer {id :?}}})
              `dp/-upsert      (fn [drawer]
                                 {:write {drawer :?}})

              `dp/-temp-data      -always
              `dp/-with-temp-data -always
              `dp/-transact       -always

              ;; optional implementations
              `dp/-fetch-by-id        (fn [drawer id only where]
                                        {:read (assoc-in {} [drawer id] only)})
              `dp/-update-at          (fn [drawer id ks _f _args]
                                        (let [request (assoc-in {} (into [drawer id] ks) :?)]
                                          {:write request
                                           :read  request}))
              `dp/-add                (fn [drawer _data]
                                        {:add drawer})
              `dp/-all-ids            never-f
              `dp/-assoc-at           (fn [drawer id ks _data]
                                        {:write (assoc-in {} (into [drawer id] ks) :?)})
              `dp/-dissoc-at          (fn [drawer id ks dissoc-ks]
                                        {:write (assoc-in {} (into [drawer id] ks)
                                                          (into {} (for [k dissoc-ks]
                                                                     [k :?])))})
              `dp/-drop               never-f
              `dp/-gen-id             never-f
              `dp/-get-at             (fn [drawer id ks]
                                        {:read (assoc-in {} (into [drawer id] ks) :?)})
              `dp/-has-drawer?        never-f
              `dp/-replace            (fn [drawer id _data]
                                        {:write {drawer {id :?}}})
              `dp/-upsert-many        (fn [drawer _docs]
                                        {:write {drawer :?}})
              `dp/-dresser-id         -always
              `dp/-rename-drawer      never-f
              `dp/-drawer-id          -always
              `dp/-upsert-drawer-id   never-f
              `dp/-register-drawer-id never-f
              `dp/-drawer-key         -always
              `dp/-start              -always
              `dp/-stop               -always
              `dp/-started?           -always
              `dp/-fetch-count        never-f}
        methods-without-permission (seq (remove (set (keys m->p))
                                                dp/dresser-symbols))]
    (when methods-without-permission
      (throw (ex-info "Missing permissions definition"
                      {:methods methods-without-permission})))
    m->p))



(defn- doc-request
  "Takes a (global) request and transform it into a specific doc request.
  Ex:
  (doc-request :users \"user1\"
    {:read {:users {\"user1\" {:name :?}}}})

  => {:read {:name :?}}"
  [drawer id request]
  (reduce-kv (fn [acc k v]
               (assoc acc k (get-in request [k drawer id])))
             {}
             request))

;; For adding a new document, knowing the current user is
;; insufficient: it's also necessary to know the future owner of the
;; new document. For example, a user could be a member of 2 orgs. In
;; this scenario, in which one should a new project be added?


(defn with-doc-adder
  "`adder-ref` will become the admin of any added document. Documents
  can only be added in `valid-drawers`."
  [dresser adder-ref]
  (db/update-temp-data dresser assoc ::doc-adder
                       {:adder-ref     adder-ref}))


(defn- post-add
  [tx drawer-key member-ref]
  ;; Do not give members/roles to system drawers
  (if (some #{drawer-key} (db/system-drawers tx))
    tx
    (let [new-doc-id (db/result tx)
          [tx new-doc-ref] (db/dr (refs/ref! tx drawer-key new-doc-id))
          doc-adder (::doc-adder (db/temp-data tx))
          {:keys [adder-ref]} doc-adder
          ;; We are within unrestricted access and must check
          ;; membership permissions manually
          mbr-request {:write {:drs_memberships {:member-of {adder-ref :?}}}}
          error-fn (fn [message]
                     (ex-info message
                              {:by      member-ref
                               :method  (name `dp/-add)
                               :request mbr-request
                               :target  adder-ref
                               :type    ::permission}))
          _ (when-not adder-ref (throw (error-fn "Missing document adder reference")))
          [tx pchain] (db/dr (permission-chain tx adder-ref mbr-request member-ref))]
      (when (empty? pchain)
        (throw (error-fn "Missing permission")))

      (-> (mbr/upsert-group-member! tx new-doc-ref adder-ref [mbr/role-admin])
          (db/with-result new-doc-id)))))


;; The 'fetch' function is peculiar; it needs to search through all
;; the docs. To not break the function while still throwing in case of
;; an unauthorized result, we allow a normal 'fetch' and then validate
;; that each returned document can be read.

(defn fetch-and-check
  [member-ref method-sym provided-permissions method tx args]
  (let [[drawer only limit where sort skip] args
        request-fn (method->request-fn method-sym)
        ;; rbac: first fetch the docs normally, making sure to include
        ;; IDs in the results.
        ?only-with-id (when (and only (not (:id only)))
                        (assoc only :id true))
        [tx1 docs] (db/dr (method tx drawer (or ?only-with-id only) limit where sort skip))
        tx2 (reduce (fn [tx {:keys [id]}]
                      (let [request (binding [*doc-id* id]
                                      (request-fn (dd/key drawer) only limit where sort skip))
                            doc-r (doc-request drawer id request)
                            [tx grp-ref] (db/dr (refs/ref! tx drawer id))
                            [tx pchain] (if (and (some? grp-ref)
                                                 (= member-ref grp-ref))
                                          [tx [:itself]] ; member can access itself
                                          (db/dr (permission-chain tx grp-ref doc-r
                                                                   member-ref)))]
                        (if (empty? pchain)
                          (throw (ex-info "Missing permission"
                                          {:by      member-ref
                                           :method  (name method-sym)
                                           :request request
                                           :target  grp-ref
                                           :type    ::permission}))
                          tx)))
                    tx1 docs)

        ;; Remove ID if not requested
        docs (if ?only-with-id
               (for [doc docs]
                 (dissoc doc :id))
               docs)]
    (db/with-result tx2 docs)))


(defn- default-check
  [member-ref method-sym method tx args request]
  (let [[drawer id & _] args
        [tx grp-ref] (db/dr (refs/ref! tx drawer id))
        error (ex-info "Missing permission"
                       {:by      member-ref
                        :method  (name method-sym)
                        :request request
                        :doc-req (doc-request drawer id request)
                        :target  grp-ref
                        :type    ::permission})
        _ (when (::never request)
            (throw error))]

    (cond

      ;; Member can update itself if it exists
      (= member-ref grp-ref)
      (let [[tx id] (db/dr (db/get-at tx drawer id [:id]))]
        (if-not id
          (throw error)
          tx))

      ;; Normal behavior for every other drawer
      :else
      (let [doc-r (doc-request drawer id request)
            [tx pchain] (db/dr (permission-chain tx grp-ref doc-r member-ref))]
        (if (empty? pchain)
          (throw error)
          tx)))))


(defn permission-wrapper
  [member-ref method-sym provided-permissions]
  (when-let [?request-fn (method->request-fn method-sym)]
    (fn [method]
      (fn [tx & args]
        (let [[drawer & rargs] args
              drawer-key (dd/key drawer)
              pass (some #{drawer-key} (db/system-drawers tx))
              ?request (if pass
                         nil
                         (->> (apply ?request-fn drawer-key rargs)
                              (missing-permissions provided-permissions)
                              (not-empty)))]
          (let [is-fetch? (= method-sym `dp/-fetch)
                fetched-tx (when (and is-fetch? ?request)
                             (fetch-and-check member-ref
                                              method-sym
                                              provided-permissions
                                              method
                                              tx
                                              args))

                tx (cond
                     fetched-tx tx

                     (nil? ?request) tx

                     :else (default-check member-ref
                                          method-sym
                                          method
                                          tx
                                          args
                                          ?request))]
            (or fetched-tx
                (cond-> (apply method tx args)
                  (= method-sym `dp/-add) (post-add drawer-key member-ref)))))))))



(ext/defext enforce-rbac
  "Dresser methods touching a document other than `member-ref` will
  check for the necessary permission and will throw an exception if it
  fails.

  To update the roles: `mbr/upsert-group-member!`.

  To update the permissions: `set-roles-permissions!`.

  Keep in mind that a member cannot escalate his permissions. Those
  functions should probably be used on a dresser that is not yet
  wrapped in this extension."
  [member-ref permissions]
  {:deps [mbr/keep-sync]
   ;:throw-on-reuse? true
   :wrap-configs
   (into {} (for [sym dp/dresser-symbols
                  :let [wrap (permission-wrapper member-ref sym permissions)]
                  :when wrap]
              [sym {:wrap wrap}]))})
