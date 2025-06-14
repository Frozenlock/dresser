(ns dresser.extensions.rbac
  (:require [clojure.string :as str]
            [dresser.base :as db]
            [dresser.extension :as ext]
            [dresser.extensions.drawer-registry :as d-reg]
            [dresser.extensions.durable-refs :as refs]
            [dresser.extensions.memberships :as mbr]
            [dresser.extensions.relations :as rel]
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
    (refs/assoc-at! dresser grp-ref [:drs_rbac :guest-roles]
                    (mbr/roles-map roles))))

(defn- members-with-permission
  "Returns a collection of member refs, or, if the permission is matched
  with a `guest-role` stored in the grp document, returns [::guest]. "
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
    (if (some (or guest-roles {}) roles-w-perm)
      (db/with-result tx [::guest])
      (mbr/members-of-group-with-roles tx grp-ref roles-w-perm))))

(defn- with-cache
  "Caches the result of f in temp-data under cache-key.
  If cache-key is nil, simply call (f tx)."
  [tx cache-key f]
  (if-not cache-key
    (f tx)
    (if-let [cached (db/temp-get-in tx [cache-key])]
      (db/with-result tx cached)
      (let [[tx result] (db/dr (f tx))]
        (-> tx
            (db/temp-assoc cache-key result)
            (db/with-result result))))))

(defn permission-chain
  "Checks if member has the requested permission for the group. If not,
  recursively checks if he's a member of the valid member groups.
  Returns a collection of member references.

  The first item in the collection is the provided ref and the last is
  the ref that has the permission for grp-ref.
  Ex: usr1 :write for Acme -> [urs1 shell-company acme-owners]

  A cache key can be provided to store the recursive results in
  temp-data, but it must be for a very limited scope where
  modifications are impossible and it must be manually removed."
  ([dresser grp-ref request member-ref]
   (permission-chain dresser grp-ref request member-ref nil))
  ([dresser grp-ref request member-ref cache-key]
   (with-cache dresser (when cache-key
                         [cache-key [:pchain grp-ref request member-ref]])
     (fn [tx]
       (db/tx-let [tx dresser]
           [valid-members (members-with-permission tx grp-ref request)]
         (if (or (some #{member-ref ::guest} valid-members)) ; Found member?
           (db/with-result tx [member-ref])
           ;; If the member isn't found directly, recursively check the groups
           (loop [tx tx, refs (remove #{member-ref} valid-members)]
             (if (empty? refs)
               (db/with-result tx [])
               (let [ref (first refs)
                     [tx chain] (db/dr (permission-chain tx ref request member-ref cache-key))]
                 (if (seq chain)
                   (db/with-result tx (conj chain ref))
                   (recur tx (next refs))))))))))))

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
        m->p {`dp/fetch       (fn [drawer only _limit _where _sort _skip]
                                {:read (assoc-in {} [drawer *doc-id*] only)})
              `dp/all-drawers never-f
              `dp/delete-many (fn [drawer _where]
                                {:delete (assoc-in {} [drawer *doc-id*] :?)})

              `dp/with-temp-data -always
              `dp/transact       -always

              ;; optional implementations
              `dp/fetch-by-id   (fn [drawer id only where]
                                  {:read (assoc-in {} [drawer id] only)})
              `dp/update-at     (fn [drawer id ks _f _args]
                                  (let [request (assoc-in {} (into [drawer id] ks) :?)]
                                    {:write request
                                     :read  request}))
              `dp/add           (fn [drawer _data]
                                  {:add drawer})
              `dp/all-ids       never-f
              `dp/assoc-at      (fn [drawer id ks _data]
                                  {:write (assoc-in {} (into [drawer id] ks) :?)})
              `dp/dissoc-at     (fn [drawer id ks dissoc-ks]
                                  {:write (assoc-in {} (into [drawer id] ks)
                                                    (into {} (for [k dissoc-ks]
                                                               [k :?])))})
              `dp/drop          never-f
              `dp/gen-id        never-f
              `dp/get-at        (fn [drawer id ks only]
                                  {:read (assoc-in {} (into [drawer id] ks) (or only :?))})
              `dp/has-drawer?   never-f
              `dp/upsert-many   (fn [drawer _docs]
                                  {:write {drawer :?}})
              `dp/dresser-id    -always
              `dp/rename-drawer never-f
              `dp/drawer-id     -always
              `dp/drawer-key    -always
              `dp/start         -always
              `dp/stop          -always
              `dp/started?      -always
              `dp/fetch-count   never-f}]
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
  (db/temp-assoc dresser ::doc-adder
                 {:adder-ref adder-ref}))

(defn- post-add
  [tx drawer-key member-ref]
  ;; Do not give members/roles to system drawers
  (if (some #{drawer-key} (db/system-drawers tx))
    tx
    (let [new-doc-id (db/result tx)
          [tx new-doc-ref] (db/dr (refs/ref! tx drawer-key new-doc-id))
          doc-adder (::doc-adder (db/temp-get tx))
          {:keys [adder-ref]} doc-adder
          ;; We are within unrestricted access and must check
          ;; membership permissions manually
          mbr-request {:write {:_relations-virtual {adder-ref :?}}}
          error-fn (fn [message]
                     (ex-info message
                              {:by      member-ref
                               :method  (name `dp/add)
                               :request mbr-request
                               :target  adder-ref
                               :type    ::permission}))
          _ (when-not adder-ref (throw (error-fn "Missing document adder reference")))
          [tx pchain] (if (= adder-ref member-ref)
                        [tx [:itself]] ; member can access itself
                        (db/dr (permission-chain tx adder-ref mbr-request member-ref)))]
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
        cache-key (gensym)
        tx2 (reduce (fn [tx {:keys [id]}]
                      (let [request (binding [*doc-id* id]
                                      (request-fn drawer only limit where sort skip))
                            doc-r (doc-request drawer id request)
                            [tx grp-ref] (db/dr (refs/ref! tx drawer id))
                            [tx pchain] (if (and (some? grp-ref)
                                                 (= member-ref grp-ref))
                                          [tx [:itself]] ; member can access itself
                                          (db/dr (permission-chain tx grp-ref doc-r
                                                                   member-ref cache-key)))]
                        (if (empty? pchain)
                          (throw (ex-info "Missing permission"
                                          {:by      member-ref
                                           :method  (name method-sym)
                                           :request request
                                           :target  grp-ref
                                           :type    ::permission}))
                          tx)))
                    tx1 docs)
        tx2 (db/temp-dissoc tx2 cache-key)

        ;; Remove ID if not requested
        docs (if ?only-with-id
               (for [doc docs]
                 (dissoc doc :id))
               docs)]
    (db/with-result tx2 docs)))

(defn check-and-delete
  [member-ref method-sym provided-permissions method tx args]
  (let [[drawer where] args
        request-fn (method->request-fn method-sym)
        ;; Do we have permission for the entire drawer?
        early-permit? (permitted? provided-permissions {:delete {drawer :?}})
        cache-key (gensym)
        tx (if early-permit?
             tx
             (db/fetch-reduce
              tx drawer
              (fn [tx doc]
                (let [id (:id doc)
                      request (binding [*doc-id* id]
                                (request-fn drawer where))
                      doc-r (doc-request drawer id request)
                      [tx grp-ref] (db/dr (refs/ref! tx drawer id))
                      [tx pchain] (if (and (some? grp-ref)
                                           (= member-ref grp-ref))
                                    [tx [:itself]] ; member can access itself
                                    (db/dr (permission-chain tx grp-ref doc-r
                                                             member-ref cache-key)))]
                  (if (empty? pchain)
                    (throw (ex-info "Missing permission"
                                    {:by      member-ref
                                     :method  (name method-sym)
                                     :request request
                                     :target  grp-ref
                                     :type    ::permission}))
                    tx)))
              {:where      where
               :chunk-size 100
               :only       {:id :?}}))
        tx (db/temp-dissoc tx cache-key)]
    (method tx drawer where)))

(defn- default-check
  [member-ref method-sym method tx args request]
  (let [[drawer id & _] args
        [tx grp-ref] (db/dr (refs/ref! tx drawer id))
        make-error (fn [] (ex-info "Missing permission"
                                   {:by      member-ref
                                    :method  (name method-sym)
                                    :request request
                                    :doc-req (doc-request drawer id request)
                                    :target  grp-ref
                                    :type    ::permission}))
        _ (when (::never request)
            (throw (make-error)))]
    (cond

      ;; Member can update itself if it exists
      (= member-ref grp-ref)
      (let [[tx id] (db/dr (db/get-at tx drawer id [:id]))]
        (if-not id
          (throw (make-error))
          tx))

      ;; Relations use the same rights as their target document
      (= drawer rel/rel-drawer)
      (let [[op rel-drawer->x] (first request)
            [_drawer rel-id->y] (first rel-drawer->x)
            [rel-id y] (first rel-id->y)
            [target-drawer-id doc-id] (map rel/decode (str/split rel-id (re-pattern rel/separator) 2))
            [tx target-drawer-key] (db/dr (d-reg/drawer-key tx target-drawer-id))
            new-req {op {target-drawer-key {doc-id {:_relations-virtual y}}}}
            [_old-drawer _old-id & new-args] args]
        (default-check member-ref method-sym method tx [target-drawer-key doc-id new-args] new-req))

      ;; Normal behavior for every other drawer
      :else
      (let [doc-r (doc-request drawer id request)
            [tx pchain] (db/dr (permission-chain tx grp-ref doc-r member-ref))]
        (if (empty? pchain)
          (throw (make-error))
          tx)))))

(defn permission-wrapper
  [member-ref method-sym provided-permissions wrapper-id]
  (when-let [?request-fn (method->request-fn method-sym)]
    (fn [method]
      (fn [tx & args]
        ;; Do not check permission for nested methods (implementations)
        (if (db/temp-get-in tx [wrapper-id])
          (apply method tx args)
          (let [tx (db/temp-assoc tx wrapper-id true)
                [drawer & rargs] args
                pass (some #{drawer} (db/system-drawers tx))
                ?request (if pass
                           nil
                           (->> (apply ?request-fn drawer rargs)
                                (missing-permissions provided-permissions)
                                (not-empty)))]
            (let [is-fetch? (= method-sym `dp/fetch)
                  fetched-tx (when (and is-fetch? ?request)
                               (fetch-and-check member-ref
                                                method-sym
                                                provided-permissions
                                                method
                                                tx
                                                args))

                  tx (cond
                       fetched-tx tx

                       (= method-sym `dp/delete-many)
                       (check-and-delete member-ref
                                         method-sym
                                         provided-permissions
                                         method
                                         tx
                                         args)

                       (nil? ?request) tx

                       :else (default-check member-ref
                                            method-sym
                                            method
                                            tx
                                            args
                                            ?request))]
              (-> (or fetched-tx
                      (cond-> (apply method tx args)
                        (= method-sym `dp/add) (post-add drawer member-ref)))
                  (db/temp-dissoc wrapper-id)))))))))

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
   (let [wrapper-id (str (gensym "RBAC-"))]
     (into {} (for [sym (keys method->request-fn)
                    :let [wrap (permission-wrapper member-ref sym permissions wrapper-id)]
                    :when wrap]
                [sym {:wrap wrap}])))})
