# Dresser

<img width="200" align="left" src="https://raw.githubusercontent.com/Frozenlock/dresser/master/doc/img/dresser.jpeg">

A transactional storage abstraction for Clojure

#### Core
[![Clojars Project](https://img.shields.io/clojars/v/org.clojars.frozenlock/dresser.svg)](https://clojars.org/org.clojars.frozenlock/dresser)

#### Status: Early alpha
We're still settling the API.
Suggestions welcome!



| Implementation                               | Dependency                                                                                                                                                              |
|----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hashmap                                      | Core                                                                                                                                                                    |
| Atom                                         | Core                                                                                                                                                                    |
| File                                         | Core                                                                                                                                                                    |
| [Codax](https://github.com/dscarpetti/codax) | [![Clojars Project](https://img.shields.io/clojars/v/org.clojars.frozenlock/dresser-impl-codax.svg)](https://clojars.org/org.clojars.frozenlock/dresser-impl-codax)     |
| MongoDB                                      | [![Clojars Project](https://img.shields.io/clojars/v/org.clojars.frozenlock/dresser-impl-mongodb.svg)](https://clojars.org/org.clojars.frozenlock/dresser-impl-mongodb) |


## Why Use It?

Store and query your Clojure data structures directly - no ORMs, no normalization, no SQL:

```clojure
;; Your data - even with records!
(defrecord Settings [theme language notifications])

(def user {:name "Alice"
           :settings (->Settings "dark" "en" {:email true :push false})
           :usage {:widgets 50
                   :bandwidth 100}})

;; Just store it - returns the ID
(def user-id (db/add! DB :users user))

;; Query nested fields directly - records are preserved!
(db/fetch DB :users {:where {:settings {:theme "dark"}}})
;=> ({:name "Alice" :settings #Settings{...} ...})

;; Update deeply nested values in a transaction
(db/tx-> DB
  (db/update-at! :users user-id [:usage :widgets] + 10)
  (db/update-at! :users user-id [:usage :bandwidth] + 25)
  (db/get-at :users user-id [:usage]))
;=> {:widgets 60, :bandwidth 125}
```

**Plus:**
- Develop with in-memory storage, deploy with persistent implementations - zero code changes
- Test at full speed without database setup or mocks
- Use vectors `[user-id timestamp]`, maps `{:year 2024 :month 3}`, even records as keys
- Every function works identically across all implementations


## Usage

### Getting Started

Let's start by using a simple atom implementation:

```clojure

(require '[dresser.base :as db])
;; Wow, look at that, the initials are DB!
;; What a crazy happenstance!

;; Define a new DB
(require '[dresser.impl.atom :as at])
(def my-db (at/build))
;=> #'user/my-db

;; As a first action, we'll store a document in `:users`.
(db/add! my-db :users {:name "Bob", :age 33})
;=> "2d304b27-8e0e-40fd-9651-86e07310e353"

;; Adding a document returns its ID.
;; Let's store it for future use.
(def user-id *1)
;=> #'user/user-id

;; Try to fetch the user back:
(db/fetch-by-id my-db :users user-id)
;=> {:name "Bob", :age 33, :id "2d304b27-8e0e-40fd-9651-86e07310e353"}

;; Great, now let's add a few more users.
(doseq [[user-name age] [["Alice" 15]
                         ["Martin" 99]
                         ["Felicia" 55]
                         ["Xander" 8]]]
  (db/add! my-db :users {:name user-name, :age age}))
;=> nil

(db/fetch my-db :users {:only [:name :age]})
;=> ({:name "Bob", :age 33}
;    {:name "Alice", :age 15}
;    {:name "Martin", :age 99}
;    {:name "Felicia", :age 55}
;    {:name "Xander", :age 8})


```

### `fetch` queries

While most functions allow for retrieval or update of a single document, `fetch` and its derivatives allow for queries that return multiple documents.

`fetch` is configured with those optional keys:

- `:only` returns only a subset of the documents.
	The map provided under `:only` will be filled with the values from the fetched document.
	```clojure
	(db/fetch my-db :users {:only {:name true}})
	;; `:only` also accepts vectors, which are converted into map entries.
	;; [:name] -> {:name true}
	;; {:address [:street :country]} -> {:address {:street true, :country true}}
	```
- `:where` returns the documents that match all the conditions.
  - Operators:
	- `lt`/`lte` 'less than' and 'less than or equal'.
	- `gt`/`gte` 'greater than' and 'greater than or equal'.
	- `exists?` Whether the field exists.
	- `any` Whether any subquery matches.
	- Equality condition is implied if no operator is provided.
    ```clojure
    ;; Fetches users with a name greater than "M" and where `:age` exists.
    (db/fetch my-db :users {:where {:name {db/gt "M"}
    				:age  {db/exists? true}}
    			:only  [:name]})
    ;=> ({:name "Martin"} {:name "Xander"})

    ;; Implied equality condition:
    (db/fetch my-db :users {:where {:age 8}
    			:only  [:name]})
    ;=> ({:name "Xander"})

	;; Users with name "Bob" or age 55
    (db/fetch my-db :users {:where {db/any [{:name "Bob"}
    					{:age 55}]}
    			:only  [:name :age]})
    ;=> ({:name "Bob", :age 33} {:name "Felicia", :age 55})
	```
- `:sort` sorts the selected document by the given fields and directions.
	```clojure
	(db/fetch my-db :users {:sort [[[:age] :asc]
                                   [[:address :street] :desc]]})
	;; Sorts users by ascending age, then by descending street in case of equality.
	```
- `:limit` limits the number of documents returned. Similar to `clojure.core/take`.
- `:skip` skips X documents that would otherwise have been returned. Similar to `clojure.core/drop`.


### Transactions

Things usually get a little more complex when you want to avoid race-conditions. Fear not! Dresser was designed with transactions in mind.

You see, dresser functions are a little magical.
If the dresser is in its normal state, the function will automatically return the result.
But if the dresser is in its transaction state, then the result is stored inside itself and the transaction continues.

Here's how you can picture this process:

```clojure
;; Normal
(db/fetch-one ...)
; => {:name "Bob"}

(db/tx-> my-db
  ;; Inside a transaction
  (db/fetch-one ...) ; => {:result {:name "Bob"}, :other "stuff"}
  (db/fetch-one ...)); => {:result {:name "Alice"}, :other "stuff"}
;=> {:name "Alice"}
```

It's only when the transaction completes that the final result is extracted from the dresser.

Taking our previous users example, let's find the youngest and oldest users and remove them in a single transaction.

```clojure
;; Let's start by testing a fetch function to find the youngest:

(db/fetch-one my-db :users {:sort [[[:age] :asc]]})
;=> {:name "Xander", :age 8, :id "d19c8e72-3cc6-4052-bd12-e4854728f858"}

;; As you can see, db/fetch-one will execute the transaction and
;; return the result. In this case we'd rather keep the transaction
;; open. To do so, we have to start a transaction ourself.

(defn remove-youngest-and-oldest!
  [my-db]
  (db/transact!
   my-db
   ;; Everything inside this function is a single transaction.
   (fn [tx]
     ;; Inside a transaction, the dresser-compatible functions return an
     ;; updated transaction instead of extracting the result.
     (let [tx2 (db/fetch-one tx :users {:sort [[[:age] :asc]]})
           ;; But how can we get the result if the function returns a
           ;; transaction?  This is where `db/result` comes into
           ;; play. It extracts the last result stored in the
           ;; transaction.
           youngest (db/result tx2)
           ;; Let's do the same thing for the oldest user.
           ;; Don't forget to use the UPDATED transaction (tx2).
           tx3 (db/fetch-one tx2 :users {:sort [[[:age] :desc]]})
           oldest (db/result tx3)
           ;; Now we can delete them. Again, always use the latest
           ;; transaction. We can use a threading macro to make it
           ;; easier:
           tx4 (-> tx3
                   (db/delete! :users (:id youngest))
                   (db/delete! :users (:id oldest)))]
       ;; By default `db/delete!` returns the doc ID. To provide a
       ;; better feedback, return both documents that were just
       ;; deleted instead.
       (db/with-result tx4 {:oldest   oldest
                            :youngest youngest})))))

;; Time to try our new function. When not inside a transaction, the
;; result will be extracted.

(remove-youngest-and-oldest! my-db)
;=> {:oldest   {:name "Martin",
;               :age  99,
;               :id   "d004aa88-0ef9-4fd6-9f5f-2db602bfbd61"},
;    :youngest {:name "Xander",
;               :age  8,
;               :id   "6a064ff3-55c2-4416-b2b2-12aa7697af7c"}}


;; Great! How about the other users, are they still there?

(db/fetch my-db :users {:only [:name :age]})
;=> ({:name "Bob", :age 33}
;    {:name "Alice", :age 15}
;    {:name "Felicia", :age 55})

;; Perfect!

```

And you know what? The function we just defined is automatically dresser-compatible.
This means that if used inside an open transaction, it will use it instead of starting a new one!

```clojure
(db/tx-> my-db                  ;; Everything occurs inside a single transaction
  (remove-youngest-and-oldest!) ;; Remove Alice and Felicia
  (remove-youngest-and-oldest!) ;; Remove Bob
  (db/fetch :users {:only [:name :age]}))
;=> ()
```

### Macros and Shorthands

Let's be honest here... handling the transaction was kind of a pain.
There are many tools to make our lives easier. A few are introduced below, in order of abstraction.

```clojure

;; Here's the previous function definition whitout the comments.

(defn remove-youngest-and-oldest!
  [my-db]
  (db/transact!
   my-db
   (fn [tx]
     (let [tx2 (db/fetch-one tx :users {:sort [[[:age] :asc]]})
           youngest (db/result tx2)
           tx3 (db/fetch-one tx2 :users {:sort [[[:age] :desc]]})
           oldest (db/result tx3)
           tx4 (-> tx3
                   (db/delete! :users (:id youngest))
                   (db/delete! :users (:id oldest)))]
       (db/with-result tx4 {:oldest   oldest
                            :youngest youngest})))))

;; Extracting the result after an operation occurs so often
;; that there's a shorthand for it.
;;
;; `db/dr` (dresser result) will return a tuple of the updated
;; dresser and the result it contains. It can be used like so:
;; (let [[tx result] (db/dr (db/fetch ...))] ...)

(defn remove-youngest-and-oldest!
  [my-db]
  (db/transact!
   my-db
   (fn [tx]
     (let [[tx2 youngest] (db/dr (db/fetch-one tx :users {:sort [[[:age] :asc]]})) ; <---
           [tx3 oldest] (db/dr (db/fetch-one tx2 :users {:sort [[[:age] :desc]]})) ; <---
            tx4 (-> tx3
                   (db/delete! :users (:id youngest))
                   (db/delete! :users (:id oldest)))]
       (db/with-result tx4 {:oldest   oldest
                            :youngest youngest})))))

;; Having to use `db/transact!` and putting everything in a function
;; can be hidden with the macro `db/with-tx`.

(defn remove-youngest-and-oldest!
  [my-db]
  (db/with-tx [tx my-db] ; <---
    (let [[tx2 youngest] (db/dr (db/fetch-one tx :users {:sort [[[:age] :asc]]}))
          [tx3 oldest] (db/dr (db/fetch-one tx2 :users {:sort [[[:age] :desc]]}))
          tx4 (-> tx3
                  (db/delete! :users (:id youngest))
                  (db/delete! :users (:id oldest)))]
      (db/with-result tx4 {:oldest   oldest
                           :youngest youngest}))))

;; And what about all those 'tx'?  We can use `db/tx-let`, which will
;; automatically rebind the 'tx' and extract the result whenever the
;; right side of the binding is a dresser.
;;
;; Before:
;; [[tx result] (db/dr (...))]
;;
;; After:
;; [result (...)]
;;
;; Finally, if the body returns a non-dresser, it is automatically
;; inserted as a result of the last dresser binding before returning it.


(defn remove-youngest-and-oldest!
  [my-db]
  (db/tx-let [tx my-db]
      [youngest (db/fetch-one tx :users {:sort [[[:age] :asc]]})
       oldest (db/fetch-one tx :users {:sort [[[:age] :desc]]})
       _ (db/delete! tx :users (:id youngest))
       _ (db/delete! tx :users (:id oldest))]
    {:oldest   oldest
     :youngest youngest}))

```

### Immutability

Unless you need some async operations, it's highly suggested to use an immutable dresser implementation for your tests: `dresser.impl.hashmap`.


## Implementations

Dresser was built with the idea that you should be able to get up and running quickly and then optimize when necessary. As such, many of the methods are optional. If there are not provided, they will fallback on a combination of fundamental methods.

For example, `db/upsert-many!` will default to calling `db/upsert!` on every document individually. It's likely slower than using a method to insert many documents at once, but it means that your code will still work even if you don't implement this method. Later on, if you feel the need for increased speed, you can define a more specialized implmentation of `db/upsert-many!`.

Even the base methods can be implemented using shortcuts.
`db/fetch` is one of the most complex methods because of its various query operations.
If you just want to get started quickly, your implementation can fetch all the documents and then filter them using a function from the hashmap implementation:

```clojure
(defn fetch
  [tx drawer only limit where sort-config skip]
  (let [all-docs (mongo-driver-3.client/find db drawer {})] ; <--- fetch all documents
    (db/with-result tx
      (hm/fetch-from-docs all-docs only limit where sort-config skip))))
```

This will be slower, but it's enough to get started and it will be fully compatible.

## Extensions

WARNING: extensions are a work in progress and will most likely have breaking changes.

Extensions are build on top of the dresser methods.
Their purpose is to provide additional capabilities that will not only work on any dresser implementation, but will continue to work even after migrating the data from one implementation to another.

## Schemas

The base Dressers don't have any support for schemas.
Similar (better) capabilities should be added via extensions.

For example, an extension could leverage Malli to validate all the data going in and conform all the data coming out. Contrary to normal DB schemas, those specs could be reused elsewhere, such as in HTTP handlers.

## Todo:
- Control automatic retries.
- More testing.
- Make hashmap and atom implementations more representative of other implementations:
  - Unsupported types should throw.
  The goal is to be able to reliably test with in-memory versions.
- Link/merge multiple instances.
- Clojurescript
- Caching/reaction
- Malli/specs extention (DB schemas, but better and reusable in HTTP forms and elsewhere).


## License

Copyright © 2024 Frozenlock

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
