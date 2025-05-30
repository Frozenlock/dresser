# Dresser - MongoDB Implementation

A better MongoDB.

[![Clojars Project](https://img.shields.io/clojars/v/org.clojars.frozenlock/dresser-impl-mongodb.svg)](https://clojars.org/org.clojars.frozenlock/dresser-impl-mongodb)

## Advantages

### Types

Most Clojure MongoDB wrappers convert strings to keywords when fetching documents and silently coerce various Clojure collections into simple lists. While convenient, this can be dangerous and confusing. The Dresser implementation preserves the data types provided:

- Keyword
- String
- Number
- Date
- Set
- Vector
- List
- Map
- Records
- Byte arrays

### Complex Keys

Contrary to vanilla MongoDB, the Dresser implementation supports `$` and `.` in key names.
In addition, the keys are not limited to strings and can be any of the supported types, including maps!

### Partial Document Matches

MongoDB has an akward interface for partial matches: the dot notation.
Dresser will automatically convert any map query:
```clj
{"a" {"b" {"c1" 1, "c2" 2}}}
```

Into a dotted query:
```js
{"a.b.c1" 1
 "a.b.c2" 2}
 ```

### Improved Multi-Document Transactions

While MongoDB disallows `listCollections`, `renameCollection` and `dropCollection` within transactions, Dresser handles `all-drawers`, `rename-drawer!` and `drop!` seamlessly.

### Compatibility with Existing MongoDB Instances

MongoDB collections are transparently mapped to Dresser drawers.

### Safe `nil` Query

Contrary to vanilla MongoDB, a query such as `{:id nil}` will NOT match everything.
This helps avoid horrible mistakes, especially with `delete-many!`.


## Limitations

Due to MongoDB limitations, renaming collections within a transaction is not possible. Therefore, Dresser maintains its own drawer registry to facilitate renames. Renaming drawers does not rename the underlying collections.
