(defproject org.clojars.frozenlock/dresser-impl-mongodb "0.1.0-SNAPSHOT"
  :description "MongoDB implementation for Dresser"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojars.frozenlock/dresser "0.1.0-SNAPSHOT"]

                 ;; MongoDB
                 [mongo-driver-3 "0.7.0"]
                 [org.mongodb/mongodb-driver-sync "4.7.1"]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-nop "2.0.7"]]}}
  :test-paths ["src" "test"]
  :repl-options {:init-ns dresser.impl.mongodb})
