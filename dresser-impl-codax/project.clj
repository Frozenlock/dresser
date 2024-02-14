(defproject org.clojars.frozenlock/dresser-impl-codax "0.1.0-SNAPSHOT"
  :description "Codax implementation for Dresser"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojars.frozenlock/dresser "0.1.0-SNAPSHOT"]
                 [codax "1.4.0"]
                 [io.replikativ/hasch "0.3.94"]]
  :plugins [[lein-modules "0.3.11"]]
  :test-paths ["src" "test"]
  :repl-options {:init-ns dresser.impl.codax})
