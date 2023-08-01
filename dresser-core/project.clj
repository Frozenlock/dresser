(defproject org.clojars.frozenlock/dresser "0.1.0-SNAPSHOT"
  :description "Storage abstraction layer"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [missionary "b.31"]
                 [com.hyperfiddle/electric "v2-alpha-349-ge9996713"]]
  :test-paths ["src" "test"]
  :repl-options {:init-ns dresser.base}
  :profiles {:test {:injections [(do (require 'dresser.test)
                                     (ns dresser.test)
                                     (def test-coverage? true))]}})
