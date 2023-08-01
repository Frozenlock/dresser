(defproject org.clojars.frozenlock/dresser-suite "0.1.0-SNAPSHOT"
  :description "Storage abstraction layer"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:test {:modules {:subprocess "lein"}}}

  :aliases {"test"    ["modules" "do" "test," "install"]
            ;"install" ["do" ["modules" "install"]]
            ;"deploy"  ["do" ["modules" "deploy" "clojars"]]
            })
