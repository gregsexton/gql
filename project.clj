(defproject sqlgen "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.github.seancorfield/honeysql "2.0.783"]
                 [org.clojure/tools.macro "0.1.2"]]
  :main ^:skip-aot sqlgen.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
