(defproject msd-to-h2 "0.1.0"
  :description "Converts lyrics files in the Million Song Dataset into an H2 database."
  :url "https://github.com/belovehq/msd-to-h2"
  :license {:name "MIT License"
            :url  "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.layerware/hugsql "0.4.8"]
                 [com.h2database/h2 "1.4.195"]]

  :source-paths ["src"]
  :target-path "target/%s"
  :resource-paths ["resources"]

  :main ^:skip-aot intoh2.core

  :profiles {:uberjar {:aot :all}})


