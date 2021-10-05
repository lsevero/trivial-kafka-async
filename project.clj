(defproject org.clojars.lsevero/trivial-kafka-async "0.1.1"
  :description "Trivial kafka client with core.async support"
  :url "https://github.com/lsevero/trivial-kafka-async"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.apache.kafka/kafka-clients "2.6.1"]
                 [org.clojure/core.async "1.3.618"]
                 [org.clojure/tools.logging "1.1.0"]
                 ]
  :repl-options {:init-ns trivial-kafka-async.core}
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :plugins [[cider/cider-nrepl "0.25.4"]
                             ]}}
  )
