(ns trivial-kafka-async.core
  (:require
    [clojure.core.async
     :as a
     :refer [>! <! go-loop chan]]
    [clojure.tools.logging :as log] 
    )
  (:import
    [java.util Properties]
    [java.time Duration]
    [org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer ConsumerRecord]
    [org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord RecordMetadata]
    ))

(defn producer!
  "Receives a map of a topic and the java properties.
  Returns a channel that will be consumed and sent each individual message to kafka in a different thread.
  "
  [{:keys [topic properties flush?] :or {flush? true}}]
  (letfn [(print-ex [e] (log/error e "Failed to deliver message."))
          (print-metadata [^RecordMetadata x]
            (log/debug (format "Produced record to topic %s partition [%d] @ offest %d\n"
                               (.topic x)
                               (.partition x)
                               (.offset x))))]

    (let [producer (KafkaProducer. ^Properties (doto (Properties.)
                                                 (.putAll properties)))
          callback (reify Callback
                     (onCompletion [this metadata exception]
                       (if exception
                         (print-ex exception)
                         (print-metadata metadata))))
          chan-producer (chan)]

      (go-loop []
               (let [record (ProducerRecord. topic (<! chan-producer))]
                 (log/trace "chan-producer received: " record)
                 (if flush?
                   (doto producer 
                     (.send record callback)
                     (.flush))
                   (.send producer record callback)))
               (recur))
      chan-producer)))

(defn consumer!
  "Receives a map of a topic and the java properties.
  Returns a channel, which will be populated with individual messages from the kafka topic."
  [{:keys [topic properties duration] :or {duration 100}}]
  (let [consumer (KafkaConsumer. ^Properties (doto (Properties.)
                                               (.putAll properties)))
        chan-consumer (chan)]

    (.subscribe consumer [topic])
    (go-loop [records []]
             (log/trace (str "records:" records))
             (doseq [^ConsumerRecord record records]
               (try
                 (let [value (.value record)
                       key- (.key record)]
                   (>! chan-consumer {:key key-
                                      :value value})
                   (log/debug (format "Consumed record with key %s and value %s\n" key- value)))
                 (catch Exception e
                   (log/error e (str "Error in kafka consumer topic:" topic)))))
             (recur (seq (.poll consumer (Duration/ofMillis duration)))))
    chan-consumer))

(defn consumer-batch!
  "Receives a map of a topic and the java properties.
  Returns a channel, which will be populated with a list of messages from the kafka topic.
  Each list of messages corresponds to a iteration of the consumer poll.
  "
  [{:keys [topic properties duration] :or {duration 100}}]
  (let [consumer (KafkaConsumer. ^Properties (doto (Properties.)
                                               (.putAll properties)))
        chan-consumer (chan)]

    (.subscribe consumer [topic])
    (go-loop [records []]
             (log/trace (str "records:" records))
             (>! chan-consumer (mapv #(let [value (.value ^ConsumerRecord %)
                                           key- (.key ^ConsumerRecord %)]
                                       (log/debug (format "Consumed record with key %s and value %s\n" key- value))
                                       {:key key-
                                        :value value})
                                     records))
             (recur (seq (.poll consumer (Duration/ofMillis duration)))))
    chan-consumer))

(defn worker!
  "Execute a function f each time a message is received at the channel.
  Checks if a exception is passed in a channel, if it is f will not be applied and the exception will be logged.
  An optional list of extra arguments can be passed to `worker!` and will be passed to `f` as the second argument.
  "
  ([channel f extra]
   (log/debug (str "Started worker with chan: " channel " fn:" f " extra: " extra))
   (go-loop []
            (let [msg (<! channel)]
              (if (instance? Throwable msg)
                (log/error msg "Received a Exception through the channel. Fn will not be applied.")
                (if extra
                  (f msg extra)
                  (f msg))) 
              (recur))))
  ([channel f]
   (worker! channel f nil)))

(defn worker-batch!
  "Execute a function `f` for every `n` messages received at the channel.
  Checks if a exception is passed in a channel, if it is f will not be applied and the exception will be logged.
  An optional extra argument can be passed to `worker-batch!` and will be passed to `f` as the second argument.
  "
  ([channel n f extra]
   (log/debug (str "Started batch worker with chan: " channel " fn:" f " extra: " extra))
   (go-loop [msgs []]
            (if (= n (count msgs))
              (do
                (if extra
                  (f msgs extra)
                  (f msgs))
                (recur []))
              (let [msg (<! channel)]
                (if (instance? Throwable msg)
                  (log/error msg "Received a Exception through the channel. Skipping it.")
                  (recur (conj msgs msg)))))))
  ([channel n f]
   (worker-batch! channel n f nil)))
