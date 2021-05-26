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
  "Receives a map of a topic and the japa properties.
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
  "Receives a map of a topic and the japa properties.
  Returns a channel that will be populated as a message is received in the kafka poll in a different thread.
  "
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

(defn worker!
  "Execute a function f each time a message is received at the channel.
  Checks if a exception is passed in a channel, if it is f will not be applied and the exception will be logged.
  "
  [channel f]
  (log/debug (str "Started worker with chan: " channel " fn:" f))
  (go-loop []
           (let [msg (<! channel)]
             (if (instance? Throwable msg)
               (log/error msg "Received a Exception through the channel. Fn will not be applied.")
               (f msg)) 
             (recur))))
