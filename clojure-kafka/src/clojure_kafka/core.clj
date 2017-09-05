(ns clojure-kafka.core
  (:require [kinsky.client :as client]
            [kinsky.async :as async]
            [cheshire.core :refer :all]
            [clojure.core.async :refer [put! go go-loop <! >!]])
  (:import (java.util UUID)))

(when (resolve 'control) (put! (var-get (resolve 'control)) {:op :stop}))

(def ^:private topic "MyCar")

(defn- makeConsumer [] (async/consumer {:bootstrap.servers "localhost:9092"
                                       :group.id          (str (UUID/randomUUID))}
                                      (client/string-deserializer)
                                      (client/json-deserializer)))

(defn makeSimpleConsumer [] (client/consumer {:bootstrap.servers "localhost:9092"
                                        :group.id          (str (UUID/randomUUID))}
                                       (client/string-deserializer)
                                       (client/json-deserializer)))

(defn- makeProducer [] (client/producer {:bootstrap.servers "localhost:9092"}
                                       (client/string-serializer)
                                       (client/json-serializer)))

(defn sendCar []
  (let [p (makeProducer)]
    (client/send! p topic "key" {:make "SL-300", :manufacturer "Mercedes", :id "Classic"})))

(defn- consume []
  (let [[out cntl] (makeConsumer)]
    (put! cntl {:op :subscribe :topic tpic})
    (go-loop []
      (when-let [record (<! out)]
        (println (pr-str record))
        (when (:key record)
          (println (:value record) true))
        (recur)))
    cntl))

(def ^:private control (consume))

(defn list-partitions[]
  (put! control {:op :partitions-for :topic topic}))

(defn stop []
  (put! control {:op :stop}))

