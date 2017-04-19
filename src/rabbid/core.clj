(ns rabbid.core
  (:require [clojure.string :as string-util :refer [join]]
            [langohr.core :as qcore]
            [langohr.channel :as qchan]
            [langohr.queue :as lq]
            [taoensso.nippy :as serializer :refer [freeze thaw]]
            [langohr.exchange :as le]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]))

(defn serialize [msg]
  (serializer/freeze msg))

(defn de-serialize [msg]
  (serializer/thaw msg))

(defn open [connection]
  (qchan/open connection))

(defn set-routing-key
  "Creates a formatted routing key from given name. Takes n number of routing params for namespaced routes.
  Appends wildcard which matches 1 word to the end of the args.
  e.g. returns the string app-ns.user.test-username.* for (set-routing-key app-ns user test-username)"
  [routing-params]
  (string-util/join "." routing-params))

(defn subscribe [{:keys [ch queue-name subs-config]} handler]
  (lc/subscribe ch queue-name handler subs-config))

(defn declare-queue [{:keys [ch queue-name queue-config]}]
  (lq/declare ch queue-name queue-config))

(defn bind
  "Binds queue to given exchange, accessible via routing key(s) seq."
  [{:keys [queue-name ch exch routing-keys]}]
  (doseq [routing-key routing-keys]
    (lq/bind ch queue-name exch {:routing-key routing-key})))

(defn set-queue-params
  "Opens managed queue and returns map of queue data needed for starting"
  [queue-key {:keys [exch msg-config queue-config exch-config]} routes]
  (let [queue-name (name queue-key)]
    {:ch           queue-key
     :queue-name   queue-name
     :exch         exch
     :msg-config   msg-config
     :exch-config  exch-config
     :queue-config queue-config
     :routing-keys (map set-routing-key routes)}))

(defn close [chan] (qchan/close chan))

(defn declare-exchange [ch config]
  (let [{:keys [exch exchange-type]} config]
    (le/declare ch exch exchange-type config)))

(defn publish
  "Queues a command to task queue for processing"
  [{:as   payload
    :keys [channel exch routing-key payload config]}]
  (lb/publish channel exch routing-key (serialize payload) config))
