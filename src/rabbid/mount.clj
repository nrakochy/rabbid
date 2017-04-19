(ns rabbid.mount
  (:require [mount.core :as mount
             rabbid.core :as core]))

(mount/defstate ^{:on-reload :noop}
                conn
                :start (core/connect)
                :stop (core/close conn))

(mount/defstate ^{:on-reload :noop}
                channel-manager
                :start (atom {})
                :stop (atom {}))

(defn shutdown-channel [k manager]
  (core/close (k @manager))
  (swap! manager dissoc k))

(defn shutdown-channels
  ([] (shutdown-channels channel-manager))
  ([manager] (doseq [chan-key (keys @manager)]
               (shutdown-channel chan-key manager))))

(defn open-channel
  ([] (open-channel conn))
  ([connection]
   (core/open connection)))

(defn open-managed-chan
  ([k] (open-managed-chan k channel-manager))
  ([k manager]
   (let [ch (future (open-channel))
         threaded-ch @ch]
     (swap! manager assoc k threaded-ch)
     threaded-ch)))

(defn initialize-exchanges [ampq-config]
  ;; TO-DO: Exchange & Queue initialization from config
  {:stop shutdown-channels})

(mount/defstate ^{:on-reload :noop}
                queueing-engine
                :start (initialize-exchanges (mount/args))
                :stop ((:stop queueing-engine)))

(defn init [config-file]
  (let [params {}]
    (mount/start-with-args params)))

