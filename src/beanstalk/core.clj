(ns beanstalk.core
  (:refer-clojure :exclude [read peek use])
  (:use [clojure.string :only [split lower-case]]
        [clojure.pprint :only [pprint]]
        [clojure.java.io])
  (:require [clj-yaml.core :as yaml]))

;(use 'clojure.contrib.condition)
;(use 'clojure.string)
;(use 'clojure.java.io)
;(import '[java.io BufferedReader])

(def ^:dynamic *debug* false)
(def ^:dynamic *crlf* (str \return \newline))


(defn beanstalk-debug [msg]
  (when *debug* (pprint msg)))


(defn beanstalk-cmd [s & args]
    (if (nil? args)
      (name s) 
      (str (name s) " " (str (reduce #(str %1 " " %2) args)))))


(defn beanstalk-data [data]
  (str data))


; type conversion might be (Integer. var)
(defn parse-reply [reply]
  (beanstalk-debug (str "parse-reply: " reply))
  (let [parts (split reply #"\s+")
        response (keyword (lower-case (first parts)))]
    (if (empty? (rest parts))
      {:response response :data nil}
      {:response response :data (reduce #(str %1 " " %2) (rest parts))})))


(defn stream-write [^java.io.Writer w msg]
  (beanstalk-debug (str "* => " msg))
  (doto w (.write msg) 
    (.write *crlf*)
    (.flush)))

(defn stream-read [r]
  (let [sb (StringBuilder.)]
    (loop [c (.read r)]
      (cond
        (neg? c) (str sb)
        (and (= \newline (char c))
             (> (.length sb) 1)
             (= (char (.charAt sb (- (.length sb) 1) )) \return))
              (str (.substring sb 0 (- (.length sb) 1)))
        true (do (.append sb (char c))
               (recur (.read r)))))))

(defn raise [message]
  (throw (IllegalStateException. message)))

; handler => (fn [beanstalk reply] {:payload (read beanstalk)})
; handler => (fn [beanstalk reply] {:payload (read beanstalk) :id (Integer.  (:data reply))})
(defn protocol-response [beanstalk reply expected handler]
       (condp = (:response reply)
         expected (handler beanstalk reply)
         ; under what conditions do we retry?
         :expected_crlf (raise (str "Protocol error. No CRLF."))
         :not_found (raise (str "Job not found."))
         :not_ignored false
         (raise (str "Unexpected response from sever: " (:response reply)))))


(defn close [this] (.close (:socket this)))
(defn read [this] (stream-read (:reader this)))
(defn write [this msg] (stream-write (:writer this) msg))


(defn protocol-case 
 ([beanstalk expected handle-response]
  (let [reply (parse-reply (read beanstalk))]
    (beanstalk-debug (str "* <= " reply))
    (protocol-response beanstalk reply expected handle-response)))
 ([beanstalk cmd-str data expected handle-response]
  (do (write beanstalk cmd-str)
    (write beanstalk data)
    (protocol-case beanstalk expected handle-response)))
 ([beanstalk cmd-str expected handle-response]
  (do (write beanstalk cmd-str)
    (protocol-case beanstalk expected handle-response))))


(defn stats [this] 
      (protocol-case 
        this 
        (beanstalk-cmd :stats) 
        :ok 
        (fn [b r] 
          (let [results (read b)]
            {:payload results :stats (yaml/parse-string results)}))))
(defn stats-tube [this tube] 
      (protocol-case 
        this 
        (beanstalk-cmd :stats-tube tube) 
        :ok 
        (fn [b r] 
          (let [results (read b)]
            {:payload results :stats (yaml/parse-string results)}))))
(defn stats-job [this id]
     (protocol-case
       this
       (beanstalk-cmd :stats-job id)
       :ok
       (fn [b r] 
          (let [results (read b)]
            {:payload results :stats (yaml/parse-string results)}))))
(defn put [this pri del ttr length data] 
    (protocol-case 
      this 
      (beanstalk-cmd :put pri del ttr length)
      (beanstalk-data data)
      :inserted
      (fn [b r] {:id (Integer. ^String (:data r))})))
(defn use [this tube] 
    (protocol-case 
      this
      (beanstalk-cmd :use tube)
      :using
      (fn [b r] (let [tube (:data r)] {:payload tube :tube tube}))))
(defn watch [this tube] 
    (protocol-case 
      this
      (beanstalk-cmd :watch tube)
      :watching
      (fn [b r] {:count (Integer. ^String (:data r))}))) 
(defn reserve [this] 
    (protocol-case 
      this
      (beanstalk-cmd :reserve)
      :reserved
      (fn [b r] {:payload (read b) 
                 ; response is "<id> <length>"
                 :id (Integer. ^String (first (split (:data r) #"\s+")) )})))
(defn reserve-with-timeout [this timeout] 
    (protocol-case 
      this
      (beanstalk-cmd :reserve-with-timeout timeout)
      :reserved
      (fn [b r] {:payload (read b) 
                 ; response is "<id> <length>"
                 :id (Integer. ^String (first (split (:data r) #"\s+")) )})))
(defn delete [this id] 
    (protocol-case 
      this
      (beanstalk-cmd :delete id)
      :deleted
      (fn [b r] true)))
(defn release [this id pri del] 
    (protocol-case 
      this
      (beanstalk-cmd :release id pri del)
      :released
      (fn [b r] true)))
(defn bury [this id pri] 
    (protocol-case 
      this
      (beanstalk-cmd :bury id pri)
      :buried
      (fn [b r] true)))
(defn touch [this id] 
    (protocol-case 
      this
      (beanstalk-cmd :touch id)
      :touched
      (fn [b r] true)))
(defn ignore [this tube] 
    (protocol-case 
      this
      (beanstalk-cmd :ignore tube)
      :watching
      (fn [b r] {:count (Integer. ^String (:data r))}))) 
(defn peek [this id] 
    (protocol-case 
      this
      (beanstalk-cmd :peek id)
      :found
      (fn [b r] {:payload (read b) 
                 ; response is "<id> <length>"
                 :id (Integer. ^String (first (split (:data r) #"\s+")) )})))
(defn peek-ready [this] 
    (protocol-case 
      this
      (beanstalk-cmd :peek-ready)
      :found
      (fn [b r] {:payload (read b) 
                 ; response is "<id> <length>"
                 :id (Integer. ^String (first (split (:data r) #"\s+")) )})))
(defn peek-delayed [this] 
    (protocol-case 
      this
      (beanstalk-cmd :peek-delayed)
      :found
      (fn [b r] {:payload (read b) 
                 ; response is "<id> <length>"
                 :id (Integer. ^String (first (split (:data r) #"\s+")) )})))
(defn peek-buried [this] 
    (protocol-case 
      this
      (beanstalk-cmd :peek-buried)
      :found
      (fn [b r] {:payload (read b) 
                 ; response is "<id> <length>"
                 :id (Integer. ^String (first (split (:data r) #"\s+")) )})))
(defn pause-tube [this name timeout]
    (protocol-case
      this
      (beanstalk-cmd :pause-tube name timeout)
      :paused
      (fn [b r] true)))

(defn kick [this bound]
  (protocol-case
    this
    (beanstalk-cmd :kick bound)
    :kicked
    (fn [b r] {:count (Integer. ^String (:data r))})))           
                    
(defn list-tubes [this]
  (protocol-case
    this
    (beanstalk-cmd :list-tubes)
    :ok
    (fn [b r] 
      (let [results (read b)]
        {:payload results :tubes (yaml/parse-string results)}))))

(defn list-tube-used [this]
  (protocol-case
    this
    (beanstalk-cmd :list-tube-used)
    :using
    (fn [b r] (let [tube (:data r)] {:payload tube :tube tube}))))

(defn list-tubes-watched [this]
  (protocol-case
    this
    (beanstalk-cmd :list-tubes-watched)
    :ok
    (fn [b r] 
      (let [results (read b)]
        {:payload results :tubes (yaml/parse-string results)}))))

(defn new-beanstalk
  ([host port] (let [s (java.net.Socket. ^String host ^Integer port)]
                {:socket s :reader (reader s) :writer (writer s)}))
  ([port]      (new-beanstalk "localhost" port))
  ([]          (new-beanstalk "localhost" 11300)))

