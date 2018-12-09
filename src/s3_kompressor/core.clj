(ns s3-kompressor.core
  (:gen-class)
  (:require [s3-kompressor.s3-crawler :as s3])
  (:require [s3-kompressor.zip-writer :as z])
  (:require [clojure.core.async :as async]))

(defn show-help
  "Shows help"
  ([cmd]
   (println (str "Unknown command: " cmd))
   (show-help))
  ([]
   (println "Usage s3-kompressor <cmd> <cmd-options>")
   (println "Supported commands: simple-zip")))

(defn make-readable
  []
  {:name "a-file"
   :input-stream-fn (fn [] (clojure.java.io/input-stream "project.clj"))})

(defn parse-int
  [txt]
  (when (some? txt)
    (Integer/parseInt (clojure.string/replace txt #"[a-zA-Z]" ""))))

(defn validate-simple-zip-options
  [options]
  (assert (contains? options "--bucket") "--bucket not specified, see help")
  (when some? (get "--split-size" options)
    (assert (nil? (parse-int (get "--split-size" options))) "--split-size must have integer value")))

(defn simple-zip-file
  "Creates one big zip file or many small ones based on options and input provided."
  [params]
  (validate-simple-zip-options (:options params))
  (let [bucket (get (:options params) "--bucket")
        prefix (get (:options params) "--prefix")
        split-size (parse-int (get (:options params) "--split-size"))
         transport-channel (async/chan 25)]
    (println "Allocated internal channel of 25 elements")
    (async/thread (s3/list-objects-to-channel bucket prefix transport-channel))
    (z/write-zip-from
     "/tmp/test"
     transport-channel
     ;split-size
     8192)))

(defn parse-params
  "Parses the command line arguments for paramsuration"
  [args]
  (let [command (first args)
        options (apply hash-map (rest args))]
    {:command command
     :options options}))

(defn -main
  "Starts the tool."
  [& args]
  (let [params (parse-params args)]
    (case (:command params)
      "simple-zip" (simple-zip-file params)
      "help" (show-help)
      (show-help (:command params)))))
