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
    (assert (nil? (parse-int (get "--split-size" options))) "--split-size must have integer value"))
  (when some? (get "--writer-threads" options)
    (assert (nil? (parse-int (get "--writer-threads" options))) "--writer-threads must have integer value")))

(defn guess-split-size
  "Returns byte count for split size (input is in megabytes or nil, in which case 10 megabytes is chosen)."
  [split-size-mb]
  (* (or split-size-mb 10) 1024 1024))

(defn pretty-timestamp[]
  (.format (java.time.LocalDateTime/now) (java.time.format.DateTimeFormatter/ofPattern "yyyyMMdd-HHmmss")))

(defn wait-for-all
  "Waits for every uploader worker to finish by trying to read their return channel."
  [uploaders]
  (doseq [upload-worker uploaders]
    (async/<!! upload-worker)))

(defn create-filename-base [pretty-typed-timestamp]
  (str "/tmp/backup.part." pretty-typed-timestamp))

(defn create-upload-prefix [pretty-typed-timestamp]
  (str "uploads/" pretty-typed-timestamp "/"))

(defn simple-zip-file
  "Creates one big zip file or many small ones based on options and input provided."
  [params]
  (validate-simple-zip-options (:options params))
  (let [bucket (get (:options params) "--bucket")
        prefix (get (:options params) "--prefix")
        upload-target (get (:options params) "--upload-bucket")
        split-size-mb (parse-int (get (:options params) "--split-size"))
        writer-threads (or (parse-int (get (:options params) "--writer-threads")) 1)
        transport-channel (async/chan 2500)
        upload-channel (async/chan 10)
        uploaders (s3/build-uploaders upload-channel upload-target 10)
        pretty-typed-timestamp (pretty-timestamp)]
    (println "Allocated internal channel of 2500 elements for downloads")
    (println "Allocated internal channel of 10 elements for uploads")
    (async/thread (s3/list-objects-to-channel bucket prefix transport-channel))
    ;; start reading downloadable data and write them to zip files
    (z/write-zips-from-channel {
                        :filename-base (create-filename-base pretty-typed-timestamp)
                        :upload-prefix (create-upload-prefix pretty-typed-timestamp)
                        :transport-channel transport-channel
                        :split-size (guess-split-size split-size-mb)
                        :writer-threads writer-threads
                        :upload-channel upload-channel
     })
    ;; signal that no more items are queued for uploads anymore
    (async/close! upload-channel)
    (wait-for-all uploaders)
    ))

(defn parse-params
  "Parses the command line arguments for paramsuration"
  [args]
  (let [command (first args)
        options (apply hash-map (rest args))]
    {:command command
     :options options}))

(defn launch[args]
  (let [params (parse-params args)]
    (case (:command params)
      "simple-zip" (simple-zip-file params)
      "help" (show-help)
      (show-help (:command params)))))

(defn -main
  "Starts the tool."
  [& args]
  (launch args))