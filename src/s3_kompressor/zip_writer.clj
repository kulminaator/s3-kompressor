(ns s3-kompressor.zip-writer
  (:gen-class)
  (:import (java.util.zip ZipOutputStream ZipEntry))
  (:require [clojure.core.async :as async]))


(defn create-zip-entry
  "Creates a zip entry"
  [^String filename ^java.util.Date modified-at-date]
  (doto
   (ZipEntry. filename)
   (.setTime (.getTime modified-at-date))))

(defn open-stream ^java.io.InputStream [fn]
  (fn))

(defn write-zip-from2
  "Keeps on taking elements from channel and writes them to the zipfile.
  Tries to create splitted files if split-size is not nil."
  [filename transport-channel split-size]
  (with-open [zip-output (ZipOutputStream. (clojure.java.io/output-stream filename))]
    (let [has-more (atom true)]
      (loop [file-to-add (async/<!! transport-channel)]
        (when file-to-add
          (println (str "Adding " (:name file-to-add)))
          (.putNextEntry zip-output (create-zip-entry (:name file-to-add)))
          (with-open [instream (open-stream (:input-stream-fn file-to-add))]
            (clojure.java.io/copy instream zip-output :buffer-size 32768))
          (recur (async/<!! transport-channel))))
      )))

(defn make-zip-name
  [^String base ^long offset]
  (format "%s-%016x.zip" base offset))

(defn increase-roll-if-needed
  [written next-roll split-size]
  (if (and (some? next-roll) (> written next-roll))
    (+ written split-size)
    next-roll))

(defn roll-zip-if-needed
  [^ZipOutputStream current-stream filename-base written next-roll]
  ;(println (str  " *** " current-stream " > " filename-base " > " written " > " next-roll))
  (if (and (some? next-roll) (>  written next-roll))
    (do
        (.flush current-stream)
        (.close current-stream)
        (ZipOutputStream. (clojure.java.io/output-stream (make-zip-name filename-base written))))
    current-stream))

(defn internal-write-zip-from
  "Keeps on taking elements from channel and writes them to the zipfile.
  Tries to create splitted files if split-size is not nil."
  [filename-base transport-channel split-size]
  (loop [zip-output (ZipOutputStream. (clojure.java.io/output-stream (make-zip-name filename-base 0)))
         file-to-add (async/<!! transport-channel)
         written 0
         next-roll split-size]
    (if file-to-add
      (do
        ;(println (str "Adding " (:name file-to-add) " at " written + " next roll " + next-roll))
        (.putNextEntry zip-output (create-zip-entry (:name file-to-add) (:modified-at file-to-add)))
        (with-open [instream (open-stream (:input-stream-fn file-to-add))]
          (clojure.java.io/copy instream zip-output :buffer-size 32768))
        (recur (roll-zip-if-needed zip-output filename-base (+ written (:size file-to-add)) next-roll)
          (async/<!! transport-channel)
          (+ written (:size file-to-add))
          (increase-roll-if-needed (+ written (:size file-to-add)) next-roll split-size)))
      (.close zip-output))
    ))

(defn irange
  "Inclusive range (irange 1 3) returns 1 2 3."
  [low high]
  (range low (inc high)))

(defn dispatch-internal-zip-writers
  "Dispatches a worker thread for filename to write contents from transport-channel into it. Returns list of channels
  that contain the results of threads once they are done."
  [transport-channel split-size filenames]
  (map #(async/thread (internal-write-zip-from % transport-channel split-size)) filenames))

(defn write-zip-from
  "Keeps on taking elements from channel and writes them to the zipfile.
  Tries to create splitted files if split-size is not nil. Uses writer threads amount of threads to work on multiple
  files at once to use more cpu cores."
  [filename-base transport-channel split-size writer-threads]
  (doall (map async/<!!
              (->>
               (irange 1 writer-threads)
               (map #(str filename-base "_" %))
               (dispatch-internal-zip-writers transport-channel split-size)))))






