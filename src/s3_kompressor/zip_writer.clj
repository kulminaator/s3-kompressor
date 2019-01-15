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

(defn make-zip-output
  [filename]
  {
    :stream (ZipOutputStream. (clojure.java.io/output-stream filename))
    :filename filename
    })

(defn flush-and-close-zip-output
  [zip-output]
  (let [^ZipOutputStream stream (:stream zip-output)]
    (.flush stream)
    (.close stream)))

(defn roll-zip-if-needed
  [current-output filename-base written next-roll]
  ;(println (str  " *** " current-stream " > " filename-base " > " written " > " next-roll))
  (if (and (some? next-roll) (>  written next-roll))
    (do
      (flush-and-close-zip-output current-output)
      (make-zip-output (make-zip-name filename-base written)))
    current-output))

(defn workpoint-offset
  "Calculates the offset after this workpoint has been done."
  [workpoint]
  (+ (:written workpoint)
     (:size (:file-to-add workpoint))))

(defn add-file-to-zip-output
  "File has to be a map of :name, :modified-at and :input-stream-fn (function to open stream for reading)."
  [file-to-add zip-output]
  (.putNextEntry ^ZipOutputStream (:stream zip-output) (create-zip-entry (:name file-to-add)
                                              (:modified-at file-to-add)))
  (with-open [instream (open-stream (:input-stream-fn file-to-add))]
    (clojure.java.io/copy instream (:stream zip-output) :buffer-size 32768)))

(defn internal-write-zip-from
  "Keeps on taking elements from channel and writes them to the zipfile.
  Tries to create splitted files if split-size is not nil."
  [settings filename-threaded-base]
  (loop [workpoint {
                     :zip-output (make-zip-output (make-zip-name filename-threaded-base 0))
                     :file-to-add (async/<!! (:transport-channel settings))
                     :written 0
                     :next-roll (:split-size settings)
                     }]
    (if (:file-to-add workpoint)
      (do
        ; actual work
        (println (str "Adding " (:name (:file-to-add workpoint))
                      " at " (:written workpoint) " next roll " (:next-roll workpoint)))
        (add-file-to-zip-output (:file-to-add workpoint) (:zip-output workpoint))
        ; recur block
        (recur
          (merge workpoint {
                             :zip-output (roll-zip-if-needed (:zip-output workpoint) filename-threaded-base
                                                             (workpoint-offset workpoint)
                                                             (:next-roll workpoint))
                             :file-to-add (async/<!! (:transport-channel settings))
                             :written (workpoint-offset workpoint)
                             :next-roll (increase-roll-if-needed
                                         (workpoint-offset workpoint)
                                         (:next-roll workpoint)
                                         (:split-size settings))
                             })
          ))
      (flush-and-close-zip-output (:zip-output workpoint)))
    ))

(defn irange
  "Inclusive range (irange 1 3) returns 1 2 3."
  [low high]
  (range low (inc high)))

(defn dispatch-internal-zip-writers
  "Dispatches a worker thread for filename to write contents from transport-channel into it. Returns list of channels
  that contain the results of threads once they are done."
  [settings filenames]
  (mapv #(async/thread (internal-write-zip-from settings %)) filenames))

(defn write-zips-from-channel
  "Keeps on taking elements from channel and writes them to the zipfile.
  Tries to create splitted files if split-size is not nil. Uses writer threads amount of threads to work on multiple
  files at once to use more cpu cores."
  [settings]
  (let [filename-base (:filename-base settings)
        transport-channel (:transport-channel settings)
        split-size (:split-size settings)
        writer-threads (:writer-threads settings)]
    (->>
     (irange 1 writer-threads)
     (map #(str filename-base "_" %))
     (dispatch-internal-zip-writers settings)
     (map async/<!!)
     (doall))
    ))






