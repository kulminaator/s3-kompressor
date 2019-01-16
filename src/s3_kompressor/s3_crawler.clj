(ns s3-kompressor.s3-crawler
  (:gen-class)
  (:import (com.amazonaws.services.s3 AmazonS3ClientBuilder AmazonS3)
           (com.amazonaws.services.s3.model ListObjectsV2Result ListObjectsV2Request S3ObjectSummary))
  (:require [clojure.core.async :as async]))

(defn build-s3-client-config ^com.amazonaws.ClientConfiguration[]
  (doto (com.amazonaws.ClientConfiguration.)
        ; 1 min timeouts
        (.setSocketTimeout 60000)
        (.setConnectionTimeout 60000)
        ; ttl for 1 hour if we can
        (.setConnectionTTL 3600000)
        ;; allow long idle, 5 minutes, keep tcp alive with tcp-keep-alive
        (.setConnectionMaxIdleMillis 900000)
        (.setUseTcpKeepAlive true)
        (.setRetryPolicy (com.amazonaws.retry.RetryPolicy.
                           com.amazonaws.retry.PredefinedRetryPolicies/DEFAULT_RETRY_CONDITION
                           com.amazonaws.retry.PredefinedRetryPolicies/DEFAULT_BACKOFF_STRATEGY
                           10
                           true))
        ))

(defn get-client ^AmazonS3
  []
  ;(println (build-s3-client-config))
  (-> (AmazonS3ClientBuilder/standard)
      (.withRegion "eu-west-1")
      (.withClientConfiguration (build-s3-client-config))
      (.build)))

(defn ^String extract-next-token
  [^ListObjectsV2Result previous-result]
  (.getNextContinuationToken previous-result))


(defn ^ListObjectsV2Request compose-listv2-request
  [^String bucket ^String prefix]
   (-> (ListObjectsV2Request.)
       (.withBucketName bucket)
       (.withPrefix prefix)))

(defn ^ListObjectsV2Request compose-listv2-request-tokenized
  [^String bucket ^String prefix ^ListObjectsV2Result previous-result]
   (-> (compose-listv2-request bucket prefix)
       (.withContinuationToken (extract-next-token previous-result))))

(defn list-objects
  [^AmazonS3 client ^String bucket ^String prefix]
  (.listObjectsV2 client (compose-listv2-request bucket prefix)))

(defn list-next-objects
  [^AmazonS3 client ^String bucket ^String prefix ^ListObjectsV2Result previous-result]
  (.listObjectsV2 client (compose-listv2-request-tokenized bucket prefix previous-result)))

(defn is-truncated?
  [^ListObjectsV2Result result]
  (.isTruncated result))

(defn extract-summaries
  [^ListObjectsV2Result result]
  (.getObjectSummaries result))

(defn ^java.io.InputStream read-from-s3
  [^AmazonS3 client ^String bucket ^String key]
  (-> (.getObject client bucket key)
      (.getObjectContent)))

(defn build-result-object
  [^AmazonS3 client ^S3ObjectSummary s3-summary]
  (let [already-opened  false] ;(read-from-s3 client (.getBucketName s3-summary) (.getKey s3-summary))]
    {
      :name (.getKey s3-summary)
      :size (.getSize s3-summary)
      :modified-at (.getLastModified s3-summary)
      ;:input-stream-fn (fn [] already-opened)
      :input-stream-fn (read-from-s3 client (.getBucketName s3-summary) (.getKey s3-summary))
      }))

(defn get-key
  [^S3ObjectSummary s3-summary]
  (.getKey s3-summary))


(defn async-pass-to-channel
  [shard channel client]
  (println (str (.getId (Thread/currentThread)) " Shard " (first shard) (last shard)))
  (future (doseq [s3-summary shard]
            (async/>!! channel (build-result-object client s3-summary))
            (println (str "Queued " (get-key s3-summary) " " (.getId (Thread/currentThread)))))))


(defn list-objects-to-channel
  "Lists the objects in the bucket with the prefix and pipes the results into a channel."
  [bucket prefix channel]
  (let [client (get-client)]
    (loop [object-list (list-objects client bucket prefix)]
      (let [sharded-work (partition-all 50 (extract-summaries object-list))]
        ;; maybe replace get-client here with just client to reutilize the client from above ?
        (let [workers-to-wait-for (doall (map #(async-pass-to-channel % channel client) sharded-work))]
          (doseq [w workers-to-wait-for]
            (deref w))
          )
        )
      (when (is-truncated? object-list)
        (recur (list-next-objects client bucket prefix object-list)))
      )
    )
  (async/close! channel))

(defn sane-s3-filename [original]
  ; discard any folder name and replace it with hardcoded "uploads/" for now
  (clojure.string/replace original #".*/" "uploads/"))

(defn handle-upload
  [upload-target entry ^AmazonS3 s3-client]
  (if upload-target
    (do
      (println (str "Uploading " entry " to " upload-target))
      (.putObject s3-client
                  upload-target
                  (sane-s3-filename (:filename entry))
                  (java.io.File. (:filename entry)))
      (.delete (java.io.File. (:filename entry))))
    (println (str "Would upload " entry " into s3 but --upload-bucket <bucketname> is not set "))
    ))

(defn build-uploader
  "Builds an async worker that fetches entries from upload channel and uploads them.
    Once upload channel is closed the worker closes itself."
  [upload-channel upload-target s3-client worker-number]
  (async/thread
   (loop [uploadable (async/<!! upload-channel)]
     (when uploadable
       (handle-upload upload-target uploadable s3-client)
       (recur (async/<!! upload-channel))))
   "upload worker done"))

(defn build-uploaders
  "Builds uploader processes that upload entries from upload-channel to s3 into upload-target"
  [upload-channel upload-target worker-count]
  (let [client (get-client)]
    (doall (map #(build-uploader upload-channel upload-target client %) (range worker-count)))))