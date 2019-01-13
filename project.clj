(defproject s3-kompressor "1.0.0-SNAPSHOT"
  :description "Small tool to create compressed snapshots of your data (objects, files) on aws s3 buckets."
  :url "https://github.com/kulminaator/s3-kompressor"
  :license {:name "MIT License"
            :url "https://en.wikipedia.org/wiki/MIT_License"}

;  :jvm-opts ["-Dclojure.compiler.elide-meta=[:doc :file :line :added]"]

  :dependencies
  [
    [org.clojure/clojure "1.10.0"]
    [org.clojure/core.async "0.4.490"]

    [com.amazonaws/aws-java-sdk-s3 "1.11.465"]

    ; not using the aws-api for now yet :
    ;; that dont work either
    ;;[org.eclipse.jetty/jetty-util "9.3.25.v20180904"]
    ; these dont behave too well if env is misconfgiured
    ;[com.cognitect.aws/api "0.8.122"]
    ;[com.cognitect.aws/endpoints "1.1.11.462"]
    ;[com.cognitect.aws/s3 "680.2.370.0"]
    ]
  ;:global-vars {*warn-on-reflection* true}
  :main ^:skip-aot s3-kompressor.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
