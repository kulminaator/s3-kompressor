(ns s3-kompressor.launcher
  (:gen-class
    :name org.kulminaator.s3.kompressor.Launcher
    :methods [#^{:static true} [launch [java.util.List] void]])
  (:require [s3-kompressor.core :as s3core]))

(defn launch[^java.util.List args]
  (println "Launching with " (seq args))
  (s3core/launch (seq args)))

; (launch (java.util.Collections/singletonList "simple-zip"))