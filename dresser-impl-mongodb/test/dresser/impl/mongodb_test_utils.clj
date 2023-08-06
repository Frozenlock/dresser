(ns dresser.impl.mongodb-test-utils
  (:require  [clojure.test :as t]
             [clojure.java.io :as io])
  (:import [java.io Closeable]
           [java.lang ProcessBuilder]
           [java.util List]))


;; Processes

(def *thread-count (atom 0))

(defn- new-thread
  [ns {:keys [line column] :as form-meta} ^Runnable target]
  (Thread. target (str ns ":" line ":" column "-" (swap! *thread-count inc))))

(defmacro start-thread
  "Runs the body in a user thread. Returns the started Thread instance."
  [& body]
  `(let [thread# (new-thread ~(str *ns*) ~(meta &form) (bound-fn [] ~@body))]
     (.start thread#)
     thread#))

(defn process-handle-tree-seq
  [process]
  ; Why not use Process/descendants instead? Because it's unclear
  ; whether it guarantees the tree traversal order.
  (tree-seq
    any?
    #(iterator-seq (.iterator (.children %)))
    (.toHandle process)))

(defn destroy!
  [process]
  (doseq [process-handle (process-handle-tree-seq process)]
    (.destroy process-handle)
    (.get (.onExit process-handle))))

(defn slurp-line-by-line
  "Prefix can be used for debugging."
  [in *s prefix]
  (with-open [reader (io/reader in)]
    (doseq [line (line-seq reader)]
      (swap! *s #(take 200 (conj % line))))))

(defn- async-spawn
  "Spawns the process asynchronously.
  *stderr and *stdout string atoms will be asynchronously populated one line at a time.
  Returns a Closeable instance to destroy the process and its children."
  [args *stderr *stdout]
  (let [process (-> (ProcessBuilder. ^List args)
                    (.start))
        ;; It's important to consume the streams otherwise the process can hang.
        stderr-thread (start-thread (slurp-line-by-line (.getErrorStream process) *stderr ""))
        stdout-thread (start-thread (slurp-line-by-line (.getInputStream process) *stdout ""))
        destroy-process (fn []
                          (destroy! process)

                          ; Wait as the process can be destroyed faster than all the output is collected
                          (.join stderr-thread)
                          (.join stdout-thread))]

    {:exit-value #(try (.exitValue process)
                       (catch Exception _))
     :destroy!   destroy-process}))


(defn- flags->args [flags]
  (apply concat (map (fn [[k v]] [(str "--" (name k)) (str v)]) flags)))

(defn spawn [cmd flags {:keys [*stderr *stdout]}]
  (async-spawn (concat [cmd] (flags->args flags))
               *stderr *stdout))


;;;;;;;;;;;;


(defn start-test-mongod
  [{:keys [dbpath port *stdout]}]
  (let [cleanup! #(when
                      (and (.exists (io/file dbpath)))
                    (run! io/delete-file (reverse (file-seq (io/file dbpath)))))
        _ (try
            (cleanup!)
            (io/make-parents dbpath)
            (.mkdir (io/file dbpath))
            (catch Exception _))
        {:keys [exit-value destroy!]} (spawn "./start-test-mongodb.sh"
                                             {:dbpath dbpath
                                              :port   port}
                                             {:*stderr (atom nil)
                                              :*stdout *stdout})]
    (reify Closeable
      (close [this]
        ;; Make sure we have a working process before cleaning up.
        (when-not (exit-value)
          (destroy!)
          (cleanup!))))))

(defn wait-while
  [predicate-fn timeout-ms]
  (let [*p (promise)]
    (future
      (while (predicate-fn) (Thread/sleep 10))
      (deliver *p true))
    (when (= (deref *p timeout-ms :timeout)
             :timeout)
      (throw (ex-info "Timeout" {:timeout-ms timeout-ms})))))

(defn port-open?
  [port]
  (zero? (-> (ProcessBuilder. (into-array ["nc" "-z" "localhost" (str port)]))
             (.start)
             (.waitFor))))

(defn ensure-test-db!
  "Starts MongoDB if not already running.
  Will try to close it at runtime shutdown."
  []
  (let [port 27018
        *stdout (atom nil)]
    (when-not (port-open? port)
      (let [_ (println "Starting MongoDB...")
            mongod (start-test-mongod {:dbpath "./target/testdb"
                                       :port   port
                                       :*stdout *stdout})
            close-mongod #(.close mongod)]
        (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable close-mongod))
        (wait-while (fn []
                      (not (some #(re-find #"MongoDB is ready" %) @*stdout)))
                    5000)
        (println "MongoDB ready")))))
