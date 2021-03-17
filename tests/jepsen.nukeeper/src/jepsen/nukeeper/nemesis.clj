(ns jepsen.nukeeper.nemesis
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen
    [nemesis :as nemesis]
    [control :as c]
    [generator :as gen]]
   [jepsen.nukeeper.constants :refer :all]
   [jepsen.nukeeper.utils :refer :all]))

(defn random-single-node-killer-nemesis
  []
  (nemesis/node-start-stopper
   rand-nth
   (fn start [test node] (kill-clickhouse! node test))
   (fn stop [test node] (start-clickhouse! node test))))

(defn hammer-time-nemesis
  []
  (nemesis/hammer-time "clickhouse"))

(defn select-last-file
  [path]
  (last (clojure.string/split
         (c/exec :find path :-type :f :-printf "%T+ %p\n" :| :grep :-v :tmp_ :| :sort :| :awk "{print $2}")
         #"\n")))

(defn random-file-pos
  [fname]
  (let [fsize (Integer/parseInt (c/exec :du :-b fname :| :cut :-f1))]
    (rand-int fsize)))

(defn corrupt-file
  [fname]
  (if (not (empty? fname))
    (do
      (info "Corrupting" fname)
      (c/exec :dd "if=/dev/zero" (str "of=" fname) "bs=1" "count=1" (str "seek=" (random-file-pos fname)) "conv=notrunc"))
    (info "Nothing to corrupt")))

(defn corruptor-nemesis
  [path corruption-op]
  (reify nemesis/Nemesis

    (setup! [this test] this)

    (invoke! [this test op]
      (cond (= (:f op) :corrupt)
            (let [nodes (list (rand-nth (:nodes test)))]
              (info "Corruption on node" nodes)
              (c/on-nodes test nodes
                          (fn [test node]
                            (c/su
                             (kill-clickhouse! node test)
                             (corruption-op path)
                             (start-clickhouse! node test))))
              (assoc op :type :info, :value :corrupted))
            :else (do (c/on-nodes test (:nodes test)
                                  (fn [test node]
                                    (c/su
                                     (start-clickhouse! node test))))
                      (assoc op :type :info, :value :done))))

    (teardown! [this test])))

(defn logs-corruption-nemesis
  []
  (corruptor-nemesis logsdir #(corrupt-file (select-last-file %1))))

(defn snapshots-corruption-nemesis
  []
  (corruptor-nemesis snapshotsdir #(corrupt-file (select-last-file %1))))

(defn logs-and-snapshots-corruption-nemesis
  []
  (corruptor-nemesis coordinationdir (fn [path]
                                       (do
                                         (corrupt-file (select-last-file (str path "/snapshots")))
                                         (corrupt-file (select-last-file (str path "/logs")))))))
(defn drop-all-corruption-nemesis
  []
  (corruptor-nemesis coordinationdir (fn [path]
                                       (c/exec :rm :-fr path))))

(defn start-stop-generator
  []
  (->>
   (cycle [(gen/sleep 5)
           {:type :info, :f :start}
           (gen/sleep 5)
           {:type :info, :f :stop}])))

(defn corruption-generator
  []
  (->>
   (cycle [(gen/sleep 5)
           {:type :info, :f :corrupt}])))

(def custom-nemesises
  {"single-node-killer" {:nemesis (random-single-node-killer-nemesis)
                         :generator (start-stop-generator)}
   "simple-partitioner" {:nemesis (nemesis/partition-random-halves)
                         :generator (start-stop-generator)}
   "logs-corruptor" {:nemesis (logs-corruption-nemesis)
                     :generator (corruption-generator)}
   "snapshots-corruptor" {:nemesis (snapshots-corruption-nemesis)
                          :generator (corruption-generator)}
   "logs-and-snapshots-corruptor" {:nemesis (logs-and-snapshots-corruption-nemesis)
                                   :generator (corruption-generator)}
   "drop-data-corruptor" {:nemesis (drop-all-corruption-nemesis)
                          :generator (corruption-generator)}})
