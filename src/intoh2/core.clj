(ns intoh2.core
  (:require [clojure.java.io :refer [reader writer file resource as-file]]
            [clojure.string :as s]
            [intoh2.sql :as sql]
            [clojure.java.jdbc :refer [with-db-connection]])
  (:gen-class))



; specs

(def msd-spec
  "the map of source MSD files"
  {:years        {:name "tracks_per_year.txt"}
   :stems        {:name "mxm_reverse_mapping.txt"}
   :matches      {:name "mxm_779k_matches.txt"}
   :genres       {:name "msd-MASD-styleAssignment.cls" :separator "\t"}
   :train        {:name "mxm_dataset_train.txt"}
   :test         {:name "mxm_dataset_test.txt"}
   :duplicates   {:name "msd_duplicates.txt"}
   :covers-train {:name "shs_dataset_train.txt"}
   :covers-test  {:name "shs_dataset_test.txt"}})


(def csv-spec
  "the map of CSV files that will be produced from the MSD files before loading into H2"
  {:years        {:name   "msdyears.csv"
                  :header ["year" "entrackid" "artistname" "tracktitle"]}
   :stems        {:name   "mxmstems.csv"
                  :header ["stem" "word"]}
   :matches      {:name   "mxmmatches.csv"
                  :header ["entrackid" "enartistname" "entracktitle"
                           "mxmtrackid" "mxmartistname" "mxmtracktitle"]}
   :genres       {:name   "masdgenres.csv"
                  :header ["entrackid", "masdgenre"]}
   :words        {:name   "mxmwords.csv"
                  :header ["wordid" "stem"]}
   :tracks       {:name   "mxmtracks.csv"
                  :header ["trackid" "entrackid" "mxmtrackid" "test"]}
   :duplicates   {:name   "msdduplicates.csv"
                  :header ["entrackid" "msdduplicateid" "msdoriginal"]}
   :covers       {:name   "shscovers.csv"
                  :header ["entrackid" "shscoverid" "shsoriginal"]}
   :final-pairs  {:name   "h2matrix.csv"
                  :header ["trackid" "wordid" "count"]}
   :final-words  {:name   "h2words.csv"
                  :header ["wordid" "stem" "word"]}
   :final-tracks {:name   "h2tracks.csv"
                  :header ["trackid" "entrackid" "mxmtrackid" "test" "entracktitle"
                           "mxmtracktitle" "enartistname" "mxmartistname" "trackyear"
                           "masdgenre" "msdduplicateid" "shscoverid" "shsoriginal"]}})


(def track-spec
  "spec for the joining of track data"
  {:in    {:csv :tracks :join-column 1}
   :out   :final-tracks
   :joins [{:csv :matches :key :entrackid :columns [:entracktitle :mxmtracktitle :enartistname :mxmartistname]}
           {:csv :years :key :entrackid :columns [:year]}
           {:csv :genres :key :entrackid :columns [:masdgenre]}
           {:csv :duplicates :key :entrackid :columns [:msdduplicateid]}
           {:csv :covers :key :entrackid :columns [:shscoverid :shsoriginal]}]})


(def word-spec
  "spec for the joining of word data"
  {:in    {:csv :words :join-column 1}
   :out   :final-words
   :joins [{:csv :stems :key :stem :columns [:word]}]})


(def db-spec
  "Base h2 database spec"
  {:classname      "org.h2.Driver"
   :subprotocol    "h2"
   :subname        ""
   :user           ""
   :password       ""
   :DB_CLOSE_DELAY "-1"})


(defn pathify [s]
  "Append a slash at the end of a string if none is present"
  (let [t (s/trim s)]
    (if (or (= t "") (#{\/ \\} (last t))) t (str t "/"))))


(def path-spec
  "the map of input and output paths:
  :in the path of the input msd files
  :csv the path where to create csv files
  :db the database file to create"
  (try
    (-> (clojure.edn/read-string (slurp "msd.edn"))
        (update-in [:in] pathify)
        (update-in [:csv] pathify))
    (catch Exception e
      {:in  "./"
       :csv "./"
       :db  "./msd"})))



; pure functions


(defn vec-to-csv
  "convert a vector to a csv line, incl. line break"
  [v] (apply str (concat (interpose "," v) "\n")))


(defn cs-to-vec
  "convert a comma separated string to a vector of strings"
  [s] (s/split s #","))


(defn msd-full-path
  "return the full path specified for the csv file that has key k"
  [k] (str (:in path-spec) (get-in msd-spec [k :name])))


(defn csv-full-path
  "return the full path specified for the csv file that has key k"
  [k] (str (:csv path-spec) (get-in csv-spec [k :name])))


(defn csv-header
  "return the comma-separated header specified for the csv file that has key k"
  [k] (vec-to-csv (get-in csv-spec [k :header])))


(defn msd-separator
  "return the separator specified for the csv file that has key k"
  [k] (let [sep (get-in msd-spec [k :separator])]
        (if (nil? sep) "<SEP>" sep)))


(defn tabular-to-csv
  "convert a tabular line (with separator sep) to a CSV line. Insert id at start of line if supplied. And replace
  double quotes with simple quotes."
  [sep line id]
  (if (= (first line) \#) ""
                          (str (if id (str id ","))
                               (-> line
                                   (s/replace sep ",")
                                   (s/replace \" \'))
                               "\n")))


(defn seq-of-ids
  "return an infinite sequence of int starting at 1 if b is true, and of nil otherwise."
  [b] (if b (rest (range))
            (repeat nil)))



; read and write


(defn convert-msd-file-to-csv
  "convert a tabular MSD file into a CSV file.
   in: path of the msd file
   out: path of the csv file to produce
   sep: separator in the MSD file
   header: header of the csv file, comma separated and including line break
   insertid: if true then insert an incremental id at the beginning of each row"
  [in out sep header insertids]
  (with-open [rdr (reader in) wtr (writer out)]
    (if header (.write wtr header))
    (doseq [line (map (partial tabular-to-csv sep) (line-seq rdr) (seq-of-ids insertids))]
      (.write wtr line))))


(defn convert-msd-files-to-csv
  "convert a set of tabular MSD files into CSV files.
   keys: sequence of file keys"
  ([keys] (doseq [k keys]
            (let [in (msd-full-path k)
                  out (csv-full-path k)
                  sep (msd-separator k)
                  header (csv-header k)]
              (convert-msd-file-to-csv in out sep header nil)))))


(defn write-string-to-csv
  "Write a comma separated string into a CSV file with one entry per line.
  out: path of the destination file
  s: input string
  header: header of the csv file, comma separated and including line break
  insertids: if true then the first column of the csv file will be an incremental integer id"
  ([out s header insertids]
   (let [tocsv (fn [w id] (str (if id (str id ",")) w "\n"))]
     (with-open [wtr (writer out)]
       (if header (.write wtr header))
       (doseq [line (map tocsv (cs-to-vec s) (seq-of-ids insertids))]
         (.write wtr line))))))


(defn ^:private write-lyricsdataset-to-csv
  "Convert a single musicXmatch lyrics matrix and append it to CSV files.
  in: the musicXmatch lyrics matrix file name, either the training file or the test file
  tw, pw: writers to the tracks csv file and the pairs csv file
  seed: the point from which to start the tracks numbering"
  [in tw pw seed]
  (let [is-test-set (> seed 0)

        test-flag (if is-test-set 1 0)

        write-words (fn [s] (write-string-to-csv (csv-full-path :words) s (csv-header :words) true))

        write-track-data (fn [s trackid] (let [[msdtrackid mxmtrackid & word-counts] (cs-to-vec s)]
                                           (.write tw (vec-to-csv [trackid msdtrackid mxmtrackid test-flag]))
                                           (doseq [word-count word-counts]
                                             (let [[wordid n] (s/split word-count #":")]
                                               (.write pw (vec-to-csv [trackid wordid n]))))))

        do-track (fn [oldid s] (case (first s)
                                 \# oldid
                                 \% (do (if (not is-test-set) (write-words (subs s 1))) oldid)
                                 (let [trackid (inc oldid)] (write-track-data s trackid) trackid)))]

    (with-open [rdr (reader in)]
      (reduce do-track seed (line-seq rdr)))))


(defn write-lyrics-to-csv
  "Convert the two musicXmatch lyrics matrices into aggregated CSV files"
  [] (let [out-tracks (csv-full-path :tracks)
           out-pairs (csv-full-path :final-pairs)]
       (with-open [tw (writer out-tracks) pw (writer out-pairs)]
         (.write tw (csv-header :tracks))
         (.write pw (csv-header :final-pairs))
         (let [seed (write-lyricsdataset-to-csv (msd-full-path :train) tw pw 0)]
           (write-lyricsdataset-to-csv (msd-full-path :test) tw pw seed)))))


(defn convert-clique-file-to-csv
  "converts a file of MSD duplicates/covers into a csv file
  in: path of the msd file
  out: path of the output csv file
  append: append to the csv file if true"
  [in out append]
  (let [do-track (fn [s groupid] (let [v (s/split s #"<SEP>")]
                                   [(first v)
                                    groupid
                                    (if (= (last v) groupid) "1" "")]))]
    (with-open [rdr (reader (msd-full-path in)) wtr (writer (csv-full-path out) :append append)]
      (let [do-line (fn [groupid s]
                      (case (first s)
                        \# groupid
                        \% (first (s/split (subs s 1) #"[ |,]"))
                        (do (.write wtr (vec-to-csv (do-track s groupid)))
                            groupid)))]
        (.write wtr (csv-header out))
        (reduce do-line nil (line-seq rdr))))))


(defn convert-clique-files-to-csv
  "converts a list of MSD duplicates/covers into csv files. Each item is in the format [in out append] where:
  in: path of the msd file
  out: path of the output csv file
  append: append to the csv file if true"
  [files]
  (doseq [f files] (apply convert-clique-file-to-csv f)))


(defn csv-file-to-map
  "convert a csv file into a map {key1 [val1 val2 ...] key2 [val1 val2 ...] ... }.
  file: full path of the csv file
  keycol: in which column to get the keys of the map
  include-cols: which columns to include in the output"
  ([file key-col include-cols]
   (with-open [rdr (reader file)]
     (let [lines (line-seq rdr)
           csvheader (map keyword (cs-to-vec (first lines)))
           add-kv-pair (fn [out line]
                         (let [m (zipmap csvheader (cs-to-vec line))
                               k (key-col m)
                               v (map #(get m %) include-cols)]
                           (assoc out k v)))]
       (reduce add-kv-pair {} (rest lines))))))


(defn join-files [{:keys [in out joins]}]
  "join files based on a joining spec. The joining spec contains:
  :in : the file that will be the primary term of the join and the index of the column to use for the join.
  :out: the output file
  :joins : the list of files to join, with the column to use for the join and the columns to append in the output file"
  (let [colidx (:join-column in)
        datasets (map #(csv-file-to-map (csv-full-path (:csv %)) (:key %) (:columns %)) joins)
        blanks (map #(repeat (count (:columns %)) "") joins)
        find-join (fn [k dataset blank]
                    (if-let [data (get dataset k)] data blank))]
    (with-open [rdr (reader (csv-full-path (:csv in)))
                wtr (writer (csv-full-path out))]
      (.write wtr (csv-header out))
      (doseq [line (rest (line-seq rdr))]
        (let [id (nth (cs-to-vec line) colidx)
              join (map (partial find-join id) datasets blanks)]
          (.write wtr (vec-to-csv (apply concat [line] join))))))))



; orchestration


(defn create-csv-files
  "Create all csv files based on all MSD files"
  [] (do
       (convert-msd-files-to-csv [:years :stems :matches :genres])
       (write-lyrics-to-csv)
       (convert-clique-files-to-csv [[:duplicates :duplicates false]
                                     [:covers-train :covers false]
                                     [:covers-test :covers true]])
       (join-files word-spec)
       (join-files track-spec)))


(defn create-database
  "Create the database based on all csv files"
  [] (let [db (assoc db-spec :subname (:db path-spec))]
       (with-db-connection [con db]
                           (sql/runscript con "sql/speed.sql")
                           (println "Creating primary tables...")
                           (sql/create-tracks-table con {:file (str "'" (csv-full-path :final-tracks) "'")})
                           (sql/create-words-table con {:file (str "'" (csv-full-path :final-words) "'")})
                           (sql/create-matrix-table con {:file (str "'" (csv-full-path :final-pairs) "'")})
                           (println "Creating indexes...")
                           (sql/runscript con "sql/indexes.sql")
                           (println "Creating derived tables...")
                           (sql/runscript con "sql/derivedtables.sql")
                           (sql/runscript con "sql/safety.sql")
                           (println "All Done!")
                           (println (:out (sql/hello con))))))


(defn -main
  "Create the csv files and the database"
  [& args]
  (try
    (do
      (if (not= #{:csv :db :in} (set (keys path-spec)))
        (throw (Exception. "msd.edn is missing or invalid.")))
      (println "Creating the csv files...")
      (create-csv-files)
      (println "Creating the database. Just a bit of patience.")
      (create-database))
    (catch Exception e
      (println (str "An error occured: " (.getMessage e))))))

