(ns intoh2.core
  (:require [clojure.java.io :refer [reader writer file resource as-file]]
            [clojure.string :as s]
            [intoh2.sql :as sql]
            [clojure.java.jdbc :refer [with-db-connection]])
  (:gen-class))



; specs

(def msd-spec
  "the map of source MSD files"
  {:years   {:name "tracks_per_year.txt"}
   :stems   {:name "mxm_reverse_mapping.txt"}
   :matches {:name "mxm_779k_matches.txt"}
   :genres  {:name "msd-MASD-styleAssignment.cls" :separator "\t"}
   :train   {:name "mxm_dataset_train.txt"}
   :test    {:name "mxm_dataset_test.txt"}})


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
   :final-pairs  {:name   "h2matrix.csv"
                  :header ["trackid" "wordid" "count"]}
   :final-words  {:name   "h2words.csv"
                  :header ["wordid" "stem" "word"]}
   :final-tracks {:name   "h2tracks.csv"
                  :header ["trackid" "entrackid" "mxmtrackid" "test" "entracktitle"
                           "mxmtracktitle" "enartistname" "mxmartistname" "trackyear" "masdgenre"]}})


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
  "convert a tabular line (with separator sep) to a CSV line. Insert id at start of line if supplied. Also replace double quotes with simple quotes."
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



(defn csv-file-to-map
  "convert a csv file into a map {key1 {:col1 ... :col2 ...} key2 {:col1 ... :col2 ...} ... }.
  file: full path of the csv file
  keycol: in which column to get the keys of the map
  include-cols: which columns to include in the output (if nil, include all columns)"
  ([file key-col] (csv-file-to-map file key-col nil))
  ([file key-col include-cols]
   (with-open [rdr (reader file)]
     (let [lines (line-seq rdr)
           csvheader (map keyword (cs-to-vec (first lines)))
           add-kv-pair (fn [out line]
                         (let [m (zipmap csvheader (cs-to-vec line))
                               k (key-col m)
                               v (if include-cols (select-keys m include-cols) m)]
                           (assoc out k v)))]
       (reduce add-kv-pair {} (rest lines))))))


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
           out-words (csv-full-path :words)
           out-pairs (csv-full-path :final-pairs)]
       (with-open [tw (writer out-tracks) pw (writer out-pairs)]
         (.write tw (csv-header :tracks))
         (.write pw (csv-header :final-pairs))
         (let [seed (write-lyricsdataset-to-csv (msd-full-path :train) tw pw 0)]
           (write-lyricsdataset-to-csv (msd-full-path :test) tw pw seed)))))


(defn join-track-files
  []
  "Consolidate track data into a single csv file. Sources are:
  a) the tracks found in the mxm training and test datasets
  b) the mxm/msd track matching list
  c) the list of tracks with years
  d) the list of tracks with genres"
  (let [outfile (csv-full-path :final-tracks)
        matches (csv-file-to-map (csv-full-path :matches) :entrackid)
        years (csv-file-to-map (csv-full-path :years) :entrackid [:year])
        styles (csv-file-to-map (csv-full-path :genres) :entrackid [:masdgenre])]
    (with-open [rdr (reader (csv-full-path :tracks)) wtr (writer outfile)]
      (.write wtr (csv-header :final-tracks))
      (doseq [line (rest (line-seq rdr))]
        (let [entrackid (second (cs-to-vec line))
              mx (get matches entrackid)
              enartist (get mx :enartistname "")
              mxmartist (get mx :mxmartistname "")
              s (vec-to-csv [line
                             (get mx :entracktitle "")
                             (get mx :mxmtracktitle "")
                             enartist
                             mxmartist
                             (get-in years [entrackid :year] "")
                             (get-in styles [entrackid :masdgenre] "")])]
          (.write wtr s))))))


(defn join-word-files
  ([]
   "Consolidate word data into a single csv file. Sources are:
   a) the list of stemmed words found in the mxm training dataset
   b) the stem/word matching list"
   (let [outfile (csv-full-path :final-words)
         stems (csv-file-to-map (csv-full-path :stems) :stem [:word])]
     (with-open [rdr (reader (csv-full-path :words)) wtr (writer outfile)]
       (.write wtr (csv-header :final-words))
       (doseq [line (rest (line-seq rdr))]
         (let [stem (second (cs-to-vec line))]
           (.write wtr (vec-to-csv [line (get-in stems [stem :word] "")]))))))))



; orchestration


(defn create-csv-files
  "Create all csv files based on all MSD files"
  [] (do
       (convert-msd-files-to-csv [:years :stems :matches :genres])
       (write-lyrics-to-csv)
       (join-track-files)
       (join-word-files)))


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

