(ns intoh2.sql
  (:require [hugsql.core :as h]
            [clojure.java.io :refer [resource]]))

; create stubs for sql queries
(h/def-db-fns "sql/primarytables.sql")
(h/def-db-fns "sql/hello.sql")


(defn runscript
  "run a script from a resource file."
  [db r]
  (h/db-run db (slurp (resource r)) {} :execute))

