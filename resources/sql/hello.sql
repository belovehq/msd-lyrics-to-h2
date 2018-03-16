-- :name hello :query :one
SELECT CONCAT('The 3 artists with the largest vocabulary in the Million Song Dataset are ',
              GROUP_CONCAT(s SEPARATOR ', ')) out
FROM
  (SELECT TOP 3 CONCAT(mxmartistname, ' with ', vocabulary, ' words') s
   FROM msdartists
   ORDER BY vocabulary DESC);