-- Command definitions for HughSQL.

-- :name create-tracks-table :execute :raw
CREATE TABLE msdtracks (
  trackid       INT NOT NULL,
  entrackid     VARCHAR(18),
  mxmtrackid    INT,
  istest          INT,
  entrackttitle VARCHAR(250),
  mxmtracktitle VARCHAR(180),
  enartistname  VARCHAR(100),
  mxmartistname VARCHAR(55),
  trackyear     INT,
  masdgenre VARCHAR(20)
) AS
  SELECT *
  FROM CSVREAD(:sql:file);

-- :name create-words-table :execute :raw
CREATE TABLE msdwords (
  wordid INT NOT NULL,
  stem   VARCHAR(15),
  word   VARCHAR(15)
) AS
  SELECT *
  FROM CSVREAD(:sql:file);

-- :name create-matrix-table :execute :raw
CREATE TABLE matrix (
  trackid INT NOT NULL,
  wordid  INT NOT NULL,
  count   INT NOT NULL
) AS
  SELECT *
  FROM CSVREAD(:sql:file);


