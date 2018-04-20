-- Command definitions for HughSQL.

-- :name create-tracks-table :execute :raw
CREATE TABLE msdtracks (
  trackid       INT NOT NULL,
  entrackid     VARCHAR(18),
  mxmtrackid    INT,
  mxmtest          INT,
  entrackttitle VARCHAR(250),
  mxmtracktitle VARCHAR(180),
  enartistname  VARCHAR(100),
  mxmartistname VARCHAR(55),
  msdtrackyear     INT,
  masdgenre VARCHAR(20),
  msdduplicateid INT,
  shscoverid INT,
  shsoriginal INT
) AS
  SELECT *
  FROM CSVREAD(:sql:file, NULL, 'charset=UTF-8');

-- :name create-words-table :execute :raw
CREATE TABLE msdwords (
  wordid INT NOT NULL,
  stem   VARCHAR(15),
  word   VARCHAR(15)
) AS
  SELECT *
  FROM CSVREAD(:sql:file, NULL, 'charset=UTF-8');

-- :name create-matrix-table :execute :raw
CREATE TABLE matrix (
  trackid INT NOT NULL,
  wordid  INT NOT NULL,
  count   INT NOT NULL
) AS
  SELECT *
  FROM CSVREAD(:sql:file);


