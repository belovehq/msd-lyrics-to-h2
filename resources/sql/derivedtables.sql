-- Create and populate artists table

CREATE TABLE msdartists (
  artistid      INT PRIMARY KEY AUTO_INCREMENT,
  mxmartistname VARCHAR(55),
  trackcount    INT,
  vocabulary    INT,
  fromyear      INT,
  toyear        INT
);


INSERT INTO msdartists (mxmartistname, trackcount, vocabulary, fromyear, toyear)
  SELECT
    t.mxmartistname           AS mxmartistname,
    COUNT(DISTINCT t.trackid) AS trackcount,
    COUNT(DISTINCT m.wordid)  AS vocabulary,
    MIN(t.trackyear)          AS fromyear,
    MAX(t.trackyear)          AS toyear
  FROM msdtracks t
    INNER JOIN matrix M ON M.trackid = t.trackid
  GROUP BY mxmartistname
  ORDER BY mxmartistname;

CREATE INDEX IX_msdartists_artistname
  ON msdartists (mxmartistname);

-- -- Create and populate genres table

CREATE TABLE msdgenres (
  genreid   INT AUTO_INCREMENT PRIMARY KEY,
  masdgenre VARCHAR(20)
);

INSERT INTO msdgenres (masdgenre)
  SELECT DISTINCT masdgenre
  FROM msdtracks
  WHERE masdgenre IS NOT NULL
  ORDER BY masdgenre;
