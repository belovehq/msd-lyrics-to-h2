-- indexes on msdtracks
CREATE PRIMARY KEY ON msdtracks (trackid);
CREATE UNIQUE INDEX ix_msdtracks_entrackid ON msdtracks (entrackid);
CREATE INDEX ix_msdtracks_masdgenre ON msdtracks (masdgenre);
CREATE INDEX ix_msdtracks_mxmartistname ON msdtracks (mxmartistname);

-- index on msdwords
CREATE PRIMARY KEY ON msdwords (wordid);

-- indexes on matrix
CREATE PRIMARY KEY ON matrix (trackid, wordid);
CREATE INDEX ix_matrix_wordid_trackid ON matrix (wordid, trackid);
