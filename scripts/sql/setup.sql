CREATE DATABASE IF NOT EXISTS stockmart;
USE stockmart;


-- Creating WMT table
-- csv temp table
CREATE TABLE IF NOT EXISTS wmtcsv(period date, open double precision, high double precision, low double precision, close double precision, adj_close double precision, volume int) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS textfile TBLPROPERTIES("skip.header.line.count"="1");

-- load csv
LOAD DATA LOCAL INPATH '/datasets/blueteam/WMT.csv' INTO TABLE wmtcsv;

-- create orc
CREATE TABLE IF NOT EXISTS wmt(period date, open double precision, high double precision, low double precision, close double precision, adj_close double precision, volume int) 
    --ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS orc ;--TBLPROPERTIES("skip.header.line.count"="1");

INSERT OVERWRITE TABLE wmt
    SELECT wmtcsv.period, wmtcsv.open, wmtcsv.high, wmtcsv.low, wmtcsv.close, wmtcsv.adj_close, wmtcsv.volume
    FROM wmtcsv 
    ;

DROP TABLE wmtcsv;


-- WMT Div table setup
CREATE TABLE IF NOT EXISTS wmt_divcsv(period date, dividend double precision) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS textfile TBLPROPERTIES("skip.header.line.count"="1");

-- load csv
LOAD DATA LOCAL INPATH '/datasets/blueteam/WMT_1.csv' INTO TABLE wmt_divcsv;

-- create orc, and new partition column
CREATE TABLE IF NOT EXISTS wmt_div(period date, dividend double precision) 
    --ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS orc;

INSERT OVERWRITE TABLE wmt_div
    SELECT period, dividend
    FROM wmt_divcsv
    ;


DROP TABLE wmt_divcsv;

-----

--TGT table setup
CREATE TABLE IF NOT EXISTS tgtcsv(period date, open double precision, high double precision, low double precision, close double precision, adj_close double precision, volume int) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS textfile TBLPROPERTIES("skip.header.line.count"="1");

-- load csv
LOAD DATA LOCAL INPATH '/datasets/blueteam/TGT.csv' INTO TABLE tgtcsv;

-- create orc
CREATE TABLE IF NOT EXISTS tgt(period date, open double precision, high double precision, low double precision, close double precision, adj_close double precision, volume int) 
    --ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS orc 
    TBLPROPERTIES("transactional"="true")
    ;--TBLPROPERTIES("skip.header.line.count"="1");

-- Partition the table by year
INSERT OVERWRITE TABLE tgt
    SELECT tgtcsv.period, tgtcsv.open, tgtcsv.high, tgtcsv.low, tgtcsv.close, tgtcsv.adj_close,
    tgtcsv.volume
    FROM tgtcsv
    ;

DROP TABLE tgtcsv;


--TGT_1 table setup
CREATE TABLE IF NOT EXISTS tgt_divcsv(period date, dividends double precision) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS textfile TBLPROPERTIES("skip.header.line.count"="1");

-- load csv
LOAD DATA LOCAL INPATH '/datasets/blueteam/TGT_1.csv' INTO TABLE tgt_divcsv;

-- create orc
CREATE TABLE IF NOT EXISTS tgt_div(period date, dividends double precision) 
    --ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS orc 
    TBLPROPERTIES("transactional"="true")
    ;--TBLPROPERTIES("skip.header.line.count"="1");

-- Partition the table by year
INSERT OVERWRITE TABLE tgt_div
    SELECT tgt_divcsv.period, tgt_divcsv.dividends
    FROM tgt_divcsv
    ;

DROP TABLE tgt_divcsv;


---test
CREATE TABLE IF NOT EXISTS tgt_div(period int, dividends int) 
    --ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS orc 
    TBLPROPERTIES("transactional"="true")
    ;--TBLPROPERTIES("skip.header.line.count"="1");

