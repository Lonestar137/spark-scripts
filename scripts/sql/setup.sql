CREATE DATABASE IF NOT EXISTS stockmart;
USE stockmart;


-- Creating WMT table
-- csv temp table
CREATE TABLE IF NOT EXISTS wmtcsv(period date, open double precision, high double precision, low double precision, close double precision, adj_close double precision, volume double precision) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS textfile TBLPROPERTIES("skip.header.line.count"="1");

-- load csv
LOAD DATA LOCAL INPATH '/datasets/blueteam/WMT.csv' INTO TABLE wmtcsv;

-- create orc
CREATE TABLE IF NOT EXISTS wmt(period date, open double precision, high double precision, low double precision, close double precision, adj_close double precision, volume double precision) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS orc ;--TBLPROPERTIES("skip.header.line.count"="1");

-- Partition the table by year
INSERT OVERWRITE TABLE wmt
    SELECT wmtcsv.period, wmtcsv.open, wmtcsv.high, wmtcsv.low, wmtcsv.close, wmtcsv.adj_close, wmtcsv.volume
    FROM wmtcsv 
    ;


DROP TABLE wmtcsv;


-- WMT Div table setup
CREATE TABLE IF NOT EXISTS wmt_dividendscsv(period date, dividend double precision) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS textfile TBLPROPERTIES("skip.header.line.count"="1");

-- load csv
LOAD DATA LOCAL INPATH '/datasets/blueteam/WMT_1.csv' INTO TABLE wmt_dividendscsv;

-- create orc, and new partition column
CREATE TABLE IF NOT EXISTS wmt_dividends(period date, dividend double precision) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS orc TBLPROPERTIES("skip.header.line.count"="1");

-- Partition the table by year
INSERT OVERWRITE TABLE wmt_dividends 
    SELECT SUBSTRING(wmt_dividendscsv.period, 0, 4), period, dividend
    FROM wmt_dividendscsv 
    ;


DROP TABLE wmt_dividendscsv;


