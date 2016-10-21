CREATE DATABASE IF NOT EXISTS ${hiveconf:dbName};
USE ${hiveconf:dbName};
show tables;

drop table if exists archiveLogData;

CREATE TABLE IF NOT EXISTS archiveLogData
(blogName string, hitRatio double, errorRadio double, year int, month int, day int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH 'hdfs:///user/maria_dev/log_analysis_output/part-r-00000'
OVERWRITE INTO TABLE archiveLogData;

CREATE TABLE IF NOT EXISTS logData
(blogName string, hitRatio double, errorRadio double)
PARTITIONED BY (year int, month int, day int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE logData PARTITION(year, month, day)
select blogname, hitratio, errorradio, year, month, day from archiveLogData;
SET hive.exec.dynamic.partition.mode=strict;

select * from logData;
select count(*) from logData;
select count(*) from archiveLogData;


