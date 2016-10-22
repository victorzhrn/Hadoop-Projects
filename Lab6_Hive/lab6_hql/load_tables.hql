use lab6_hive;
show tables;

drop table if exists RoseEmployees;

CREATE TABLE RoseEmployees
(first string, last string, specialty string, dept string, num int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as TEXTFILE;

load data inpath 'hdfs:///user/hive/Lab6_Input/allEmployees.txt' 
overwrite into table RoseEmployees;

drop table if exists RoseStaticEmployees;
CREATE TABLE RoseStaticEmployees
(first string, last string, specialty string, num int)
PARTITIONED BY (dept string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as TEXTFILE;

load data inpath 'hdfs:///user/hive/Lab6_Input/adminEmployees.txt' 
overwrite into table RoseStaticEmployees PARTITION (dept = 'admin');
load data inpath 'hdfs:///user/hive/Lab6_Input/csseEmployees.txt' 
overwrite into table RoseStaticEmployees PARTITION (dept = 'csse');
load data inpath 'hdfs:///user/hive/Lab6_Input/eceEmployees.txt' 
overwrite into table RoseStaticEmployees PARTITION (dept = 'ece');


CREATE TABLE RoseDynamicEmployees
(first string, last string, specialty string, num int)
PARTITIONED BY(dept string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as TEXTFILE;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE RoseDynamicEmployees partition(dept) 
SELECT first,last,specialty,num,dept FROM RoseStaticEmployees;
SET hive.exec.dynamic.partition.mode=strict;

CREATE TABLE RoseDynamicEmployeesORC
(first string, last string, specialty string, num int)
PARTITIONED BY(dept string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as ORC;

INSERT INTO TABLE RoseDynamicEmployeesORC partition(dept = 'csse') 
select first, last, specialty, num from RoseEmployees where dept = 'csse';
INSERT INTO TABLE RoseDynamicEmployeesORC partition(dept = 'admin') 
select first, last, specialty, num from RoseEmployees where dept = 'admin';
INSERT INTO TABLE RoseDynamicEmployeesORC partition(dept = 'ece') 
select first, last, specialty, num from RoseEmployees where dept = 'ece';

select count(*) from RoseDynamicEmployees;
select count(*) from RoseDynamicEmployeesORC;
select count(*) from RoseStaticEmployees;
select count(*) from RoseEmployees;

show partitions RoseDynamicEmployees;
show partitions RoseStaticEmployees;
show partitions RoseDynamicEmployeesORC;

CREATE TABLE RoseDynamicEmployeesManualAdd
(first string, last string, specialty string, num int)
PARTITIONED BY(dept string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
stored as ORC;
