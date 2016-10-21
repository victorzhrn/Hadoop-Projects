REGISTER 'hdfs:///user/maria_dev/Lab5_Pig-0.0.1-SNAPSHOT.jar';
DEFINE filterQuality filterQuality();
records = load '$input' as (year:int, temperature:int, quality:int);
filtered_records = filter records by filterQuality(quality);
grouped_records = group records by year;
min_temp = foreach grouped_records generate group, MIN(records.temperature) as min_t;
max_temp = foreach grouped_records generate group, MAX(records.temperature) as max_t;
avg_temp = foreach grouped_records generate group, AVG(records.temperature) as avg_t;

--dump min_temp;
store min_temp into '$MinOutput' using PigStorage(',');
--store max_temp into '$MaxOutput' using PigStorage(',');
--store avg_temp into '$AvgOutput' using PigStorage(',');