records = load '$input' as (year:int, temperature:int, quality:int);
filtered_records = filter records by quality in (0,1);
grouped_records = group records by year;
min_temp = foreach grouped_records generate group, MIN(records.temperature) as min_t;
max_temp = foreach grouped_records generate group, MAX(records.temperature) as max_t;
avg_temp = foreach grouped_records generate group, AVG(records.temperature) as avg_t;

store min_temp into '$MinOutput' using PigStorage(',');
store max_temp into '$MaxOutput' using PigStorage(',');
store avg_temp into '$AvgOutput' using PigStorage(',');
