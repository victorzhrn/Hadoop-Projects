records = load 'tempInput.txt' as (year:int, temperature:int, quality:int);
filtered_records = filter records by quality in (0,1);
grouped_records = group records by year;
min_temp = foreach grouped_records generate group, MIN(records.temperature);
dump min_temp;
