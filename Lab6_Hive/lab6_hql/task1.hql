use ${hiveconf:dbName};

drop table if exists temperature_table;

CREATE TABLE temperature_table(
  year int, temp int, quality int
)
row format delimited fields terminated by '\t'
STORED AS TEXTFILE;

load data inpath '${hiveconf:input}' into table temperature_table;

select year, MAX(temp)as max_of_year
from temperature_table
group by year;

select year, MIN(temp)as max_of_year
from temperature_table
group by year;

select year, AVG(temp)as max_of_year
from temperature_table
group by year;
