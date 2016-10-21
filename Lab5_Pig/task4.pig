REGISTER 'hdfs:///user/maria_dev/Lab5_Pig-0.0.1-SNAPSHOT.jar';
DEFINE countHit countHit();
DEFINE countError countError();
records = load '$input' as (date: chararray, time: chararray, x_edge_location: chararray, 
sc_bytes:int, c_ip:chararray, cs_method: chararray, cs_host: chararray, cs_uri_stem: chararray,
sc_status:int, cs_referer: chararray, cs_user_agent: chararray, cs_uri_query: chararray, 
cs_cookie:chararray, x_edge_result_type: chararray);

uri_records = foreach (group records by cs_uri_stem) generate 
flatten(STRSPLIT(group,'/',3))as (u1:chararray, u2:chararray, u3:chararray), 
flatten(BagToTuple(records.x_edge_result_type)) as (result:chararray);

blog_record = filter uri_records by (u2=='blogs');
blog_name_records = foreach blog_record generate flatten(STRSPLIT(u3,'/',2)) 
as (blog_name: chararray, other:chararray), result as (result_type:chararray);

blog_group = group blog_name_records by blog_name;

outcome = foreach blog_group generate group, 
countHit(blog_name_records.result_type) as hitRatio,
countError(blog_name_records.result_type) as errorRatio,
GetYear(CurrentTime()) as Year, 
GetMonth(CurrentTime()) as Month,
GetDay(CurrentTime()) as Say;

STORE outcome into '$output' using PigStorage('\t');
--dump outcome;