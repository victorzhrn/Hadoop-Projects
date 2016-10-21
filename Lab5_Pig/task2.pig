lines = load '$input' AS (line:chararray);
words = foreach lines GENERATE flatten(TOKENIZE(line)) as word;
groups = GROUP words BY word;
wordcount = FOREACH groups GENERATE group, COUNT(words);

store wordcount into '$Output' using PigStorage(',');