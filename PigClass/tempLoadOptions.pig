--records = LOAD 'sample.txt' AS (year:chararray, temperature:int, quality:int);
--records = LOAD 'sample.txt' using PigStorage('\t') AS (year:chararray, temperature:int, quality:int);
--records = LOAD 'sample.txt' using PigStorage('\t') AS (year, temperature, quality);
--records = LOAD 'sample.txt';
frecords = FILTER records by temperature!=9999 and (quality == 0 OR quality == 1 OR quality == 4 OR quality == 5 or quality == 9);
grecords = GROUP frecords by year;
temp = FOREACH grecords GENERATE group, MAX(frecords.temperature) as MaxTemp;
STORE temp into '/tmp/PigOutput' using PigStorage(',');
