REGISTER 'hdfs:///tmp/input/pigUdf.jar';
DEFINE checkQuality edu.rosehulman.mohan.IsGoodQuality();
records = LOAD '/tmp/input/tempInput.txt' using PigStorage('\t') AS (year:chararray, temperature:int, quality:int);
frecords = FILTER records by temperature!=9999 and checkQuality(quality);
grecords = GROUP frecords by year;
temp = FOREACH grecords GENERATE group, MAX(frecords.temperature) as MaxTemp;
STORE temp into '/tmp/PigOutput' using PigStorage(',');
