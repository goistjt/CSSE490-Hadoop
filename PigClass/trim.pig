REGISTER 'hdfs:///tmp/input/pigUdf.jar';
DEFINE TRIMFUNC edu.rosehulman.mohan.Trim();
input1 = LOAD '/tmp/input/testData.txt' using PigStorage('\t') As (word:chararray);
result = FOREACH input1 GENERATE TRIMFUNC(word);
STORE result into '/tmp/PigOutput2' using PigStorage('\t');
