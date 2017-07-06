input1 = LOAD 'courses.txt' using PigStorage(',') AS (cno:chararray,cname:chararray);
input3 = LOAD 'courses.txt' using PigStorage(',') AS (cno:chararray,cname:chararray);
input2 = LOAD 'students.txt' using PigStorage(',') AS (sno:int,sname:chararray,cno:chararray);
unionResult = UNION input1, input3;
union2 = UNION input1, input2;
