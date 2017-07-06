input1 = LOAD 'courses.txt' using PigStorage(',') AS (cno:chararray,cname:chararray);
input2 = LOAD 'students.txt' using PigStorage(',') AS (sno:int,sname:chararray,cno:chararray);
result = CROSS input1, input2;
