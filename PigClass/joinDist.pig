input1 = LOAD 'courses.txt' using PigStorage(',') AS (cno:chararray,cname:chararray);
input2 = LOAD 'students.txt' using PigStorage(',') AS (sno:int,sname:chararray,cno:chararray);
joinDist = JOIN input1 by cno,input2 by cno USING 'replicated';
result = FOREACH joinDist GENERATE $2,$3,$0,$1;
dump result;
