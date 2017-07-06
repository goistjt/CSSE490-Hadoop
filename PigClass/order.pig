input1 = LOAD 'courses.txt' using PigStorage(',') AS (cno:chararray,cname:chararray);
input2 = LOAD 'students.txt' using PigStorage(',') AS (sno:int,sname:chararray,cno:chararray);
joinResult = JOIN input1 by cno,input2 by cno;
result = FOREACH joinResult GENERATE input2::sno,input2::sname,input1::cno,input1::cname;
sortedResult = ORDER result by $0, $2 DESC;
dump sortedResult;

--limitedResult = LIMIT sortedResult 2;
--dump limitedResult;
