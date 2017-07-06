input1 = LOAD 'courses.txt' using PigStorage(',') AS (cno:chararray,cname:chararray);
SPLIT input1 INTO awesomeCourse IF cno == 'CSSE333', greatCourse IF cno == 'CSSE490', otherCourse OTHERWISE; 
