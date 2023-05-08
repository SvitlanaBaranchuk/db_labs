INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 1, out_id, ukr_test_stat, ukr_100, ukr_12, ukr_ball, ukr_adapt, null,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.ukrPTName AND Place.TerName=zno.ukrPTTer AND Place.RegName=zno.ukrPTReg AND Place.AreaName=zno.ukrPTArea
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 2, out_id, hist_test_stat, hist_100, hist_12, hist_ball,null , null,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.histPTName AND Place.TerName=zno.histPTName AND Place.AreaName=zno.histPTArea AND Place.RegName=zno.histPTReg
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 3, out_id, math_test_stat, math_100, math_12, math_ball, null, null,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.mathPTName AND Place.TerName=zno.mathPTName AND Place.AreaName=zno.mathPTArea AND Place.RegName=zno.mathPTReg
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 4, out_id, phys_test_stat, phys_100, phys_12, phys_ball,null , null,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.physPTName AND Place.TerName=zno.physPTName AND Place.AreaName=zno.physPTArea AND Place.RegName=zno.physPTReg
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 5, out_id, chem_test_stat, chem_100, chem_12, chem_ball,null , null,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.chemPTName AND Place.TerName=zno.chemPTName AND Place.AreaName=zno.chemPTArea AND Place.RegName=zno.chemPTReg
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 6, out_id, bio_test_stat, bio_100, bio_12, bio_ball, null , null,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.bioPTName AND Place.TerName=zno.bioPTName AND Place.AreaName=zno.bioPTArea AND Place.RegName=zno.bioPTReg
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 7, out_id, geo_test_stat, geo_100, geo_12, geo_ball,null , null,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.geoPTName AND Place.TerName=zno.geoPTName AND Place.AreaName=zno.geoPTArea AND Place.RegName=zno.geoPTReg
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 8, out_id, eng_test_stat, eng_100, eng_12, eng_ball,null , eng_dpa,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.engPTName AND Place.TerName=zno.engPTName AND Place.AreaName=zno.engPTArea AND Place.RegName=zno.engPTReg
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 9, out_id, fra_test_stat, fra_100, fra_12, fra_ball,null , fra_dpa,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.fraPTName AND Place.TerName=zno.fraPTName AND Place.AreaName=zno.fraPTArea AND Place.RegName=zno.fraPTReg
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 10, out_id, deu_test_stat, deu_100, deu_12, deu_ball,null , deu_dpa,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.deuPTName AND Place.TerName=zno.deuPTName AND Place.AreaName=zno.deuPTArea AND Place.RegName=zno.deuPTReg
AND School.ParentName IS NULL)
FROM zno;

INSERT INTO Test (Subject_ID, Student_ID, TestStatus, Ball100, Ball12, Ball,
 UkrAdaptScale, DPALevel, School_ID)
SELECT 11, out_id, spa_test_stat, spa_100, spa_12, spa_ball,null , spa_dpa,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
USING (Place_ID)
WHERE School.Name=zno.spaPTName AND Place.TerName=zno.spaPTName AND Place.AreaName=zno.spaPTArea AND Place.RegName=zno.spaPTReg
AND School.ParentName IS NULL)
FROM zno;