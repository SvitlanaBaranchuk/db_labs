INSERT INTO Student(Student_ID, BirthDate, Sex, RegTypeName, ClassProfile, ClassLang, ExamYear, Place_ID, School_ID)
SELECT DISTINCT out_id, birth, sex, reg_type, class_profile, class_lang, year,
(SELECT DISTINCT Place.Place_ID
FROM Place
WHERE Place.RegName=zno.region AND Place.AreaName=zno.area AND Place.TerName=zno.tername) as pid,
(SELECT DISTINCT School_ID
FROM School
LEFT JOIN Place
using(Place_ID)
WHERE School.Name=zno.EOName AND School.ParentName=zno.EOParent
AND Place.TerName=zno.EOTer AND Place.AreaName=zno.EOArea ) as school_id
FROM zno
WHERE birth IS NOT NULL
ON CONFLICT DO NOTHING;