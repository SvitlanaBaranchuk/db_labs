INSERT INTO Subject(SubjectName)
SELECT DISTINCT ukr_test
FROM zno
WHERE ukr_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT hist_test
FROM zno
WHERE hist_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT math_test
FROM zno
WHERE math_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT phys_test
FROM zno
WHERE phys_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT chem_test
FROM zno
WHERE chem_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT bio_test
FROM zno
WHERE bio_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT geo_test
FROM zno
WHERE geo_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT eng_test
FROM zno
WHERE eng_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT fra_test
FROM zno
WHERE fra_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT deu_test
FROM zno
WHERE deu_test != '-1';

INSERT INTO Subject(SubjectName)
SELECT DISTINCT spa_test
FROM zno
WHERE spa_test != '-1';
