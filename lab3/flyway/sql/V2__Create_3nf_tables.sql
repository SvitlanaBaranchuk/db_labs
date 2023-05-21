CREATE TABLE IF NOT EXISTS School (
    School_ID SERIAL PRIMARY KEY,
    Name VARCHAR,
    ParentName VARCHAR,
    Place_ID INT,
    CONSTRAINT school_unique UNIQUE (Name, ParentName, Place_ID)
);

CREATE TABLE IF NOT EXISTS Place (
    Place_ID SERIAL PRIMARY KEY,
    RegName VARCHAR,
    AreaName VARCHAR,
    TerName VARCHAR,
    CONSTRAINT place_unique UNIQUE (RegName, AreaName, TerName)
);

CREATE TABLE IF NOT EXISTS Subject (
    Subject_ID SERIAL PRIMARY KEY,
    SubjectName VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS Test (
    Subject_ID INT,
    Student_ID VARCHAR,
    TestStatus VARCHAR,
    Ball100 FLOAT,
    Ball12 FLOAT,
    Ball FLOAT,
    UkrAdaptScale INT,
    DPALevel VARCHAR,
    School_ID INT,
    PRIMARY KEY (Subject_ID, Student_ID)
);

CREATE TABLE IF NOT EXISTS Student (
    Student_ID VARCHAR PRIMARY KEY,
    BirthDate VARCHAR,
    Sex VARCHAR,
    RegTypeName VARCHAR,
    ClassProfile VARCHAR,
    ClassLang VARCHAR,
    ExamYear VARCHAR,
    Place_ID INT,
    School_ID INT
);


ALTER TABLE Test
ADD FOREIGN KEY (Subject_ID) REFERENCES Subject (Subject_ID);

ALTER TABLE School
ADD FOREIGN KEY (Place_ID) REFERENCES Place (Place_ID);

ALTER TABLE Student
ADD FOREIGN KEY (Place_ID) REFERENCES Place (Place_ID);

ALTER TABLE Student
ADD FOREIGN KEY (School_ID) REFERENCES School (School_ID);

ALTER TABLE Test
ADD FOREIGN KEY (Student_ID) REFERENCES Student (Student_ID);

ALTER TABLE Test
ADD FOREIGN KEY (School_ID) REFERENCES School (School_ID);
