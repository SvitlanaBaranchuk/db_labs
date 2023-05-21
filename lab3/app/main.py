from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import select, join, func, Numeric, cast, Integer
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.expression import desc
from sqlalchemy.orm import aliased
from sqlalchemy.dialects import postgresql
from flask_wtf import FlaskForm
from wtforms import Form, StringField, validators, SubmitField, HiddenField, SelectField, IntegerField, \
    SelectMultipleField, DecimalField
import redis
import math
import random
import string

app = Flask(__name__)
app.secret_key = 'TeamSecretKey'

username = 'postgres'
password = 'postgres'
database = 'test'
host = 'db'

# SQLAlchemy
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql+psycopg2://{username}:{password}@{host}:5432/test'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Redis connection
redis_url = 'redis://redis:6379/0'
redisClient = redis.from_url(redis_url)
CACHELIFETIME = 360

NUM_OF_ENTRIES = 100


# *******************************
# ORM mapping
# *******************************

class Place(db.Model):
    __tablename__ = 'place'

    place_id = db.Column(db.Integer, primary_key=True)
    regname = db.Column(db.String(80), nullable=False)
    areaname = db.Column(db.String(80), nullable=False)
    tername = db.Column(db.String(80), nullable=False)

    schools = db.relationship("School", backref="Place", lazy=True, cascade="all, delete-orphan")


class School(db.Model):
    __tablename__ = 'school'

    school_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(400), nullable=False)
    parentname = db.Column(db.String(250))
    place_id = db.Column(db.Integer, db.ForeignKey('place.place_id'), nullable=False)

    students = db.relationship("Student", backref = 'School', lazy=True, cascade="all, delete-orphan")
    tests = db.relationship("Test", backref="School", lazy=True, cascade="all, delete-orphan")


class Student(db.Model):
    __tablename__ = 'student'

    student_id = db.Column(db.String(36), primary_key=True)
    examyear = db.Column(db.SmallInteger, nullable=False)
    birthdate = db.Column(db.SmallInteger, nullable=False)
    sex = db.Column(db.String(10), nullable=False)
    place_id = db.Column(db.Integer, db.ForeignKey('place.place_id'))
    regtypename = db.Column(db.String(120), nullable=False)
    classprofile = db.Column(db.String(40))
    classlang = db.Column(db.String(40))
    school_id = db.Column(db.Integer, db.ForeignKey('school.school_id'))

    tests = db.relationship("Test", backref="Student", lazy=True, cascade="all, delete-orphan")

class Test(db.Model):
    __tablename__ = 'test'

    subject_id = db.Column(db.Integer, db.ForeignKey('subject.subject_id'), primary_key=True)
    student_id = db.Column(db.String(36), db.ForeignKey('student.student_id'), primary_key=True)
    teststatus = db.Column(db.String(60))
    ball100 = db.Column(db.Numeric(3,2))
    ball12 = db.Column(db.SmallInteger)
    ball = db.Column(db.SmallInteger)
    ukradaptscale = db.Column(db.SmallInteger)
    dpalevel = db.Column(db.String(40))
    school_id = db.Column(db.SmallInteger, db.ForeignKey('school.school_id'))


class Subject(db.Model):
    __tablename__ = 'subject'

    subject_id = db.Column(db.Integer, primary_key=True)
    subjectname = db.Column(db.String(400), nullable=False)

    tests = db.relationship("Test", backref="Subject", lazy=True, cascade="all, delete-orphan")

# *******************************
# Flask forms
# *******************************

class PlaceForm(FlaskForm):
    place_id = HiddenField()
    regname = StringField('RegName', validators=[validators.DataRequired("Please enter your field"),
                                                 validators.Length(min=5, max=80)])
    areaname = StringField('AreaName', validators=[validators.DataRequired("Please enter your field"),
                                                   validators.Length(min=5, max=80)])
    tername = StringField('TerName', validators=[validators.DataRequired("Please enter your field"),
                                                 validators.Length(min=5, max=80)])
    submit = SubmitField("Submit")

class SchoolForm(FlaskForm):
    school_id = HiddenField('School ID')
    name = StringField('Name', validators=[validators.DataRequired("Please enter your field"),
                                           validators.Length(min=5, max=400)])
    parentname = StringField('Parentname', validators=[validators.Length(min=0, max=250)])
    place_id = SelectField('Place', coerce=int)
    submit = SubmitField("Submit")

    def __init__(self, *args, **kwargs):
        super(SchoolForm, self).__init__(*args, **kwargs)
        self.place_id.choices = [
            (str(place.place_id), f"{place.regname} {place.areaname} {place.tername}") for place in
            Place.query.all()]

    def validate(self):
        if not super(SchoolForm, self).validate():
            return False
        place_id = self.place_id.data
        place = Place.query.get(place_id)
        if not place:
            self.place_id.errors.append('Invalid place')
            return False
        return True


class StudentForm(FlaskForm):
    student_id = HiddenField('Student id')
    examyear = IntegerField('ExamYear')
    birthdate = IntegerField('BirthDate')
    sex = StringField('Sex', validators=[validators.DataRequired("Please enter your field"),
                                         validators.Length(min=5, max=10)])
    place_id = SelectField('Place', coerce=int)
    regtypename = StringField('RegTypeName', validators=[validators.DataRequired("Please enter your field"),
                                                         validators.Length(min=5, max=120)])
    classprofile = StringField('ClassProfile', validators=[validators.Length(min=5, max=40)])
    classlang = StringField('ClassLang', validators=[validators.Length(min=5, max=40)])
    school_id = SelectField('School', coerce=int)
    submit = SubmitField("Submit")

    def __init__(self, *args, **kwargs):
        super(StudentForm, self).__init__(*args, **kwargs)
        self.place_id.choices = [
            (int(place.place_id), f"{place.regname}, {place.areaname}, {place.tername}") for place in
            Place.query.all()]

        self.school_id.choices = [
            (int(school.school_id), f"{school.name}, {school.parentname} {school.place_id}") for school in
            School.query.all()]

    def validate(self):
        if not super(StudentForm, self).validate():
            return False
        place_id = self.place_id.data
        place = Place.query.get(place_id)
        if not place:
            self.place_id.errors.append('Invalid place')
            return False
        school_id = self.school_id.data
        school = School.query.get(school_id)
        if not school:
            self.school_id.errors.append('Invalid school')
            return False
        return True

class TestForm(FlaskForm):
    subject_id = SelectField('Subject id', coerce=int)
    student_id = SelectField('Student id', coerce=str)
    teststatus = StringField('TestStatus', validators=[validators.Length(min=5, max=60)])
    ball100 = DecimalField('Ball100', validators=[validators.NumberRange(min=100.00, max=200.00, message='Value must be between 100.00 and 200.00'), validators.InputRequired()])
    ball12 = IntegerField('Ball12', validators=[validators.NumberRange(min=0, max=12, message='Value must be between 0 and 12')])
    ball = IntegerField('Ball', validators=[validators.NumberRange(min=0, max=200, message='Value must be between 0 and 200')])
    ukradaptscale = IntegerField('UkrAdaptScale', validators=[validators.NumberRange(min=0, max=999, message='Value must be between 0 and 999')])
    dpalevel = StringField('DPALevel', validators=[validators.Length(min=5, max=40)])
    school_id = SelectField('School', coerce=int)
    submit = SubmitField("Submit")

    def __init__(self, *args, **kwargs):
        super(TestForm, self).__init__(*args, **kwargs)
        self.subject_id.choices = [
            (int(subject.subject_id), f"{subject.subjectname}") for subject in
            Subject.query.all()]

        self.student_id.choices = [
            (str(student.student_id), f"{student.examyear}, {student.birthdate}, {student.sex}, {student.place_id} \n"
                                      f"{student.regtypename}, {student.classprofile}, {student.classlang}, {student.school_id} ")
            for student in Student.query.all()]

        self.school_id.choices = [
            (int(school.school_id), f"{school.name}, {school.parentname} {school.place_id}") for school in
            School.query.all()]

    def validate_on_submit(self):
        return True

    def validate(self):
        if not super(TestForm, self).validate():
            for field, errors in self.errors.items():
                for error in errors:
                    flash(f"Error in field '{getattr(self, field).label.text}': {error}", 'error')
            raise Exception(f"Error in field '{getattr(self, field).label.text}': {error}", 'error')
            return False
        subject_id = self.subject_id.data
        subject = Subject.query.get(subject_id)
        if not subject:
            self.subject_id.errors.append('Invalid subject')
            raise Posos_two
            return False
        student_id = self.student_id.data
        student = Student.query.get(student_id)
        if not student:
            self.student_id.errors.append('Invalid student')
            raise Posos_three
            return False
        school_id = self.school_id.data
        school = School.query.get(school_id)
        if not school:
            self.school_id.errors.append('Invalid school')
            raise Posos_four
            return False
        return True


class SubjectForm(FlaskForm):
    subject_id = HiddenField()
    subjectname = StringField('Subject Name', validators=[validators.DataRequired("Please enter your field"),
                                           validators.Length(min=5, max=400)])
    submit = SubmitField("Submit")


class StatisticsForm(FlaskForm):
    regname = SelectMultipleField('Region Name', validators=[validators.DataRequired()], coerce=str)
    examyear = SelectField('Exam Year', validators=[validators.DataRequired()], coerce=int)
    subjectname = SelectField('Subject name', validators=[validators.DataRequired()], coerce=str)
    submit = SubmitField('Submit')

    def __init__(self, *args, **kwargs):
        super(StatisticsForm, self).__init__(*args, **kwargs)

        regions = [(place.regname, place.regname) for place in
                   Place.query.with_entities(Place.regname).distinct()]
        regions.insert(0, ('all', 'Усі регіони'))
        self.regname.choices = regions

        self.examyear.choices = [(student.examyear, student.examyear) for student in
                                 Student.query.with_entities(Student.examyear).distinct()]
        self.subjectname.choices = [(subject.subjectname, subject.subjectname) for subject in
                             Subject.query.with_entities(Subject.subjectname).distinct()]

# *******************************
# Routing
# *******************************

@app.route('/', methods=['GET', 'POST'])
def root():
    return render_template('index.html')

# *******************************
# Place
# *******************************

@app.route('/places/<int:page>')
def showPlaces(page=1):
    start = (page-1) * NUM_OF_ENTRIES
    end = page * NUM_OF_ENTRIES
    last_page = math.ceil(Place.query.count() / NUM_OF_ENTRIES)
    return render_template('showPlaces.html', places=Place.query.order_by(desc(Place.place_id)).slice(start, end),
                           last_page=last_page)


@app.route('/places/add', methods=['GET', 'POST'])
def addPlace():
    form = PlaceForm(request.form)

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('formPlaces.html', form=form, action='addPlace')
        newPlace = Place (
            regname=form.regname.data,
            areaname=form.areaname.data,
            tername=form.tername.data,
            )
        db.session.add(newPlace)
        db.session.commit()
        return redirect(url_for('showPlaces', page=1))

    return render_template('formPlaces.html', form=form, action='addPlace')

@app.route('/places/update', methods=['GET', 'POST'])
def updatePlace():
    form = PlaceForm(request.form)

    if request.method == 'GET':
        place_id = request.args.get('place_id')
        place = db.session.query(Place).filter(Place.place_id == place_id).one()

        form.place_id.data = place_id
        form.regname.data = place.regname
        form.areaname.data = place.areaname
        form.tername.data = place.tername
        return render_template('formPlaces.html', form=form, action="updatePlace")

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('formPlaces.html', form=form, action="updatePlace")
        place = db.session.query(Place).filter(Place.place_id == form.place_id.data).one()
        place.regname = form.regname.data,
        place.areaname = form.areaname.data,
        place.tername = form.tername.data,

        db.session.commit()
        return redirect(url_for('showPlaces', page=1))


@app.route('/places/delete', methods=['POST'])
def deletePlace():
    place_id = request.form['place_id']
    result = db.session.query(Place).filter(Place.place_id == place_id).one()

    db.session.delete(result)
    db.session.commit()
    return redirect(url_for('showPlaces', page=1))


# *******************************
# School
# *******************************

@app.route('/schools/<int:page>')
def showSchools(page=1):
    start = (page - 1) * NUM_OF_ENTRIES
    end = page * NUM_OF_ENTRIES
    last_page = math.ceil(School.query.count() / NUM_OF_ENTRIES)
    return render_template('showSchools.html',
                           schools=School.query.order_by(desc(School.school_id)).slice(start, end),
                           last_page=last_page)


@app.route('/schools/add', methods=['GET', 'POST'])
def addSchool():
    form = SchoolForm(request.form)

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('formSchools.html', form=form, action='addSchool')
        newSchool = School(
            name=form.name.data,
            parentname=form.parentname.data,
            place_id=form.place_id.data
        )
        db.session.add(newSchool)
        db.session.commit()
        return redirect(url_for('showSchools', page=1))

    return render_template('formSchools.html', form=form, action='addSchool')


@app.route('/schools/update', methods=['GET', 'POST'])
def updateSchool():
    form = SchoolForm(request.form)

    if request.method == 'GET':
        school_id = request.args.get('school_id')
        print(school_id)
        school = db.session.query(School).filter(School.school_id == school_id).one()

        form.school_id.data = school_id
        form.name.data = school.name
        form.parentname.data = school.parentname
        form.place_id.data = school.place_id
        return render_template('formSchools.html', form=form, action="updateSchool")

    if request.method == 'POST':
        if not form.validate():
            return render_template('formSchools.html', form=form, action="updateSchool")
        school = db.session.query(School).filter(School.school_id == form.school_id.data).one()
        school.name = form.name.data
        school.parentname = form.parentname.data
        school.place_id = form.place_id.data

        db.session.commit()
        return redirect(url_for('showSchools', page=1))


@app.route('/schools/delete', methods=['POST'])
def deleteSchool():
    school_id = request.form['school_id']
    result = db.session.query(School).filter(School.school_id == school_id).one()

    db.session.delete(result)
    db.session.commit()
    return redirect(url_for('showSchools', page=1))


# *******************************
# Student
# *******************************

@app.route('/students/<int:page>')
def showStudents(page=1):
    start = (page-1) * NUM_OF_ENTRIES
    end = page * NUM_OF_ENTRIES
    last_page = math.ceil(Student.query.count() / NUM_OF_ENTRIES)
    return render_template('showStudents.html', students=Student.query[start:end], last_page=last_page)


def generate_random_string():
    random_string = ''.join(random.choices(string.digits + string.ascii_lowercase, k=8))
    random_string += '-' + ''.join(random.choices(string.digits + string.ascii_lowercase, k=4))
    random_string += '-' + ''.join(random.choices(string.digits + string.ascii_lowercase, k=4))
    random_string += '-' + ''.join(random.choices(string.digits + string.ascii_lowercase, k=4))
    random_string += '-' + ''.join(random.choices(string.digits + string.ascii_lowercase, k=12))
    return random_string


@app.route('/students/add', methods=['GET', 'POST'])
def addStudents():
    form = StudentForm(request.form)

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('formStudents.html', form=form, action='addStudents')
        while True:
            new_student_id = generate_random_string()
            student = Student.query.filter_by(student_id=new_student_id).first()
            if student is None:
                break

        newStudent = Student (
            student_id=new_student_id,
            examyear=form.examyear.data,
            birthdate=form.birthdate.data,
            sex=form.sex.data,
            place_id=form.place_id.data,
            regtypename=form.regtypename.data,
            classprofile=form.classprofile.data,
            classlang=form.classlang.data,
            school_id=form.school_id.data
            )
        db.session.add(newStudent)
        db.session.commit()
        return redirect(url_for('showStudents', page=1))

    return render_template('formStudents.html', form=form, action='addStudents')


@app.route('/students/update', methods=['GET', 'POST'])
def updateStudent():
    form = StudentForm(request.form)

    if request.method == 'GET':
        student_id = request.args.get('student_id')
        student = db.session.query(Student).filter(Student.student_id == student_id).one()
        form.student_id.data = student_id
        form.examyear.data = student.examyear
        form.birthdate.data = student.birthdate
        form.sex.data = student.sex
        form.place_id.data = student.place_id
        form.regtypename.data = student.regtypename
        form.classprofile.data = student.classprofile
        form.classlang.data = student.classlang
        form.school_id.data = student.school_id

        return render_template('formStudents.html', form=form, action="updateStudent")

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('formStudents.html', form=form, action="updateStudent")
        student = db.session.query(Student).filter(Student.student_id == form.student_id.data).one()
        student.examyear = int(form.examyear.data)
        student.birthdate = int(form.birthdate.data)
        student.sex = form.sex.data
        student.place_id = form.place_id.data
        student.regtypename = form.regtypename.data
        student.classprofile = form.classprofile.data
        student.classlang = form.classlang.data
        student.school_id = form.school_id.data
        db.session.commit()
        return redirect(url_for('showStudents', page=1))


@app.route('/students/delete', methods=['POST'])
def deleteStudent():
    student_id = request.form['student_id']
    result = db.session.query(Student).filter(Student.student_id == student_id).one()
    db.session.delete(result)
    db.session.commit()
    return redirect(url_for('showStudents', page=1))

# *******************************
# Test
# *******************************

@app.route('/tests/<int:page>')
def showTests(page=1):
    start = (page-1) * NUM_OF_ENTRIES
    end = page * NUM_OF_ENTRIES
    last_page = math.ceil(Test.query.count() / NUM_OF_ENTRIES)
    return render_template('showTests.html', tests = Test.query[start:end], last_page=last_page)

@app.route('/tests/add', methods=['GET', 'POST'])
def addTests():
    form = TestForm(request.form)

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('formTests.html', action='addTests', form=form)
        newTest = Test (
            subject_id=form.subject_id.data,
            student_id=form.student_id.data,
            teststatus=form.teststatus.data,
            ball100=form.ball100.data,
            ball12=form.ball12.data,
            ball=form.ball.data,
            ukradaptscale=form.ukradaptscale.data,
            dpalevel=form.dpalevel.data,
            school_id=form.school_id.data
            )
        db.session.add(newTest)
        db.session.commit()
        return redirect(url_for('showTests', page=1))

    return render_template('formTests.html', form=form, action='addTests')


@app.route('/tests/update', methods=['GET', 'POST'])
def updateTest():
    form = TestForm(request.form)

    if request.method == 'GET':
        student_id = request.args.get('student_id')
        subject_id = request.args.get('subject_id')
        print(student_id, subject_id)

        test = db.session.query(Test).filter(Test.student_id == student_id, Test.subject_id == subject_id).one()

        form.subject_id.data = subject_id,
        form.student_id.data = student_id,
        form.teststatus.data = test.teststatus,
        form.ball100.data = test.ball100,
        form.ball12.data = test.ball12,
        form.ball.data = test.ball,
        form.ukradaptscale.data = test.ukradaptscale,
        form.dpalevel.data = test.dpalevel,
        form.school_id.data = test.school_id
        return render_template('formTests.html', form=form, action="updateTest")

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('formTests.html', form=form, action="updateTest")
        test = db.session.query(Test).filter(Test.student_id == student_id, Test.subject_id == subject_id).one()
        test.teststatus = form.teststatus.data
        test.ball100 = form.ball100.data
        test.ball12 = form.ball12.data
        test.ball = form.ball.data
        test.ukradaptscale = form.ukradaptscale.data
        test.dpalevel = form.dpalevel.data
        test.school_id = form.school_id.data
        db.session.commit()
        return redirect(url_for('showTests', page=1))


@app.route('/tests/delete', methods=['POST'])
def deleteTest():
    student_id = request.form['student_id']
    subject_id = request.form['subject_id']
    result = db.session.query(Test).filter(Test.student_id == student_id, Test.subject_id == subject_id).one()

    db.session.delete(result)
    db.session.commit()

    return redirect(url_for('showTests', page=1))

# *******************************
# Subject
# *******************************

@app.route('/subjects/<int:page>')
def showSubjects(page=1):
    start = (page-1) * NUM_OF_ENTRIES
    end = page * NUM_OF_ENTRIES
    last_page = math.ceil(Subject.query.count() / NUM_OF_ENTRIES)
    return render_template('showSubjects.html', subjects = Subject.query.order_by(desc(Subject.subject_id)).slice(start, end), last_page=last_page)

@app.route('/subjects/add', methods=['GET', 'POST'])
def addSubjects():
    form = SubjectForm(request.form)

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('formSubjects.html', form=form, action='addSubjects')
        newSubject = Subject (
            subjectname = form.subjectname.data,
            )
        db.session.add(newSubject)
        db.session.commit()
        return redirect(url_for('showSubjects', page=1))

    return render_template('formSubjects.html', form=form, action='addSubjects')

@app.route('/subjects/update', methods=['GET', 'POST'])
def updateSubject():
    form = SubjectForm(request.form)

    print(request.method)
    if request.method == 'GET':
        subject_id = request.args.get('subject_id')
        print(subject_id)
        subject = db.session.query(Subject).filter(Subject.subject_id == subject_id).one()
        form.subject_id.data = subject_id
        form.subjectname.data = subject.subjectname
        return render_template('formSubjects.html', form=form, action="updateSubject")

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('formSubjects.html', form=form, action="updateSubject")
        subject = db.session.query(Subject).filter(Subject.subject_id == form.subject_id.data).one()
        subject.subjectname = form.subjectname.data,
        db.session.commit()
        return redirect(url_for('showSubjects', page=1))

@app.route('/subjects/delete', methods=['POST'])
def deleteSubject():
    subject_id= request.form['subject_id']
    result = db.session.query(Subject).filter(Subject.subject_id == subject_id).one()

    db.session.delete(result)
    db.session.commit()
    return redirect(url_for('showSubjects', page=1))

# *******************************
# Statistics
# *******************************

@app.route('/showStatistics', methods=['GET', 'POST'])
def showStatistics():
    form = StatisticsForm(request.form)

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('showStatistics.html', form=form, action='showStatistics')

        selectedRegions = form.regname.data
        if 'all' in selectedRegions:
            selectedRegions = [region[0] for region in form.regname.choices[1:]]
        subjectname = form.subjectname.data
        examYear = form.examyear.data

        statisticsResults = []

        # Getting data from Cache
        regionsTakeFromDB = []
        for region in selectedRegions:
            cacheKey = f"{region}_{subjectname}_{examYear}"
            ball100 = redisClient.get(cacheKey)
            if ball100 is not None and ball100 != -1:
                statisticsResults.append({'regname': region, 'ball100': float(ball100)})
            else:
                regionsTakeFromDB.append(region)

        # Getting data from DB
        if len(regionsTakeFromDB) != 0:
            query = (
                select(
                    func.round(cast(func.avg(Test.ball100), Numeric(precision=10, scale=2)), 2).label('ball100'),
                    Place.regname
                )
                    .select_from(Test)
                    .join(Student)
                    .join(Place)
                    .join(Subject, Test.subject_id == Subject.subject_id)
                    .filter(
                    Test.teststatus == 'Зараховано',
                    Subject.subjectname == subjectname,
                    cast(Student.examyear, Integer) == examYear,
                    Place.regname.in_(regionsTakeFromDB)
                )
            )

            query = query.group_by(Place.regname)
            regionsFromDB = db.session.execute(query).fetchall()

            for region in regionsFromDB:
                statisticsResults.append(region)
                # Caching data
                cacheKey = f"{region.regname}_{subjectname}_{examYear}"
                redisClient.set(cacheKey, float(region.ball100))
                redisClient.expire(cacheKey, CACHELIFETIME)

        statisticsResults = sorted(statisticsResults, key=lambda x: x['ball100'] if isinstance(x, dict) else x.ball100,
                                   reverse=True)
        return render_template('showStatistics.html', statistics=statisticsResults, form=form)

    return render_template('showStatistics.html', statistics=[], form=form, action='showStatistics')

# *******************************
# Main
# *******************************

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)
    db.create_all()
