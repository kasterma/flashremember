import sqlite3
import unittest
import datetime

db_filename = "data/flashremember.db"


CREATE_SUBJECTS_TABLE = """CREATE TABLE subjects (subject text, note text)"""
CREATE_QUESTIONS_TABLE = """CREATE TABLE questions
    (question text,
     note text,
     rowid_subject INTEGER)"""
CREATE_ANSWERS_TABLE = """CREATE TABLE answers
    (rowid_question INTEGER,
     answer text,
     note text)"""
CREATE_STATS_TABLE = """CREATE TABLE stats
    (rowid_question INTEGER,
     date TEXT,
     correct INTEGER)"""


class SubjectError(Exception):
    pass


class storage:
    def __init__(self):
        self.conn = sqlite3.connect(db_filename)
        self.cursor = self.conn.cursor()

    def setup(self):
        """Create the tables"""
        self.cursor.execute(CREATE_SUBJECTS_TABLE)
        self.cursor.execute(CREATE_QUESTIONS_TABLE)
        self.cursor.execute(CREATE_ANSWERS_TABLE)
        self.cursor.execute(CREATE_STATS_TABLE)

        subjs = [('R', "R language features"),
                 ('Python', 'Python language features'),
                 ('Probability', 'Probability Qs')]
        self.cursor.executemany("INSERT INTO subjects VALUES (?,?)", subjs)

        self.insert_question("How do you filter out incomplete cases from a data frame",
                             "",
                             "R",
                             "df[complete.cases(df)]",
                             "Key is function complete.cases {stats}")

        self.insert_question("How do you filter out incomplete cases from a pandas.DataFrame",
                             "",
                             "Python",
                             "df.dropna()",
                             "")
        self.conn.commit()

    def insert_question(self, question, qnote, subject, answer, anote):
        get_subject_q = "SELECT ROWID FROM subjects WHERE subject IS '{subject}'"\
            .format(subject=subject)
        self.cursor.execute(get_subject_q)
        rowids = self.cursor.fetchall()
        if len(rowids) != 1:
            subjs = self.cursor.execute("SELECT subject FROM subjects").fetchall()
            raise SubjectError("{0} is not among {1}".format(subject,
                                                             [s[0] for s in subjs]))

        rowid_subject = rowids[0][0]
        self.cursor.execute("INSERT INTO questions VALUES (?,?,?)",
                            [question, qnote, rowid_subject])
        rowid_question = self.cursor.lastrowid
        self.cursor.execute("INSERT INTO answers VALUES (?,?,?)",
                            [rowid_question, answer, anote])

    def subject_table(self):
        subject_table_query = \
            """SELECT subjects.ROWID, subject, note, q_ct
               FROM (subjects
                       LEFT JOIN
                     (SELECT rowid_subject, COUNT(*) q_ct FROM questions GROUP BY rowid_subject)
                       ON subjects.ROWID IS rowid_subject)"""
        return self.cursor.execute(subject_table_query).fetchall()

    def get_question(self, rowid_subject):
        questions_query = \
            """SELECT ROWID, question, note FROM questions WHERE rowid_subject = {rowids}"""\
            .format(rowids=rowid_subject)
        return self.cursor.execute(questions_query).fetchall()

    def get_answer(self, rowid_question):
        answer_query = \
            """SELECT answer, note FROM answers WHERE rowid_question = {rowid}""" \
            .format(rowid=rowid_question)
        print(answer_query)
        return self.cursor.execute(answer_query).fetchall()

    def set_answer_stat(self, rowid_question, correct):
        answer_stat_insert = """INSERT INTO stats values ({rowidq}, '{date}', {correct})"""\
            .format(rowidq=rowid_question,
                    date=datetime.date.today().strftime("%Y-%m-%d"),
                    correct=int(correct))
        self.cursor.execute(answer_stat_insert)

    def update_question(self, rowid_question, question, qnote):
        update_query = """UPDATE questions
            SET question = {question}, note = {qnote}
            WHERE ROWID = {rowid}""".format(rowid=rowid_question,
                                            qnote=qnote,
                                            question=question)
        self.cursor.execute(update_query)

    def update_answer(self, rowid_answer, answer, anote):
        update_query = """UPDATE answers
            SET answer = {answer}, note = {anote}
            WHERE ROWID = {rowid}""".format(rowid=rowid_answer,
                                            anote=anote,
                                            answer=answer)
        self.cursor.execute(update_query)

    def teardown(self):
        """Drop everything"""
        self.cursor.execute("DROP TABLE subjects")
        self.cursor.execute("DROP TABLE questions")
        self.cursor.execute("DROP TABLE answers")
        self.cursor.execute("DROP TABLE stats")


class Runner:
    def __init__(self):
        self.s = storage()

    def show_subjects(self):
        stab = self.s.subject_table()
        for line in stab:
            print(line)
        return stab

    def run_question(self, rowid_question):
        q = self.s.get_question(rowid_question)
        a = self.s.get_answer(rowid_question)
        print(q)
        print(input("Continue?"))
        print(a)
        c = input("*C*orrect or not")
        c_val = True if c in ['c', 'C'] else False
        print(c_val)
        self.s.set_answer_stat(rowid_question, c_val)


class BaseRun(unittest.TestCase):
    def setUp(self):
        self.s = storage()
        self.s.setup()

    def tearDown(self):
        self.s.teardown()

    def test_baserun(self):
        print(self.s.cursor.execute("SELECT * from subjects").fetchall())

        print(self.s.subject_table())

