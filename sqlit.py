import sqlite3
from sqlite3 import Error


def create_connection(db_file=":memory:", close=None):
    """create a database connection to a SQLite database"""
    conn = None
    if close == None:
        try:
            conn = sqlite3.connect(db_file)
            print(f"Connected to {db_file}, sqlite version: {sqlite3.sqlite_version}")
            return conn
        except Error as e:
            print(e)
    else:
        try:
            conn = sqlite3.connect(db_file)
            print(f"Connected to {db_file}, sqlite version: {sqlite3.sqlite_version}")
            return conn
        except Error as e:
            print(e)
        finally:
            if conn:
                conn.close()


def create_project(conn):
    try:
        cursorObj = conn.cursor()
        cursorObj.execute(
            """CREATE TABLE IF NOT EXISTS Projekt(id integer PRIMARY KEY AUTOINCREMENT, nazwa text NOT NULL, start_date text, end_date text)"""
        )
        conn.commit()
    except Error as e:
        print(e)


def create_task(conn):
    try:
        cursorObj = conn.cursor()
        cursorObj.execute(
            """CREATE TABLE IF NOT EXISTS Zadanie(id integer PRIMARY KEY AUTOINCREMENT, projekt_id integer, nazwa text NOT NULL, opis text, status text, start_date text, end_date text, FOREIGN KEY (projekt_id) REFERENCES Projekt(id))"""
        )
        conn.commit()
    except Error as e:
        print(e)


"""
def insert_data_Projekt(conn, entities):
    cursorObj = conn.cursor()
    cursorObj.execute(
        "INSERT INTO Projekt(id, nazwa, start_date, end_date) VALUES(?, ?, ?, ?)",
        entities,
    )
    conn.commit()


def insert_data_Zadanie(conn, entities):
    cursorObj = conn.cursor()
    cursorObj.execute(
        "INSERT INTO Zadanie(id, projekt_id, nazwa, opis, status, start_date date, end_date date) VALUES(?, ?, ?, ?, ?, ?, ?)",
        entities,
    )
    conn.commit()
"""


def add_project(conn, project):
    """
    Create a new project into the projects table
    :param conn:
    :param project: name, start_date, end_date
    :return: project id
    """
    sql = """INSERT INTO Projekt(nazwa, start_date, end_date) VALUES(?,?,?)"""
    cur = conn.cursor()
    cur.executemany(sql, project)
    conn.commit()
    return cur.lastrowid


def add_task(conn, task):
    """
    Create a new task into the task table
    :param conn:
    :param task: projekt_id (FK), nazwa, opis, status, start_date, end_date
    :return: task id
    """
    sql = """INSERT INTO Zadanie(projekt_id, nazwa, opis, status, start_date, end_date) VALUES(?, ?, ?, ?, ?, ?)"""
    cur = conn.cursor()
    cur.executemany(sql, task)
    conn.commit()
    return cur.lastrowid


def select_task_by(conn, table, column, value):
    # sql = f"SELECT * FROM Zadanie WHERE {column}={value}"
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE {column} = {value}")
    rows = cur.fetchall()
    return rows


def delete_project(conn, id):
    """
    Delete project from projects table
    """
    try:
        sql = "DELETE FROM Projekt WHERE id=?"
        cur = conn.cursor()
        cur.execute(sql, id)
        conn.commit()
    except Error as e:
        print(e)


def delete_task(conn, id):
    """
    Delete task from tasks table
    """
    try:
        sql = "DELETE FROM Zadanie WHERE id=?"
        cur = conn.cursor()
        cur.execute(sql, id)
        conn.commit()
    except Error as e:
        print(e)


def drop_table(conn, table):
    """
    Usuwa całą tabelę
    """
    try:
        sql = "DROP TABLE " + table
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except Error as e:
        print(e)


if __name__ == "__main__":
    conn = create_connection("projekt.db")
    drop_table(conn, "Zadanie")
    drop_table(conn, "Projekt")
    create_project(conn)
    create_task(conn)
    add_project(
        conn,
        [
            ("Kurs Kodilla", "04.01.2023", "24.05.2023"),
            ("Życie", "15.10.1973", "unknown"),
        ],
    )
    add_task(
        conn,
        [
            (1, "Prework", "Wstęp", "Finished", "04.01.2023", "05.01.2023"),
            (
                1,
                "Podstawy Pythona cz. 1",
                "wstęp do pythona",
                "finished",
                "10.01.2023",
                "17.01.2023",
            ),
            (2, "Narodziny", "początek", "Finished", "15.10.1973", "15.10.1973"),
            (
                2,
                "Podstawówka",
                "szkoła podstawowa",
                "Finished",
                "01.09.1980",
                "10.06.1990",
            ),
        ],
    )
    # delete_task(conn, "2")
    # delete_project(conn, "2")
    # drop_table(conn, "Zadanie")
    # drop_table(conn, "Projekt")
    print(select_task_by(conn, "Zadanie", "status", "'Finished'"))
    conn.close()
