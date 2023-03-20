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
    """
    Query rows from table with given value of atribut
    :param conn: the Connection object
    :param table: table name
    :param column: atribute name
    :param value: atribute value
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE {column} = {value}")
    rows = cur.fetchall()
    return rows


def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()

    return rows


def select_where(conn, table, **query):
    """
    Query tasks from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows


'''
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
'''


def delete_record(conn, table, **kwargs):
    """DELETE record from table with given parameteres
    :param conn:
    :table: table name"""
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    sql = f""" DELETE FROM {table} WHERE {parameters} """
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("RECORDS DELETED")
    except sqlite3.OperationalError as e:
        print(e)


def drop_table(conn, table):
    """
    Delete table
    :param table: table name to delete
    """
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE {table}")
        conn.commit()
    except Error as e:
        print(e)


def update_table(conn, table, id, **kwargs):
    """Update table parameters
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id,)
    sql = f""" UPDATE {table} SET {parameters} WHERE id = ?"""
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("TABLE UPDATE OK")
    except sqlite3.OperationalError as e:
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
                "Finished",
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
            (
                1,
                "Podstawy Pythona cz. 2",
                "kolekcje, pętle itp.",
                "Started",
                "17.01.2023",
                "unknown",
            ),
        ],
    )

    print(select_where(conn, "Zadanie", projekt_id=1, status="Finished"))
    update_table(conn, "Zadanie", id=5, status="Finished", end_date="25.01.2023")
    print(select_where(conn, "Zadanie", projekt_id=1, status="Finished"))
    delete_record(conn, "Zadanie", status="Finished")
    print(select_where(conn, "Zadanie", projekt_id=1, status="Finished"))
    conn.close()
