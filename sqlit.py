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


def create_writer(conn):
    try:
        cursorObj = conn.cursor()
        cursorObj.execute(
            """CREATE TABLE IF NOT EXISTS Writers (id integer PRIMARY KEY AUTOINCREMENT, Autor text NOT NULL, narodowość text)"""
        )
        conn.commit()
    except Error as e:
        print(e)


def create_book(conn):
    try:
        cursorObj = conn.cursor()
        cursorObj.execute(
            """CREATE TABLE IF NOT EXISTS Books (id integer PRIMARY KEY AUTOINCREMENT, Writer_id integer, Tytuł text NOT NULL, cykl text DEFAULT 'Null', gatunek text NOT NULL, status text, ocena integer,  FOREIGN KEY (Writer_id) REFERENCES Writers(id))"""
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


def add_writer(conn, writer):
    """
    Create a new Writer into the Writers table
    :param conn:
    :param Writers: Autor, narodowość
    :return: project id
    """
    sql = """INSERT INTO Writers(Autor, narodowość) VALUES(?,?)"""
    cur = conn.cursor()
    cur.executemany(sql, writer)
    conn.commit()
    return cur.lastrowid


def add_book(conn, book):
    """
    Create a new book into the Books table
    :param conn:
    :param book: Writers_id (FK), Tytuł, cykl, gatunek, status, ocena
    :return: task id
    """
    sql = """INSERT INTO Books(Writer_id, Tytuł, cykl, gatunek, status, ocena) VALUES(?, ?, ?, ?, ?, ?)"""
    cur = conn.cursor()
    cur.executemany(sql, book)
    conn.commit()
    return cur.lastrowid


def select_book_by(conn, table, column, value):
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
    parameters = " AND ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += ()
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
    conn = create_connection("library.db")
    drop_table(conn, "Books")
    drop_table(conn, "Writers")

    create_writer(conn)
    create_book(conn)
    add_writer(
        conn,
        [
            ("Terry Pratchett", "Anglik"),
            ("Frank Herbert", "Amerykanin"),
            ("Glen Cook", "Amerykanin"),
            ("Stanisław Lem", "Polak"),
            ("Richard Feynman", "Amerykanin"),
            ("Neil Gaiman", "Anglik"),
        ],
    )

    add_book(
        conn,
        [
            (1, "Kolor magii", "Świat Dysku", "fantasy", "przeczytana", 10),
            (1, "Kosiarz", "Świat Dysku", "fantasy", "przeczytana", 10),
            (2, "Diuna", "Diuna", "S-F", "przeczytana", 10),
            (2, "Mesjasz Diuny", "Diuna", "S-F", "przeczytana", 9),
            (3, "Tyrania nocy", "Czarna Kompania", "fantasy", "nie przeczytana", None),
            (4, "Bajki robotów", None, "S-F", "przeczytana", 6),
            (5, "Wykłady z fizyki t.1", "Wykłady z fizyki", "fizyka", "przeczytana", 9),
            (
                5,
                "Wykłady z fizyki t.2",
                "Wykłady z fizyki",
                "fizyka",
                "nie przeczytana",
                None,
            ),
            (6, "Dobry omen", None, "S-F", "przeczytana", 10),
        ],
    )

    print(select_where(conn, "Books", Writer_id=1, status="przeczytana"))
    update_table(conn, "Books", id=5, status="przeczytana", ocena=10)
    print(select_where(conn, "Books", Writer_id=1, status="przeczytana"))
    delete_record(conn, "Books", status="nie przeczytana", ocena="6")
    print(select_where(conn, "Books", Writer_id=5, status="przeczytana"))
    print(select_where(conn, "Books", Writer_id=6))
    # print(select_all(conn, "Writers"))
    conn.close()
