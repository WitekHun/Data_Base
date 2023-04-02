import sqlite3
from sqlite3 import Error


def create_connection(db_file=":memory:", close=0):
    """create a database connection to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to {db_file}, sqlite version: {sqlite3.sqlite_version}")
        if close != 0:
            conn.close()
        return conn
    except Error as e:
        print(e)


def execute_sql(conn, sql):
    """Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
        conn.commit()
    except Error as e:
        print(e)


def create_writer(conn):
    sql = """CREATE TABLE IF NOT EXISTS writers (id integer PRIMARY KEY AUTOINCREMENT, Autor_imię text, Autor_nazwisko text NOT NULL, narodowość text)"""
    execute_sql(conn, sql)


def create_book(conn):
    sql = """CREATE TABLE IF NOT EXISTS books (id integer PRIMARY KEY AUTOINCREMENT, Tytuł text NOT NULL, cykl text DEFAULT '', gatunek text NOT NULL, status text, ocena integer)"""
    execute_sql(conn, sql)


def create_writers_books(conn):
    """
    Create join table between writers and books
    """
    sql = """CREATE TABLE IF NOT EXISTS writers_books (id integer PRIMARY KEY AUTOINCREMENT, writer_id integer, book_id integer, FOREIGN KEY (writer_id) REFERENCES writers(id), FOREIGN KEY (book_id) REFERENCES books(id))"""
    execute_sql(conn, sql)


def add_writer(conn, writer):
    """
    Add a new writer into the writers table
    :param conn:
    :param writers: Autor_imię, Autor_nazwisko, narodowość
    :return:
    """
    for data in writer:
        sql = (
            f"INSERT INTO writers(Autor_imię, Autor_nazwisko, narodowość) VALUES{data}"
        )
        execute_sql(conn, sql)


def add_book(conn, book):
    """
    Add a new book into the books table
    :param conn:
    :param book: Tytuł, cykl, gatunek, status, ocena
    :return: books id
    """
    for data in book:
        sql = f"INSERT INTO books(Tytuł, cykl, gatunek, status, ocena) VALUES{data}"
        execute_sql(conn, sql)
        c = conn.cursor()


def add_one_book(conn, book, author=0):
    """
    Add a new book into the books table
    :param conn:
    :param book: Tytuł, cykl, gatunek, status, ocena
    :param author: if given adds book to writers_books table with given writer_id
    :return:
    """
    sql = f"INSERT INTO books(Tytuł, cykl, gatunek,status, ocena) VALUES{book}"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    if author != 0:
        sql = f"INSERT INTO writers_books(writer_id, book_id) VALUES{author, cur.lastrowid}"
        execute_sql(conn, sql)
    else:
        print("No Author given")


def join_writer_book(conn, writer_book):
    """
    Join writers and books
    :param conn:
    :param writer: writer_id
    :param book: books_id
    :return: writers_books id
    """
    for data in writer_book:
        sql = f"INSERT INTO writers_books(writer_id, book_id) VALUES{data}"
        cur = conn.cursor()
        execute_sql(conn, sql)
    return cur.lastrowid


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


def find_id(conn, idx, find_books=True):
    """
    Finds book_id or writer_id (default book_id)
    :param conn:
    :param idx: writer_id or book_id
    :param find_books: input False for books_id -> returns writer_id
    :return: book_id or writer_id
    """
    cur = conn.cursor()
    if find_books:
        field = "book_id"
        field_id = "writer_id"
    else:
        field = "writer_id"
        field_id = "book_id"
    cur.execute(f"SELECT {field} FROM writers_books WHERE {field_id}={idx}")
    rows = cur.fetchall()
    return rows


def select_linked(conn, idx, find_books=True):
    """
    Select books or writers (default books) of given id
     :param conn:
     :param id: writer_id or book_id
     :param find_books: input False for book_id -> returns writers
    """
    x = find_id(conn, idx, find_books)
    line = []
    for i in range(len(x)):
        if find_books:
            y = x[i]
            book = select_where(conn, "books", id=y[0])
            line.append(book)
        else:
            y = x[i]
            writer = select_where(conn, "writers", id=y[0])
            line.append(writer)
    return line


def delete_record(conn, table, **kwargs):
    """DELETE record from table with given parameteres
    :param conn:
    :table: table name"""
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = " AND ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += ()
    sql = f"DELETE FROM {table} WHERE {parameters}"
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
    sql = f"DROP TABLE {table}"
    execute_sql(conn, sql)


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
    conn = create_connection("library_2.db")
    drop_table(conn, "books")
    drop_table(conn, "writers")
    drop_table(conn, "writers_books")

    create_writer(conn)
    create_book(conn)
    create_writers_books(conn)
    add_writer(
        conn,
        [
            ("Terry", "Pratchett", "Anglik"),
            ("Frank", "Herbert", "Amerykanin"),
            ("Glen", "Cook", "Amerykanin"),
            ("Stanisław", "Lem", "Polak"),
            ("Richard", "Feynman", "Amerykanin"),
            ("Neil", "Gaiman", "Anglik"),
            ("Stephen", "Hawkings", "Anglik"),
            ("Leonard", "Mlodinow", "Amerykanin"),
        ],
    )

    add_book(
        conn,
        [
            ("Kolor magii", "Świat Dysku", "fantasy", "przeczytana", 10),
            ("Kosiarz", "Świat Dysku", "fantasy", "przeczytana", 10),
            ("Diuna", "Diuna", "S-F", "przeczytana", 10),
            ("Mesjasz Diuny", "Diuna", "S-F", "przeczytana", 9),
            ("Tyrania nocy", "Czarna Kompania", "fantasy", "nie przeczytana", ""),
            ("Bajki robotów", "", "S-F", "przeczytana", 6),
            ("Wykłady z fizyki t.1", "Wykłady z fizyki", "fizyka", "przeczytana", 9),
            (
                "Wykłady z fizyki t.2",
                "Wykłady z fizyki",
                "fizyka",
                "nie przeczytana",
                "",
            ),
            ("Dobry omen", "", "S-F", "przeczytana", 10),
            (
                "Jeszcze krótsza historia czasu",
                "",
                "popularnonaukowa",
                "nie przeczytana",
                "",
            ),
        ],
    )

    join_writer_book(
        conn,
        (
            (1, 1),
            (1, 9),
            (1, 2),
            (2, 3),
            (2, 4),
            (3, 5),
            (4, 6),
            (5, 7),
            (5, 8),
            (6, 9),
            (7, 10),
            (8, 10),
        ),
    )

    add_one_book(
        conn, ("Blask fantastyczny", "Świat Dysku", "fantasy", "przeczytana", 10), 1
    )

    """
    print(select_all(conn, "books"))
    print(select_where(conn, "writers_books", writer_id=1))
    update_table(conn, "books", id=5, status="przeczytana", ocena=10)
    print(select_where(conn, "books", writer_id=1, status="przeczytana"))
    delete_record(conn, "books", status="nie przeczytana", ocena="6")
    print(select_where(conn, "books", writer_id=5, status="przeczytana"))
    print(select_where(conn, "books", writer_id=6))
    """
    update_table(conn, "books", id=10, status="przeczytana", ocena=8)
    # print(select_all(conn, "books"))
    # print(find_id(conn, 1, False))
    # print(select_linked(conn, 1))
    print(*select_linked(conn, 9, False), sep="\n")
    print(*select_where(conn, "books", status="przeczytana"), sep="\n")
    # delete_record(conn, "books", status="przeczytana", gatunek="fizyka")
    print(*select_linked(conn, 1), sep="\n")
    # print(*select_all(conn, "writers_books"), sep="\n")
    conn.close()
