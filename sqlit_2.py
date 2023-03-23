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
            """CREATE TABLE IF NOT EXISTS Writers (id integer PRIMARY KEY AUTOINCREMENT, Autor_imię text, Autor_nazwisko ext NOT NULL, narodowość text)"""
        )
        conn.commit()
    except Error as e:
        print(e)


def create_book(conn):
    try:
        cursorObj = conn.cursor()
        cursorObj.execute(
            """CREATE TABLE IF NOT EXISTS Books (id integer PRIMARY KEY AUTOINCREMENT, Tytuł text NOT NULL, cykl text DEFAULT 'Null', gatunek text NOT NULL, status text, ocena integer)"""
        )
        conn.commit()
    except Error as e:
        print(e)


def create_writers_books(conn):
    """
    Create join table between Writers and Books
    """
    try:
        cursorObj = conn.cursor()
        cursorObj.execute(
            """CREATE TABLE IF NOT EXISTS Writers_Books (id integer PRIMARY KEY AUTOINCREMENT, Writer_id integer, Book_id integer, FOREIGN KEY (Writer_id) REFERENCES Writers(id), FOREIGN KEY (Book_id) REFERENCES Books(id))"""
        )
        conn.commit()
    except Error as e:
        print(e)


def add_writer(conn, writer):
    """
    Add a new Writer into the Writers table
    :param conn:
    :param Writers: Autor_imię, Autor_nazwisko, narodowość
    :return: Writers id
    """
    sql = (
        """INSERT INTO Writers(Autor_imię, Autor_nazwisko, narodowość) VALUES(?,?,?)"""
    )
    cur = conn.cursor()
    cur.executemany(sql, writer)
    conn.commit()
    return cur.lastrowid


def add_book(conn, book):
    """
    Add a new book into the Books table
    :param conn:
    :param book: Tytuł, cykl, gatunek, status, ocena
    :return: Books id
    """
    sql = """INSERT INTO Books(Tytuł, cykl, gatunek, status, ocena) VALUES( ?, ?, ?, ?, ?)"""
    cur = conn.cursor()
    cur.executemany(sql, book)
    conn.commit()
    return cur.lastrowid


def join_writer_book(conn, writer_book):
    """
    Join Writers and Books
    :param conn:
    :param writer: Writer_id
    :param book: Books_id
    :return: Writers_Books id
    """
    sql = """INSERT INTO Writers_Books(Writer_id, Book_id) VALUES( ?, ?)"""
    cur = conn.cursor()
    cur.executemany(sql, writer_book)
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


def find_id(conn, idx, find_books=True):
    """
    Finds Book_id or Writer_id (default Book_id)
    :param conn:
    :param idx: Writer_id or Book_id
    :param find_books: input 0 for Books_id -> returns Writer_id
    :return: Book_id or Writer_id
    """
    cur = conn.cursor()
    if find_books == True:
        cur.execute(f"SELECT Book_id FROM Writers_Books WHERE Writer_id={idx}")
        rows = cur.fetchall()
    else:
        cur.execute(f"SELECT Writer_id FROM Writers_Books WHERE Book_id={idx}")
        rows = cur.fetchall()
    return rows


def select_linked(conn, idx, find_books=True):
    """
    Select books or Writers (default books) od given id
     :param conn:
     :param id: Writer_id or Book_id
     :param find_books: input 0 for Book_id -> returns Writers
    """
    x = find_id(conn, idx, find_books)
    line = []
    for i in range(len(x)):
        if find_books == True:
            y = x[i]
            book = select_book_by(conn, "Books", "id", y[0])
            line.append(book)
        else:
            y = x[i]
            writer = select_book_by(conn, "Writers", "id", y[0])
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
    conn = create_connection("library_2.db")
    drop_table(conn, "Books")
    drop_table(conn, "Writers")
    drop_table(conn, "Writers_Books")

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
            ("Tyrania nocy", "Czarna Kompania", "fantasy", "nie przeczytana", None),
            ("Bajki robotów", None, "S-F", "przeczytana", 6),
            ("Wykłady z fizyki t.1", "Wykłady z fizyki", "fizyka", "przeczytana", 9),
            (
                "Wykłady z fizyki t.2",
                "Wykłady z fizyki",
                "fizyka",
                "nie przeczytana",
                None,
            ),
            ("Dobry omen", None, "S-F", "przeczytana", 10),
            (
                "Jeszcze krótsza historia czasu",
                None,
                "popularnonaukowa",
                "nie przeczytana",
                None,
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

    """
    print(select_all(conn, "Books"))
    print(select_where(conn, "Writers_Books", Writer_id=1))
    update_table(conn, "Books", id=5, status="przeczytana", ocena=10)
    print(select_where(conn, "Books", Writer_id=1, status="przeczytana"))
    delete_record(conn, "Books", status="nie przeczytana", ocena="6")
    print(select_where(conn, "Books", Writer_id=5, status="przeczytana"))
    print(select_where(conn, "Books", Writer_id=6))
    """
    update_table(conn, "Books", id=10, status="przeczytana", ocena=8)
    print(select_all(conn, "Books"))
    # print(find_id(conn, 9, 0))
    print(select_linked(conn, 1))
    print(select_linked(conn, 9, 0))
    conn.close()
