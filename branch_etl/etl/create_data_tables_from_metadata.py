import sqlite3
import sys


def create_db_tables_from_file(file):
    with open(file) as f:
        lines = f.readlines()
    try:
        [cur.execute(l) for l in lines]
    except sqlite3.OperationalError:
        print("DB already created. Delete and run again")


if __name__ == "__main__":
    con = sqlite3.connect("branch_etl/resources/database/testing_db")
    cur = con.cursor()
    create_db_tables_from_file(sys.argv[2])
    con.commit()
    con.close()
