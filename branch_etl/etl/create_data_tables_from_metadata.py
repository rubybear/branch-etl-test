import sqlite3
import sys


def create_db_tables_from_file(file):
    with open(file) as f:
        lines = f.readlines()
    print(lines)
    [cur.execute(l) for l in lines]


if __name__ == "__main__":
    con = sqlite3.connect(sys.argv[1])
    cur = con.cursor()
    create_db_tables_from_file(sys.argv[2])
