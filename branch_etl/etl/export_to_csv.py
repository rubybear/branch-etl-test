import sqlite3
import sys

import pandas as pd


def export_db_table(table_name, path, con):
    df = pd.read_sql("SELECT * FROM " + table_name, con)
    df.to_csv(path + table_name + ".csv", index=False)


if __name__ == '__main__':
    con = sqlite3.connect("branch_etl/resources/database/testing_db")
    cur = con.cursor()

    path = "branch_etl/export/"

    tables = [table[0] for table in cur.execute("select name from sqlite_master where type='table'").fetchall()]

    [export_db_table(table_name, path, con) for table_name in tables]
