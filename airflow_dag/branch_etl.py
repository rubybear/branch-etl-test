import json
import pickle
import pandas as pd
import requests
import sqlite3

from datetime import datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
}

with DAG(
        'branch_etl_dag',
        default_args=default_args,
        description='Branch ETL',
        schedule_interval='*/5 * * * *',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['take_home_test'],
) as dag:
    dag.doc_md = __doc__


    def extract(**kwargs):
        api_endpoint = requests.get("https://randomuser.me/api/?results=500")
        json_string = api_endpoint.text
        data = json.loads(json_string)['results']
        # save json to file
        with open('json_response.json', 'w') as outfile:
            json.dump(data, outfile)
        return data


    def transform(**kwargs):

        def get_uuid(data, i):
            return [data[i]['login']['uuid']]

        with open("json_response.json") as f:
            data = json.load(f)

        name_data = ("name", [get_uuid(data, i) + list(data[i]
                                                       ['name'].values()) for i in range(len(data))])
        dob_data = ("dob", [get_uuid(data, i) + list(data[i]
                                                     ['dob'].values()) for i in range(len(data))])
        users_data = (
            "users", [get_uuid(data, i) + [data[i][r] for r in ['gender', 'email', 'phone', 'cell', 'nat']] for i in
                      range(len(data))])
        login_data = ("login", [list(data[i]['login'].values())
                                for i in range(len(data))])
        registered_data = (
            "registered", [get_uuid(data, i) + list(data[i]['registered'].values()) for i in range(len(data))])
        id_data = ("id", [get_uuid(data, i) + list(data[i]['id'].values())
                          for i in range(len(data))])
        location = ("location", [get_uuid(data, i) + list(
            {x: data[i]['location'][x] for x in data[i]['location'] if
             x not in ["street", "coordinates", "timezone"]}.values())
                                 for i in range(len(data))])
        street = ("street", [get_uuid(data, i) + list(data[i]
                                                      ['location']['street'].values()) for i in range(len(data))])
        timezone = (
            "timezone", [get_uuid(data, i) + list(data[i]['location']['timezone'].values()) for i in range(len(data))])
        coordinates = (
            "coordinates",
            [get_uuid(data, i) + list(data[i]['location']['coordinates'].values()) for i in range(len(data))])
        picture = ("picture", [get_uuid(data, i) + list(data[i]
                                                        ['picture'].values()) for i in range(len(data))])

        tables = [name_data, dob_data, users_data, login_data, registered_data, id_data, location, street, timezone,
                  coordinates, picture]

        # Will pickle and read the table of tables for later use
        # If this was production size data I'd put it in some staging directory and have
        # a process to serialize/deserialize or write each single table as csv first
        with open("transformed_json_tables", "wb") as f:
            pickle.dump(tables, f)


    def load(**kwargs):

        def create_db_tables_from_file():
            # This needs to be an absolute path in order to work
            with open("branch_etl/resources/database_tables.txt") as f:
                lines = f.readlines()
            print(lines)
            [cur.execute(l) for l in lines]

        def insert_values_into_table(cur, table):
            table_name = table[0]
            table_values = table[1]
            insert_sql = "INSERT INTO " + table_name + \
                         " VALUES(" + ",".join(["?" for i in table_values[0]]) + ")"
            print(insert_sql, len(table_values))
            cur.executemany(insert_sql,
                            table_values)

        con = sqlite3.connect("branch_test_db.db")
        cur = con.cursor()

        try:
            create_db_tables_from_file()
            tables = pd.read_pickle("transformed_json_tables")
            [insert_values_into_table(cur, table) for table in tables]
            con.commit()
        except sqlite3.OperationalError:
            print("database already exists")
            con.close()


    def export_csv(**kwargs):
        import os

        outdir = './csv'
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        con = sqlite3.connect("branch_test_db.db")
        cur = con.cursor()

        def export_db_table(table_name, outdir, con):
            df = pd.read_sql("SELECT * FROM " + table_name, con)
            path = outdir + "/" + table_name + ".csv"
            df.to_csv(path, index=False)
            print(path)

        tables = [table[0] for table in cur.execute("select name from sqlite_master where type='table'").fetchall()]
        print(tables)
        [export_db_table(table_name, outdir, con) for table_name in tables]


    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    The extract task reads JSON data from the API endpoint and writes it to a json file.
    This is to make it available for the next part of the process
    """
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    The transform task reads the json file and transforms it into tables and dumps them into a
    pickle object to be easily digestible in the nexxt step
    """
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    The load task also takes care of setting up this test database. It creates the database if it doesn't exist and 
    creates tables from a metadata file "database_tables.txt"
    Then it inserts the tables created in the transform step into the database
    """
    )

    export_csv_task = PythonOperator(
        task_id='export',
        python_callable=export_csv,
    )
    export_csv_task.doc_md = dedent(
        """\
    #### Export task
    Finally, the export step will read all tables in database and export them to CSV using pandas.to_csv
    """
    )

    extract_task >> transform_task >> load_task >> export_csv_task
