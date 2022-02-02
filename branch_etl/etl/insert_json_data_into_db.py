import json
import sys

import requests
import sqlite3


def get_uuid(data, i):
    return [data[i]['login']['uuid']]


def get_data(url):
    api_endpoint = requests.get(url)
    json_string = api_endpoint.text
    data = json.loads(json_string)['results']
    with open('../../json_data.json', 'w') as outfile:
        json.dump(data, outfile)
    return data


def insert_values_into_table(cur, table):
    table_name = table[0]
    table_values = table[1]
    insert_sql = "INSERT INTO " + table_name + " VALUES(" + ",".join(["?" for i in table_values[0]]) + ")"
    print(insert_sql, len(table_values))
    cur.executemany(insert_sql,
                    table_values)


if __name__ == '__main__':
    data = get_data(sys.argv[1])

    name_data = ("name", [get_uuid(data, i) + list(data[i]['name'].values()) for i in range(len(data))])
    dob_data = ("dob", [get_uuid(data, i) + list(data[i]['dob'].values()) for i in range(len(data))])
    users_data = (
    "users", [get_uuid(data, i) + [data[i][r] for r in ['gender', 'email', 'phone', 'cell', 'nat']] for i in
              range(len(data))])
    login_data = ("login", [list(data[i]['login'].values()) for i in range(len(data))])
    registered_data = (
    "registered", [get_uuid(data, i) + list(data[i]['registered'].values()) for i in range(len(data))])
    id_data = ("id", [get_uuid(data, i) + list(data[i]['id'].values()) for i in range(len(data))])
    location = ("location", [get_uuid(data, i) + list(
        {x: data[i]['location'][x] for x in data[i]['location'] if
         x not in ["street", "coordinates", "timezone"]}.values())
                             for i in range(len(data))])
    street = ("street", [get_uuid(data, i) + list(data[i]['location']['street'].values()) for i in range(len(data))])
    timezone = (
    "timezone", [get_uuid(data, i) + list(data[i]['location']['timezone'].values()) for i in range(len(data))])
    coordinates = (
    "coordinates", [get_uuid(data, i) + list(data[i]['location']['coordinates'].values()) for i in range(len(data))])
    picture = ("picture", [get_uuid(data, i) + list(data[i]['picture'].values()) for i in range(len(data))])

    tables = [name_data, dob_data, users_data, login_data, registered_data, id_data, location, street, timezone,
              coordinates, picture]
    print(tables)

    con = sqlite3.connect(sys.argv[2])
    cur = con.cursor()

    [insert_values_into_table(cur, table) for table in tables]
    con.commit()
