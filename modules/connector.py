import pyodbc as pyodbc
import sys

def connect():
    print('Conectando ao prod...')

    server = '19.360.30.105\NAME'
    database = 'name'
    username = 'data'
    password = '7JCP'

    try:
        connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = connection.cursor()
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)
    else:
        print('Conectado.')
    return cursor