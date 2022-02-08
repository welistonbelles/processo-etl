import csv
import enum 
import mysql.connector
from functions import *
from db import *

con = mysql.connector.connect(
    host="localhost",
    database="store",
    user="root",
    password=""
)

drop_tables = [
    'sales_salesorderdetail',
    'sales_specialofferproduct',
    'production_product',
    'sales_salesorderheader',
    'sales_customer',
    'person_person',
]

if con.is_connected():
    print(f"{bcolors.WARNING}----------Iniciando o programa--------------{bcolors.ENDC}")
    print(f"🖥️  A conexão com o banco de dados foi iniciada.")

    cursor = con.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    cursor.close()
    newList = [tables[index][0] for index in range(0, len(tables))]

    for index, table in enumerate(drop_tables):
        if table in newList:
            cursor = con.cursor()
            cursor.execute(f"DROP TABLE {table}")
            cursor.close()
            print(f"{bcolors.OKGREEN}✅ A tabela {table} foi deletada.{bcolors.ENDC}")

    if newList == []:
        print(f"{bcolors.WARNING}✴️  Não foi encontrado nenhuma tabela no banco de dados.{bcolors.ENDC}")


if con.is_connected():
    con.close()
    print(f"🖥️  A conexão com o MySQL foi encerrada.")