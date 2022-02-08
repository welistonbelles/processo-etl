import csv 
import mysql.connector
from functions import *
from db import *
import time 

con = mysql.connector.connect(
    host="localhost",
    database="store",
    user="root",
    password=""
)
currentTime = time.time()

if con.is_connected():
    print(f"{bcolors.WARNING}----------Iniciando o programa--------------{bcolors.ENDC}")
    print("🖥️  A conexão com o banco de dados foi iniciada.\n")

    # Querys
    print(f"🔷 Rodando a primeira query...\n{first_query(con)}\n")
    print(f"🔷 Rodando a segunda query...\n{second_query(con)}\n")
    print(f"🔷 Rodando a terceira query...\n{third_query(con)}\n")
    print(f"🔷 Rodando a quarta query...\n{fourth_query(con)}\n")
    print(f"🔷 Rodando a quinta query...\n{fifth_query(con)}\n")


if con.is_connected():
    con.close()
    print("\n🖥️  A conexão com o MySQL foi encerrada.")
    print(f"Duração total do programa: {round((time.time()-currentTime), 3)} segundos.")
    print(f"{bcolors.WARNING}----------Programa finalizado.--------------{bcolors.ENDC}")