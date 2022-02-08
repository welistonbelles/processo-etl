import mysql.connector
from functions import *
from db import *
import time 

database_name = "store"

def main():
    try:
        con = mysql.connector.connect(
            host="localhost",
            database=database_name,
            user="root",
            password=""
        )
    except:
        print(f"{bcolors.WARNING}⚠️  Banco de dados não encontrado{bcolors.ENDC}")
        print(f"🕛 Criando banco de dados...")
        con = mysql.connector.connect(
            host="localhost",
            user="root",
            password=""
        )

        create_database(con, name=database_name)
        if con.is_connected():
            cursor = con.cursor()
            cursor.execute('SHOW DATABASES')
            for database in cursor:
                if database[0] == database_name:
                    print(f"{bcolors.OKGREEN}✅ O banco de dados {database_name} foi criado com sucesso. {bcolors.ENDC}")
            con.close()
        

        
        main()
        return

    currentTime = time.time()

    if con.is_connected():
        print(f"{bcolors.WARNING}----------Iniciando o programa--------------{bcolors.ENDC}")
        print("🖥️  A conexão com o banco de dados foi iniciada.")

        # Verifica se todas as tabelas já estão criadas, do contrário cria elas
        tables_exists(con)

        # Realiza a inserção dos dados Person no bd
        res = load_and_insert_person(con, 'dados/Person.Person.csv')
        print(res)

        # Realiza a inserção dos dados Customer no bd
        res = load_and_insert_customer(con, 'dados/Sales.Customer.csv')
        print(res)

        # Realiza a inserção dos dados SalesOrderHeader no bd
        res = load_and_insert_order_header(con, 'dados/Sales.SalesOrderHeader.csv')
        print(res)

        # Realiza a inserção dos dados Product no bd
        res = load_and_insert_product(con, 'dados/Production.Product.csv')
        print(res)

        # Realiza a inserção dos dados SpecialOfferProduct no bd
        res = load_and_insert_special_offer(con, 'dados/Sales.SpecialOfferProduct.csv')
        print(res)

        # Realiza a inserção dos dados SalesOrderDetail no bd
        res = load_and_insert_sale_detail(con, 'dados/Sales.SalesOrderDetail.csv')
        print(res)

    if con.is_connected():
        con.close()
        print("🖥️  A conexão com o MySQL foi encerrada.")
        print(f"Duração total do programa: {round((time.time()-currentTime), 3)} segundos.")
        print(f"{bcolors.WARNING}----------Programa finalizado.--------------{bcolors.ENDC}")
        print(f"Inicializando as querys de análise...")
        import analise


if __name__ == '__main__':
    main()
