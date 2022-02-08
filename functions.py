import csv
import mysql.connector
from datetime import datetime
import time 


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def load_and_insert_person(db, path):
    cursor = db.cursor()
    print("Tentando inserir os dados de Person no BD...")
    currentTime = time.time()
    try:
        with open(path, 'r', encoding='utf-8') as csvfiles:
            content = csv.reader(csvfiles, delimiter=';', quotechar='|')
            for index, row in enumerate(content):
                if index == 0:
                    continue
                else:
                    sql = ("INSERT INTO person_person (BusinessEntityID, PersonType, NameStyle, Title, FirstName, MiddleName, LastName, Suffix, EmailPromotion, AdditionalContactInfo, Demographics, rowguid, ModifiedDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                    values = (row[0], f"{row[1]}", row[2], f"{row[3]}", f"{row[4]}", f"{row[5]}", f"{row[6]}", f"{row[7]}", row[8], f"{row[9]}", row[10], f"{row[11]}", f"{row[12]}")
                    cursor.execute(sql, values)
            
            db.commit()
            cursor.close()
            csvfiles.close()
            return f"{bcolors.OKGREEN}✅ Operação realizada com sucesso. Duração: {round((time.time()-currentTime), 3)} segundos.{bcolors.ENDC}"
    except Exception as error:
        cursor.close()
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função load_and_insert_person(db, {path}):\n{error}\n\n")
            log.close()

        #return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"
        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"


def load_and_insert_customer(db, path):
    cursor = db.cursor()
    print("Tentando inserir os dados de Customer no BD...")
    currentTime = time.time()
    try:
        with open(path, 'r', encoding='utf-8') as csvfiles:
            content = csv.reader(csvfiles, delimiter=';', quotechar='|')
            for index, row in enumerate(content):
                if index == 0:
                    continue
                else:
                    sql = ("INSERT INTO sales_customer (CustomerID, PersonID, StoreID, TerritoryID, AccountNumber, Rowguid, ModifiedDate) VALUES (%s, %s, %s, %s, %s, %s, %s)")
                    if row[1] == 'NULL':
                        row[1] = None
                    values = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])
                    cursor.execute(sql, values)
                    
            db.commit()
            cursor.close()
            csvfiles.close()
            return f"{bcolors.OKGREEN}✅ Operação realizada com sucesso. Duração: {round((time.time()-currentTime), 3)} segundos.{bcolors.ENDC}"
    except Exception as error:
        cursor.close()
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função load_and_insert_customer(db, {path}):\n{error}\n\n")
            log.close()

        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"


def load_and_insert_order_header(db, path):
    cursor = db.cursor()
    print("Tentando inserir os dados de SalesOrderHeader no BD...")
    currentTime = time.time()
    try:
        with open(path, 'r', encoding='utf-8') as csvfiles:
            content = csv.reader(csvfiles, delimiter=';', quotechar='|')
            for index, row in enumerate(content):
                if index == 0:
                    continue
                else:
                    sql = ("INSERT INTO sales_salesorderheader (SalesOrderId, RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber, CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, CreditCardID, CreditCardApprovalCode, CurrencyRateID, SubTotal, TaxAmt, Freight, TotalDue, Comment,rowguid, ModifiedDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                    
                    values = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25])
                    cursor.execute(sql, values)
                    
            db.commit()
            cursor.close()
            csvfiles.close()
            return f"{bcolors.OKGREEN}✅ Operação realizada com sucesso. Duração: {round((time.time()-currentTime), 3)} segundos.{bcolors.ENDC}"
    except Exception as error:
        cursor.close()
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função load_and_insert_order_header(db, {path}):\n{error}\n\n")
            log.close()

        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"


def load_and_insert_product(db, path):
    cursor = db.cursor()
    print("Tentando inserir os dados de Product no BD...")
    currentTime = time.time()
    try:
        with open(path, 'r', encoding='utf-8') as csvfiles:
            content = csv.reader(csvfiles, delimiter=';', quotechar='|')
            for index, row in enumerate(content):
                if index == 0:
                    continue
                else:
                    sql = ("INSERT INTO production_product (ProductID, Name, ProductNumber, MakeFlag, FineshedGoodsFlag, Color, SafetyStockLevel, ReorderPoint, StandartCost, ListPrice, Size, SizeUnitMeasureCode, WeightUnitMeasureCode, Weight, DaysToManufacture, ProductLine, Class, Style, ProductSubcategoryID, ProductModelID, SellStartDate, SellEndDate, DiscontinuedDate, rowguid, ModifiedDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

                    values = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24])
                    cursor.execute(sql, values)
                    
            db.commit()
            cursor.close()
            csvfiles.close()
            return f"{bcolors.OKGREEN}✅ Operação realizada com sucesso. Duração: {round((time.time()-currentTime), 3)} segundos.{bcolors.ENDC}"
    except Exception as error:
        cursor.close()
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função load_and_insert_product(db, {path}):\n{error}\n\n")
            log.close()

        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"


def load_and_insert_special_offer(db, path):
    cursor = db.cursor()
    print("Tentando inserir os dados de SpecialOfferProduct no BD...")
    currentTime = time.time()
    try:
        with open(path, 'r', encoding='utf-8') as csvfiles:
            content = csv.reader(csvfiles, delimiter=';', quotechar='|')
            for index, row in enumerate(content):
                if index == 0:
                    continue
                else:
                    sql = ("INSERT INTO sales_specialofferproduct (SpecialOfferID, ProductID, rowguid, ModifiedDate) VALUES (%s, %s, %s, %s)")
                    
                    values = (row[0], row[1], row[2], row[3])
                    cursor.execute(sql, values)
                    
            db.commit()
            cursor.close()
            csvfiles.close()
            return f"{bcolors.OKGREEN}✅ Operação realizada com sucesso. Duração: {round((time.time()-currentTime), 3)} segundos.{bcolors.ENDC}"
    except Exception as error:
        cursor.close()
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função load_and_insert_special_offer(db, {path}):\n{error}\n\n")
            log.close()

        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"


def load_and_insert_sale_detail(db, path):
    cursor = db.cursor()
    print("Tentando inserir os dados de SaleOrderDetail no BD...")
    currentTime = time.time()
    try:
        with open(path, 'r', encoding='utf-8') as csvfiles:
            content = csv.reader(csvfiles, delimiter=';', quotechar='|')
            for index, row in enumerate(content):
                if index == 0:
                    continue
                else:
                    sql = ("INSERT INTO sales_salesorderdetail (SalesOrderId, SalesOrderDetailID, CarrierTrackingNumb, OrderQty, ProductID, SpecialOfferId, UnitPrice, UnitPriceDiscount, LineTotal, rowguid, ModifiedDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                    
                    values = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
                    cursor.execute(sql, values)
                    
            db.commit()
            cursor.close()
            csvfiles.close()
            return f"{bcolors.OKGREEN}✅ Operação realizada com sucesso. Duração: {round((time.time()-currentTime), 3)} segundos.{bcolors.ENDC}"
    except Exception as error:
        cursor.close()
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função load_and_insert_sale_detail(db, {path}):\n{error}\n\n")
            log.close()

        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"

