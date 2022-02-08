import csv 
import mysql.connector
from datetime import datetime
from functions import bcolors

table_querys = {
    'person_person': "CREATE TABLE person_person (`BusinessEntityID` int(11) NOT NULL, `PersonType` varchar(2), `NameStyle` int(11) NOT NULL, `Title` varchar(100) NOT NULL, `FirstName` varchar(50) NOT NULL, `MiddleName` varchar(50) NOT NULL, `LastName` varchar(50), `Suffix` varchar(10) DEFAULT NULL, `EmailPromotion` int(11) NOT NULL, `AdditionalContactInfo` varchar(400) DEFAULT NULL, `Demographics` varchar(300) DEFAULT NULL, `rowguid` varchar(100) NOT NULL, `ModifiedDate` datetime(3) NOT NULL, PRIMARY KEY (`BusinessEntityID`))",
    
    'sales_customer': "CREATE TABLE sales_customer (`CustomerID` int(11) NOT NULL, `PersonID` int(11), `StoreID` int(11) DEFAULT NULL, `TerritoryID` int(11) NOT NULL, `AccountNumber` varchar(100) NOT NULL, `Rowguid` varchar(100) NOT NULL, `ModifiedDate` datetime(3) NOT NULL, PRIMARY KEY (`CustomerID`), CONSTRAINT FK_Customer_Person_PersonID FOREIGN KEY (`PersonID`) REFERENCES `person_person` (`BusinessEntityID`) ON DELETE CASCADE ON UPDATE CASCADE)",

    'sales_salesorderheader': "CREATE TABLE `sales_salesorderheader` (`SalesOrderId` int(11) NOT NULL, `RevisionNumber` int(11) NOT NULL, `OrderDate` datetime NOT NULL, `DueDate` datetime NOT NULL, `ShipDate` datetime NOT NULL, `Status` int(11) NOT NULL, `OnlineOrderFlag` int(11) NOT NULL, `SalesOrderNumber` varchar(100) NOT NULL, `PurchaseOrderNumber` varchar(100) DEFAULT NULL, `AccountNumber` varchar(100) NOT NULL, `CustomerID` int(11) NOT NULL, `SalesPersonID` int(11) DEFAULT NULL, `TerritoryID` int(11) NOT NULL, `BillToAddressID` int(11) NOT NULL, `ShipToAddressID` int(11) NOT NULL, `ShipMethodID` int(11) NOT NULL, `CreditCardID` int(11) NOT NULL, `CreditCardApprovalCode` varchar(100) NOT NULL, `CurrencyRateID` int(11) DEFAULT NULL, `SubTotal` float NOT NULL, `TaxAmt` float NOT NULL, `Freight` float NOT NULL, `TotalDue` float NOT NULL, `Comment` varchar(100) DEFAULT NULL, `rowguid` varchar(100) NOT NULL, `ModifiedDate` datetime NOT NULL, PRIMARY KEY (`SalesOrderId`), KEY `FK_SalesOrderHeader_Customer_CustomerID` (`CustomerID`), CONSTRAINT `FK_SalesOrderHeader_Customer_CustomerID` FOREIGN KEY (`CustomerID`) REFERENCES `sales_customer` (`CustomerID`) ON DELETE CASCADE ON UPDATE CASCADE)",

    'production_product': "CREATE TABLE production_product (`ProductID` int(11) NOT NULL, `Name` varchar(100) NOT NULL, `ProductNumber` varchar(100) NOT NULL, `MakeFlag` int(11) NOT NULL, `FineshedGoodsFlag` int(11) NOT NULL, `Color` varchar(100) DEFAULT NULL, `SafetyStockLevel` int(11) NOT NULL, `ReorderPoint` int(11) NOT NULL, `StandartCost` float NOT NULL, `ListPrice` float NOT NULL, `Size` varchar(100) DEFAULT NULL, `SizeUnitMeasureCode` varchar(100) DEFAULT NULL, `WeightUnitMeasureCode` varchar(100) DEFAULT NULL, `Weight` float DEFAULT NULL, `DaysToManufacture` int(11) NOT NULL, `ProductLine` varchar(10) DEFAULT NULL, `Class` varchar(10) DEFAULT NULL,`Style` varchar(10) DEFAULT NULL, `ProductSubcategoryID` int(11) DEFAULT NULL, `ProductModelID` int(11) DEFAULT NULL, `SellStartDate` datetime NOT NULL, `SellEndDate` datetime DEFAULT NULL, `DiscontinuedDate` datetime DEFAULT NULL, `rowguid` varchar(100) NOT NULL, `ModifiedDate` datetime NOT NULL, PRIMARY KEY (`ProductID`))",

    'sales_specialofferproduct': "CREATE TABLE `sales_specialofferproduct` ( `SpecialOfferID` int(11) NOT NULL, `ProductID` int(11) NOT NULL, `rowguid` varchar(100) NOT NULL, `ModifiedDate` datetime NOT NULL, PRIMARY KEY (`SpecialOfferID`,`ProductID`), KEY `FK_SpecialOfferProduct_Product_ProductID` (`ProductID`), CONSTRAINT `FK_SpecialOfferProduct_Product_ProductID` FOREIGN KEY (`ProductID`) REFERENCES `production_product` (`ProductID`) ON DELETE CASCADE ON UPDATE CASCADE)",

    'sales_salesorderdetail': "CREATE TABLE `sales_salesorderdetail` (`SalesOrderId` int(11) NOT NULL, `SalesOrderDetailID` int(11) NOT NULL, `CarrierTrackingNumb` varchar(50) DEFAULT NULL, `ProductID` int(11) NOT NULL, `SpecialOfferId` int(11) DEFAULT NULL, `UnitPrice` float NOT NULL, `UnitPriceDiscount` float NOT NULL, `LineTotal` float NOT NULL, `rowguid` varchar(100) NOT NULL, `ModifiedDate` datetime NOT NULL, `OrderQty` int(11) NOT NULL, PRIMARY KEY (`SalesOrderDetailID`), KEY `FK_SalesOrderDetail_SpecialOfferProduct_SpecialOfferIDProductID` (`SpecialOfferId`,`ProductID`), KEY `FK_SalesOrderDetail_SalesOrderHeader_SalesOrderID` (`SalesOrderId`), CONSTRAINT `FK_SalesOrderDetail_SalesOrderHeader_SalesOrderID` FOREIGN KEY (`SalesOrderId`) REFERENCES `sales_salesorderheader` (`SalesOrderId`) ON DELETE CASCADE ON UPDATE CASCADE, CONSTRAINT `FK_SalesOrderDetail_SpecialOfferProduct_SpecialOfferIDProductID` FOREIGN KEY (`SpecialOfferId`, `ProductID`) REFERENCES `sales_specialofferproduct` (`SpecialOfferID`, `ProductID`) ON DELETE CASCADE ON UPDATE CASCADE)"
}


def create_database(db, name):
    cursor = db.cursor()
    cursor.execute(f"CREATE DATABASE {name}")


def tables_exists(db):
    cursor = db.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    required_tables = {
        'person_person': False,
        'sales_customer': False,
        'sales_salesorderheader': False,
        'production_product': False,
        'sales_specialofferproduct': False,
        'sales_salesorderdetail': False
    }

    for table in tables:
        td = table[0]
        if td in required_tables:
            required_tables[td] = True

    for table in required_tables:
        if required_tables[table] is False:
            create_table(db, table, table_querys[table])

def create_table(db, table_name, query):
    try:
        cursor = db.cursor()
        cursor.execute(query)
        db.commit()
        cursor.close()
        print(f"{bcolors.OKGREEN}✅ A tabela {table_name} foi criada{bcolors.ENDC}")
    except Exception as error:
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função create_table(db, {table_name}):\n{error}\n\n")
            log.close()
        print(f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}")


def first_query(db):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT SUM(COUNT(SalesOrderId)) OVER() AS Sum FROM sales_salesorderdetail GROUP BY SalesOrderId HAVING COUNT(SalesOrderId) > 2 LIMIT 1;")
        result = cursor.fetchone()[0]
        cursor.close()
        with open('report.txt', 'w', encoding='utf-8') as report:
            report.write(f"✅ Resultado da primeira query: \nO número de linhas é: {result}\n\n")
            report.close()
        return f"{bcolors.OKGREEN}✅ O número de linhas é: {bcolors.OKCYAN}{bcolors.BOLD}{result}.{bcolors.ENDC}"
    except Exception as error:
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função first_query(db):\n{error}\n\n")
            log.close()
    return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"


def second_query(db):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT Product.Name FROM sales_salesorderdetail AS Detail INNER JOIN production_product AS Product ON Detail.ProductID = Product.ProductID GROUP BY Product.DaysToManufacture ORDER BY SUM(Detail.OrderQty) desc limit 3;")
        result = cursor.fetchall()
        cursor.close()
        txt = ""
        for product in result:
            txt += f"\n{product[0]}"

        with open('report.txt', 'a', encoding='utf-8') as report:
            report.write(f"✅ Resultado da segunda query: \nOs 3 produtos mais vendidos(pela soma de OrderQty) agrupados pelo campo DaysToManufacture:{txt}\n\n")
            report.close()

        return f"{bcolors.OKGREEN}✅ Os 3 produtos mais vendidos(pela soma de OrderQty) agrupados pelo campo DaysToManufacture: {bcolors.ENDC}{txt}"
    except Exception as error:
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função first_query(db):\n{error}\n\n")
            log.close()
        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"


def third_query(db):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT Person.FirstName, Person.LastName, COUNT(SalesOrderId) AS cnt FROM sales_salesorderheader AS Sales INNER JOIN sales_customer AS Customer ON Customer.CustomerID  =  Sales.CustomerID LEFT JOIN person_person AS Person ON Customer.PersonID = Person.BusinessEntityID GROUP BY Sales.CustomerID ORDER BY cnt DESC LIMIT 5")
        result = cursor.fetchall()
        txt = "Nome,  Contagem de Pedidos\n"
        for row in result:
            txt += f"{row[0]} {row[1]}, {row[2]}\n"

        with open('report.txt', 'a', encoding='utf-8') as report:
            report.write(f"✅ Resultado da terceira query: \nListagem dos nomes dos clientes juntamente da contagem de pedidos efetuados por cada um (Limitado a 5 resultados):\n{txt}\n\n")
            report.close()

        cursor.close()
        return f"{bcolors.OKGREEN}✅ Listagem dos nomes dos clientes e contagem dos pedidos (Limitado a 5 resultados): {bcolors.ENDC}\n{txt}"

    except Exception as error:
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função first_query(db):\n{error}\n\n")
            log.close()
        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"

def fourth_query(db):
    try:
        cursor = db.cursor()
        cursor.execute("select Product.ProductID, Product.Name, Sum(Detail.OrderQty) as Sum, Header.OrderDate from production_product as Product inner join sales_salesorderdetail as Detail on Product.ProductID = Detail.ProductID inner join sales_salesorderheader as Header on Detail.SalesOrderId = Header.SalesOrderId group by Detail.ProductID order by Sum(Detail.OrderQty) desc limit 5;")
        result = cursor.fetchall()
        txt = "ProductID; Name; OrderQty; OrderDate"
        for row in result:
            txt += f"\n{row[0]}; {row[1]}; {row[2]}; {row[3]}"

        with open('report.txt', 'a', encoding='utf-8') as report:
            report.write(f"✅ Resultado da quarta query: \nSoma total de produtos(OrderQty), juntamente do ProductID e OrderDate (Limitado a 5 resultados):\n{txt}\n\n")
            report.close()

        cursor.close()
        return f"{bcolors.OKGREEN}✅ Soma total de produtos(OrderQty), juntamente do ProductID e OrderDate (Limitado a 5 resultados): {bcolors.ENDC}\n{txt}"

    except Exception as error:
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função first_query(db):\n{error}\n\n")
            log.close()
        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"

def fifth_query(db):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT Header.SalesOrderId, Header.OrderDate, Header.TotalDue FROM sales_salesorderheader AS Header WHERE MONTH(Header.OrderDate) = 9 AND Header.TotalDue >= 1000 ORDER BY Header.TotalDue DESC LIMIT 5")
        result = cursor.fetchall()
        txt = "SalesOrderId; OrderDate; TotalDue"
        for row in result:
            txt += f"\n{row[0]}; {row[1]}; {row[2]}"

        with open('report.txt', 'a', encoding='utf-8') as report:
            report.write(f"✅ Resultado da quinta query: \nOrdens de vendas limitadas ao mês de setembro com a dívido superior a 1.000 (Limitado a 5 resultados):\n{txt}\n\n")
            report.close()

        cursor.close()
        return f"{bcolors.OKGREEN}✅ Ordens de vendas limitadas ao mês de setembro com a dívido superior a 1.000 (Limitado a 5 resultados): {bcolors.ENDC}\n{txt}"

    except Exception as error:
        with open('logs.txt', 'a', encoding='utf-8') as log:
            log.write(f"{datetime.now()} Ocorreu um erro ao chamar a função first_query(db):\n{error}\n\n")
            log.close()
        return f"{bcolors.FAIL}⛔ A operação falhou.{bcolors.ENDC}"