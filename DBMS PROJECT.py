import psycopg2
from tabulate import tabulate
from decimal import Decimal

con = psycopg2.connect(
         host="localhost",
         database="deepi",
         user="postgres",
         password="deepi1",
         port=5433)



#For isolation: SERIALIZABLE
con.set_isolation_level(3)
#For atomicity
con.autocommit = False

try:
    cur = con.cursor()
    # QUERY
    # drop the table if already present in postgre
    cur.execute("DROP TABLE IF EXISTS Stock CASCADE")
    cur.execute("DROP TABLE IF EXISTS Product CASCADE")
    cur.execute("DROP TABLE IF EXISTS Depot CASCADE")
     #create product table with prod_id, pname, price
    create_Product = """CREATE TABLE IF NOT EXISTS Product(
                            prod_id varchar(4),
                            pname varchar(10),
                            price decimal CHECK (price > 0))"""
    cur.execute(create_Product)
    # define the ALTER TABLE statement to add a Primary key constraint
    alter_Product = """ALTER TABLE Product ADD CONSTRAINT pk_Product PRIMARY KEY (prod_id)"""
    cur.execute(alter_Product)
    #create depot table
    create_Depot= """ CREATE TABLE IF NOT EXISTS Depot (
                            dep_id varchar(4),
                            addr varchar(15),
                            volume decimal )"""
    cur.execute(create_Depot)
    # define the ALTER TABLE statement to add a Primary key constraint
    alter_Depot = """ALTER TABLE Depot ADD CONSTRAINT pk_Depot PRIMARY KEY (dep_id)"""
    cur.execute(alter_Depot)
    create_Stock= """ CREATE TABLE IF NOT EXISTS Stock (
                            prod_id varchar(4) REFERENCES Product (prod_id),
                            dep_id varchar(4) REFERENCES Depot (dep_id),
                            quantity integer)"""
    cur.execute(create_Stock)
    cur.execute("ALTER TABLE Stock ADD CONSTRAINT pk_Stock PRIMARY KEY (prod_id, dep_id)")
    cur.execute("ALTER TABLE Stock ADD CONSTRAINT fk_Stock_Product FOREIGN KEY (prod_id) REFERENCES Product (prod_id) ON DELETE CASCADE on update cascade")
    cur.execute("ALTER TABLE Stock ADD CONSTRAINT fk_Stock_Depot FOREIGN KEY (dep_id) REFERENCES Depot (dep_id) ON DELETE CASCADE on update cascade")
    cur.execute("ALTER TABLE Stock DROP CONSTRAINT stock_dep_id_fkey")
     # Insert data into Product table
    cur.execute("INSERT INTO Product (prod_id , pname, price) VALUES ('p1', 'tape', 2.5)")
    cur.execute("INSERT INTO Product (prod_id , pname, price) VALUES ('p2', 'tv', 250)")
    cur.execute("INSERT INTO Product (prod_id , pname, price) VALUES ('p3', 'ver', 80)")
     # Insert data into Depot table
    cur.execute("INSERT INTO Depot (dep_id, addr, volume) VALUES ('d1', 'New York', 9000)")
    cur.execute("INSERT INTO Depot (dep_id, addr, volume) VALUES ('d2', 'Syracuse', 6000)")
    cur.execute("INSERT INTO Depot (dep_id, addr, volume) VALUES ('d4', 'New York', 2000)")
    #insert data into stock table
    cur.execute("INSERT INTO Stock (prod_id, dep_id, quantity) VALUES ('p1', 'd1', 1000)")
    cur.execute("INSERT INTO Stock (prod_id, dep_id, quantity) VALUES ('p1', 'd2', -100)")
    cur.execute("INSERT INTO Stock (prod_id, dep_id, quantity) VALUES ('p1', 'd4', 1200)")
    cur.execute("INSERT INTO Stock (prod_id, dep_id, quantity) VALUES ('p3', 'd1', 3000)")
    cur.execute("INSERT INTO Stock (prod_id, dep_id, quantity) VALUES ('p3', 'd4', 2000)")
    cur.execute("INSERT INTO Stock (prod_id, dep_id, quantity) VALUES ('p2', 'd4', 1500)")
    cur.execute("INSERT INTO Stock (prod_id, dep_id, quantity) VALUES ('p2', 'd1', -400)")
    cur.execute("INSERT INTO Stock (prod_id, dep_id, quantity) VALUES ('p2', 'd2', 2000)")
    # Select all rows from the Product table and print them
    cur.execute("SELECT * FROM Product")
    rows = cur.fetchall()
    print("Product Table:")
    print(tabulate(rows, headers=["prod_id", "pname", "price"], tablefmt="psql"))

    # Select all rows from the Depot table and print them
    cur.execute("SELECT * FROM Depot")
    rows = cur.fetchall()
    print("Depot Table:")
    print(tabulate(rows, headers=["dep_id", "addr", "volume"], tablefmt="psql"))

     # Select all rows from the stock table and print them
    cur.execute("SELECT * FROM Stock")
    rows = cur.fetchall()
    print("Stock Table:")
    print(tabulate(rows, headers=["prod_id", "dep_id", "quantity"], tablefmt="psql"))
    
    #update d1 to dd1
    cur.execute("UPDATE Depot SET dep_id = 'dd1' WHERE dep_id = 'd1'")

    #To depot d1 is deleted from Depot and Stock
    cur.execute("DELETE FROM depot WHERE dep_id='d1'")

    # Insert the depot record
    cur.execute("INSERT INTO Depot (dep_id, addr, volume) VALUES ('d100','Chicago', 100)")
    #insert the stock record
    cur.execute("INSERT INTO stock (prod_id ,dep_id, quantity) VALUES ('p1','d100', 100)")

    print("\n")
    print("TRANSACTION COMPLETED SUCESSFULLY")

    # Select all rows from the Depot table and print them
    cur.execute("SELECT * FROM Depot")
    rows = cur.fetchall()
    print("Depot Table:")
    print(tabulate(rows, headers=["dep_id", "addr", "volume"], tablefmt="psql"))


    # Select all rows from the stock table and print them
    cur.execute("SELECT * FROM Stock")
    rows = cur.fetchall()
    print("Stock Table:")
    print(tabulate(rows, headers=["prod_id", "dep_id", "quantity"], tablefmt="psql"))

except (Exception, psycopg2.DatabaseError) as err:
    print(err)
    print("Transactions could not be completed so database will be rolled back before start of transactions")
    con.rollback()
finally:
    if con:
        con.commit()
        cur.close()
        con.close()
        print("PostgreSQL connection is now closed")