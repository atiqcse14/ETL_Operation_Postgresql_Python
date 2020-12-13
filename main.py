import psycopg2

con = psycopg2.connect(database="employee1", user="postgres", password="1234", host="127.0.0.1", port="5432")

con2 = psycopg2.connect(database="employee2", user="postgres", password="1234", host="127.0.0.1", port="5432")


print("Database connected successfully")

cur = con.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
db_tables1 = cur.fetchall()
print(db_tables1)

cur2 = con2.cursor()
cur2.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
db_tables2 = cur2.fetchall()
print(db_tables2)

# query = ("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =  '"+ db_tables1[3][0] +"';")
# cur.execute(query)
# columns = cur.fetchall()
# print(columns)

tableNames = []
for tablesName in db_tables1:
    tableNames.append(tablesName[0])
print(tableNames)

for tableName in tableNames:
    cur.execute("SELECT count(*) FROM %s" %tableName)
    row_count = cur.fetchone()[0]
    print("No. of row in 1st DB ["+tableName+"]: %s" %row_count)

    if row_count == 0:
        continue;
    else:
        cur2.execute("SELECT max(id) FROM %s" %tableName)
        max_row = cur2.fetchone()[0]
        print("No. of row in 2nd DB: %s" %max_row)

        if row_count > max_row and max_row != None:
            query = "SELECT * FROM "+tableName+" WHERE id > "+str(max_row);
            #print(query)
            cur.execute(query)
            data = cur.fetchall()
            print(data)
            col = ("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =  '" + tableName + "';")
            cur.execute(col)
            columns = cur.fetchall()

            columnNames = []
            for columnName in columns:
                if columnName[0] == 'id':
                    continue;
                else:
                    columnNames.append(columnName[0])
            print(columnNames)
            for j in range(len(data)):
                value = str(data[j])[4:-1]
                print(value)
                insert_statement = "INSERT INTO "+tableName+" (%s) VALUES ( %s);" %(','.join(columnNames), value)
                print(insert_statement)
                cur2.execute(insert_statement)


con2.commit()

