
import pyodbc 
import datetime

def getFieldsOfTable(server, database, username, password, tablename): 
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    strSQL  = "DECLARE @tabla varchar(max)" + "\n"  
    strSQL += "SET @tabla = '" + tablename + "'" + "\n"   
    strSQL += "SELECT C.COLUMN_NAME"  + "\n"  
    strSQL += "     , CASE"  + "\n"  
    strSQL += "             WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN DATA_TYPE"  + "\n"  
    strSQL += "             ELSE DATA_TYPE+'('+CONVERT(VARCHAR,CHARACTER_MAXIMUM_LENGTH)+')'"  + "\n"  
    strSQL += "       END"  + "\n"  
    strSQL += "     , CASE WHEN TC.CONSTRAINT_TYPE IS NOT NULL THEN 'S' ELSE 'N' END "  + "\n"  
    strSQL += "  FROM INFORMATION_SCHEMA.COLUMNS C "  + "\n"  
    strSQL += "       LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE P "  + "\n"  
    strSQL += "       ON C.TABLE_CATALOG = P.TABLE_CATALOG"  + "\n"  
    strSQL += "       AND C.TABLE_SCHEMA = P.TABLE_SCHEMA"  + "\n"  
    strSQL += "       AND C.TABLE_NAME = P.TABLE_NAME"  + "\n"  
    strSQL += "       AND C.COLUMN_NAME = P.COLUMN_NAME"  + "\n"  
    strSQL += "	      LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC"  + "\n"
    strSQL += "	      ON C.TABLE_CATALOG = TC.TABLE_CATALOG "  + "\n"       
    strSQL += "       AND C.TABLE_SCHEMA = TC.TABLE_SCHEMA"  + "\n"       
    strSQL += "       AND C.TABLE_NAME = TC.TABLE_NAME"  + "\n"   
    strSQL += "       AND P.CONSTRAINT_NAME = TC.CONSTRAINT_NAME"  + "\n"           
    strSQL += "	      AND TC.CONSTRAINT_TYPE = 'Primary Key'"  + "\n"
    strSQL += " WHERE C.TABLE_NAME = rtrim(@tabla)"
    #print(strSQL)
    cursor = cnxn.cursor()
    cursor.execute(strSQL) 
    row = cursor.fetchone() 
    listed = []
    while row: 
        listed.append([row[0], row[1], row[2]])
        #print(row[0], row[1], row[2])
        row = cursor.fetchone()
    return listed


def getTablesForExtract():
    f = open('tablas.txt')
    lines = f.readlines()
    tables = []
    for line in lines:
        
        line = line.replace('\n','')
        # Es configuración de base de datos
        if ':' in line:
            config_conexion=[]                                             
            config_conexion = line.split(':')
        # Es tabla a extraer
        if ':' not in line:
            tmp_conex=[]
            tmp_conex.extend(config_conexion)
            tmp_conex.append(line)                                         
            tables.append(tmp_conex)
            #print(tmp_conex)
    return tables


def getScriptCreateTable(table, fields):
    sql = "CREATE TABLE " + table[4] + " ( \n"
    for i, field in enumerate(fields, start=1):
        sql += '   ' + field[0] + ' ' + field[1]
        if field[2] == 'S':
            sql += ' NOT NULL'
        else:
            sql += ' NULL'
        if i: 
            if i<len(fields):
                sql += (',')
        sql += '\n'
    sql += ')\n'
    sql += 'GO'
    return sql


def getScriptCreateTableType(table, fields):
    subfijo_type = ''
    sql = "CREATE TYPE " + table[4] + subfijo_type + " AS TABLE ( \n"
    for i, field in enumerate(fields, start=1):
        sql += '   ' + field[0] + ' ' + field[1] 
        if field[2] == 'S':
            sql += ' NOT NULL'
        else:
            sql += ' NULL'
        if i: 
            if i<len(fields):
                sql += (',')
        sql += '\n'
    sql += ')\n'
    sql += 'GO'
    return sql


def getScriptCreateSP(table, fields):
    sql = "CREATE PROCEDURE sp_upsert_" + table[4] + ' @' + table[4] + ' ' + table[4] + "_type READONLY \n"
    sql += 'AS \n'
    sql += 'BEGIN \n'
    sql += '  MERGE ' + table[4] + ' AS ' + 'tg \n'
    sql += '  USING @' + table[4] + ' AS ' + 'src \n'
    
    q_pk=0
    for field in fields:
        if field[2] == 'S':
            q_pk += 1

    if q_pk:
        n_pk=0
        sql += '  ON ('
        for i, field in enumerate(fields, start=1):
            if field[2]=='S' and n_pk==0: 
                n_pk+=1
                sql += ' tg.' + field[0] + ' = src.' + field[0]
            elif field[2]=='S' and n_pk>0:
                sql += '\n       AND tg.' + field[0] + ' = src.' + field[0]
        sql += ' ) \n' if q_pk > 0 else '\n'
    
    sql += '  WHEN MATCHED THEN \n'
    sql += '      UPDATE SET \n'
    for i, field in enumerate(fields, start=1):
        if field[2]=='N':
            sql += '      ' + field[0] + ' = src.' + field[0] 
            if i<len(fields):
                sql += (',')    
            sql += '\n'
    
    sql += '  WHEN NOT MATCHED THEN \n'
    sql += '      INSERT ( \n'    
    for i, field in enumerate(fields, start=1):
        sql += '         ' + field[0] 
        if i: 
            if i<len(fields):
                sql += (',')
        sql += '\n'
    sql += '      )\n'
    sql += '      VALUES ( \n'
    for i, field in enumerate(fields, start=1):
        sql += '         src.' + field[0] 
        if i: 
            if i<len(fields):
                sql += (',')
        sql += '\n'
    sql += '      )\n'      

    sql += '  WHEN NOT MATCHED BY SOURCE THEN DELETE \n'
    sql += 'END \n'
    sql += 'GO \n'
 
    return sql


def setPlainTexto(name, sql):
    ext = '.sql'
    dt = str(datetime.datetime.now())
    dt = dt.replace('-',"")
    dt = dt.replace(' ',"")
    dt = dt.replace(':',"")
    dt = dt.replace('.',"")
    try:
        file = open(name + ' ' + dt + ext, 'w')
        file.write(sql) 
        return 1
    except:
        return 0

def main():
    tables = getTablesForExtract()
    for table in tables:
        server = table[0]
        database = table[1]
        username = table[2]
        password = table[3]
        tablename = table[4]
        fields = getFieldsOfTable(server, database, username, password, tablename)
        sqlCreateTable = getScriptCreateTable(table, fields)
        sqlCreateTableType = getScriptCreateTableType(table, fields)
        sqlScriptCreateSP = getScriptCreateSP(table, fields)
        createTable = setPlainTexto('TABLA '+tablename,sqlCreateTable)
        createTableType = setPlainTexto('TYPE '+tablename,sqlCreateTableType)
        createSP = setPlainTexto('SP '+tablename,sqlScriptCreateSP)

        if not createTable : print('Script Creación de Tablas OK...')
        if not createTableType : print('Script Creación de Type Tablas OK...')
        if not createSP : print('Script Creación de SP OK...')
            
main()    
