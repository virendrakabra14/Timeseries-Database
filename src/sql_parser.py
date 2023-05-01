import sqlparse
import write

# A very minimal parser to tokenise. Need  to modify to specific needs of the pipe interface
def parse(sql,pipe):
    # parsed = sqlparse.parse(sql)[0]
    # tokens = [i for i in parsed.tokens if i.ttype != sqlparse.tokens.Whitespace]
    # query_type = parsed.get_type()
    query_type = sql.split(' ')
    N = len(query_type)
    if query_type[0] == 'SELECT':
        print("SELECT")
    elif query_type[0] == 'INSERT':
        print("INSERT")
    elif query_type[0] == 'UPDATE':
        print("UPDATE")
    elif query_type[0] == 'CREATE':
        if query_type[1] == "TABLE":
            db_name = query_type[2]
            tab_name = query_type[3]
            names = []
            size = []
            col_type = []
            for i in range(4,N,3):
                names.append(query_type[i])
                size.append(query_type[i+1])
                col_type.append(query_type[i+2])
            output = write.create_table(db_name,tab_name,names,size,col_type,10,pipe)       
        elif query_type[1] == "DATABASE":
            db_name = query_type[2]
            output = write.create_db(db_name,pipe)

    elif query_type[0] == 'DELETE':
        print("DELETE")

    # for i in tokens:
    #     if i.ttype == sqlparse.tokens.Punctuation:
    #         tokens.remove(i)

    return output

if __name__ == "__main__" :

    pipe = write.init_pipe()
    parse("CREATE DATABASE test_db",pipe)
    names = ["char field1", "char field2", "int field", "foat field1", "float field2"]
    size = [5,8,0,0,0] #Only used for char fields, rest can contain any value but need to be filled
    col_type = [0,0,1,2,2] # 0-> char* field, 1-> int, 2-> float
    #create_table("test_db\0", "tab1\0", names, col_type, size, 10)

    query_sql = "CREATE TABLE test_db tab1 " + len(names) + " "
    for  i in range(len(names)):
        query_sql += names[i] + " " + str(size[i]) + " " + str(col_type[i]) + " " 
    query_sql += ";"
    parse(query_sql,pipe)
    