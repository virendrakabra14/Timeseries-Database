# import sqlparse
import write

# A very minimal parser to tokenise. Need  to modify to specific needs of the pipe interface

"""
Expected syntax:
CREATE DATABASE <db_name>
CREATE TABLE <db_name> <table_name> (<col_name> <col_size> <col_type>)*   #Without brackets
INSERT INTO <db_name> <table_name> VALUES <timestamp> <num_char_entries> <char_entries>* <num_int_entries> <int_entries>* <num_float_entries> <float_entries>* 
UPDATE <db_name> <table_name> SET <timestamp> <num_char_entries> <char_entries>* <num_int_entries> <int_entries>* <num_float_entries> <float_entries>*
DELETE FROM <db_name> <table_name> <timestamp> <num_char_entries> <num_int_entries> <num_float_entries>
"""
def parse(sql,pipe):
    # parsed = sqlparse.parse(sql)[0]
    # tokens = [i for i in parsed.tokens if i.ttype != sqlparse.tokens.Whitespace]
    # query_type = parsed.get_type()
    query_type = sql.split(' ')
    N = len(query_type)
    if query_type[0] == 'SELECT':
        print("SELECT")
    elif query_type[0] == 'INSERT':
        try:
            db_name = query_type[2]+"\0"
            tab_name = query_type[3]+ "\0"
            timestamp = query_type[5]
            char_entries = []
            int_entries = []
            float_entries = []
            present = []
            num_char_entries = int(query_type[6])
            for i in range(7,7+num_char_entries):
                char_entries.append(query_type[i]+"\0")
                if char_entries[-1] == "NULL\0":
                    present.append(0)
                else:
                    present.append(1)
            num_int_entries = int(query_type[7+num_char_entries])
            for i in range(8+num_char_entries,8+num_char_entries+num_int_entries):
                if query_type[i] == "NULL":
                    present.append(0)
                    int_entries.append(0)
                else:
                    int_entries.append(int(query_type[i]))
                    present.append(1)

            num_float_entries = int(query_type[8+num_char_entries+num_int_entries])
            for i in range(9+num_char_entries+num_int_entries,9+num_char_entries+num_int_entries+num_float_entries):
                if query_type[i] == "NULL":
                    present.append(0)
                    float_entries.append(0)
                else:
                    float_entries.append(float(query_type[i]))
                    present.append(1)
            
            output = write.insert(db_name,tab_name,timestamp,char_entries,int_entries,float_entries,present,pipe)
        except:
            return "Invalid query"

        print("INSERT")
    elif query_type[0] == 'UPDATE':
        """
        UPDATE <db_name> <table_name> SET <timestamp> <num_char_entries> <char_entries>* <num_int_entries> <int_entries>* <num_float_entries> <float_entries>*
        """
        try:
            db_name = query_type[1]+"\0"
            tab_name = query_type[2]+ "\0"
            timestamp = query_type[4]
            char_entries = []
            int_entries = []
            float_entries = []
            present = []
            num_char_entries = int(query_type[5])
            for i in range(6, 6+num_char_entries):
                char_entries.append(query_type[i]+"\0")
                if char_entries[-1] == "NULL\0":
                    present.append(0)
                else:
                    present.append(1)
            num_int_entries = int(query_type[6+num_char_entries])
            for i in range(7+num_char_entries, 7+num_char_entries+num_int_entries):
                if int_entries[-1] == "NULL":
                    present.append(0)
                    int_entries.append(0)
                else:
                    int_entries.append(int(query_type[i]))
                    present.append(1)
            num_float_entries = int(query_type[7+num_char_entries+num_int_entries])
            for i in range(8+num_char_entries+num_int_entries, 8+num_char_entries+num_int_entries+num_float_entries):
                if float_entries[-1] == "NULL":
                    present.append(0)
                    float_entries.append(0)
                else:
                    float_entries.append(float(query_type[i]))
                    present.append(1)
            
            output = write.insert(db_name,tab_name,timestamp,char_entries,int_entries,float_entries,present,pipe)
        except:
            return "Invalid query"

        print("UPDATE")
    elif query_type[0] == 'CREATE':
        if query_type[1] == "TABLE":
            db_name = query_type[2] + "\0"
            tab_name = query_type[3] + "\0"
            names = []
            size = []
            col_type = []
            for i in range(4,N,3):
                names.append(query_type[i]+"\0")
                size.append(query_type[i+1])
                col_type.append(query_type[i+2])
            output = write.create_table(db_name,tab_name,names,size,col_type,10,pipe)       
        elif query_type[1] == "DATABASE":
            db_name = query_type[2]+"\0"
            output = write.create_db(db_name,pipe)

    elif query_type[0] == 'DELETE':
        """
        DELETE FROM <db_name> <table_name> <timestamp> <num_char_entries> <num_int_entries> <num_float_entries>
        """
        try:
            db_name = query_type[2]+"\0"
            tab_name = query_type[3]+ "\0"
            timestamp = query_type[4]
            num_char_entries = int(query_type[5])
            num_int_entries = int(query_type[6])
            num_float_entries = int(query_type[7])
            char_entries = ["NULL\0" for i in range(num_char_entries)]
            int_entries = [0 for i in range(num_int_entries)]
            float_entries = [0 for i in range(num_float_entries)]
            present = [0 for i in range(num_char_entries+num_int_entries+num_float_entries)]
            output = write.insert(db_name,tab_name,timestamp,char_entries,int_entries,float_entries,present,pipe)
        except:
            return "Invalid query"

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
    