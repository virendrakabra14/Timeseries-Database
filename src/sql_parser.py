import sqlparse

# A very minimal parser to tokenise. Need  to modify to specific needs of the pipe interface
def parse(sql):
    parsed = sqlparse.parse(sql)[0]
    tokens = [i for i in parsed.tokens if i.ttype != sqlparse.tokens.Whitespace]
    query_type = parsed.get_type()
    if query_type == 'SELECT':
        print("SELECT")
    elif query_type == 'INSERT':
        print("INSERT")
    elif query_type == 'UPDATE':
        print("UPDATE")
    elif query_type == 'CREATE':
        if tokens[1].value.upper() == "TABLE":
            print("CREATE TABLE")
        elif tokens[1].value.upper() == "DATABASE":
            print("CREATE DATABASE")

    elif query_type == 'DELETE':
        print("DELETE")

    for i in tokens:
        if i.ttype == sqlparse.tokens.Punctuation:
            tokens.remove(i)

    return tokens