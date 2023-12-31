#Run in the same directory as compiled main.cpp

import os
import struct


def init_pipe(pipe_path = 'pipe'):

    if not os.path.exists(pipe_path):
        os.mkfifo(pipe_path)
    pipe = open(pipe_path, 'wb')

    return pipe

def create_db(db_name,pipe):
    length = 1 + len(db_name)
    encoded_data = length.to_bytes(4, byteorder='little', signed=True)
    pipe.write(encoded_data)
    req_type = 0
    encoded_data = req_type.to_bytes(1, byteorder='little', signed=True)
    pipe.write(encoded_data)    
    pipe.write(bytes(db_name, 'utf-8'))
    pipe.flush()

def create_table(db_name, table_name, col_names, col_type,col_size,time_step,pipe):
    length = 1+ len(db_name) + len(table_name) + 2 + len(col_names)*2 + 4
    for i in range(len(col_names)):
        length += len(col_names[i])
    encoded_data = length.to_bytes(4, byteorder='little', signed=True)
    pipe.write(encoded_data)
    
    req_type = 1
    encoded_data = req_type.to_bytes(1, byteorder='little', signed=True)
    pipe.write(encoded_data)    
    pipe.write(bytes(db_name, 'utf-8'))
    pipe.write(bytes(table_name, 'utf-8'))

    pipe.write(len(col_names).to_bytes(2, byteorder='little', signed=False))
    for i in range(len(col_names)):
        pipe.write(bytes(col_names[i], 'utf-8'))

    for i in range(len(col_type)):
        pipe.write(col_type[i].to_bytes(1, byteorder='little', signed=True))

    for i in range(len(col_size)):
        pipe.write(col_size[i].to_bytes(1, byteorder='little', signed=True))

    pipe.write(time_step.to_bytes(4, byteorder='little', signed=True))
    pipe.flush()

def insert(db_name, table_name,timestamp, char_entries, int_entries, float_entries, present,pipe):
    length = 1 + len(db_name) + len(table_name) + 8+ 1+ 1+len(int_entries)*5 + 1+len(float_entries)*5
    for i in range(len(char_entries)):
        length += len(char_entries[i]) + 2 +1
    encoded_data = length.to_bytes(4, byteorder='little', signed=True)
    pipe.write(encoded_data)
    

    req_type = 2
    encoded_data = req_type.to_bytes(1, byteorder='little', signed=True)
    pipe.write(encoded_data)    
    pipe.write(bytes(db_name, 'utf-8'))
    pipe.write(bytes(table_name, 'utf-8'))

    encoded_data = timestamp.to_bytes(8, byteorder='little', signed=False)
    pipe.write(encoded_data)

    pipe.write(len(char_entries).to_bytes(1, byteorder='little', signed=True))
    for i in range(len(char_entries)):
        pipe.write(len(char_entries[i]).to_bytes(2, byteorder='little', signed=True))
        pipe.write(bytes(char_entries[i], 'utf-8'))

    pipe.write(len(int_entries).to_bytes(1, byteorder='little', signed=True))
    for i in range(len(int_entries)):
        pipe.write(int_entries[i].to_bytes(4, byteorder='little', signed=True))

    pipe.write(len(float_entries).to_bytes(1, byteorder='little', signed=True))
    for i in range(len(float_entries)):
        float_bytes = struct.pack('f',float_entries[i])
        pipe.write(struct.pack('f',float_entries[i]))

    for i in range(len(present)):
        pipe.write(present[i].to_bytes(1, byteorder='little', signed=True))
    pipe.flush()


def query(db_name, table_name, min_time, max_time,pipe="pipe",res_path="resp"):
    length = len(db_name) + len(table_name) + 16 +1
    encoded_data = length.to_bytes(4, byteorder='little', signed=True)
    pipe.write(encoded_data)
    req_type = 3
    encoded_data = req_type.to_bytes(1, byteorder='little', signed=True)
    pipe.write(encoded_data)
    pipe.write(bytes(db_name, 'utf-8'))
    pipe.write(bytes(table_name, 'utf-8'))
    encoded_data = min_time.to_bytes(8, byteorder='little', signed=False)
    pipe.write(encoded_data)
    encoded_data = max_time.to_bytes(8, byteorder='little', signed=False)
    pipe.write(encoded_data)
    pipe.flush()
    if not os.path.exists(res_path):
        os.mkfifo(res_path)
    with open(res_path,'rb') as fifo:
        data = fifo.read(4)
        num_entries = int.from_bytes(data, byteorder='little')
        res = []
        for i in range(num_entries):
            res.append([])
            data = fifo.read(8)
            time = int.from_bytes(data, byteorder='little')
            res[i].append(time)
        data = fifo.read(4)
        num_chars = int.from_bytes(data, byteorder='little')
        for i in range(num_chars):
            lent = fifo.read(4)
            lent = int.from_bytes(lent, byteorder='little')
            for j in range(num_entries):
                data = fifo.read(lent)
                res[j].append(data.decode())

        data = fifo.read(4)
        num_ints = int.from_bytes(data, byteorder='little')
        for i in range(num_ints):
            for j in range(num_entries):
                data = fifo.read(4)
                res[j].append(int.from_bytes(data, byteorder='little'))

        data = fifo.read(4)
        num_floats = int.from_bytes(data, byteorder='little')
        for i in range(num_floats):
            for j in range(num_entries):
                data = fifo.read(4)
                res[j].append(struct.unpack('f', data)[0])
        return res

if __name__ == "__main__":
    #insert( "pk123\0", "qwert8\0", 8080874, char_entries, int_entries, float_entries, present)
    create_db("test_db\0")
    names = ["char field1", "char field2", "int field", "foat field1", "float field2"]
    size = [5,8,0,0,0] #Only used for char fields, rest can contain any value but need to be filled
    col_type = [0,0,1,2,2] # 0-> char* field, 1-> int, 2-> float
    create_table("test_db\0", "tab1\0", names, col_type, size, 10)
    insert("test_db", "tab_1", timestamp, char_entries, int_entries, float_entries, present)