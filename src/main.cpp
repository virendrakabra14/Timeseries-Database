#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <string.h>

#include "tmpfs.cpp"
#define MAX_BUF 4096

int insert_pipe;
uint8_t buf[MAX_BUF];

int init_insert_pipe()
{
    string fifo = "./pipe";
    insert_pipe = open(fifo.c_str(), O_RDONLY);
    return 0;
}

int service_insert_pipe()
{
    while (1)
    {
        /***
         * Pipe data Format :
         * Data size (4 bytes) | Type(0:Create db, 1: Create table, 2:Insert, 3:Query) | Body
         *
         * Create db body :
         * db_name
         *
         * Create table body :
         * db_name | table_name | num_columns(2 byte) |col_name1| col_name n | col_type | col_size | time_step
         *
         *
         * Insert :
         *  Db name(Null terminated) |
         * Table name(Null terminated) | Timestamp(8 bytes) | num of strings(1 byte) | size1(2 bytes) | char entry 1| size 2
         * |char entry2 | ..... | char entry n  | num of ints(1 byte)| ints (4 byte each) | number or floats(1 byte) |
         * floats (4 bytes each)| present bits (strings | int | float)
         *
         ***/
        uint64_t size = 0;
        int ret = read(insert_pipe, &size, 4);
        while (ret != 4)
            ret += read(insert_pipe, &size, 4);
        fflush(stdout);
        ret = read(insert_pipe, buf, size);
        while (ret < size)
        {
            ret += read(insert_pipe, buf, size);
        }
        if (buf[0] == 0) // Create db
        {
            cout << "db"<< endl;
            string db_name((char *)&buf[1]);
            create_db(db_name);
        }
        if (buf[0] == 1) // Create table
        {
            cout << "table"<< endl;
            string db_name((char *)&buf[1]);
            string table_name((char *)&buf[db_name.length() + 2]);
            int offs = 3 + db_name.length() + table_name.length();
            uint16_t num_cols = *(uint16_t *)&buf[offs];
            offs += 2;
            vector<string> names;
            for (int i = 0; i < num_cols; i++)
            {
                string name((char *)&buf[offs]);
                names.push_back(name);
                offs += name.length() + 1;
            }
            vector<int> col_type;
            for (int i = 0; i < num_cols; i++)
            {
                col_type.push_back((int)buf[offs]);
                offs++;
            }
            vector<int> col_size;
            for (int i = 0; i < num_cols; i++)
            {
                col_size.push_back((int)buf[offs]);
                offs++;
            }
            int time_step = *(uint32_t *)&buf[offs];
            create_table(db_name, table_name, names, col_type, col_size, time_step);
        }
        if (buf[0] == 2) // Insert
        {
            string db_name((char *)&buf[1]);
            string table_name((char *)&buf[db_name.length() + 2]);
            int offs = 3 + db_name.length() + table_name.length();
            unsigned long int timestamp = *(uint64_t *)&buf[offs];
            uint8_t num_strings = buf[offs + 8];
            offs = offs + 9;
            vector<char *> char_entries;
            for (int i = 0; i < num_strings; i++)
            {
                int len = buf[offs] + buf[offs + 1] * 256;
                char_entries.push_back((char *)&buf[offs + 2]);
                offs = offs + len + 2;
            }
            uint8_t num_int = buf[offs];
            offs += 1;
            vector<int> int_entries;
            for (int i = 0; i < num_int; i++)
            {
                int_entries.push_back(*(int *)&buf[offs]);
                offs += 4;
            }

            uint8_t num_float = buf[offs];
            offs += 1;
            vector<float> float_entries;
            for (int i = 0; i < num_float; i++)
            {
                float f = *(float *)&buf[offs];
                float_entries.push_back(f);
                offs += 4;
            }

            vector<int> present;
            for (int i = 0; i < (num_strings + num_float + num_int); i++)
            {
                present.push_back(buf[offs]);
                offs += 1;
            }
            insertEntry(db_name, table_name, timestamp, char_entries, int_entries, float_entries, present);
        }
        
    }
    return 0;
}

int main()
{

    #ifndef __ONLYBACKEND__
    init();
    init_insert_pipe();
    service_insert_pipe();
    #endif

    #ifdef __ONLYBACKEND__
    string db_name = "test_db";
    string table_name = "tab1";

    init();
    create_db(db_name);
    vector<string> names;
    names.push_back("char field");
    names.push_back("char2 field");
    names.push_back("char3 field");
    names.push_back("char4 field");

    names.push_back("integer field");
    names.push_back("integer2 field");

    names.push_back("float field");
    names.push_back("float2 field");
    names.push_back("float3 field");
    vector<int> size;
    size.push_back(5);
    size.push_back(8);
    size.push_back(10);
    size.push_back(15);
    vector<int> type;
    type.push_back(0);
    type.push_back(0);
    type.push_back(0);
    type.push_back(0);

    type.push_back(1);
    type.push_back(1);

    type.push_back(2);
    type.push_back(2);
    type.push_back(2);
    int step = 1;
    create_table(db_name, table_name, names, type, size, step);

    for (int i = 0; i < 50; i++)
    {
        // chrono::milliseconds ms = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());
        long time = 2*i;
        vector<int> present;
        // printf("present : ");
        for (int j = 0; j < 9; j++)
        {
            present.push_back(rand() % 2);
            //  printf("%d",present[j]);
        }
        if (rand() % 2 == 0)
        {
            for (int j = 0; j < 9; j++)
            {
                present[j] = 0;
            }
        }
        // printf("\n");
        char c1[5], c2[8], c3[10], c4[15];
        for (int j = 0; j < 5; j++)
        {
            c1[j] = 'a' + (int)(((float)rand() / RAND_MAX) * ('z' - 'a'));
        }
        for (int j = 0; j < 8; j++)
        {
            c2[j] = 'a' + (int)(((float)rand() / RAND_MAX) * ('z' - 'a'));
        }
        for (int j = 0; j < 10; j++)
        {
            c3[j] = 'a' + (int)(((float)rand() / RAND_MAX) * ('z' - 'a'));
        }
        for (int j = 0; j < 15; j++)
        {
            c4[j] = 'a' + (int)(((float)rand() / RAND_MAX) * ('z' - 'a'));
        }

        vector<char *> cvec;
        cvec.push_back(c1);
        cvec.push_back(c2);
        cvec.push_back(c3);
        cvec.push_back(c4);

        vector<int> ivec;
        ivec.push_back(rand());
        ivec.push_back(rand());
        vector<float> fvec;
        fvec.push_back((float)rand() / 393);
        fvec.push_back((float)rand() / 393);
        fvec.push_back((float)rand() / 393);
        if (i % 10000 == 0)
        {
            // sleep(1);
        }
        time = (time/step)*step;
        insertEntry(db_name, table_name, time, cvec, ivec, fvec, present);
    }
    
    printf("\nBefore ooo inserts:\n");
    print_table(db_name, table_name, 0, 100);

    for(int i = 1; i < 20; i += 2)
    {
        char c1[5], c2[8], c3[10], c4[15];
        for(int i = 0; i < sizeof(c1); i++)
        {
            c1[i] = 'o';
        }
        for(int i = 0; i < sizeof(c2); i++)
        {
            c2[i] = 'o';
        }
        for(int i = 0; i < sizeof(c3); i++)
        {
            c3[i] = 'o';
        }
        for(int i = 0; i < sizeof(c4); i++)
        {
            c4[i] = 'o';
        }

        insertEntry
        (
            db_name, table_name, i,
            vector<char *>{c1, c2, c3, c4},
            vector<int>{12345, 123456},
            vector<float>{123.45, 123.456, -125.0625},
            vector<int>(9, 1)
        );
    }

    printf("\nAfter ooo inserts:\n");
    print_table(db_name, table_name, 0, 100);
    #endif
}   
