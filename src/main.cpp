#include "tmpfs.cpp"

int main()
{

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

}
