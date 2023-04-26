
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include "sys/stat.h"
#include <time.h>
#include "errno.h"
#include <vector>
#include <pthread.h>
#include <cstring>
#include <unistd.h>
#include "ctime"
#include "sstream"
#include "chrono"

#include "disk.cpp"

#include "tmpfs.hpp"


void print_table(string db_name, string table_name);
int buffer_swap(table* t);

int init()
{
    db_memory = new memory_engine;
    mkdir(BASE_DIR, 0777);
    return 0;
}

// Unmount and delete contents
int deinit() //TODO : free buffers 
{
    return 0;
}

// Adds a new database if it does not exists.
// Creates the db directory
int create_db(string db_name)
{
    pthread_mutex_lock(&(db_memory->db_mem_lock));
    for (int i = 0; i < db_memory->databases.size(); i++)
    {
        if (db_memory->databases[i]->name == db_name)
        {
            printf("ERROR : Database with name '%s' exists", db_name.c_str());
            pthread_mutex_unlock(&(db_memory->db_mem_lock));
            return 1;
        }
    }

    database *db = new database;
    if (db == NULL)
    {
        printf("ERROR : Malloc failed......\n");
        pthread_mutex_unlock(&(db_memory->db_mem_lock));
        return 1;
    }
    db->name = db_name;
    db_memory->databases.push_back(db);
    mkdir((BASE_DIR + db_name + "/").c_str(), 0777);
    pthread_mutex_unlock(&(db_memory->db_mem_lock));
    return 0;
}

template <typename T>
int free_pointer_vec(vector<T> vec)
{
    for (int i = 0; i < vec.size(); i++)
    {
        if (vec[i] != NULL)
        {
            free(vec[i]);
        }
    }
    return 0;
}

// Creates a new table in the database and creates its directories
// Field_type : 0 => char[], 1=> int, 2=> float
int create_table(string db_name, string table_name, vector<string> field_names, vector<int> field_type, vector<int> field_size, int step)
{
    for (int i = 0; i < db_memory->databases.size(); i++)
    {
        pthread_mutex_lock(&db_memory->databases[i]->database_lock);
        if (db_memory->databases[i]->name == db_name) // Db exists
        {
            for (int j = 0; j < db_memory->databases[i]->tables.size(); j++)
            {
                if (db_memory->databases[i]->tables[j]->name == table_name) // Abort if table exists
                {
                    printf("ERROR : Table with name '%s' exists", table_name.c_str());
                    pthread_mutex_unlock(&db_memory->databases[i]->database_lock);
                    return 1;
                }
            }

            // Checks passed, create new table
            table *t = new table;

            t->name = table_name;
            t->parent = db_memory->databases[i];
            t->table_insert_head = 0;
            t->table_insert_head_secondary = 0;
            t->min_time = 0;
            t->min_time_secondary = 0;
            t->step = step;
            t->inst = 1;

            int failure = 0;
            for (int j = 0; j < field_names.size(); j++)
            {
                if (field_type[j] == 0) // Char[] column
                {
                    t->char_field_name.push_back(field_names[j]);
                    t->char_field_size.push_back(field_size[j]);
                    char *arr = (char *)malloc(FIELD_BUFFER_SIZE * field_size[j]);
                    if (arr == NULL)
                    {
                        failure = 1;
                        break;
                    }
                    char *arr2 = (char *)malloc(OOO_BUFFER_SIZE * field_size[j]);
                    if (arr == NULL)
                    {
                        failure = 1;
                        break;
                    }
                    t->char_field_array.push_back(arr);
                    t->char_field_ooo.push_back(arr2);
                }
                else if (field_type[j] == 1) // Int column
                {
                    t->int_field_name.push_back(field_names[j]);
                    int *arr = (int *)malloc(FIELD_BUFFER_SIZE * INT_SIZE);
                    if (arr == NULL)
                    {
                        failure = 1;
                        break;
                    }
                    int *arr2 = (int *)malloc(OOO_BUFFER_SIZE * INT_SIZE);
                    if (arr == NULL)
                    {
                        failure = 1;
                        break;
                    }
                    t->int_field_array.push_back(arr);
                    t->int_field_ooo.push_back(arr2);
                }
                else if (field_type[j] == 2)
                {
                    t->float_field_name.push_back(field_names[j]);
                    float *arr = (float *)malloc(FIELD_BUFFER_SIZE * FLOAT_SIZE);
                    if (arr == NULL)
                    {
                        failure = 1;
                        break;
                    }
                    float *arr2 = (float *)malloc(OOO_BUFFER_SIZE * FLOAT_SIZE);
                    if (arr == NULL)
                    {
                        failure = 1;
                        break;
                    }
                    t->float_field_array.push_back(arr);
                    t->float_field_ooo.push_back(arr2);
                }
            }
            if (failure)
            {
                free_pointer_vec<char *>(t->char_field_array);
                free_pointer_vec<int *>(t->int_field_array);
                free_pointer_vec<float *>(t->float_field_array);

                free_pointer_vec<char *>(t->char_field_ooo);
                free_pointer_vec<int *>(t->int_field_ooo);
                free_pointer_vec<float *>(t->float_field_ooo);
                printf("ERROR : Memory allocation failed, cannot create table.\n");
                return 1;
            }

            db_memory->databases[i]->tables.push_back(t);
            mkdir((BASE_DIR + db_name + "/" + table_name + "/").c_str(), 0777);
            pthread_mutex_unlock(&db_memory->databases[i]->database_lock);
            return 0;
        }
        pthread_mutex_unlock(&db_memory->databases[i]->database_lock);
    }
    printf("ERROR : Database with name '%s' does not exist", db_name.c_str());
    return 1;
}

int insertEntry(string db_name, string table_name, long int timestamp, vector<char *> char_entries, vector<int> int_entries, vector<float> float_entries, vector<int> present)
{
    for (int i = 0; i < db_memory->databases.size(); i++)
    {
        if (db_memory->databases[i]->name == db_name) // db exists
        {
            pthread_mutex_lock(&db_memory->databases[i]->database_lock);

            for (int j = 0; j < db_memory->databases[i]->tables.size(); j++)
            {
                if (db_memory->databases[i]->tables[j]->name == table_name) // table exists
                {
                    table *tab = db_memory->databases[i]->tables[j];
                    if (tab->inst == 1) // First insert after creation/swap
                    {
                        tab->min_time = timestamp;
                        tab->max_time = timestamp;
                        tab->inst = 0;
                    }
                    if (timestamp >= tab->min_time) // Data belongs to current interval
                    {
                        if ((timestamp - tab->max_time) != tab->step) // Reserve space in buffer for missing data
                        {
                            vector<int> present(tab->char_field_array.size() + tab->int_field_array.size() + tab->float_field_array.size(), 0);
                            for (long int time = tab->max_time + tab->step; time < timestamp; time += tab->step)
                            {
                                tab->timestamp.push_back(time);
                                tab->field_present.push_back(present);
                                tab->table_insert_head++;

                                if (tab->table_insert_head >= FIELD_BUFFER_SIZE)
                                {
                                    // Initiate compression and disk write....
                                    buffer_swap(tab);
                                    tab->table_insert_head = 0;
                                    // printf("buffer overflow \n");
                                }
                            }
                        }

                        if (tab->max_time < timestamp)
                        {
                            tab->max_time = timestamp;
                        }

                        tab->timestamp.push_back(timestamp); // Insert timestamp
                        vector<int> pres(present.begin(), present.end());
                        tab->field_present.push_back(pres);
                        for (int k = 0; k < char_entries.size(); k++) // Insert entries
                        {
                            memcpy(&tab->char_field_array[k][tab->table_insert_head * tab->char_field_size[k]],
                                   char_entries[k], tab->char_field_size[k]);
                        }
                        for (int k = 0; k < int_entries.size(); k++)
                        {
                            tab->int_field_array[k]
                                                [tab->table_insert_head] = int_entries[k];
                        }
                        for (int k = 0; k < float_entries.size(); k++)
                        {

                            tab->float_field_array[k]
                                                  [tab->table_insert_head] = float_entries[k];
                        }
                        tab->table_insert_head += 1;
                        if (tab->table_insert_head >= FIELD_BUFFER_SIZE)
                        {
                            // Initiate compression and disk write....
                            buffer_swap(tab);
                            tab->table_insert_head = 0;
                            // printf("buffer overflow \n");
                        }
                    }
                    else if (timestamp > tab->min_time_secondary && timestamp < tab->max_time_secondary) // Entry belongs to previous period
                    {
                        int index = (timestamp - tab->min_time_secondary) / tab->step;

                        for (int i = 0; i < present.size(); i++)
                        {
                            tab->field_secondary_present[index][i] = present[i];
                        }
                        for (int k = 0; k < char_entries.size(); k++) // Insert entries
                        {
                            memcpy(&tab->char_field_array[k][index * tab->char_field_size[k]],
                                   char_entries[k], tab->char_field_size[k]);
                        }
                        for (int k = 0; k < int_entries.size(); k++)
                        {
                            tab->int_field_array[k]
                                                [index] = int_entries[k];
                        }
                        for (int k = 0; k < float_entries.size(); k++)
                        {

                            tab->float_field_array[k]
                                                  [index] = float_entries[k];
                        }
                    }
                    else
                    {
                        tab->timestamp_ooo.push_back(timestamp);
                        vector<int> pres(present.begin(), present.end());
                        tab->field_present_ooo.push_back(pres);

                        for (int k = 0; k < char_entries.size(); k++) // Insert entries
                        {
                            memcpy(&tab->char_field_ooo[k][tab->table_insert_head_ooo * tab->char_field_size[k]],
                                   char_entries[k], tab->char_field_size[k]);
                        }
                        for (int k = 0; k < int_entries.size(); k++)
                        {
                            tab->int_field_ooo[k]
                                              [tab->table_insert_head_ooo] = int_entries[k];
                        }
                        for (int k = 0; k < float_entries.size(); k++)
                        {

                            tab->float_field_ooo[k]
                                                [tab->table_insert_head_ooo] = float_entries[k];
                        }
                        tab->table_insert_head_ooo += 1;
                        if (tab->table_insert_head_ooo >= OOO_BUFFER_SIZE)
                        {
                            // Initiate disk write....
                            flush_ooo_buffer(tab);

                            // printf("buffer overflow \n");
                        }
                    }

                    pthread_mutex_unlock(&db_memory->databases[i]->database_lock);
                    printf("here! ");
                    return 0;
                }
            }
            printf("ERROR : Table with name '%s' does not exist", table_name.c_str());
            pthread_mutex_unlock(&db_memory->databases[i]->database_lock);
            return 1;
        }
    }
    printf("ERROR : Database with name '%s' does not exist", db_name.c_str());
    return 1;
}

int flush_ooo_buffer(table *tab)
{

    string path;
    vector<char *> char_entries;
    vector<int> int_entries;
    vector<float> float_entries;
    vector<int> present;

    for(int i=0; i<tab->table_insert_head_ooo; i++)
    {
        for(auto&& p: tab->file_timestamps)
        {
            // get interval bounds (for file path)
            if(tab->timestamp_ooo[i] >= p.first && tab->timestamp_ooo[i] <= p.second)
            {
                path = BASE_DIR + tab->parent->name + "/" + tab->name + "/" + to_string(p.first) + "-" + to_string(p.second);
                present = tab->field_present_ooo[i];
                for(int k = 0; k < tab->char_field_ooo.size(); k++)
                {
                    char_entries.push_back(&tab->char_field_ooo[k][i * tab->char_field_size[k]]);
                }
                for (int k = 0; k < tab->int_field_ooo.size(); k++)
                {
                    tab->int_field_ooo[k][i] = int_entries[k];
                }
                for (int k = 0; k < float_entries.size(); k++)
                {
                    tab->float_field_ooo[k][i] = float_entries[k];
                }
                random_insert(path, tab->timestamp_ooo[i], char_entries, int_entries, float_entries, present);
            }
        }
    }

    tab->table_insert_head_ooo = 0;
    return 0;
}

template <typename T>
int swap_buffers(vector<T> *vec1, vector<T> *vec2)
{
    for (int i = 0; i < (*vec1).size(); i++)
    {
        T tmp = (*vec1)[i];
        (*vec1)[i] = (*vec2)[i];
        (*vec2)[i] = tmp;
    }
    return 0;
}

int buffer_swap(table *t)
{
    int exists = 1;
    if (t->char_field_secondary_array.size() == 0 && t->int_field_secondary_array.size() == 0 && t->float_field_secondary_array.size() == 0)
    {
        exists = 0;
        int failure = 0;
        for (int i = 0; i < t->char_field_name.size(); i++)
        {
            char *arr = (char *)malloc(FIELD_BUFFER_SIZE * t->char_field_size[i]);
            if (arr == NULL)
            {
                failure = 1;
                break;
            }
            t->char_field_secondary_array.push_back(arr);
        }
        for (int i = 0; i < t->int_field_name.size(); i++)
        {
            int *arr = (int *)malloc(FIELD_BUFFER_SIZE * INT_SIZE);
            if (arr == NULL)
            {
                failure = 1;
                break;
            }
            t->int_field_secondary_array.push_back(arr);
        }
        for (int i = 0; i < t->float_field_name.size(); i++)
        {
            float *arr = (float *)malloc(FIELD_BUFFER_SIZE * FLOAT_SIZE);
            if (arr == NULL)
            {
                failure = 1;
                break;
            }
            t->float_field_secondary_array.push_back(arr);
        }
        // if (failure)
        //     printf("Failed!!!!\n");
        // else
        //     printf("successss\n");
    }
    // if (fff == 1)
    // {
    //     fflush(stdout);
    //     print_table("test_db", "tab1");
    //     fflush(stdout);
    //     fff = 1;
    // }
    // fff++;
    if (exists)
    {
        file_write(t);
    }
    swap_buffers(&t->char_field_array, &t->char_field_secondary_array);
    swap_buffers(&t->int_field_array, &t->int_field_secondary_array);
    swap_buffers(&t->float_field_array, &t->float_field_secondary_array);
    t->field_secondary_present.swap(t->field_present);
    t->field_present.clear();
    t->table_insert_head_secondary = t->table_insert_head;
    t->timestamp.swap(t->timestamp_secondary);
    t->timestamp.clear();

    t->min_time_secondary = t->min_time;
    t->max_time_secondary = t->max_time;

    t->min_time = t->max_time + t->step;
    t->max_time = t->max_time;
    fflush(stdout);
    return 0;
}

void print_table(string db_name, string table_name) // for debug
{
    for (int i = 0; i < db_memory->databases.size(); i++)
    {
        if (db_memory->databases[i]->name == db_name) // db exists
        {
            // pthread_mutex_lock(&db_memory->databases[i]->database_lock);
            for (int j = 0; j < db_memory->databases[i]->tables.size(); j++)
            {
                if (db_memory->databases[i]->tables[j]->name == table_name) // table exists
                {
                    printf("Name : %s\n", db_memory->databases[i]->tables[j]->name.c_str());
                    printf("Insert Head at : %d\n", db_memory->databases[i]->tables[j]->table_insert_head);
                    for (int k = 0; k < db_memory->databases[i]->tables[j]->char_field_array.size(); k++)
                    {
                        printf("Column name : %s\n", db_memory->databases[i]->tables[j]->char_field_name[k].c_str());
                        printf("Field sizes : %d\n", db_memory->databases[i]->tables[j]->char_field_size[k]);
                        for (int m = 0; m < db_memory->databases[i]->tables[j]->table_insert_head; m++)
                        {
                            if (db_memory->databases[i]->tables[j]->field_present[m][k])
                                for (int l = 0; l < db_memory->databases[i]->tables[j]->char_field_size[k]; l++)
                                {

                                    printf("%c", db_memory->databases[i]->tables[j]->char_field_array[k]
                                                                                                     [m * db_memory->databases[i]->tables[j]->char_field_size[k] + l]);
                                }
                            else
                                printf("null");
                            printf("  ");
                        }
                        printf("\n\n");
                    }

                    for (int k = 0; k < db_memory->databases[i]->tables[j]->int_field_array.size(); k++)
                    {
                        printf("Column name : %s\n", db_memory->databases[i]->tables[j]->int_field_name[k].c_str());
                        for (int m = 0; m < db_memory->databases[i]->tables[j]->table_insert_head; m++)
                        {
                            if (db_memory->databases[i]->tables[j]->field_present[m][k + db_memory->databases[i]->tables[j]->char_field_array.size()])
                                printf("%d", db_memory->databases[i]->tables[j]->int_field_array[k][m]);
                            else
                                printf("null");
                            printf("  ");
                        }
                        printf("\n\n");
                    }

                    for (int k = 0; k < db_memory->databases[i]->tables[j]->float_field_array.size(); k++)
                    {
                        printf("Column name : %s\n", db_memory->databases[i]->tables[j]->float_field_name[k].c_str());
                        for (int m = 0; m < db_memory->databases[i]->tables[j]->table_insert_head; m++)
                        {
                            if (db_memory->databases[i]->tables[j]->field_present[m][k + db_memory->databases[i]->tables[j]->int_field_array.size() + db_memory->databases[i]->tables[j]->char_field_array.size()])
                                printf("%f", db_memory->databases[i]->tables[j]->float_field_array[k][m]);
                            else
                                printf("null");
                            printf("  ");
                        }
                        printf("\n\n");
                    }
                    fflush(stdout);
                    return;
                }
            }
        }
    }
    printf("not found \n");
}

int main()
{
    init();
    create_db("test_db");
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
    int step = 10;
    create_table("test_db", "tab1", names, type, size, step);

    for (int i = 0; i < 50; i++)
    {
        chrono::milliseconds ms = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());
        long time = ms.count();
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
        insertEntry("test_db", "tab1", time, cvec, ivec, fvec, present);
    }
    print_table("test_db", "tab1");
}
