#pragma once
#define BLOCK_SIZE 4096
#define FIELD_BUFFER_SIZE 20
#define OOO_BUFFER_SIZE 5

#define INT_SIZE sizeof(int)
#define FLOAT_SIZE sizeof(float)

using namespace std;

struct database;
struct table
{
    int inst; //Set on instantiation, cleared on first insert
    string name;
    struct database *parent;

    int table_insert_head;
    int table_insert_head_secondary;
    int table_insert_head_ooo;

    vector<vector<int>> field_present;
    vector<vector<int>> field_secondary_present;
    vector<vector<int>> field_present_ooo; //Out Of Order buffer

    vector<string> char_field_name;
    vector<int> char_field_size;
    vector<char *> char_field_array;
    vector<char *> char_field_secondary_array;
    vector<char *> char_field_ooo;

    vector<string> int_field_name;
    vector<int*> int_field_array;
    vector<int*> int_field_secondary_array;

    vector<int*> int_field_ooo;

    vector<string> float_field_name;
    vector<float*> float_field_array;
    vector<float*> float_field_secondary_array;
    vector<float*> float_field_ooo;

    int step;

    vector<long int> timestamp;
    long int min_time;
    long int max_time;

    vector<long int> timestamp_secondary;
    long int min_time_secondary;
    long int max_time_secondary;

    vector<long int> timestamp_ooo;

    string disk_base_path;
    vector<long > disk_file_paths;

    vector<pair<long int, long int>> file_timestamps;
};

struct database
{
    string name;
    vector<struct table *> tables;
    pthread_mutex_t database_lock;
};
struct memory_engine
{
    pthread_mutex_t db_mem_lock;
    vector<struct database *> databases;
};

struct memory_engine *db_memory;


int init();
int deinit();
int create_db(string db_name);
int create_table(string db_name, string table_name, vector<string> field_names, vector<int> field_type, vector<int> field_size, int step);
int insertEntry(string db_name, string table_name, long int timestamp, vector<char *> char_entries, vector<int> int_entries, vector<float> float_entries, vector<int> present);
int flush_ooo_buffer(table * t);