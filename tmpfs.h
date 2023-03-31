#define BLOCK_SIZE 4096
#define FIELD_BUFFER_SIZE 100000

#define INT_SIZE sizeof(int)
#define FLOAT_SIZE sizeof(float)

using namespace std;

struct database;
struct table
{
    string name;
    struct database *parent;

    int table_insert_head;
    vector<vector<int>> field_present;

    vector<string> char_field_name;
    vector<int> char_field_size;
    vector<char *> char_field_array;

    vector<string> int_field_name;
    vector<int*> int_field_array;

    vector<string> float_field_name;
    vector<float*> float_field_array;

    // types :
    // INT float char*(fixed)

    // two types of time;
    // optional millisecond column
    // remove timestamp
    vector<string> timestamp;

    string disk_base_path;
    vector<string> disk_file_paths;
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

int fs_init();
int fs_deinit();
int fs_create_db(string db_name);
int fs_create_table(string db_name, string table_name, vector<string> field_names, vector<int> field_type, vector<int> field_size);
int fs_insertEntry(string db_name, string table_name, string timestamp, vector<char *> char_entries, vector<int> int_entries, vector<float> float_entries, vector<int> present);