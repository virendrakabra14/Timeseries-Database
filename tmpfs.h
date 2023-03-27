#define BLOCK_SIZE 4096
#define TMPFS_BUFFER_SIZE 10 //Number of files in tmpfs after which we start flusing to disk
using namespace std;

string TMPFS_MOUNTPOINT = "./tmp_mountpoint";
string TMPFS_SIZE = "10m";

struct database;
struct table
{
    string name;
    string tmp_base_file_path;
    struct database *parent;

    
    vector<string> tmp_file_paths;
    FILE* active_file;
    int active_size;
    time_t curr_file_time;
    pthread_mutex_t table_lock; 


    string disk_base_path;
    vector<string> disk_file_paths;
    
};
struct database
{
    string name;
    vector<struct table*> tables;
    pthread_mutex_t database_lock; 
};
struct memory_engine
{
    pthread_mutex_t db_mem_lock;
    vector<struct database*> databases;
};

struct memory_engine* db_memory;

int tmpfs_init();
int tmpfs_deinit();
int tmpfs_create_db(string db_name);
int tmpfs_create_table(string db_name, string table_name);
int tmpfs_insertEntry(string db_name, string table_name, string entry, time_t timestamp);