#define DISK_BLOCK

using namespace std;

string DISK_DIR = "./disk_dir";

struct read_buff
{
    vector<vector<int>> present;

    vector<char*> char_cols;
    vector<int> char_col_sizes;

    vector<int*> int_cols;
    vector<float*> float_cols;
    vector<long int> timestamp; 

    int min_index;
    int max_index;
};

// int file_write(table *t);
// vector<read_buff *> file_read(string db_name, string table_name, long int min_time, long int max_time);
// int load_file(read_buff *r, string path);
// vector<read_buff *> file_read(string db_name, string table_name, long int min_time, long int max_time);