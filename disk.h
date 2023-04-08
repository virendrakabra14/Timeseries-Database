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