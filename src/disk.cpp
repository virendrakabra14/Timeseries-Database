#define BASE_DIR "../db/"
#include "stdio.h"
#include "stdint.h"
#include "disk.h"
/**
 * Todo write disk functions
 */
#include "tmpfs.hpp"
#include <algorithm>
#include "filesystem"
vector<read_buff *> file_read(string db_name, string table_name, long int min_time, long int max_time);

// int load_file(read_buff *r, string file_path);
/**
 * File format
 * Metadata : 2 byte (number of entries) | 1 byte (number of char columns) | 1 byte (number of int  columns) | 1 byte (number of float columns) |
 *            Bitmap length 2 bytes | BITMAPS (tot_num columns* num_entries) | Char column lengths (1 Byte * num of char columns)
 *
 * Data :     Timestamps | Char | Int | Float
 */

int flag = 0;
long int min_tme;
int file_write(table *t)
{
    stringstream s;
    long int max_time = *max_element(t->timestamp_secondary.begin(), t->timestamp_secondary.end());
    long int min_time = *min_element(t->timestamp_secondary.begin(), t->timestamp_secondary.end());

    s << min_time;
    s << "-";
    s << max_time;
    FILE *file = fopen((BASE_DIR + t->parent->name + "/" + t->name + "/" + s.str()).c_str(), "wb+");
    fflush(stdout);

    uint16_t num_entries = t->table_insert_head_secondary;

    uint8_t num_char = t->char_field_secondary_array.size();
    uint8_t num_int = t->int_field_secondary_array.size();
    uint8_t num_float = t->float_field_secondary_array.size();
    int tot_columns = t->char_field_secondary_array.size() + t->int_field_secondary_array.size() + t->float_field_secondary_array.size();
    vector<vector<uint8_t>> bitmaps;

    for (int i = 0; i < tot_columns; i++)
    {
        vector<uint8_t> tmp;
        tmp.push_back(0);
        bitmaps.push_back(tmp);
    }
    uint8_t head = 0;
    int iter = 0;
    for (int i = 0; i < t->table_insert_head_secondary; i++)
    {
        for (int j = 0; j < tot_columns; j++)
        {
            bitmaps[j][iter] |= (t->field_secondary_present[i][j] << head);
        }
        head++;
        if (head == 8)
        {
            iter += 1;
            for (int j = 0; j < tot_columns; j++)
            {
                bitmaps[j].push_back(0);
            }
            head = 0;
        }
    }

    fwrite(&num_entries, 2, 1, file);
    fwrite(&num_char, 1, 1, file);
    fwrite(&num_int, 1, 1, file);
    fwrite(&num_float, 1, 1, file);
    uint16_t len = bitmaps[0].size();
    fwrite(&len, 2, 1, file);
    for (int i = 0; i < bitmaps.size(); i++)
    {
        for (int j = 0; j < bitmaps[i].size(); j++)
        {
            fwrite(&bitmaps[i][j], 1, 1, file);
        }
    }
    for (int i = 0; i < t->char_field_secondary_array.size(); i++)
    {
        uint8_t size = t->char_field_size[i];
        fwrite(&size, 1, 1, file);
    }
    fwrite(&(t->timestamp_secondary[0]), sizeof(long int), num_entries, file);
    for (int i = 0; i < t->char_field_secondary_array.size(); i++)
    {
        fwrite(t->char_field_secondary_array[i], t->char_field_size[i], num_entries, file);
    }

    for (int i = 0; i < t->int_field_secondary_array.size(); i++)
    {
        fwrite(t->int_field_secondary_array[i], INT_SIZE, num_entries, file);
    }

    for (int i = 0; i < t->float_field_secondary_array.size(); i++)
    {
        fwrite(t->float_field_secondary_array[i], FLOAT_SIZE, num_entries, file);
    }
    fclose(file);
    if (flag == 0)
    {
        // load_file(tab, min_time, max_time);
        min_tme = t->min_time_secondary+10;
        flag = 1;
    }
    vector<read_buff *> entries;
    entries = file_read(t->parent->name, t->name, min_tme, t->max_time_secondary);
    int tot = 0;
    for(int i = 0;i<entries.size();i++)
    {
        tot+=entries[i]->timestamp.size();
    }
    printf("ENTRIES SIZE : %d\n", tot);
    return 0;
}

/**
 * File format
 * Metadata : 2 byte (number of entries) | 1 byte (number of char columns) | 1 byte (number of int  columns) | 1 byte (number of float columns) |
 *            Bitmap length 2 bytes | BITMAPS (tot_num columns* num_entries) | Char column lengths (1 Byte * num of char columns)
 *
 * Data :     Timestamps | Char | Int | Float
 */
int load_file(read_buff *r, string path)
{
    uint16_t num_entries, bitmap_len;
    uint8_t num_char_cols, num_int_cols, num_float_cols;

    FILE *file = fopen(path.c_str(), "r");

    uint8_t metabuf[7];
    fread(metabuf, 7, 1, file);
    num_entries = metabuf[0] + metabuf[1] * 256;
    num_char_cols = metabuf[2];
    num_int_cols = metabuf[3];
    num_float_cols = metabuf[4];

    bitmap_len = metabuf[5] + metabuf[6] * 256;

    int tot_cols = (int)num_char_cols + (int)num_int_cols + (int)num_float_cols;
    char *bitmap[tot_cols];
    for (int i = 0; i < tot_cols; i++)
    {
        bitmap[i] = (char *)malloc(bitmap_len);
        fread(bitmap[i], bitmap_len, 1, file);
    }

    uint8_t head = 0;
    int iter = 0;
    for (int i = 0; i < num_entries; i++) // Inflate bitmap
    {
        vector<int> v;
        r->present.push_back(v);
        for (int j = 0; j < tot_cols; j++)
        {
            r->present[i].push_back((bitmap[j][iter] & (1 << head)) >> head);
        }
        head++;
        if (head == 8)
        {
            iter += 1;
            head = 0;
        }
    }

    uint8_t char_col_len[num_char_cols];
    fread(char_col_len, num_char_cols, 1, file);

    r->timestamp.resize(num_entries, 0);
    fread(&r->timestamp[0], sizeof(long int), num_entries, file);

    for (int i = 0; i < num_char_cols; i++)
    {
        char *col = (char *)malloc(num_entries * char_col_len[i]);
        fread(col, char_col_len[i], num_entries, file);
        r->char_cols.push_back(col);
        r->char_col_sizes.push_back(char_col_len[i]);
    }

    for (int i = 0; i < num_int_cols; i++)
    {
        int *col = (int *)malloc(num_entries * INT_SIZE);
        fread(col, INT_SIZE, num_entries, file);
        r->int_cols.push_back(col);
    }

    for (int i = 0; i < num_float_cols; i++)
    {
        float *col = (float *)malloc(num_entries * FLOAT_SIZE);
        fread(col, FLOAT_SIZE, num_entries, file);
        r->float_cols.push_back(col);
    }
    fclose(file);
    return 0;
}

int random_insert(string path, long int timestamp, vector<char *> char_entries, vector<int> int_entries, vector<float> float_entries, vector<int> present)
{
    // insert in random reserved space on disk for out of order data

    uint16_t num_entries;

    FILE *file = fopen(path.c_str(), "r+");

    uint8_t metabuf[2];
    fread(metabuf, 2, 1, file);
    num_entries = metabuf[0] + metabuf[1] * (1<<8);

    size_t num_cols = present.size();
    size_t metadata_size = 7 + num_entries * num_cols + 1 * char_entries.size();
    size_t timestamps_size = sizeof(long int) * num_entries;

    // get char col sizes
    uint8_t char_col_len[char_entries.size()];
    fseek(file, metadata_size-char_entries.size(), SEEK_SET);
    fread(char_col_len, char_entries.size(), 1, file);

    // get initial timestamp (in this file)
    // can do this using path or metadata; using latter here
    long int init_timestamp;
    fseek(file, metadata_size, SEEK_SET);
    fread(&init_timestamp, sizeof(long int), 1, file);

    long int time_diff = timestamp - init_timestamp;
    unsigned long int col_offset = 0;

    for(int i=0; i<char_entries.size(); i++) {
        fseek(file, metadata_size + timestamps_size + col_offset + time_diff*char_col_len[i], SEEK_SET);
        fwrite(char_entries[i], char_col_len[i], 1, file);
        col_offset += char_col_len[i] * num_entries;
    }

    for(int i=0; i<int_entries.size(); i++) {
        fseek(file, metadata_size + timestamps_size + col_offset + time_diff*INT_SIZE, SEEK_SET);
        fwrite(&int_entries[i], INT_SIZE, 1, file);
        col_offset += INT_SIZE * num_entries;
    }

    for(int i=0; i<float_entries.size(); i++) {
        fseek(file, metadata_size + timestamps_size + col_offset + time_diff*FLOAT_SIZE, SEEK_SET);
        fwrite(&float_entries[i], FLOAT_SIZE, 1, file);
        col_offset += FLOAT_SIZE * num_entries;
    }

    fclose(file);

    return 0;
}

vector<read_buff *> file_read(string db_name, string table_name, long int min_time, long int max_time)
{
    //printf("%ld   tme   %ld", min_time, max_time);
    string path = BASE_DIR + db_name + "/" + table_name + "/";
    vector<vector<long int>> timestamps;
    for (const auto &entry : filesystem::directory_iterator(path))
    {
        string name(entry.path().c_str());
        name = name.substr(path.length(), name.length());
        vector<long int> time;
        time.push_back(stol(name.substr(0, name.find('-')).c_str()));
        time.push_back(stol(name.substr(name.find('-') + 1, name.length())));
        timestamps.push_back(time);
    }
    int min_index = -1, max_index = -1;
    sort(timestamps.begin(), timestamps.end());
    for (int i = 0; i < timestamps.size(); i++)
    {
        if (max_index == -1 && timestamps[i][1] >= max_time && timestamps[i][0] <= max_time)
            max_index = i;
        if (min_index == -1 && timestamps[i][1] >= min_time && timestamps[i][0] <= min_time)
            min_index = i;
    }
    if(max_time > timestamps[timestamps.size()-1][1])
    max_index = timestamps.size()-1;
    vector<read_buff *> entries;
    if (min_index != -1 && max_index != -1)
    {
        // printf("%ld l %ld\n", timestamps[min_index][0], timestamps[max_index][1]);
        fflush(stdout);

        for (int i = min_index; i <= max_index; i++)
        {
            read_buff *r = new read_buff;
            stringstream s;
            s << timestamps[i][0];
            s << "-";
            s << timestamps[i][1];

            string path = BASE_DIR + db_name + "/" + table_name + "/" + s.str();
            // printf("%s", path.c_str());
            fflush(stdout);
            load_file(r, path);
            //printf("size : %ld \n", r->timestamp.size());
            entries.push_back(r);
        }
    }

    printf("\n");
    return entries;
}