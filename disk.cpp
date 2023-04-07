#define BASE_DIR "./db/"
#include "stdio.h"
#include "stdint.h"
/**
 * Todo write disk functions
 */
#include "tmpfs.hpp"
#include <algorithm>
#include "filesystem"

void print_table_ptr(table *t) // for debug
{
    printf("Name : %s\n", t->name.c_str());
    for (int k = 0; k < t->char_field_array.size(); k++)
    {
        printf("Field sizes : %d\n", t->char_field_size[k]);
        for (int m = 0; m < t->table_insert_head; m++)
        {
            if (t->field_present[m][k])
                for (int l = 0; l < t->char_field_size[k]; l++)
                {
                    printf("%c", t->char_field_array[k][m * t->char_field_size[k] + l]);
                }
            else
                printf("null");

            printf("  ");
        }
        printf("\n\n");
    }
    fflush(stdout);
    for (int k = 0; k < t->int_field_array.size(); k++)
    {
        // printf("Column name : %s\n", t->int_field_name[k].c_str());
        for (int m = 0; m < t->table_insert_head; m++)
        {
            if (t->field_present[m][k + t->char_field_array.size()])
                printf("%d", t->int_field_array[k][m]);
            else
                printf("null");
            printf("  ");
        }
        printf("\n\n");
    }

    for (int k = 0; k < t->float_field_array.size(); k++)
    {
        // printf("Column name : %s\n", t->float_field_name[k].c_str());
        for (int m = 0; m < t->table_insert_head; m++)
        {
            if (t->field_present[m][k + t->int_field_array.size() + t->char_field_array.size()])
                printf("%f", t->float_field_array[k][m]);
            else
                printf("null");
            printf("  ");
        }
        printf("\n\n");
    }
    fflush(stdout);
    return;
}

int load_file(table *t, long int min_time, long int max_time);
/**
 * File format
 * Metadata : 2 byte (number of entries) | 1 byte (number of char columns) | 1 byte (number of int  columns) | 1 byte (number of float columns) |
 *            Bitmap length 2 bytes | BITMAPS (tot_num columns* num_entries) | Char column lengths (1 Byte * num of char columns)
 *
 * Data :     Timestamps | Char | Int | Float
 */

int flag = 0;
int file_write(table *t)
{
    stringstream s;
    long int max_time = *max_element(t->timestamp_secondary.begin(), t->timestamp_secondary.end());
    long int min_time = *min_element(t->timestamp_secondary.begin(), t->timestamp_secondary.end());

    s << min_time;
    s << "-";
    s << max_time;
    FILE *file = fopen((BASE_DIR + t->parent->name + "/" + t->name + "/" + s.str()).c_str(), "wb+");
    printf("%s\n", (BASE_DIR + t->parent->name + "/" + t->name + "/" + s.str()).c_str());
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

    table *tab = new table;
    tab->name = t->name;
    tab->parent = t->parent;
    if (flag == 0)
    {
        load_file(tab, min_time, max_time);
        flag = 1;
    }
    return 0;
}

/**
 * File format
 * Metadata : 2 byte (number of entries) | 1 byte (number of char columns) | 1 byte (number of int  columns) | 1 byte (number of float columns) |
 *            Bitmap length 2 bytes | BITMAPS (tot_num columns* num_entries) | Char column lengths (1 Byte * num of char columns)
 *
 * Data :     Timestamps | Char | Int | Float
 */
int load_file(table *t, long int min_time, long int max_time)
{
    stringstream s;
    s << min_time;
    s << "-";
    s << max_time;
    uint16_t num_entries, bitmap_len;
    uint8_t num_char_cols, num_int_cols, num_float_cols;
    FILE *file = fopen((BASE_DIR + t->parent->name + "/" + t->name + "/" + s.str()).c_str(), "r");
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
    for (int i = 0; i < num_entries; i++)//Inflate bitmap
    {
        vector<int> v;
        t->field_present.push_back(v);
        for (int j = 0; j < tot_cols; j++)
        {
            t->field_present[i].push_back(bitmap[j][iter] & (1 << head));
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

    t->timestamp.resize(num_entries, 0);
    t->table_insert_head = num_entries;
    fread(&t->timestamp[0], sizeof(long int), num_entries, file);

    for (int i = 0; i < num_char_cols; i++)
    {
        char *col = (char *)malloc(num_entries * char_col_len[i]);
        fread(col, char_col_len[i], num_entries, file);
        t->char_field_array.push_back(col);
        t->char_field_size.push_back(char_col_len[i]);
    }

    for (int i = 0; i < num_int_cols; i++)
    {
        int *col = (int *)malloc(num_entries * INT_SIZE);
        fread(col, INT_SIZE, num_entries, file);
        t->int_field_array.push_back(col);
    }

    for (int i = 0; i < num_float_cols; i++)
    {
        float *col = (float *)malloc(num_entries * FLOAT_SIZE);
        fread(col, FLOAT_SIZE, num_entries, file);
        t->float_field_array.push_back(col);
    }

    print_table_ptr(t);
    fclose(file);
    return 0;
}

int file_read(vector<table *> t, long int min_time, long int max_time)
{
    return 0;
}