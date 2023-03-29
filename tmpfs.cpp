
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include "sys/stat.h"
#include <time.h>
#include "errno.h"
#include <vector>
#include <pthread.h>
#include <cstring>

#include "tmpfs.h"
// Creates and initialises the tmpfs. RAM based fast filesystem to store data before writing to disk.
// All tmpfs init errors are fatal, terminate database on error.
int tmpfs_init()
{
    // // Check if mountpoint exists
    // struct stat sb;
    // int err_ret = 0;
    // if (stat(TMPFS_MOUNTPOINT.c_str(), &sb) == 0)
    // {
    //     printf("WARN : Mountpoint already exists, the database may overwrite the contents.....\n");
    // }
    // else
    // {
    //     printf("INFO : Creating mount directory......");
    //     string cmd = "mkdir " + TMPFS_MOUNTPOINT;
    //     err_ret = system(cmd.c_str());
    //     if (!err_ret)
    //         printf("Success\n");
    //     else
    //     {
    //         printf("Failed.....Aborting\n");
    //         return 1;
    //     }
    // }

    // // Create and mount tmpfs, user will be prompted for sudo
    // string cmd = "sudo mount -t tmpfs -o size=" + TMPFS_SIZE + " tmpfs " + TMPFS_MOUNTPOINT;
    // printf("INFO : Creating amd mounting tmpfs......");
    // err_ret = system(cmd.c_str());
    // if (!err_ret)
    //     printf("Success\n");
    // else
    // {
    //     printf("Failed.....Aborting\n");
    //     return 1;
    // }
    db_memory = (memory_engine *)malloc(sizeof(memory_engine));
    return 0;
}

// Unmount and delete contents
int tmpfs_deinit()
{
    printf("WARN : Preparing to delete temporary filesystem.....\n");
    int err_ret = 0;
    string cmd = "sudo umount " + TMPFS_MOUNTPOINT;
    printf("INFO : Unmounting tmpfs........");
    err_ret = system(cmd.c_str());
    if (!err_ret)
        printf("Success\n");
    else
    {
        printf("Failed.....Check if the fs is being used by another process/service\n");
        return 1;
    }
    cmd = "sudo rm -rf " + TMPFS_MOUNTPOINT;
    printf("INFO : Deleting tmpfs......");
    err_ret = system(cmd.c_str());
    if (!err_ret)
        printf("Success\n");
    else
    {
        printf("Failed.....Check if the mountpoint is being used by another process/service\n");
        return 1;
    }
    return 0;
}

// Adds a new database if it does not exists.
// Creates the db directory
int tmpfs_create_db(string db_name)
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

    database *db = (database *)malloc(sizeof(database));
    if (db == NULL)
    {
        printf("ERROR : Malloc failed......\n");
        pthread_mutex_unlock(&(db_memory->db_mem_lock));
        return 1;
    }
    db->name = db_name;
    db_memory->databases.push_back(db);
    // mkdir((TMPFS_MOUNTPOINT + "/" + db_name).c_str(), 0777);
    pthread_mutex_unlock(&(db_memory->db_mem_lock));
    return 0;
}

// Creates a new table in the database and creates its directories
int tmpfs_create_table(string db_name, string table_name, vector<string> field_names, vector<int> field_sizes)
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
            table *t = (table *)malloc(sizeof(table));

            string table_dir = TMPFS_MOUNTPOINT + "/" + db_name + "/" + table_name + "/";
            t->name = table_name;
            t->parent = db_memory->databases[i];
            for (int i = 0; i < field_names.size(); i++)
            {
                t->field_name.push_back(field_names[i]);
                t->field_size.push_back(field_sizes[i]);
                t->array_index.push_back(0);
                char *arr = (char *)malloc(FIELD_BUFFER_SIZE * field_sizes[i]);
                if (arr)
                {
                    t->field_array.push_back(arr);
                }
                else 
                {
                    printf("ERROR : Malloc failed, cannot create table..\n");
                }
            }

            db_memory->databases[i]->tables.push_back(t);
            // Create table directory
            // mkdir((TMPFS_MOUNTPOINT + "/" + db_name + "/" + table_name).c_str(), 0777);
            pthread_mutex_unlock(&db_memory->databases[i]->database_lock);
            return 0;
        }
        pthread_mutex_unlock(&db_memory->databases[i]->database_lock);
    }
    printf("ERROR : Database with name '%s' does not exist", db_name.c_str());
    return 1;
}

/**
 * Insert data into table. For each table data gets inserted into the currently open file,
 * to be changed to handle late packets.
 * Creates the data file if it was not created or got flushed to the disk
 */
int tmpfs_insertEntry(string db_name, string table_name, string timestamp, vector<char *> entries)
{
    for (int i = 0; i < db_memory->databases.size(); i++)
    {
        if (db_memory->databases[i]->name == db_name) // db exists
        {
            for (int j = 0; j < db_memory->databases[i]->tables.size(); j++)
            {

                pthread_mutex_lock(&db_memory->databases[i]->tables[j]->table_lock);
                if (db_memory->databases[i]->tables[j]->name == table_name) // table exists
                {

                    db_memory->databases[i]->tables[j]->timestamp.push_back(timestamp); // Insert timestamp
                    for (int k = 0; k < entries.size(); k++)                            // Insert entries
                    {
                        
                        db_memory->databases[i]->tables[j]->field_array[k][db_memory->databases[i]->tables[j]->array_index[k]];
                        

                        memcpy(&db_memory->databases[i]->tables[j]->field_array[k][db_memory->databases[i]->tables[j]->array_index[k]],
                               entries[k], db_memory->databases[i]->tables[j]->field_size[k]);

                        db_memory->databases[i]->tables[j]->array_index[k] += db_memory->databases[i]->tables[j]->field_size[k];
                    }
                    if (db_memory->databases[i]->tables[j]->timestamp.size() >= BUFFER_SIZE_TRIGGER)
                    {
                        // Initiate compression and disk write....
                    }
                    pthread_mutex_unlock(&db_memory->databases[i]->tables[j]->table_lock);
                    return 0;
                }
                pthread_mutex_unlock(&db_memory->databases[i]->tables[j]->table_lock);
            }
            printf("ERROR : Table with name '%s' does not exist", table_name.c_str());
            return 1;
        }
    }
    printf("ERROR : Database with name '%s' does not exist", db_name.c_str());
    return 1;
}

int main()
{
    tmpfs_init();
    tmpfs_create_db("test_db");
    vector<string> names;
    names.push_back("pk123");
    names.push_back("kush234");
    vector<int> siz;
    siz.push_back(3);
    siz.push_back(4);

    tmpfs_create_table("test_db", "tab1", names, siz);
    char e1[] = {2, 4, 8};
    char e2[] = {9, 6, 7, 5};
    vector<char *> ent;
    ent.push_back(e1);
    ent.push_back(e2);
    string time = "egfh";
    tmpfs_insertEntry("test_db", "tab1", time, ent);

    // tmpfs_deinit();
}
