#include "defs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

cache lru_cache;

int add_to_cache(node *node)
{
    pthread_mutex_lock(&lru_cache_lock);
    char path[SBUFF];
    int storage_id = node->belong_to_storage_server_id;
    strcpy(path, node->total_path);
    // #ifdef DEBUG
    // printf("--------------------------------\n");
    // for(int i = 0;i < lru_cache.cache_count; i++) {
    //     printf("[DEBUG] Cache %d: %s\n", i, lru_cache.cache_nodes[i].path_node->total_path);
    // }
    // printf("--------------------------------\n");
    // #endif
    for (int i = 0; i < lru_cache.cache_count; i++)
    {
        if (strcmp(lru_cache.cache_nodes[i].path_node->total_path, path) == 0)
        {
            lru_cache.cache_nodes[i].time = get_time();
#ifdef DEBUG
            printf("[DEBUG] Updated cache\n");
#endif
            pthread_mutex_unlock(&lru_cache_lock);

            return 1;
        }
    }
    if (lru_cache.cache_count < C_CNT)
    {
        lru_cache.cache_nodes[lru_cache.cache_count].path_node = node;
        lru_cache.cache_nodes[lru_cache.cache_count].time = get_time();
        lru_cache.cache_count++;
#ifdef DEBUG
        printf("[DEBUG] Added to cache, cache_count = %d\n", lru_cache.cache_count);
#endif
        pthread_mutex_unlock(&lru_cache_lock);
        return 1;
    }
    else
    {
        int min = 0;
        for (int i = 1; i < C_CNT; i++)
        {
            if (lru_cache.cache_nodes[i].time < lru_cache.cache_nodes[min].time)
            {
                min = i;
            }
        }
        lru_cache.cache_nodes[min].path_node = node;
        lru_cache.cache_nodes[min].time = get_time();
#ifdef DEBUG
        printf("[DEBUG] Updated cache at index = %d\n", min);
#endif
        pthread_mutex_unlock(&lru_cache_lock);
        return 1;
    }
}

node *get_from_cache(char *path)
{
    // #ifdef DEBUG
    // printf("--------------------------------\n");
    // for(int i = 0;i < lru_cache.cache_count; i++) {
    //     printf("[DEBUG] Cache %d: %s,%d\n", i, lru_cache.cache_nodes[i].path_node->total_path, lru_cache.cache_nodes[i].path_node->belong_to_storage_server_id);
    // }
    // printf("--------------------------------\n");
    // #endif
    pthread_mutex_lock(&lru_cache_lock);
    for (int i = 0; i < lru_cache.cache_count; i++)
    {
        if (strcmp(lru_cache.cache_nodes[i].path_node->total_path, path) == 0)
        {
#ifdef DEBUG
            printf("[DEBUG] Found in cache, sending %d\n", lru_cache.cache_nodes[i].path_node->belong_to_storage_server_id);
#endif
            lru_cache.cache_nodes[i].time = get_time();
            pthread_mutex_unlock(&lru_cache_lock);

            return lru_cache.cache_nodes[i].path_node;
        }
    }
#ifdef DEBUG
    printf("[DEBUG] Not found in cache\n");
#endif
    pthread_mutex_unlock(&lru_cache_lock);

    return NULL;
}

int delete_from_cache(int storage_id)
{
    pthread_mutex_lock(&lru_cache_lock);
    cache temp;
    temp.cache_count = 0;
    for (int i = 0; i < lru_cache.cache_count; i++)
    {
        if (lru_cache.cache_nodes[i].path_node->belong_to_storage_server_id != storage_id)
        {
            temp.cache_nodes[temp.cache_count].path_node = lru_cache.cache_nodes[i].path_node;
            temp.cache_nodes[temp.cache_count].time = lru_cache.cache_nodes[i].time;
            temp.cache_count++;
        }
    }
    for (int i = 0; i < temp.cache_count; i++)
    {
        lru_cache.cache_nodes[i].path_node = temp.cache_nodes[i].path_node;
        lru_cache.cache_nodes[i].time = temp.cache_nodes[i].time;
    }
    lru_cache.cache_count = temp.cache_count;
#ifdef DEBUG
    printf("[DEBUG] storage_ig %d Deleted from cache\n", storage_id);
#endif
    pthread_mutex_unlock(&lru_cache_lock);

    return 1;
}

void delete_path_from_cache(char *path)
{
    // check if path is tehre in cache, store a temp and then delete the path from cache
    pthread_mutex_lock(&lru_cache_lock);
    cache temp;
    temp.cache_count = 0;
    for (int i = 0; i < lru_cache.cache_count; i++)
    {
        if (lru_cache.cache_nodes[i].path_node == NULL)
        {
            continue;
        }
        if (strcmp(lru_cache.cache_nodes[i].path_node->total_path, path) != 0)
        {
            temp.cache_nodes[temp.cache_count].path_node = lru_cache.cache_nodes[i].path_node;
            temp.cache_nodes[temp.cache_count].time = lru_cache.cache_nodes[i].time;
            temp.cache_count++;
        }
    }
    for (int i = 0; i < temp.cache_count; i++)
    {
        lru_cache.cache_nodes[i].path_node = temp.cache_nodes[i].path_node;
        lru_cache.cache_nodes[i].time = temp.cache_nodes[i].time;
    }
    lru_cache.cache_count = temp.cache_count;
#ifdef DEBUG
    printf("[DEBUG] %s Deleted from cache\n", path);
#endif
    pthread_mutex_unlock(&lru_cache_lock);
    return;
}