#ifndef _DEFS_H_
#define _DEFS_H_

#define DEBUG

// for NS
#include "../common/comm.h"
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <error.h>
#include <string.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/mount.h>
#include <fcntl.h>
#include <time.h>
#include <sys/select.h>
#include <pthread.h>
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <netinet/tcp.h>


#define AVAILABLE 100
#define NOT_AVAILABLE 101

int get_time();

typedef struct ss_data
{
    char ip[SBUFF];
    int ns_port;
    int cl_port;
    int available;
}  ss_data;

typedef struct send_to_ping {
    int ping_socket;
    int storage_id;
} send_to_ping;

typedef struct backup_node {
    int bss1;
    int bss2;
} backup_node;

#define MAX_CHILDREN 256
#define TIME_OUT_TIME 5

#define C_CNT 5
#define NOT_FOUND 10000

typedef struct node
{
    bool is_leaf;
    int belong_to_storage_server_id;
    char *path;
    char total_path[MBUFF];
    struct node *next_node[MAX_CHILDREN];
    int busy;
    int readers;
} node;

typedef struct copy_request{
    char ip[SBUFF];
    int port;
    char all_paths[SBUFF][MBUFF];
}copy_request;

typedef struct wait_ss_recv{
    int ssid;
    storage_comm resp;
    char* path;
    int client_socket;
    int sock_ind;
}wait_ss_recv;

typedef struct cache_node {
    node* path_node;
    time_t time;
} cache_node;

typedef struct cache {
    cache_node cache_nodes[C_CNT];
    int cache_count;
} cache;

extern cache lru_cache;
// lock

int add_to_cache(node*);
node* get_from_cache(char *path);
int delete_from_cache(int);
void delete_path_from_cache(char* path);

extern int storage_count; // locks done
extern int socket_fd[SBUFF]; // locks done
extern int init_socket_client;
extern int init_socket_ss;
extern ss_data storage_servers[SBUFF]; // done
extern backup_node backup[SBUFF]; // done

extern node *parent; // no need of lock

extern bool first_time_done; // to check if backup was done once

// tries lock done

extern pthread_t init_ss_thread;
extern pthread_t init_client_thread;
extern pthread_mutex_t trie_lock; // done
extern pthread_mutex_t storage_cnt_lock; // done
extern pthread_mutex_t ss_packet_copy_lock; // done
extern pthread_mutex_t lru_cache_lock; // done
extern pthread_mutex_t sockfd_lock; // done
extern pthread_mutex_t backup_lock;
extern pthread_mutex_t bool_lock;

void print_error(char *str);
bool Check_Node_Null(node *check_node);
node *create_node(char *parent_path, char *segment);
node* get_node(char* path);
bool insert_path(char *path, int storage_id);
int get_storage_id(char *path);
bool delete_path(char *path);

int init_server_socket(int port);
void print_error_response(char *msg, int socket, int type);
int get_rand_ssid();
void *get_server_data(void *args);
void get_ss_data();
int get_total_available();
backup_node get_backup_ss(int ssid);
int send_backup_data (int type,char* path1,char* path2,int bssid, int ssid);
bool check_file_exist(char* path);
int get_rand_ssid_without_lock();

#endif