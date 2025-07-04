#include "defs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEBUG

int init_socket_client;
int init_socket_ss;

pthread_t init_ss_thread;
pthread_t init_client_thread;
// pthread_t pinger;
pthread_mutex_t trie_lock;
pthread_mutex_t storage_cnt_lock;
pthread_mutex_t ss_packet_copy_lock;
pthread_mutex_t lru_cache_lock;
pthread_mutex_t bool_lock;
pthread_mutex_t sockfd_lock;
pthread_mutex_t backup_lock;

ss_data storage_servers[SBUFF];
int storage_count = 0;
int cnt_cache = 0;
int socket_fd[SBUFF];

int get_time()
{
    return (int)time(NULL);
}

// add this function to init.c at the end
void init()
{
    init_socket_client = init_server_socket(PORT_CLIENT);
    init_socket_ss = init_server_socket(PORT_STORAGE);
    char emp1[SBUFF];
    char emp2[SBUFF];
    strcpy(emp1, "");
    strcpy(emp2, "");
    parent = create_node(emp1, emp2);
    pthread_mutex_init(&trie_lock, NULL);
    pthread_mutex_init(&storage_cnt_lock, NULL);
    pthread_mutex_init(&ss_packet_copy_lock, NULL);
    pthread_mutex_init(&lru_cache_lock, NULL);
    pthread_mutex_init(&sockfd_lock, NULL);
    pthread_mutex_init(&backup_lock, NULL);
    pthread_mutex_init(&bool_lock, NULL);
    first_time_done = false;
    lru_cache.cache_count = 0;
    for (int i = 0; i < C_CNT; i++)
    {
        lru_cache.cache_nodes[i].path_node = NULL;
        lru_cache.cache_nodes[i].time = 0;
    }
    for(int i = 0;i < SBUFF;i++) {
        backup[i].bss1 = i;
        backup[i].bss2 = i;
    }
    return;
}

void send_to_socket(int socket, nfs_comm response)
{
#ifdef DEBUG
    printf("[DEBUG] Sending %d\n", socket);
#endif
#ifdef DEBUG
    printf("[DEBUG] Sending %s\n", response.field1);
#endif
    if (send(socket, &response, sizeof(nfs_comm), 0) < 0)
    {
        char *error_msg = "Unable to send path to client in list";
        print_error(error_msg);
        return;
    }
}

void send_all_paths(node *tmp, int socket, char *parent_path)
{
    if (!tmp)
        return;
    char full_path[MBUFF];
    for (int i = 0; i < MAX_CHILDREN; i++)
    {
        if (tmp->next_node[i])
        {
            if (parent_path && parent_path[0] != '\0')
            {
                snprintf(full_path, sizeof(full_path), "%s/%s", parent_path, tmp->next_node[i]->path);
            }
            else
            {
                snprintf(full_path, sizeof(full_path), "%s", tmp->next_node[i]->path);
            }
            int ssid = tmp->next_node[i]->belong_to_storage_server_id;
            if (storage_servers[ssid].available == NOT_AVAILABLE && storage_servers[(backup[ssid].bss1)].available == NOT_AVAILABLE && storage_servers[backup[ssid].bss2].available == NOT_AVAILABLE)
            {
                continue;
            }
            nfs_comm response;
            response.type = LIST_REQ;
            response.field2[0] = '\0';
            strncpy(response.field1, full_path, sizeof(response.field1) - 1);
            response.field1[sizeof(response.field1) - 1] = '\0';
            send_to_socket(socket, response);
            send_all_paths(tmp->next_node[i], socket, full_path);
        }
    }
}

void get_paths_recursive(node *current, char *current_path, char *dest, char *bef_src_path)
{
    if (!current)
        return;

    int is_leaf = 1;
    char temp_path[MBUFF];
#ifdef DEBUG
    printf("[DEBUG] Current path = %s\n", current_path);
#endif
    for (int i = 0; i < MAX_CHILDREN; i++)
    {
        if (current->next_node[i])
        {
            is_leaf = 0;
            // Append the current child index to the path
            snprintf(temp_path, MBUFF, "%s/%s", current_path, current->next_node[i]->path);
            get_paths_recursive(current->next_node[i], temp_path, dest, bef_src_path);
        }
    }

    if (is_leaf)
    {
        #ifdef DEBUG
        printf("[DEBUG] Current path leaf = %s\n", current_path);
        #endif
        // send current_path to storage server
        char final_copy[MBUFF];
        snprintf(final_copy, MBUFF, "%s/%s", dest, current_path);
        char initial_copy[MBUFF];
        snprintf(initial_copy, MBUFF, "%s", current->total_path);
        while (1)
        {
            int stat = check_file_exist(final_copy);
            if (!stat)
                break;
            char temp[MBUFF];
            snprintf(temp, MBUFF + 10, "%s_copy", final_copy);
            strcpy(final_copy, temp);
        }
        nfs_comm packet;
        packet.type = COPY_REQ;
        int ssind = get_rand_ssid_without_lock();
        #ifdef DEBUG
        printf("[DEBUG] Storage server id = %d\n", ssind);
        #endif
        if (ssind == -1)
        {
            print_error_response("No storage servers available\n", -1, ERROR);
            return;
        }
        int sock_ind = socket_fd[ssind];
        #ifdef DEBUG
        printf("[DEBUG] Sending to storage server %d, %s is final path, and %s is initial path\n", ssind, final_copy, initial_copy);
        #endif
        int ssid = get_storage_id(initial_copy);
        int storage_id = ssid;
        if (ssid == -1)
        {
            print_error_response("Source file/folder does not exist!\n", -1, FILE_NOT_FOUND);
            return;
        }
        if (storage_servers[ssid].available == NOT_AVAILABLE) {
            storage_id = backup[ssid].bss1;
            if (storage_servers[storage_id].available == NOT_AVAILABLE) {
                storage_id = backup[ssid].bss2;
            }
            if (storage_servers[storage_id].available == NOT_AVAILABLE) {
                print_error_response("No storage servers available\n", -1, ERROR);
                return;
            }
        }
        #ifdef DEBUG
        printf("[DEBUG] Storage server(src) id = %d\n", storage_id);
        #endif
        snprintf(packet.field1, 2 * MBUFF, "%s:%d", storage_servers[storage_id].ip, storage_servers[storage_id].cl_port);
        snprintf(packet.field2, 2 * MBUFF, "%s:%s", initial_copy, final_copy);
        if (send(sock_ind, &packet, sizeof(nfs_comm), 0) > 0)
        {
            #ifdef DEBUG
            // printf("[DEBUG] entered if\n");
            #endif
            insert_path(final_copy, ssind);
        }
        // should send backup data to backup servers

        char path1[MBUFF];
        snprintf(path1, MBUFF+15, "%s", final_copy);
        char path2[MBUFF];
        snprintf(path2, MBUFF+15, "BACKUP/%s", path1);
        pthread_mutex_(&backup_lock);
        if (ssind == backup[ssind].bss1) {
            pthread_mutex_unlock(&backup_lock);
            return;
        }
        else {
            send_backup_data(COPY_REQ,path1,path2,backup[ssind].bss1,ssind);
            send_backup_data(COPY_REQ,path1,path2,backup[ssind].bss2,ssind);
            #ifdef DEBUG
            printf("[DEBUG] For %d, Sent backup data to %d and %d\n", ssind, backup[ssind].bss1, backup[ssind].bss2);
            #endif
            pthread_mutex_unlock(&backup_lock);
        }
    }
}

void get_all_paths(node *root, char *dest, char *bef_src_path)
{
    if (!root)
    {
        return;
    }
    char curr_path[MBUFF];
    strcpy(curr_path, root->path);
    get_paths_recursive(root, curr_path, dest, bef_src_path);
}

bool check_file_exist(char *path)
{
    node *check_same_file = get_node(path);
    if (check_same_file)
    {
        return true;
    }
    return false;
}

void print_error_response(char *msg, int socket, int type)
{
    nfs_comm response;
    response.type = type;
    strcpy(response.field1, msg);
    send_to_socket(socket, response);
}

int get_rand_ssid_without_lock()
{
    srand((unsigned int)time(NULL));
    if (storage_count == 0)
    {
        return -1;
    }
    int num = rand() % storage_count;
    int iter = num;
    for (int i = 0; i < storage_count; i++)
    {
        if (storage_servers[iter].available == AVAILABLE)
        {
            return iter;
        }
        iter = (iter + 1) % storage_count;
    }
    return -1;
}

int get_rand_ssid()
{
    srand((unsigned int)time(NULL));
    pthread_mutex_lock(&storage_cnt_lock);
    if (storage_count == 0)
    {
        pthread_mutex_unlock(&storage_cnt_lock);
        return -1;
    }
    int num = rand() % storage_count;
    int iter = num;
    pthread_mutex_lock(&ss_packet_copy_lock);
    for (int i = 0; i < storage_count; i++)
    {
        if (storage_servers[iter].available == AVAILABLE)
        {
            pthread_mutex_unlock(&ss_packet_copy_lock);
            pthread_mutex_unlock(&storage_cnt_lock);
            return iter;
        }
        iter = (iter + 1) % storage_count;
    }
    pthread_mutex_unlock(&storage_cnt_lock);
    pthread_mutex_unlock(&ss_packet_copy_lock);
    return -1;
}

void *manage_recv_from_ss(void *arg)
{
    wait_ss_recv *respon = (wait_ss_recv *)arg;
#ifdef DEBUG
    printf("[DEBUG] Waiting for response from storage server\n");
#endif
    int stat = recv(respon->ssid, &(respon->resp), sizeof(storage_comm), MSG_WAITALL);
    if (stat <= 0)
    {
        print_error_response("Storage server closed\n", respon->client_socket, ERROR);
        return NULL;
    }
#ifdef DEBUG
    printf("[DEBUG] Received response from storage server %d\n", respon->resp.type);
#endif
    if (respon->resp.type == REQ_SUCCESS)
    {
        nfs_comm *toclient = (nfs_comm *)malloc(sizeof(nfs_comm));
        toclient->type = REQ_SUCCESS;
        toclient->field1[0] = '\0';
        toclient->field2[0] = '\0';
#ifdef DEBUG
        printf("[DEBUG] client socket = %d, path = %s\n", respon->client_socket, respon->path);
#endif
        send_to_socket(respon->client_socket, *toclient);
        pthread_mutex_lock(&trie_lock);
        insert_path(respon->path, respon->sock_ind);
        pthread_mutex_unlock(&trie_lock);
    }
    else
    {
        print_error_response("Failed to create file\n", respon->client_socket, ERROR);
    }
    return NULL;
}

void comm_with_ss(nfs_comm *packet, int socket, char *buff, int type)
{
    storage_comm *request = (storage_comm *)malloc(sizeof(storage_comm));
    if (!request)
    {
        print_error_response("Failed to allocate memory for request\n", socket, ERROR);
        return;
    }
    request->type = type;
    strcpy(request->field1, packet->field1);
    strcpy(request->field2, packet->field2);
    int new_ssid = get_rand_ssid();
    if (new_ssid == -1)
    {
        print_error_response("No storage servers available\n", socket, ERROR);
        free(request);
        return;
    }
    int store = new_ssid;
    pthread_mutex_lock(&sockfd_lock);
    new_ssid = socket_fd[new_ssid];
    pthread_mutex_unlock(&sockfd_lock);
    int stat = send(new_ssid, request, sizeof(nfs_comm), 0);
    if (stat < 0)
    {
        print_error_response("Storage server closed\n", socket, ERROR);
        free(request);
        return;
    }
    #ifdef DEBUG
    printf("[DEBUG] Sent to storage server %d, total count = %d\n", new_ssid, storage_count);
    #endif
    free(request);
    storage_comm response;
    pthread_t recvthread;
    wait_ss_recv wait_recv;
    wait_recv.ssid = new_ssid;
    wait_recv.resp = response;
    wait_recv.path = strdup(buff);
    wait_recv.sock_ind = store;
    wait_recv.client_socket = socket;
    pthread_create(&recvthread, NULL, manage_recv_from_ss, &wait_recv);
    // pthread_detach(recvthread);
    pthread_join(recvthread, NULL);
    int storage_id = store;
    char path1[MBUFF];
    snprintf(path1, MBUFF+15, "%s", packet->field1);
    char path2[MBUFF];
    snprintf(path2, MBUFF+15, "BACKUP/%s", path1);
    pthread_mutex_lock(&backup_lock);
    send_backup_data(COPY_REQ,path1,path2,backup[storage_id].bss1,storage_id);
    send_backup_data(COPY_REQ,path1,path2,backup[storage_id].bss2,storage_id);
    #ifdef DEBUG
    printf("[DEBUG] Sent backup data to %d and %d\n", backup[storage_id].bss1, backup[storage_id].bss2);
    #endif
    pthread_mutex_unlock(&backup_lock);


    return;
}

void get_path_before_src(char *source, char *finalpath)
{
    int n = strlen(source);
    int i = n - 1;
    while (i > 0)
    {
        if (source[i] == '/')
        {
            i--;
            break;
        }
        i--;
    }
    int j = 0;
    for (int j = 0; j <= i; j++)
    {
        finalpath[j] = source[j];
    }
    finalpath[j] = '\0';
}

void check_leaves_recursive(node *current, bool *send_error)
{
    if (!current)
        return;

    int is_leaf = 1;
    for (int i = 0; i < MAX_CHILDREN; i++)
    {
        if (current->next_node[i])
        {
            is_leaf = 0;
            check_leaves_recursive(current->next_node[i], send_error);
        }
    }

    if (is_leaf)
    {
        if (current->readers > 0 || current->busy == 1)
        {
            *send_error = true;
        }
    }
}

void check_leaves_recursive_copy(node *current, bool *send_error)
{
    if (!current)
        return;

    int is_leaf = 1;
    for (int i = 0; i < MAX_CHILDREN; i++)
    {
        if (current->next_node[i])
        {
            is_leaf = 0;
            check_leaves_recursive_copy(current->next_node[i], send_error);
        }
    }

    if (is_leaf)
    {
        if (current->busy == 1)
        {
            *send_error = true;
        }
    }
}

void process_request(int socket, nfs_comm *packet)
{
    int command = packet->type;
    if (command == LIST_REQ)
    {
        nfs_comm temp;
        temp.type = REQ_SUCCESS;
        temp.field1[0] = '\0';
        temp.field2[0] = '\0';
        send_to_socket(socket, temp);
        node *tmp = parent;
        if (!tmp)
        {
            return;
        }
        char empty[MBUFF];
        empty[0] = '\0';
        pthread_mutex_lock(&trie_lock);
        pthread_mutex_lock(&ss_packet_copy_lock);
        pthread_mutex_lock(&backup_lock);
        send_all_paths(tmp, socket, empty);
        pthread_mutex_unlock(&trie_lock);
        pthread_mutex_unlock(&backup_lock);
        pthread_mutex_unlock(&ss_packet_copy_lock);
        nfs_comm done;
        done.type = STOP;
        done.field1[0] = '\0';
        done.field2[0] = '\0';
        send_to_socket(socket, done);
        #ifdef DEBUG
        printf("[DEBUG] Done sending all paths, also sent STOP\n");
        #endif
        close(socket);
        return;
    }
    else if (command == READ_REQ || command == RETREIVE_REQ || command == STREAM)
    {
        nfs_comm *response = (nfs_comm *)malloc(sizeof(nfs_comm));
        response->type = REQ_SUCCESS;
        pthread_mutex_lock(&trie_lock);
        int storage_id = get_storage_id(packet->field1);
        pthread_mutex_unlock(&trie_lock);
        #ifdef DEBUG
        printf("[DEBUG] Storage id = %d\n", storage_id);
        #endif
        if (storage_id == -1)
        {
            response->type = FILE_NOT_FOUND;
            strcpy(response->field1, "File not found");
            send(socket, response, sizeof(nfs_comm), 0);
            #ifdef DEBUG
            printf("[DEBUG] File not found\n");
            #endif
            free(response);
            close(socket);
            return;
        }
        pthread_mutex_lock(&trie_lock);
        node *temp = get_node(packet->field1);
        if (temp->busy == 1)
        {
            response->type = FILE_BUSY;
            strcpy(response->field1, "File is busy");
            send(socket, response, sizeof(nfs_comm), 0);
            #ifdef DEBUG
            printf("[DEBUG] File is busy\n");
            #endif
            free(response);
            close(socket);
            pthread_mutex_unlock(&trie_lock);
            return;
        }
        pthread_mutex_unlock(&trie_lock);
        int ssid = temp->belong_to_storage_server_id;
        pthread_mutex_lock(&ss_packet_copy_lock);
        if (storage_servers[ssid].available == NOT_AVAILABLE)
        {
            storage_id = backup[ssid].bss1;
            if (storage_servers[storage_id].available == NOT_AVAILABLE)
            {
                storage_id = backup[ssid].bss2;
            }
            if (storage_servers[storage_id].available == NOT_AVAILABLE)
            {
                print_error_response("No storage servers available\n", socket, FILE_NOT_FOUND);
                pthread_mutex_unlock(&ss_packet_copy_lock);
                return;
            }
        }
        pthread_mutex_unlock(&ss_packet_copy_lock);
        #ifdef DEBUG
        printf("[DEBUG] File found at id = %d\n", storage_id);
        #endif
        pthread_mutex_lock(&ss_packet_copy_lock);
        strcpy(response->field1, storage_servers[storage_id].ip);
        sprintf(response->field2, "%d", storage_servers[storage_id].cl_port);
        pthread_mutex_unlock(&ss_packet_copy_lock);
        send(socket, response, sizeof(*response), 0);
        pthread_mutex_lock(&trie_lock);
        temp->readers++;
        pthread_mutex_unlock(&trie_lock);
        recv(socket, response, sizeof(nfs_comm), MSG_WAITALL);
        pthread_mutex_lock(&trie_lock);
        temp->readers--;
        pthread_mutex_unlock(&trie_lock);
        free(response);
        close(socket);
        return;
    }
    else if (command == WRITE_REQ || command == APPEND_REQ)
    {
        nfs_comm *response = (nfs_comm *)malloc(sizeof(nfs_comm));
        response->type = REQ_SUCCESS;
        pthread_mutex_lock(&trie_lock);
        int storage_id = get_storage_id(packet->field1);
        pthread_mutex_unlock(&trie_lock);
        if (storage_id == -1)
        {
            response->type = FILE_NOT_FOUND;
            strcpy(response->field1, "File not found");
            send(socket, response, sizeof(nfs_comm), 0);
            #ifdef DEBUG
            printf("[DEBUG] File not found\n");
            #endif
            free(response);
            close(socket);
            return;
        }
        pthread_mutex_lock(&trie_lock);
        node *temp = get_node(packet->field1);
        if (temp->busy == 1 || temp->readers > 0)
        {
            response->type = FILE_BUSY;
            strcpy(response->field1, "File is busy");
            send(socket, response, sizeof(nfs_comm), 0);
            #ifdef DEBUG
            printf("[DEBUG] File is busy\n");
            #endif
            free(response);
            close(socket);
            pthread_mutex_unlock(&trie_lock);
            return;
        }
        pthread_mutex_unlock(&trie_lock);

        int ssid = temp->belong_to_storage_server_id;
        pthread_mutex_lock(&ss_packet_copy_lock);
        if (storage_servers[ssid].available == NOT_AVAILABLE)
        {
            print_error_response("No storage servers available\n", socket, FILE_NOT_FOUND);
            return;
            storage_id = backup[ssid].bss1;
            if (storage_servers[storage_id].available == NOT_AVAILABLE)
            {
                storage_id = backup[ssid].bss2;
            }
            if (storage_servers[storage_id].available == NOT_AVAILABLE)
            {
                pthread_mutex_unlock(&ss_packet_copy_lock);
                return;
            }
        }
        pthread_mutex_unlock(&ss_packet_copy_lock);


        pthread_mutex_lock(&ss_packet_copy_lock);
        strcpy(response->field1, storage_servers[storage_id].ip);
        sprintf(response->field2, "%d", storage_servers[storage_id].cl_port);
        pthread_mutex_unlock(&ss_packet_copy_lock);
        pthread_mutex_lock(&trie_lock);
        temp->busy = 1;
        pthread_mutex_unlock(&trie_lock);
        send(socket, response, sizeof(*response), 0);
        #ifdef DEBUG
        printf("[DEBUG] File found\n");
        #endif
        recv(socket, response, sizeof(nfs_comm), MSG_WAITALL);
        #ifdef DEBUG
        printf("[DEBUG] Recieved response from client\n");
        #endif
        pthread_mutex_lock(&trie_lock);
        temp->busy = 0;
        pthread_mutex_unlock(&trie_lock);


        char path1[MBUFF];
        snprintf(path1, MBUFF+15, "%s", packet->field1);
        if (storage_id != ssid) {
            snprintf(path1, MBUFF+15, "BACKUP/%s", packet->field1);
        }
        char path2[MBUFF];
        snprintf(path2, MBUFF+15, "BACKUP/%s", path1);
        pthread_mutex_lock(&backup_lock);
        if (storage_id == backup[ssid].bss1) {
            pthread_mutex_unlock(&backup_lock);
            return;
        }
        if (storage_id != backup[ssid].bss1) send_backup_data(BACKUP_REQ,path1,path2,backup[ssid].bss1,storage_id);
        if (storage_id != backup[ssid].bss2) send_backup_data(BACKUP_REQ,path1,path2,backup[ssid].bss2,storage_id);
        #ifdef DEBUG
        printf("[DEBUG] For %d, Sent backup data to %d and %d\n", storage_id, backup[storage_id].bss1, backup[storage_id].bss2);
        #endif
        pthread_mutex_unlock(&backup_lock);


        free(response);
        close(socket);
        return;
    }
    else if (command == CREATE_FILE_REQ || command == DELETE_REQ || command == CREATE_FOLDER_REQ)
    {
        if (command == CREATE_FILE_REQ || command == CREATE_FOLDER_REQ) {
            char buff[2 * MBUFF];
            snprintf(buff, sizeof(buff), "%s/%s", packet->field1, packet->field2);
            #ifdef DEBUG
            printf("[DEBUG] Creating file/folder %s\n", buff);
            #endif
            pthread_mutex_lock(&trie_lock);
            if (check_file_exist(packet->field1) == false)
            {
                print_error_response("Parent folder does not exist!\n", socket, ERROR);
                pthread_mutex_unlock(&trie_lock);
                return;
            }
            if (check_file_exist(buff))
            {
                print_error_response("File already exists!\n", socket, ERROR);
                pthread_mutex_unlock(&trie_lock);
                return;
            }
            pthread_mutex_unlock(&trie_lock);
            #ifdef DEBUG
            printf("[DEBUG] client socket bef = %d\n", socket);
            #endif
            comm_with_ss(packet, socket, buff, command);
        }
        else {
            pthread_mutex_lock(&trie_lock);
            int storage_id = get_storage_id(packet->field1);
            pthread_mutex_unlock(&trie_lock);
            if (storage_id == -1)
            {
                print_error_response("Path not found\n", socket, FILE_NOT_FOUND);
                return;
            }
            storage_comm *request = (storage_comm *)malloc(sizeof(storage_comm));
            request->type = packet->type;
            strcpy(request->field1, packet->field1);
            storage_comm *request1 = (storage_comm*) malloc(sizeof(storage_comm));
            request1->type = packet->type;
            snprintf(request1->field1, 15 + sizeof(request1->field1), "BACKUP/%s", packet->field1);
            // run a dfs, get all leaves, if in any leaf with the path : check if busy or if readers > 0.
            // If for any of them true, then send error to client (file busy)
            int max_ssid = 0;

            // get all leaves and check if their reader count or busy is set if so then send error to client
            bool send_error = false;
            pthread_mutex_lock(&trie_lock);
            node *tmp = get_node(packet->field1);
            check_leaves_recursive(tmp, &send_error);
            pthread_mutex_unlock(&trie_lock);
            if (send_error)
            {
                // send error
                nfs_comm nodelete;
                nodelete.type = FILE_BUSY;
                nodelete.field1[0] = '\0';
                nodelete.field2[0] = '\0';
                send_to_socket(socket, nodelete);
                return;
            }

            pthread_mutex_lock(&storage_cnt_lock);
            max_ssid = storage_count;
            pthread_mutex_unlock(&storage_cnt_lock);
            for (int i = 0; i < max_ssid; i++)
            {
                pthread_mutex_lock(&sockfd_lock);
                pthread_mutex_lock(&backup_lock);
                send(socket_fd[i], request, sizeof(storage_comm), 0);
                send(socket_fd[backup[i].bss1], request1, sizeof(storage_comm), 0);
                send(socket_fd[backup[i].bss2], request1, sizeof(storage_comm), 0);
                pthread_mutex_unlock(&backup_lock);
                pthread_mutex_unlock(&sockfd_lock);
                storage_comm *response = (storage_comm *)malloc(sizeof(storage_comm));
                pthread_mutex_lock(&sockfd_lock);
                recv(socket_fd[i], response, sizeof(storage_comm), MSG_WAITALL);
                pthread_mutex_unlock(&sockfd_lock);
                #ifdef DEBUG
                printf("[DEBUG] recieved : type %d, ip: %s\n", packet->type, storage_servers[i].ip);
                #endif
                free(response);
            }
            nfs_comm final_suc;
            if (delete_path(packet->field1))
            {
            #ifdef DEBUG
            printf("[DEBUG] Deleted %s successfully\n", packet->field1);
            #endif
                final_suc.type = REQ_SUCCESS;
            }
            else
            {
                print_error("Failed to delete\n");
                final_suc.type = ERROR;
                return;
            }
            delete_path_from_cache(packet->field1);
            final_suc.field1[0] = '\0';
            final_suc.field2[0] = '\0';
            send_to_socket(socket, final_suc);
            free(request);
        }
    }
    else if (command == COPY_REQ) {
        char *source = packet->field1;
        char *dest = packet->field2;
        if (!check_file_exist(source))
        {
            print_error_response("Source file/folder does not exist!", socket, FILE_NOT_FOUND);
            return;
        }
        char dest_path[2 * MBUFF];
        snprintf(dest_path, sizeof(dest_path), "%s", dest);

        bool send_error = false;
        pthread_mutex_lock(&trie_lock);
        node *tmp = get_node(packet->field1);
        check_leaves_recursive_copy(tmp, &send_error);
        pthread_mutex_unlock(&trie_lock);
        if (send_error)
        {
            nfs_comm nodelete;
            nodelete.type = FILE_BUSY;
            nodelete.field1[0] = '\0';
            nodelete.field2[0] = '\0';
            send_to_socket(socket, nodelete);
            return;
        }

        char path_before_src[MBUFF];
        pthread_mutex_lock(&trie_lock);
        get_path_before_src(source, path_before_src);
        pthread_mutex_unlock(&trie_lock);
        pthread_mutex_lock(&trie_lock);
        pthread_mutex_lock(&ss_packet_copy_lock);
        get_all_paths(tmp, dest, path_before_src);
        pthread_mutex_unlock(&ss_packet_copy_lock);
        pthread_mutex_unlock(&trie_lock);
        nfs_comm done;
        done.type = REQ_SUCCESS;
        done.field1[0] = '\0';
        done.field2[0] = '\0';
        send(socket, &done, sizeof(nfs_comm), 0);
        // finalpath is source just before source
    }
    return;
}

int get_command(int socket)
{
    nfs_comm *packet = (nfs_comm *)malloc(sizeof(nfs_comm));
    int stat = recv(socket, packet, sizeof(nfs_comm), MSG_WAITALL);
    if (stat < 0)
    {
        perror("Error in receiving data from client");
        free(packet);
        close(socket);
        return -1;
    }
#ifdef DEBUG
    printf("[DEBUG] recieved packet --> type : %d, field1 : %s, field2 : %s\n", packet->type, packet->field1, packet->field2);
#endif
    process_request(socket, packet);
    free(packet);

    return 1;
}

void *start_client_communication(void *args)
{
    int sock = *(int *)args;
    int stat = get_command(sock);
    if (stat < 0)
    {
#ifdef DEBUG
        printf("[DEBUG] Client removed for %d\n", sock);
#endif
        close(sock);
        return NULL;
    }
    close(sock);
    return NULL;
}

void getclient()
{
    struct sockaddr_in *client_addr = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
    socklen_t client_addr_len = sizeof(*client_addr);
    int *client_socket = (int *)malloc(sizeof(int));
    *client_socket = accept(init_socket_client, (struct sockaddr *)client_addr, &client_addr_len);
    if (*client_socket < 0)
    {
        char *error_msg = "Error in accepting connection from client";
        print_error(error_msg);
        return;
    }

#ifdef DEBUG
    printf("[DEBUG] Accepted connection from client!\n");
#endif

    pthread_t client_thread;
    pthread_create(&client_thread, NULL, start_client_communication, client_socket);
    // pthread_detach(client_thread);
    // pthread_join(client_thread, NULL);
    return;
}

void *get_client_data(void *args)
{
#ifdef DEBUG
    printf("[DEBUG] Waiting for clients...\n");
#endif
    while (1)
    {
        getclient();
    }
    return NULL;
}

int main()
{
    init();
    // thread for getting new storage servers
    pthread_create(&init_ss_thread, NULL, get_server_data, NULL);
    // thread for getting new clients
    pthread_create(&init_client_thread, NULL, get_client_data, NULL);

    pthread_join(init_client_thread, NULL);
    pthread_join(init_ss_thread, NULL);
    close(init_socket_client);
    close(init_socket_ss);
}