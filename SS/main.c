#include "../common/comm.h"
#include "../common/params.h"
#include "../common/messages.h"
char SS_IP[SBUFF];

#define DEBUG
char allpathsgiven[SBUFF][MBUFF];
char allpathsinthisdir[SBUFF][MBUFF];
int accessable_paths_count = 0;
char accessable_paths[SBUFF][MBUFF];
int ns_socket_fd;
int ping_socket;
int send_data(nfs_comm* response, int ns_sock);
pthread_mutex_t create_lock = PTHREAD_MUTEX_INITIALIZER;


void acknowledge(int socket_fd, int ack)
{
    nfs_comm ackn;
    ackn.type = ack;
    int ret = send_data(&ackn, socket_fd);
    if (ret < 0)
    {
        perror("could not send acknolege");
        // exit(1);
    }
    #ifdef DEBUG
    printf("[DEBUG] Size of nfs_comm %d\n", sizeof(nfs_comm));
    #endif
    #ifdef DEBUG
    printf("[DEBUG] sent ack of size %d\n", sizeof(ackn));
    #endif
}

void connect_to_ns(initstorage *storage)
{
#ifdef DEBUG
    printf("[DEBUG] Connecting to ns\n");
    printf("[DEBUG] %s %d %d %d and also %d\n", storage->ip, storage->ns_port, storage->cl_port, storage->path_count, accessable_paths_count);
#endif
    ns_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ns_socket_fd < 0)
    {
        perror("socket");
        // exit(1);
    }
    struct sockaddr_in ns_addr;
    ns_addr.sin_family = AF_INET;
    ns_addr.sin_port = htons(PORT_STORAGE);
    if (inet_pton(AF_INET, NS_IP, &ns_addr.sin_addr) <= 0)
    {
        perror("Invalid address/ Address not supported");
        close(ns_socket_fd);
        // exit(EXIT_FAILURE);
    }
    if (connect(ns_socket_fd, (struct sockaddr *)&ns_addr, sizeof(ns_addr)) < 0)
    {
        perror("could not connect to ns");
        // exit(1);
    }

    printf("Connect to NS Socket connected\n");
    // connnected to ns
    // send the storage server details
    #ifdef DEBUG
    printf("Printing paths\n");
    // i dont want to send first 2 characters so edit the list 
    for (int i = 0; i < storage->path_count; i++) {
        char *path = storage->paths[i];
        if (strlen(path) > 2) {  // Ensure the path has at least 2 characters
            memmove(path, path + 2, strlen(path) - 1); // Shift characters 2 places
        } else {
            path[0] = '\0'; // Empty the string if it's less than 2 characters
        }
        strcpy(storage->paths[i], path);
        printf("%s\n", storage->paths[i]);

    }
    #endif

    if (send(ns_socket_fd, storage, sizeof(*storage), 0) < 0)
    {
        perror("could not send storage details to ns");
        // exit(1);
    }

    close(ns_socket_fd);
};

int files_in_current_directory = 0;
void get_all_accessable_paths(int allpaths)
{
    int flag[allpaths];
    #ifdef DEBUG
    printf("[DEBUG] %d\n", allpaths);
    for(int i = 0; i < allpaths; i++){
        printf("[DEBUG] %s\n", allpathsgiven[i]);
    }
    #endif
    #ifdef DEBUG
    printf("[DEBUG] %d\n", files_in_current_directory);
    for(int i = 0; i < files_in_current_directory; i++){
        printf("[DEBUG] %s\n", allpathsinthisdir[i]);
    }
    #endif
    for (int i = 0; i < allpaths; i++)
    {
        for (int j = 0; j < files_in_current_directory; j++)
        {
            if (strcmp(allpathsgiven[i], allpathsinthisdir[j]) == 0)
            {
                flag[i] = 1;
                break;
            }
        }
    }
    for (int i = 0; i < allpaths; i++)
    {
        if (flag[i] == 1)
        {
            strcpy(accessable_paths[accessable_paths_count], allpathsgiven[i]);
            accessable_paths_count++;
        }
    }
};

void get_all_files_in_this_directory(char *dir_path)
{
    DIR *dir = opendir(dir_path);
    if (dir == NULL)
    {
        fprintf(stderr, "opendir: Cannot open directory %s: %s\n", dir_path, strerror(errno));
        return;
    }
    
    struct dirent *entry;
    struct stat path_stat;

    
    while ((entry = readdir(dir)) != NULL)
    {
        // Skip . and .. directories
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }
        char file_path[MBUFF];
        
        // Create full path
        snprintf(file_path, MBUFF, "%s/%s", dir_path, entry->d_name);

        // Get file/directory information
        if (stat(file_path, &path_stat) == -1)
        {
            fprintf(stderr, "Cannot get stats for %s: %s\n", file_path, strerror(errno));
            continue;
        }
        
        // Store the full file/directory path
        strcpy(allpathsinthisdir[files_in_current_directory], file_path);
        files_in_current_directory++;
        // Recursively explore subdirectories
        if (S_ISDIR(path_stat.st_mode))
        {
            char arr[MBUFF];
            strcpy(arr, file_path);


            get_all_files_in_this_directory(arr);
        }
        
        if (files_in_current_directory >= SBUFF)
        {
            fprintf(stderr, "Warning: Maximum path limit (%d) reached for directory %s.\n", SBUFF, dir_path);
            break;
        }
    }
    
    closedir(dir);
};


int send_data(nfs_comm* response, int ns_sock){
    int expected_size = sizeof(nfs_comm);
    int total_received = 0;
    // while (total_received < expected_size) {
        int n_bytes = send(ns_sock, response, sizeof(nfs_comm), MSG_WAITALL);
        #ifdef DEBUG
        printf("[DEBUG] Sent %d bytes\n", n_bytes);
        #endif
        if (n_bytes < 0) {
            perror("Error in receiving data");
            return -1; // Failure
        } else if (n_bytes == 0) {
            // Connection closed by the sender before full data was received
            fprintf(stderr, "Connection closed by peer\n");
            return -1;
        }

        // total_received += n_bytes; // Increment total bytes received
    // }
    return 0;
};




/////////////////////////////////////////////////////////////////////////////////
char* chunk_receive(int ss_sock) {
    #define TEXT_DATA
    char *message_buffer = malloc(1); // Dynamically allocate a buffer
    int buffer_size = 0;

    if (message_buffer == NULL) {
        perror("Failed to allocate memory for message buffer\n");
        return NULL;
    }

    while (1) {
        nfs_comm ss_response;
        memset(&ss_response, 0, sizeof(ss_response)); // Clear previous response

        // Ensure the full structure is received
        // int n_bytes = nfs_recv(&ss_response, ss_sock);
        // if (n_bytes <= 0) {
        //     free(message_buffer);
        //     return -1;
        // }
        if (recv(ss_sock, &ss_response, sizeof(ss_response), MSG_WAITALL) < 0)
        {
            perror("could not receive request details");
            free(message_buffer);
            return NULL;
        }

        if (ss_response.type == STOP) {
            // End of data
            return message_buffer;
          
        } else if (ss_response.type == ERROR) {
            // Handle server error
            fprintf(stderr, "Server error: %s\n", ss_response.field1);
            free(message_buffer);
            return NULL;
        } else if (ss_response.type != PACKET) {
            // Handle unexpected response
            fprintf(stderr, "Unexpected response type: %d\n", ss_response.type);
            free(message_buffer);
            return NULL;
        }

        // Resize the buffer and append the received data
        char *temp_buffer = realloc(message_buffer, buffer_size + MBUFF);
        if (temp_buffer == NULL) {
            perror("Failed to resize buffer");
            free(message_buffer);
            return NULL;
        }
        message_buffer = temp_buffer;
        memcpy(message_buffer + buffer_size, ss_response.field1, MBUFF);

        #ifdef TEXT_DATA
        // Null-terminate the received data
        if (strlen(ss_response.field1) < MBUFF) {
            ss_response.field1[strlen(ss_response.field1)] = '\0';
            buffer_size += strlen(ss_response.field1);
        }
        else {
            buffer_size += MBUFF;
        }
        #endif
    }
        
}

////////////////////////////////////////////////////////////////////////////////////////
int create(char *path, int type)
{
    // pthread_mutex_lock(&create_lock);
    #ifdef DEBUG
    printf("[DEBUG] create function %s\n",path);
    #endif
    char temp[2 * MBUFF];
    char temp2[2 * MBUFF];
    strcat(temp2,"./");
    strcat(temp2,path);
    strcat(temp,path);
    char folders[SBUFF][SBUFF];

    char *token = strtok(temp, "/");
    int count = 0;

    // Split the path into directory names
    while (token != NULL) {
        strcpy(folders[count], token);
        count++;
        token = strtok(NULL, "/");
    }
    printf("count: %d\n", count);

    char current_path[2 * MBUFF] ;
    //clear the currentpath
    current_path[0] = '\0';
    strcat(current_path, ".");
    printf("current path: %s\n", current_path);


    for (int i = 0; i < count-1; i++) {
        // Append each folder to the current path
        strcat(current_path, "/");
        strcat(current_path, folders[i]);
        printf("current path: %s\n", current_path);

        
        struct stat file_stat;
        if (stat(current_path, &file_stat) < 0) { 
            // Folder does not exist, create it
            if (mkdir(current_path, 0777) < 0) {
                perror("Could not create folder");
                // pthread_mutex_unlock(&create_lock);
                return -1;
            }
            else
            {
                #ifdef DEBUG
                printf("[DEBUG] Successfully created folder: %s\n", current_path);
                #endif
            }
        }

    }


    // Prepare the full path for the final file or folder
    #ifdef DEBUG
    printf("[DEBUG] temp: %s\n", temp2);
    #endif


    if (type == CREATE_FILE_REQ) {
        // Create the file
        FILE *file = fopen(temp2, "w");
        if (file == NULL) {
            // perror("Could not create sssssfile");
            // pthread_mutex_unlock(&create_lock);
            return 0;
        }
        fclose(file);

        // acknowledge(ns_socket_fd, REQ_SUCCESS);
    } else if (type == CREATE_FOLDER_REQ) {
        // Create the folder
        if (mkdir(temp2, 0777) < 0) {
            perror("Could not create folder");
            // pthread_mutex_unlock(&create_lock);
            return -1;
        }


    }
    #ifdef DEBUG
    printf("[DEBUG] Successfully created file/folder: %s\n", temp2);
    #endif
    // pthread_mutex_unlock(&create_lock);
    return 0;
}
////////////////////////////////////////////////////////////////////////////////////////
void delete_directory(const char *path) {
    struct dirent *entry;
    DIR *dir = opendir(path);

    if (dir == NULL) {
        acknowledge(ns_socket_fd, ERROR);
        perror("opendir");
        return;
    }

    while ((entry = readdir(dir)) != NULL) {
        char full_path[1024];
        
        // Skip "." and ".." entries
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);

        struct stat statbuf;
        if (stat(full_path, &statbuf) == 0) {
            if (S_ISDIR(statbuf.st_mode)) {
                // Recursively delete subdirectory
                delete_directory(full_path);
            } else {
                // Remove file
                if (remove(full_path) != 0) {
                    acknowledge(ns_socket_fd, ERROR);
                    perror("remove");
                }
            }
        } else {
            acknowledge(ns_socket_fd, ERROR);
            perror("stat");
        }
    }

    closedir(dir);

    // Finally, remove the directory itself
    if (rmdir(path) != 0) {
        perror("rmdir");
        acknowledge(ns_socket_fd, ERROR);
    }
}

////////////////////////////////////////////////////////////////////////////////







void *handle_ns_request(void *arg)
{

    nfs_comm request;
    request= *(nfs_comm *)arg;

    // tokensise the request "/"
    if (request.type == CREATE_FILE_REQ || request.type == CREATE_FOLDER_REQ) {

        char folders[SBUFF][SBUFF];
        char temp[2 * MBUFF];
        strcat(temp,"./");
        strcat(temp,request.field1);
        strcat(temp,"/");
        strcat(temp,request.field2);
        char *token = strtok(request.field1, "/");
        int count = 0;

        // Split the path into directory names
        while (token != NULL) {
            strcpy(folders[count], token);
            count++;
            token = strtok(NULL, "/");
        }
        printf("count: %d\n", count);

        char current_path[2 * MBUFF] ;
        strcat(current_path, ".");


        for (int i = 0; i < count; i++) {
            // Append each folder to the current path
            strcat(current_path, "/");
            strcat(current_path, folders[i]);
            printf("current path: %s\n", current_path);

            
            struct stat file_stat;
            if (stat(current_path, &file_stat) < 0) { 
                // Folder does not exist, create it
                if (mkdir(current_path, 0777) < 0) {
                    perror("Could not create folder");
                    acknowledge(ns_socket_fd, ERROR);
                    return NULL;
                }
                else
                {
                    #ifdef DEBUG
                    printf("[DEBUG] Successfully created folder: %s\n", current_path);
                    #endif
                }
            }

        }

        // Prepare the full path for the final file or folder
        #ifdef DEBUG
        printf("[DEBUG] temp: %s\n", temp);
        #endif


        if (request.type == CREATE_FILE_REQ) {
            // Create the file
            FILE *file = fopen(temp, "w");
            if (file == NULL) {
                perror("Could not create file");
                acknowledge(ns_socket_fd, ERROR);
                return NULL;
            }
            fclose(file);
            acknowledge(ns_socket_fd, REQ_SUCCESS);
        } else if (request.type == CREATE_FOLDER_REQ) {
            // Create the folder
            if (mkdir(temp, 0777) < 0) {
                perror("Could not create folder");
                acknowledge(ns_socket_fd, ERROR);
                return NULL;
            }
            acknowledge(ns_socket_fd, REQ_SUCCESS);
        }
    }

    if (request.type == DELETE_REQ) {
        #ifdef DEBUG
        printf("[DEBUG] delete request\n");
        #endif

        char temp[2 * MBUFF] = {0}; // Initialize temp to avoid garbage data
        strcat(temp, "./");
        strcat(temp, request.field1);

        struct stat statbuf;

        // Check if temp exists and whether it's a file or a directory
        if (stat(temp, &statbuf) == 0) {
            if (S_ISDIR(statbuf.st_mode)) {
                #ifdef DEBUG
                printf("[DEBUG] '%s' is a directory. Deleting...\n", temp);
                #endif

                // Recursive directory deletion
                delete_directory(temp);
                #ifdef DEBUG
                printf("[DEBUG] Directory '%s' deleted successfully.\n", temp);
                #endif
                acknowledge(ns_socket_fd, REQ_SUCCESS);

            } else if (S_ISREG(statbuf.st_mode)) {
                #ifdef DEBUG
                printf("[DEBUG] '%s' is a file. Deleting...\n", temp);
                #endif

                // Delete the file
                if (remove(temp) != 0) {
                    perror("Error deleting file");
                    acknowledge(ns_socket_fd, ERROR);
                } else {
                    printf("File '%s' deleted successfully.\n", temp);
                    acknowledge(ns_socket_fd, REQ_SUCCESS);
                }
            } else {
            
                printf("Error: '%s' is neither a file nor a directory.\n", temp);
                acknowledge(ns_socket_fd, ERROR);
            }
        } else {
            perror("stat failed");
            printf("Error: Path '%s' does not exist.\n", temp);
            acknowledge(ns_socket_fd, ERROR);
        }
    }
    if (request.type == COPY_REQ || request.type == BACKUP_REQ)
    {
        // pthread_mutex_lock(&copy_lock);
        printf("copy request\n");
        int main_type = request.type;
        // ip:port in field1
        // src_path:dest_path in field2
        char ip[SBUFF];
        char ports[SBUFF];
        char src_path[MBUFF];
        char dest_path[MBUFF];

        char field1_copy[MBUFF];
        char field2_copy[MBUFF];

        // Make copies of the original strings
        strcpy(field1_copy, request.field1);
        strcpy(field2_copy, request.field2);
        printf("field1_copy: %s\n", field1_copy);
        printf("field2_copy: %s\n", field2_copy);


        char *saveptr1, *saveptr2;
        char *token = strtok_r(field1_copy, ":", &saveptr1);
        int count = 0;
        while (token != NULL) {
            if (count == 0) {
                strcpy(ip, token);
                printf("ip: %s\n", ip);
            }
            if (count == 1) {
                strcpy(ports, token);
                printf("port: %s\n", ports);
            }
            token = strtok_r(NULL, ":", &saveptr1);
            count++;
        }

        count = 0;
        char *token2 = strtok_r(field2_copy, ":", &saveptr2);
        while (token2 != NULL) {
            if (count == 0) {
                strcpy(src_path, token2);
                printf("src: %s\n", src_path);
            }
            if (count == 1) {
                strcpy(dest_path, token2);
                printf("dest: %s\n", dest_path);
            }
            token2 = strtok_r(NULL, ":", &saveptr2);
            count++;
        }

        int port=atoi(ports);

        #ifdef DEBUG
        printf("[DEBUG] src: %s\n", src_path);
        printf("[DEBUG] dest: %s\n", dest_path);
        printf("[DEBUG] ip: %s\n", ip);
        printf("[DEBUG] port: %d\n", port);
        #endif
        int ss_socket_fd;
        struct sockaddr_in ss_addr;
        ss_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (ss_socket_fd < 0)
        {
            perror("ss socket while in cl");
            // exit(1);
        }
        ss_addr.sin_family = AF_INET;
        ss_addr.sin_port = htons(port);
        ss_addr.sin_addr.s_addr = INADDR_ANY;
        if (inet_pton(AF_INET, ip, &ss_addr.sin_addr) <= 0)
        {
            perror("Invalid address/ Address not supported");
            close(ss_socket_fd);
            return NULL;
            // exit(EXIT_FAILURE);
        }
        if (connect(ss_socket_fd, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0)
        {
            perror("could not connect to ns");
            return NULL;
            // exit(1);
        }
        nfs_comm request;
        request.type = READ_REQ;
        strcpy(request.field1, src_path);
        if (send(ss_socket_fd, &request, sizeof(request), 0) < 0)
        {
            perror("could not send request to storage server");
            return NULL;
        }
        nfs_comm response;
        if (recv(ss_socket_fd, &response, sizeof(response), MSG_WAITALL) < 0)
        {
            perror("could not receive response from storage server");
            return NULL;
        }
    

        // pthread_mutex_lock(&create_lock);
        if (response.type == ERROR)
        {
            printf("ERROR : %s\n", response.field1);
            close(ss_socket_fd);
            // pthread_mutex_unlock(&create_lock);
            return NULL;
        }
        if (response.type == REQ_SUCCESS)
        {
            // IS A FILE
         
            #ifdef DEBUG
            printf("[DEBUG] Src is a path: Got data from src path\n");
            #endif
            char* data = chunk_receive(ss_socket_fd);
            #ifdef DEBUG
            printf("copy : data received from src: %s\n", data);
            #endif
            if (main_type == COPY_REQ){
                    
                    create(dest_path, CREATE_FILE_REQ);
                    FILE *file = fopen(dest_path, "w");
                    if (file == NULL)
                    {
                        perror("could not open file");
                        close(ss_socket_fd);
                        // pthread_mutex_unlock(&create_lock);
                        return NULL;
                    }
                    fwrite(data,1,strlen(data),file);
                    fclose(file);

            }
            else if (main_type == BACKUP_REQ)
            {
                // check whether the file exists or not
                if (access(dest_path, F_OK) != 0) {
                    perror("File does not exist");
                    nfs_comm ack;
                    ack.type = FILE_NOT_FOUND;
                    if (send(ns_socket_fd, &ack, sizeof(ack), 0) < 0)
                    {
                        perror("could not send response to ns");
                        close(ss_socket_fd);
                        // pthread_mutex_unlock(&create_lock);
                        return NULL;
                    }
                    close(ss_socket_fd);
                    // pthread_mutex_unlock(&create_lock);
                    return NULL;
                }


                FILE *file = fopen(dest_path, "w");
                if (file == NULL)
                {
                    perror("could not open file");
                    acknowledge(ns_socket_fd, ERROR);
                    close(ss_socket_fd);
                    // pthread_mutex_unlock(&create_lock);
                    return NULL;
                }
                fwrite(data,1,strlen(data),file);
                fclose(file);
            }

        }
        else if (response.type == IS_FOLDER)
        {
            
                // IS A FOLDER
                #ifdef DEBUG
                printf("[DEBUG] Src is a folder\n");
                #endif
                create(dest_path, CREATE_FOLDER_REQ);
            
        }
        // pthread_mutex_unlock(&create_lock);
        printf("type is %d\n",response.type);
        nfs_comm ack;
        ack.type = REQ_SUCCESS; 
        if (send(ns_socket_fd, &ack, sizeof(ack), 0) < 0)
        {
            perror("could not send ack");
            return NULL;
        }



        close(ss_socket_fd);
    }

};










void *handle_cl_request(void *nun)
{
    #ifdef DEBUG
    printf("[DEBUG] entered client with socket = %d\n", *(int *)nun);
    #endif
    nfs_comm request;
    int socket_fd = *(int *)nun;
    if (recv(socket_fd, &request, sizeof(request), MSG_WAITALL) < 0)
    {
        perror("could not receive request details");
        // exit(1);
    }
    #ifdef DEBUG
    // printf stuff in the request
    printf("[DEBUG]request type: %d %s %s\n", request.type, request.field1, request.field2);
    
    #endif
    // handle the request
    if (request.type == READ_REQ)
    {
        struct stat path_stat;
        char buffer2[MBUFF];
        strcat(buffer2, "./");
        strcat(buffer2, request.field1);
    
        if (stat(buffer2, &path_stat) != 0) {
            perror("stat failed");
            // clear buffer
            buffer2[0] = '\0';
            strcat(buffer2, "./BACKUP/");
            strcat(buffer2, request.field1);
            printf("buffer2: %s\n", buffer2);
            if(stat(buffer2, &path_stat) != 0){
                perror("stat failed");
                acknowledge(socket_fd, FILE_NOT_FOUND);
                return NULL;
            }
   

        }

        if (S_ISDIR(path_stat.st_mode)) {
            // It's a directory
            fprintf(stderr, "Path is a directory, not a file: %s\n", buffer2);
            acknowledge(socket_fd, IS_FOLDER);  // Send appropriate response for directories
            return NULL;
        } else if (!S_ISREG(path_stat.st_mode)) {
            // It's not a regular file
            fprintf(stderr, "Path is not a regular file: %s\n", buffer2);
            // add BACKUP to the buffer2 at start
       


            acknowledge(socket_fd, FILE_NOT_FOUND);  // Send appropriate response
            return NULL;
        }
        FILE *file = fopen(buffer2, "r");
        if(file == NULL){
            perror("could not open file in normal folders");

            acknowledge(socket_fd, FILE_NOT_FOUND);
            return NULL;
        }
        acknowledge(socket_fd, REQ_SUCCESS);
        #ifdef DEBUG
        printf("[DEBUG] sent acknowledgement\n");
        #endif
        // sleep(1);
        char buffer[MBUFF];
        while(fgets(buffer, MBUFF, file) != NULL){
            #ifdef DEBUG
            printf("[DEBUG] got line  %s\n", buffer);
            #endif
            nfs_comm response;
            response.type = PACKET;
            strcpy(response.field1, buffer);
            int stat = send_data(&response, socket_fd);
            if(stat< 0){
                perror("could not send file content");
                // exit(1);
            }
            #ifdef DEBUG
            printf("[DEBUG] sent %s\n", buffer);
            printf("[DEBUG] size of send %d\n", sizeof(response));
            #endif
            // sleep(1);
        }
        nfs_comm response;
        response.type = STOP;
        int stat = send_data(&response, socket_fd);
        if(stat < 0){
            perror("could not send stop");
            // exit(1);
        }
        #ifdef DEBUG
        printf("[DEBUG] size of send %d\n", sizeof(response));
        printf("[DEBUG] Done sending all packets and sent STOP :)\n");
        #endif


        close(socket_fd);

        fclose(file);
        sleep(1);

        return NULL;
    }
    else if(request.type == WRITE_REQ || request.type == APPEND_REQ)
    {
       #define CHUNK_THRESHOLD 0 // Define a threshold for asynchronous write
        #define FLUSH_INTERVAL 5   // Define how frequently to flush chunks to disk in async mode

         int main_type = request.type;
        char choice = request.field2[0];
        #ifdef DEBUG
        printf("[DEBUG] write request\n");
        #endif
        char buffer2[MBUFF];
        strcat(buffer2, "./");
        strcat(buffer2, request.field1);
        // Check if file exists
        if (access(buffer2, F_OK) == -1) {
            
            buffer2[0] = '\0';
            strcat(buffer2, "./BACKUP/");
            strcat(buffer2, request.field1);
            if (access(buffer2, F_OK) == -1) {
                perror("File does not exist");
                acknowledge(socket_fd, FILE_NOT_FOUND);
                return NULL;
            }
       
        }

        FILE* file;
        if (main_type == WRITE_REQ) {
            file = fopen(buffer2, "w");
             if (file == NULL) 
             {
                perror("could not open file");
                acknowledge(socket_fd, FILE_NOT_FOUND);
                return NULL;
            }
            fclose(file);
            file = fopen(buffer2,"a");
        } else {
            file = fopen(buffer2, "a");
        }

        if (file == NULL) {
            perror("could not open file");
            acknowledge(socket_fd, FILE_NOT_FOUND);
            return NULL;
        }

        // Send acknowledgment of request success
        acknowledge(socket_fd, REQ_SUCCESS);

        nfs_comm response;
        int chunk_count = 0;
        int async_mode = FALSE;
        #define MAX_LOCAL_BUFFER_SIZE 500000
        char local_buffer[MAX_LOCAL_BUFFER_SIZE];
        int bufsize = 0;
        int cnt = 0;
        int SYNC = 1;

        while (TRUE) {
            memset(&response, 0, sizeof(response));

            // Receive the next chunk of data from the client
            // if (nfs_recv(&response, socket_fd) <= 0) {
            //     fclose(file);
            //     return NULL;
            // }
                cnt ++;
                if (cnt > 100)
                {
                    if (fwrite(local_buffer, 1, bufsize, file) != bufsize) {
                        perror("[ERROR] Failed to write data to disk");
                        fclose(file);
                        acknowledge(socket_fd, ERROR);
                        return NULL;
                    }
                    bufsize = 0;
                    cnt = 0;
                    if (SYNC == 1 && choice=='N')
                    {
                        SYNC = 0;
                        acknowledge(socket_fd, ACK1);
                    }

                }
                if (recv(socket_fd, &response, sizeof(response), MSG_WAITALL) < 0)
                {
                    perror("could not receive request details");
                    fclose(file);
                    return NULL;
                    // exit(1);
                }

                // Check for ERROR response
                if (response.type == ERROR) {
                    fprintf(stderr, "[ERROR] Received an ERROR response from the client\n");

                    // Clear the file if this is a WRITE request
                    if (main_type == WRITE_REQ) {
                        freopen(buffer2, "w", file); // Clear file by reopening in write mode
                        #ifdef DEBUG
                        printf("[DEBUG] Cleared file: %s due to ERROR\n", buffer2);
                        #endif
                    }

                    fclose(file);
                    acknowledge(socket_fd, ERROR);
                    return NULL;
                }

                // Check for STOP signal from client
                if (response.type == STOP) {
                    break;
                }

                // Write the chunk of data to the file
                // fwrite(response.field1, 1, MBUFF, file);
                memcpy(local_buffer + bufsize, response.field1, MBUFF);
                bufsize += MBUFF;

                chunk_count++;
            }
            if (bufsize > 0)
            {
                 if (fwrite(local_buffer, 1, bufsize, file) != bufsize) {
                        perror("[ERROR] Failed to write data to disk");
                        fclose(file);
                        acknowledge(socket_fd, ERROR);
                        return NULL;
                    }
            }


            if (SYNC == 1)
            {
                acknowledge(socket_fd, ACK1);
                acknowledge(socket_fd, ACK2);
            }
            else if (SYNC == 0)
            {
                sleep(10);
                acknowledge(socket_fd, ACK2);
            }

            
            fclose(file);
            return NULL;
    }
    if (request.type == RETREIVE_REQ)
    {
        // get the file info
        char file_path[MBUFF];
        strcpy(file_path, request.field1);
        struct stat file_stat;
        if(stat(file_path, &file_stat) < 0){
            perror("could not get file info");
            // prepend ./BACKUP/ to the file path
            snprintf(file_path, sizeof(file_path), "%s%s", "./BACKUP/", request.field1);
            if(stat(file_path, &file_stat) < 0){
                perror("could not get file info");
                acknowledge(socket_fd, ERROR);
                return NULL;
            }

        }
        acknowledge(socket_fd, REQ_SUCCESS);
        char to_send[MBUFF];
        nfs_comm response;
        // get permission;
        strcat(to_send,"Permission: ");
        strcat(to_send, (S_ISDIR(file_stat.st_mode)) ? "d" : "-");
        strcat(to_send, (file_stat.st_mode & S_IRUSR) ? "r" : "-");
        strcat(to_send, (file_stat.st_mode & S_IWUSR) ? "w" : "-");
        strcat(to_send, (file_stat.st_mode & S_IXUSR) ? "x" : "-");
        strcat(to_send, (file_stat.st_mode & S_IRGRP) ? "r" : "-");
        strcat(to_send, (file_stat.st_mode & S_IWGRP) ? "w" : "-");
        strcat(to_send, (file_stat.st_mode & S_IXGRP) ? "x" : "-");
        strcat(to_send, (file_stat.st_mode & S_IROTH) ? "r" : "-");
        strcat(to_send, (file_stat.st_mode & S_IWOTH) ? "w" : "-");
        strcat(to_send, (file_stat.st_mode & S_IXOTH) ? "x" : "-");
        strcat(to_send, "\n");
        // get size
        strcat(to_send, "Size: ");
        char size[MBUFF];
        sprintf(size, "%ld", file_stat.st_size);
        strcat(to_send, size);
        strcat(to_send, "\n");
        // get last access time
        strcat(to_send, "Last access time: ");
        strcat(to_send, ctime(&file_stat.st_atime));
        strcat(to_send, "\n");
        // get last modification time
        strcat(to_send, "Last modification time: ");
        strcat(to_send, ctime(&file_stat.st_mtime));
        strcat(to_send, "\n");
        response.type = REQ_SUCCESS;
        strcpy(response.field1, to_send);
        # ifdef DEBUG
        printf("[DEBUG] %s\n", to_send);
        #endif
        if(send(socket_fd, &response, sizeof(nfs_comm), 0) < 0){
            perror("could not send file info");
            // exit(1);
        }
        #ifdef DEBUG
        printf("[DEBUG] Size of nfs_comm %d\n", sizeof(nfs_comm));
        #endif
        // acknowledge(socket_fd, REQ_SUCCESS);
        return NULL;
        



    }
    if(request.type==STREAM)
    {
        FILE *file = fopen(request.field1, "rb");
        if(file == NULL){
            perror("could not open file");
            acknowledge(socket_fd, FILE_NOT_FOUND);
            return NULL;
        }
        acknowledge(socket_fd, REQ_SUCCESS);
        
        // Get file size
        fseek(file, 0, SEEK_END);
        long file_size = ftell(file);
        rewind(file);

        // Send file size first
        nfs_comm size_response;
        size_response.type = PACKET;
  

        char buffer[MBUFF];
        size_t bytes_read;
        long total_sent = 0;

        while((bytes_read = fread(buffer, 1, MBUFF, file)) > 0){
            nfs_comm chunk_response;
            chunk_response.type = PACKET;
            memcpy(chunk_response.field1, buffer, bytes_read);
            if(send(socket_fd, &chunk_response, sizeof(nfs_comm), 0) < 0){
                perror("could not send file chunk");
            }

            total_sent += bytes_read;
            
            #ifdef DEBUG
            printf("[DEBUG] Sent chunk: %zu bytes (Total: %ld/%ld)\n", 
                   bytes_read, total_sent, file_size);
            #endif
        }

        // Send final stop packet
        nfs_comm stop_response;
        stop_response.type = STOP;
        if(send(socket_fd, &stop_response, sizeof(nfs_comm), 0) < 0){
            perror("could not send stop packet");
        }
        fclose(file);
        close(socket_fd);
        return NULL;



    }

};

void *ns_port(void *arg)
{
    int ss_socket_fd;
    struct sockaddr_in ss_addr;
    ss_socket_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (ss_socket_fd == -1)
    {
        perror("ss socket");
        // exit(1);
    }
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(NS_PORT);
    ss_addr.sin_addr.s_addr = INADDR_ANY;

    if (inet_pton(AF_INET, SS_IP, &ss_addr.sin_addr) <= 0)
    {
        perror("Invalid address/ Address not supported");
        close(ss_socket_fd);
        // exit(EXIT_FAILURE);
    }
    int opt = 1;
    if (setsockopt(ss_socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        perror("setsockopt failed");
        // exit(EXIT_FAILURE);
    }
    if (bind(ss_socket_fd, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0)
    {
        perror("ss bind");

    }
    if (listen(ss_socket_fd, 100) < 0)
    {
        perror("ss listen");
    }
    char temp[SBUFF];
    socklen_t addr_len = sizeof(ss_addr);
    ns_socket_fd = accept(ss_socket_fd, (struct sockaddr *)&ss_addr, &addr_len);
    ping_socket =accept(ss_socket_fd, (struct sockaddr *)&ss_addr, &addr_len);
    if (ns_socket_fd < 0)
    {
        perror("could not accept ns connection");
        // exit(1);
    }
    printf("Connected to naming server\n");
    close(ss_socket_fd);
};
void* handle_cl_thread(void *arg)
{
    int ss_socket_fd;
    struct sockaddr_in ss_addr;
    ss_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ss_socket_fd < 0)
    {
        perror("ss socket while in cl");
        // exit(1);
    }
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(CL_PORT);
    ss_addr.sin_addr.s_addr = INADDR_ANY;
    int opt = 1;
    if (setsockopt(ss_socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        perror("setsockopt failed");
        // exit(EXIT_FAILURE);
    }
    if (bind(ss_socket_fd, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0)
    {   
        perror("ss bind while in cl");
        // exit(1);
    }
    if (listen(ss_socket_fd, SBUFF) < 0)
    {
        perror("ss listen while in cl");
        // exit(1);
    }
    while (TRUE)
    {
        int client_socket_fd;
        struct sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        if ((client_socket_fd = accept(ss_socket_fd, (struct sockaddr *)&client_addr, &len)) < 0)
        {
            perror("cl accept");
        }
        pthread_t client_thread;
        if (pthread_create(&client_thread, NULL, &handle_cl_request, &client_socket_fd) != 0)
        {
            perror("could not create client thread");
        }
    }

}
void *pings(void *arg)
{
    while (TRUE)
    {
        sleep(3);
        ping pings;
        pings.type = PING;
        if (send(ping_socket, &pings, sizeof(pings), 0) < 0)
        {
            perror("could not send ping");
        }
    }
}
void *handle_ns_thread(void *arg)
{
    nfs_comm request;
    while(1)
    {
        memset(&request, 0, sizeof(request));
        recv(ns_socket_fd, &request, sizeof(request), MSG_WAITALL);
        #ifdef DEBUG
        printf("[DEBUG]request type: %d %s %s\n", request.type, request.field1, request.field2);
        #endif
        pthread_t handle_ns_requests;

        if (pthread_create(&handle_ns_requests, NULL, &handle_ns_request, &request) != 0)
        {
            perror("could not create client thread");
            // exit(1);
        }
        // sleep for 200 ms
        usleep(700000);

    }

}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void initialize_storage(initstorage *storage_server)
{
    storage_server->path_count = accessable_paths_count;
    for (int i = 0; i < accessable_paths_count; i++)
    {
        strcpy(storage_server->paths[i], accessable_paths[i]);
    }
    storage_server->cl_port = CL_PORT;
    storage_server->ns_port = NS_PORT;
    strcpy(storage_server->ip, SS_IP);
};
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int main(int argc, char *argv[])
{
    FILE *fp = popen("hostname -I", "r"); // Command to get IP on Linux
    if (fp) {
        fgets(SS_IP, sizeof(SS_IP), fp);
        //remove backslash n
        SS_IP[strlen(SS_IP) - 2] = '\0';
        pclose(fp);
    }
    printf("IP: %s\n", SS_IP);
    initstorage storage_server;
    char server_ip[SBUFF];
    storage_server.ns_port = NS_PORT;
    storage_server.cl_port = CL_PORT;
    // initialize the mutex

    int allpaths = argc -1;
    for (int i = 0; i < allpaths; i++)
    {
        // add ./ to the path
        char file_path[MBUFF];
        snprintf(file_path, MBUFF, "./%s", argv[i + 1]);
        strcpy(allpathsgiven[i], file_path);
    }
    get_all_files_in_this_directory(".");
    get_all_accessable_paths(allpaths);
    initialize_storage(&storage_server);
    connect_to_ns(&storage_server);
    ns_port(NULL);
    pthread_t cl_thread;
    pthread_t ns_thread;
    pthread_t ss_thread;
    if (pthread_create(&cl_thread, NULL, &handle_cl_thread, NULL) != 0)
    {
        perror("could not create client thread");
        // exit(1);
    }
    if (pthread_create(&ns_thread, NULL, &handle_ns_thread, NULL) != 0)
    {
        perror("could not create client thread");
        // exit(1);
    }

    pthread_t ping_thread;
    if (pthread_create(&ping_thread, NULL, &pings, NULL) != 0)
    {
        perror("could not create ping thread");

    }
    pthread_join(ping_thread, NULL);
    pthread_join(cl_thread, NULL);
    pthread_join(ns_thread, NULL);
    while(1)
    {


    };
}
