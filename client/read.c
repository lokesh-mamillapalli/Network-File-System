#include "headers.h"

// #define PRINT_DIRECTLY
#define TEXT_DATA

int chunk_receive(int ss_sock) {
    char *message_buffer = malloc(1); // Dynamically allocate a buffer
    int buffer_size = 0;

    if (message_buffer == NULL) {
        perror("Failed to allocate memory for message buffer\n");
        return -1;
    }

    while (1) {
        nfs_comm ss_response;
        memset(&ss_response, 0, sizeof(ss_response)); // Clear previous response

        // Ensure the full structure is received
        int n_bytes = nfs_recv(&ss_response, ss_sock);
        if (n_bytes <= 0) {
            fprintf(stderr,"Server Error: COuld not receive full data\n");
            free(message_buffer);
            return -1;
        }

        if (ss_response.type == STOP) {
            // End of data
            #ifndef PRINT_DIRECTLY
            write(STDOUT_FILENO, message_buffer, buffer_size);
            printf("\n");
            #endif
            free(message_buffer);
            return 0;
        } else if (ss_response.type == ERROR) {
            // Handle server error
            fprintf(stderr, "Server error: %s\n", ss_response.field1);
            free(message_buffer);
            return ERROR;
        } else if (ss_response.type != PACKET) {
            // Handle unexpected response
            fprintf(stderr, "Unexpected response type: %d\n", ss_response.type);
            free(message_buffer);
            return -1;
        }

        // Resize the buffer and append the received data
        char *temp_buffer = realloc(message_buffer, buffer_size + MBUFF);
        if (temp_buffer == NULL) {
            perror("Failed to resize buffer");
            free(message_buffer);
            return -1;
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

        #elif defined(RAW_DATA)
        buffer_size += MBUFF;
        #endif
        
        
        #ifdef PRINT_DIRECTLY
        write(STDOUT_FILENO, ss_response.field1, MBUFF);
        #endif
    }

}






// Communicates with storage server to read file
int ss_read(int ss_sock, char* file_path)
{
  
    nfs_comm ss_response;
    memset(&ss_response, 0, sizeof(ss_response));
    request_receive(ss_sock, READ_REQ, file_path, NULL, &ss_response);
    
    int type = response_type_check(ss_response);
    if (type == REQ_SUCCESS)
    {
        #ifdef DEBUG
        printf("[DEBUG] SS accepted read request\n");
        #endif
        return chunk_receive(ss_sock);
    }
    return type;
}
  




   




int handle_read(char* file_path, int ns_sock)
{
    #ifdef DEBUG
    printf("[DEBUG] Initiated read request with NS\n");
    #endif


    nfs_comm ns_response;
    if (request_receive(ns_sock, READ_REQ, file_path, NULL, &ns_response) == -1)
    {
        fprintf(stderr, "Error in communicating with server\n");
        return -1;
    }
    #ifdef DEBUG
    printf("[DEBUG] Received response from NS\n");
    #endif
    int type = response_type_check(ns_response);
    // printf("Response type: %d\n", ns_response.type);
    if (type == REQ_SUCCESS)
    {
        #ifdef DEBUG
        printf("[DEBUG] REQ_SUCCESS: Server info received- ip:%s port:%d\n", ns_response.field1, atoi(ns_response.field2));
        #endif
        int ss_sock = connect_to_server(ns_response.field1,atoi(ns_response.field2));
      
        if (ss_sock == -1)
        {
            fprintf(stderr,"Error in connecting to storage server\n");
            return -1;
        }
        #ifdef DEBUG
        printf("[DEBUG]Connected to storage Server\n");
        #endif
      
        int rcode = ss_read(ss_sock, file_path);
        close(ss_sock);
        return rcode;
    }

    return type;
}







    