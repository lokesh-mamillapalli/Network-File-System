#include "headers.h"


int handle_delete(char* file_path)
{
    #ifdef DEBUG
    printf("[DEBUG] Initiated delete request with NS\n");
    #endif
    int ns_sock = connect_to_server(ns_ip, ns_port);
    nfs_comm ns_response;
    memset(&ns_response, 0, sizeof(ns_response));
    if (request_receive(ns_sock, DELETE_REQ, file_path, NULL, &ns_response) == -1) {
        
        return -1;
    }
    if (ns_response.type == ERROR) {
        printf("%s\n", ns_response.field1);
        return -1;
    }
    else if (ns_response.type == FILE_NOT_FOUND) {
        printf("File not found\n");
        return -1;
    }
    else if (ns_response.type == REQ_SUCCESS)
    {
        printf("Delete operation Successful\n");
    }
    close(ns_sock);
    return 0;
}