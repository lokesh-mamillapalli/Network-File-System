#include "headers.h"


int handle_create(char* file_path, char* file_name, int type)
{
    int ns_sock = connect_to_server(ns_ip, ns_port);
    if (ns_sock == -1) {
        fprintf(stderr,"Error in connecting to NS\n");
        return -1;
    }
    #ifdef DEBUG
    printf("[DEBUG] Initiated create request with NS\n");
    #endif

    nfs_comm ns_response;
    memset(&ns_response, 0, sizeof(ns_response));
    if (request_receive(ns_sock, type , file_path, file_name, &ns_response) == -1) {
        close(ns_sock);
        return -1;
    }
    int rtype = response_type_check(ns_response);
    if (rtype == REQ_SUCCESS) {
        printf("Create operation successful\n");
    }
    close(ns_sock);
    return 0;
}