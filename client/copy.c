#include "headers.h"


int handle_copy(char*src, char* dest)
{
    #ifdef DEBUG
    printf("[DEBUG] Initiated copy request with NS\n");
    #endif
    int ns_sock = connect_to_server(ns_ip, ns_port);
    nfs_comm ns_response;
    if (request_receive(ns_sock,COPY_REQ,src,dest,&ns_response) == -1)
    {
        return -1;
    }

    if (ns_response.type == ERROR)
    {
        printf("%s\n",ns_response.field1);
        return -1;
    }
    else if (ns_response.type == REQ_SUCCESS)
    {
        printf("Copy operation successful\n");
    }
    else if (ns_response.type == FILE_NOT_FOUND)
    {
        printf("Error : FILE NOT FOUND\n");
    }
    return 0;

}



