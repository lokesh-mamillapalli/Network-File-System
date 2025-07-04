#include "headers.h"




int get_paths(int ns_sock)
{
    nfs_comm response;
    printf("Accessible Paths\n");
    while(1){
        memset(&response, 0, sizeof(response));
        #ifdef DEBUG
        printf("[DEBUG] Reading new data packet\n");
        #endif
        if (nfs_recv(&response,ns_sock)==-1)
        {
            fprintf(stderr, "Error in receiving list paths\n");
            return -1;
        }
        if (response.type == STOP)
        {
            printf("End of paths\n");
            break;
        }
        printf("%s\n",response.field1);
    }
}


int handle_list()
{
    nfs_comm ns_response;
    int ns_sock = connect_to_server(ns_ip,ns_port);
    if (ns_sock == -1)
    {
        fprintf(stderr,"Error in connecting to NS\n");
        return -1;
    }
    if (request_receive(ns_sock,LIST_REQ,NULL,NULL,&ns_response)==-1)
    {
        return -1;
    }
    if (ns_response.type == ERROR)
    {
        printf("%s\n", ns_response.field1);
        close(ns_sock);
        return -1;
    }
    else if (ns_response.type == REQ_SUCCESS)
    {
        get_paths(ns_sock);
    }
    close(ns_sock);
    return 0;

}