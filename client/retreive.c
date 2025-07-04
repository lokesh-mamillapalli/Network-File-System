#include "headers.h"





int print_file_info(ss_sock)
{
    nfs_comm response;
   if (nfs_recv(&response,ss_sock) == -1)
   {
       return -1;
   }
     printf("FILE INFO Received\n");
    write(STDOUT_FILENO,response.field1,strlen(response.field1));
    return 0;

}

int ss_retreive(int ss_sock, char* path)
{
    #ifdef DEBUG
        printf("[DEBUG] Connected to storage server: requesting for file info\n");
    #endif
    nfs_comm response;
    if (request_receive(ss_sock,RETREIVE_REQ,path,NULL,&response) == -1)
    {
        return -1;
    }
    int rtype = response_type_check(response);
    if (rtype == REQ_SUCCESS)
    {
        print_file_info(ss_sock);
    }
    // printf("FILE INFO Received\n");
    // write(STDOUT_FILENO,response.field1,strlen(response.field1));
    return rtype;
}


int handle_retreive(char* path , int ns_sock)
{
    #ifdef DEBUG
    printf("[DEBUG] Initiated retreive request with NS\n");
    #endif
    nfs_comm ns_response;
    if (request_receive(ns_sock,RETREIVE_REQ,path,NULL,&ns_response)  == -1)
    {
        return -1;
    }

    if (ns_response.type == ERROR)
    {
        printf("%s\n",ns_response.field1);
    }
    else if (ns_response.type == FILE_NOT_FOUND)
    {
        printf("File not found\n");
    }
    else if (ns_response.type == REQ_SUCCESS)
    {
        int ss_sock = connect_to_server(ns_response.field1,atoi(ns_response.field2));
        if (ss_sock == -1)
        {
            fprintf(stderr,"Error in connecting to storage server\n");
            return -1;
        }
      
        ss_retreive(ss_sock,path);
        close(ss_sock);
    }


}