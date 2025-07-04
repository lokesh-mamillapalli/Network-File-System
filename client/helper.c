#include "headers.h"

int connect_to_server(char* hostname, int portno)
{
    int sockfd, n;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    #ifdef DEBUG
    printf("[DEBUG] connect_to_server attempting connect to %s : %d\n",hostname, portno);
    #endif
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
        {
            perror("connect_to_server : socket :");
            return -1; 
        }
    int flag = 1;
    if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag)) < 0) {
        perror("setsockopt TCP_NODELAY failed");
        close(sockfd);
        return 1;
    }

        #ifdef TIMEOUT
    printf("[DEBUG]TIMEOUT SET to %d\n",TIMEOUT);
    struct timeval timeout;
    timeout.tv_sec = TIMEOUT;  // Set timeout to 5 seconds
    timeout.tv_usec = 0;

    // Set socket receive timeout
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(timeout)) < 0) {
        perror("Error setting socket timeout");
        return -1;
    }
    #endif
    server = gethostbyname(hostname);

    if (server == NULL) {
        fprintf(stderr, "ERROR: no such host\n");
        return -1;
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memmove(&serv_addr.sin_addr.s_addr,server->h_addr,server->h_length);
    serv_addr.sin_port = htons(portno);
    // int done = 0;
    // for(int i = 0;i < 10;i++){
    //     if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) >= 0) 
    //         {
    //             done = 1;
    //             break;
    //         }
    //     else if ()
    //         sleep(1);
    // }
    // if (done == 0) 
    //     {
    //         perror("connect_to_server : connect");
    //         return -1;
    //     }

    if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) 
        {
            if (errno == ECONNREFUSED)
            {
                fprintf(stderr, "Connection refused by server\n");
            }
            else if (errno == EWOULDBLOCK || errno == EAGAIN)
            {
                fprintf(stderr, "Connection attempt timed out\n");
            }
            else
            {
                perror("connect_to_server : connect");
            }
            return -1;
        }
    // Set timeout for recv
    
    #ifdef DEBUG
    printf("[DEBUG] connect_to_server function successful.\n");
    #endif
    return sockfd;
}

// Sends a request of the type nfs_comm to servers, both ss and ns
int nfs_send(int ns_sock, int type, char* field1, char* field2)
{
    nfs_comm request;
    memset(&request,0,sizeof(nfs_comm));

    request.type = type;
    if (field1 != NULL) {
        strncpy(request.field1, field1, MBUFF);
    }
    if (field2 != NULL) {
        strncpy(request.field2, field2, MBUFF);
    }

    int total_sent = 0;
    int expected_size = sizeof(nfs_comm);
    // Loop until all bytes are sent

    int retry_count = 0;

    while (total_sent < expected_size) {
        int n_bytes = send(ns_sock, ((char *)&request) + total_sent, expected_size - total_sent, 0);

        if (n_bytes < 0) {
            #ifdef DEBUG
            perror("[DEBUG] nfs_send: Error in sending data");
            #endif
            #ifdef RETRIES
            if (RETRIES == 0)
            {
                return -1;
            }
            fprintf(stderr, "Retrying...\n");
            if (++retry_count >= RETRIES) {
                fprintf(stderr, "Max retries reached. Aborting send.\n");
                return -1;
            }
            continue; // Retry on failure
            #endif
            return -1;
        } else if (n_bytes == 0) {
            fprintf(stderr, "Connection closed by peer\n");
            return -1;
        }

        total_sent += n_bytes;
        retry_count = 0; // Reset
    }
    return 0;
}

// Receives a response, stores it in first argument and returns n_bytes 
int nfs_recv(nfs_comm* response, int ns_sock)
{
  // Loop until all expected bytes are received
    int total_received = 0;
    int expected_size = sizeof(nfs_comm);

    int n_bytes = recv(ns_sock, response, expected_size, MSG_WAITALL);
    if (n_bytes < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            #ifdef DEBUG
            #ifdef TIMEOUT
            fprintf(stderr, "recv: Timeout occurred: Packet took longer than %d seconds\n", TIMEOUT);
            #endif
            #endif
            fprintf(stderr,"TIMEOUT: Server took too long to respond\n");
        } else {
            perror("recv");
        }
        return -1;
    } else if (n_bytes == 0) {
        // Connection closed by the sender before full data was received
        fprintf(stderr, "Connection closed by peer\n");
        return -1;
    }
    return n_bytes;
}


// Sends a request, stores response in ns_response
int request_receive(int ns_sock, int type, char* field1, char* field2, nfs_comm* ns_response)
{
     // Send read request to NS
    #ifdef DEBUG
    printf("[DEBUG] nfs_req_recv : Sending request to NS for type %d\n", type);
    #endif
    if (nfs_send(ns_sock,type,field1,field2)==-1)
    {
        return -1;
    }
    #ifdef DEBUG
    printf("[DEBUG] nfs_req_recv: Request sent. Waiting for response of type %d\n", type);
    #endif
    // Blocking call 
    if (nfs_recv(ns_response, ns_sock) == -1)
    {
        return -1;
    }
    #ifdef DEBUG
    printf("[DEBUG] nfs_req_recv : Response-> type: %d, field1: %s, field2 : %s\n", ns_response->type, ns_response->field1, ns_response->field2);
    #endif
    return 0;

}



int response_type_check(nfs_comm response)
{
    if (response.type == ERROR)
    {
        printf("ERROR : %s\n", response.field1);
    }
    else if (response.type == FILE_NOT_FOUND)
    {
        printf("Error : FILE NOT FOUND\n");
    }
    else if (response.type == IS_FOLDER)
    {
        printf("Error : REQUESTED PATH IS FOLDER\n");
    }
    else if (response.type == FILE_BUSY)
    {
        printf("There are other clients currently accessing the same file. Kindly wait and try again later\n");
    }
    else if (response.type != REQ_SUCCESS && response.type!= PACKET)
    {
        printf("Unknown type: %d\n", response.type);
    }
    
    return response.type;
}
   