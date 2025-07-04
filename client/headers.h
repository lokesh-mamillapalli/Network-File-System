#ifndef _HEADERS_H_
#define _HEADERS_H_
#include "../common/params.h"
#include "../common/messages.h"
#include "../common/comm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/types.h> 
#include <arpa/inet.h> 
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <unistd.h>
#include <error.h>
#include <netdb.h>
#include <sys/time.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <fcntl.h> 

#define ns_ip "10.42.0.1"
#define ns_port 2000

#define NUM_THREADS 100
extern pthread_mutex_t thread_mutex;
extern pthread_t threads[NUM_THREADS];
extern bool thread_busy[NUM_THREADS];
// Currently uses only a single child thread, but added for future scalability


// Establishes connection to servers, and returns the socket file descriptor.
// requires the ip(or hostname of server) and the port it is listening on.
int connect_to_server(char* hostname, int portno);



// int request_server(int type, char* field1, char* field2, int ns_sock);

// Receives a response, stores it in first argument and returns n_bytes 
int nfs_recv(nfs_comm* response, int ns_sock);

int nfs_send(int ns_sock, int type, char* field1, char* field2);

// Sends a request, stores response in ns_response
int request_receive(int ns_sock, int type, char* field1, char* field2, nfs_comm* ns_response);




int response_type_check(nfs_comm response);



// Hnadling client opearations as a whole
int handle_read(char* file_path, int ns_sock);
int handle_write(char* file_path, int type, char choice, int ns_sock);
int handle_create(char* file_path, char* file_name, int type);
int handle_list();
int handle_delete(char* file_path);
int handle_copy(char*src, char* dest);
int handle_stream(int ns_sock, char* file_path, char* output_path);
int handle_retreive(char* path , int ns_sock);

#endif


