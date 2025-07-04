#ifndef _PARAMS_H_
#define _PARAMS_H_

#include <error.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <pthread.h>
#include <netinet/tcp.h>
#include <ftw.h>





#define PORT_CLIENT 2000
#define PORT_STORAGE 2500
#define SBUFF 128
#define MBUFF 1024
#define LBUFF 4096
#define NS_PORT 3000
#define CL_PORT 4000
#define SS_PORT 5000
#define MAX_FILES 1000
#define NS_IP "10.42.0.1"
#define MAX_THREADS 100
#define TRUE 1
#define FALSE 0





#endif