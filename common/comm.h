#ifndef _COMMM_H_
#define _COMMM_H_


#include "messages.h"
#include "params.h"


// Communicating the kind of command and the fields that are required for the command.
// Each command, (atleast those between NS and cleint requires atmost 2 fields).








/////////// CLIENT
typedef struct nfs_comm{
    int type;
    char field1[MBUFF];
    char field2[MBUFF];
} nfs_comm;
// Later define packets for sending and receiving data chunks between client and server... Will do later. 
/*

Here add what type refers to what type of packet of comm

*/
////////// END CLIENT

////////// STORAGE
typedef struct initstorage {
    char ip[SBUFF];
    int ns_port;
    int cl_port;
    int path_count;
    char paths[SBUFF][MBUFF];
} initstorage;

typedef struct ping {
    int type;
} ping;

typedef nfs_comm storage_comm;


////////// END STORAGE




#endif