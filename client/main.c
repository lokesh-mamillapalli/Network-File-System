#include "headers.h"
#define PATH_SIZE 1024


#define NUM_THREADS 100

// Currently uses only a single child thread, but added for future scalability
pthread_mutex_t thread_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_t threads[NUM_THREADS];
bool thread_busy[NUM_THREADS];
 


// Cleanup function to join threads
void cleanup_threads() {
    printf("Checking for any asynchronous write acks pending\n");
    for (int i = 0; i < NUM_THREADS; i++) {
        if (thread_busy[i]==false)
        {
            continue;
        }
        if (threads[i] != 0) { // Ensure the thread was created
            pthread_join(threads[i], NULL);
            printf("Joined thread %d\n", i);
        }
    }
}

int main()
{

    if (atexit(cleanup_threads) != 0) {
        fprintf(stderr, "Failed to set exit handler\n");
        exit(EXIT_FAILURE);
    }
    for (int i=0;i<NUM_THREADS;i++)
    {
        thread_busy[i] = false;
    }

    printf("Welcome to NFS\n");
    #ifdef DEBUG
    printf("Connecitng to NS\n");
    #endif

    while(1)
    {
        char command[100];
        printf("Enter operation: ");
        fflush(stdout);
        scanf("%s",command);
        printf("\n");

        if (strcmp(command,"EXIT")==0)
        {
            break;
        }
        else if (strcmp(command,"MAN")==0)
        {
            // Implement MAN pages of NFS for client
        }
        else if (strcmp(command,"READ")==0)
        {

            char file_path[PATH_SIZE];
            printf("...Enter file name: ");
            scanf("%s",file_path);
            int ns_sock = connect_to_server(ns_ip, ns_port);
            if (ns_sock==-1)
            {
                printf("ERROR : Error connecting to NS\n");
                continue;
            }
            handle_read(file_path, ns_sock);
            nfs_send(ns_sock,ACK1,NULL,NULL);
            close(ns_sock);
        }
        else if (strcmp(command, "WRITE")==0)
        {
            char file_path[PATH_SIZE];
            printf("...Do you want to forcefully wait for synchronous write[Y/N]\n");
              char c;
             while ((c = getchar()) != '\n' && c != EOF) {
            // Clear all input until a newline is encountered
             }
            char choice;
            scanf("%c",&choice);
            if (choice != 'Y' && choice != 'N')
            {
                printf("...Invalid choice\n");
                continue;
            }
            printf("...Enter file name: ");
            scanf("%s",file_path);
            int ns_sock = connect_to_server(ns_ip, ns_port);
            if (ns_sock == -1)
            {
                printf("ERROR : Error connecting to NS\n");
                continue;
            }
            handle_write(file_path , WRITE_REQ, choice, ns_sock);

            // closing of ns_sock will happen in the thread waiting for the second ack
        }
        else if (strcmp(command, "APPEND")==0)
        {
            char file_path[PATH_SIZE];
            printf("...Do you want to forcefully wait for synchronous write[Y/N]\n");
            char c;
             while ((c = getchar()) != '\n' && c != EOF) {
            // Clear all input until a newline is encountered
             }
            char choice;
            scanf("%c",&choice);
            if (choice != 'Y' && choice != 'N')
            {
                printf("...Invalid choice\n");
                continue;
            }
            printf("...Enter file name: ");
            scanf("%s",file_path);
            int ns_sock = connect_to_server(ns_ip, ns_port);
            if (ns_sock == -1)
            {
                printf("ERROR : Error connecting to NS\n");
                continue;
            }
            handle_write(file_path,APPEND_REQ, choice,ns_sock);
            // nfs_send(ns_sock,ACK1,NULL,NULL);
            // close(ns_sock);
        }
        else if (strcmp(command, "CREATE_FILE")==0)
        {
            char file_path[PATH_SIZE];
            printf("...Enter path: ");
            scanf("%s",file_path);
            char file_name[PATH_SIZE];
            printf("...Enter file name: ");
            scanf("%s",file_name);
            handle_create(file_path, file_name, CREATE_FILE_REQ);            
        }
        else if (strcmp(command, "CREATE_FOLDER") == 0)
        {
            char file_path[PATH_SIZE];
            printf("...Enter path: ");
            scanf("%s",file_path);
            char file_name[PATH_SIZE];
            printf("...Enter folder name: ");
            scanf("%s",file_name);
            handle_create(file_path, file_name, CREATE_FOLDER_REQ);       
        }
        else if (strcmp(command, "LIST")==0)
        {
            handle_list();
        }
        else if (strcmp(command, "DELETE")==0)
        {
            char file_path[PATH_SIZE];
            printf("...Enter file path: ");
            scanf("%s",file_path);
            handle_delete(file_path);
        }
        else if (strcmp(command, "COPY")==0)
        {
            char file_path[PATH_SIZE];
            printf("...Enter source file path: ");
            scanf("%s",file_path);
            char dest_path[PATH_SIZE];
            printf("...Enter destination file path: ");
            scanf("%s",dest_path);
            handle_copy(file_path, dest_path);
        }
        else if (strcmp(command, "CREATE_FILE")==0)
        {   
            char file_path[PATH_SIZE];
            printf("...Enter file path: ");
            scanf("%s",file_path);
            char file_name[PATH_SIZE];
            printf("...Enter file name: ");
            handle_create(file_path,file_name,CREATE_FILE_REQ);
        }
        else if(strcmp(command,"CREATE_FOLDER")==0)
        {
            char file_path[PATH_SIZE];
            printf("...Enter file path: ");
            scanf("%s",file_path);
            char file_name[PATH_SIZE];
            printf("...Enter folder name: ");
            handle_create(file_path,file_name,CREATE_FOLDER_REQ);
        }
        else if(strcmp(command, "RETREIVE")==0)
        {
            char file_path[PATH_SIZE];
            printf("...Enter file path: ");
            scanf("%s",file_path);
            int ns_sock = connect_to_server(ns_ip, ns_port);
            if (ns_sock == -1)
            {
                printf("ERROR : Error connecting to NS\n");
                continue;
            }
            handle_retreive(file_path,ns_sock);
            nfs_send(ns_sock,ACK1,NULL,NULL);
            close(ns_sock);
        }
        else if (strcmp(command, "EXIT")==0)
        {
            exit(0);
        }
        else if (strcmp(command, "STREAM")==0)
        {
            char file_path[PATH_SIZE];
            printf("...Enter file path: ");
            scanf("%s",file_path);



            const char *extension = ".mp3";
            size_t path_len = strlen(file_path);
            size_t ext_len = strlen(extension);

            // Compare the last characters of the file path with ".mp3"
            if (path_len >= ext_len && strcmp(&file_path[path_len - ext_len], extension) == 0) {
                printf("The file is an MP3 file.\n");
            } else {
                printf("Error: The file is not an MP3 file.\n");
                continue;
            }








            char output_path[PATH_SIZE];
            printf("...Enter output path: ");
            scanf("%s",output_path);
            int ns_sock = connect_to_server(ns_ip, ns_port);
            handle_stream(ns_sock, file_path, output_path);
            close(ns_sock);
        }
        else
        {
            printf("Invalid command\n");
        }


    }
}
            

