#include "headers.h"


int chunk_receive_audio(int ss_sock, const char *output_path) {
    int audio_fd = -1;

    // Open the output file or pipe
    if (output_path) {
        

        audio_fd = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (audio_fd == -1) {
            perror("Failed to open output file");
            return -1;
        }
    } else {
        audio_fd = STDOUT_FILENO; // Default to standard output
    }

    while (1) {
        nfs_comm ss_response;
        memset(&ss_response, 0, sizeof(ss_response)); // Clear the structure

        // Receive a chunk of data from the server
        int n_bytes = nfs_recv(&ss_response, ss_sock);
        if (n_bytes <= 0) {
            if (audio_fd != STDOUT_FILENO) close(audio_fd); // Close file if opened
            fprintf(stderr, "Error receiving data or connection closed\n");
            return -1;
        }

        if (ss_response.type == STOP) {
            // End of audio streaming
            printf("[INFO] Audio streaming complete.\n");
            if (audio_fd != STDOUT_FILENO) close(audio_fd);
            return 0;
        } else if (ss_response.type == ERROR) {
            // Handle server-side error
            fprintf(stderr, "[ERROR] Server error: %s\n", ss_response.field1);
            if (audio_fd != STDOUT_FILENO) close(audio_fd);
            return -1;
        } else if (ss_response.type != PACKET) {
            // Handle unexpected response
            fprintf(stderr, "[ERROR] Unexpected response type: %d\n", ss_response.type);
            if (audio_fd != STDOUT_FILENO) close(audio_fd);
            return -1;
        }

        // Write the received data to the output file or pipe
        if (write(audio_fd, ss_response.field1, MBUFF) == -1) {
            perror("[ERROR] Failed to write audio data");
            if (audio_fd != STDOUT_FILENO) close(audio_fd);
            return -1;
        }
    }
}


// Communicates with storage server to read file
int ss_stream(int ss_sock, char* file_path, char* output_path)
{
  
    nfs_comm ss_response;
    memset(&ss_response, 0, sizeof(ss_response));
    request_receive(ss_sock, STREAM, file_path, NULL, &ss_response);
    
    int type = response_type_check(ss_response);
    if (type == REQ_SUCCESS)
    {
        #ifdef DEBUG
        printf("[DEBUG] SS accepted read request\n");
        #endif
        return chunk_receive_audio(ss_sock,output_path);
    }
    return type;
}
  


int handle_stream(int ns_sock, char* file_path, char* output_path)
{
    nfs_comm ns_response;
    if (request_receive(ns_sock, STREAM, file_path, NULL, &ns_response) == -1)
    {
        return -1;
    }
    int rtype = response_type_check(ns_response);
    if (rtype == REQ_SUCCESS)
    {

        int ss_sock = connect_to_server(ns_response.field1, atoi(ns_response.field2));
        if (ss_sock == -1)
        {
            fprintf(stderr,"Error in connecting to storage server\n");
            return -1;
        }

        if (ss_stream(ss_sock,file_path,output_path) == -1)
        {
            close(ss_sock);
            return -1;
        }
    }
    return rtype;
}