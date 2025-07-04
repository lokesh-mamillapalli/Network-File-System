# Potential issues:
Using fixed number of threads for ACK receiving in write asynchronous
Threads are marked as "free" (thread_busy[thread_index] = false) only after receiving ACK2. If the server doesn't send ACK2, threads could remain stuck as "busy."
You are using a fixed number of threads (NUM_THREADS), which may cause deadlock if all threads are busy and no ACK2 is received.