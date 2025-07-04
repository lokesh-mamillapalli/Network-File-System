# NFS
---

This project is a complete implementation of a **distributed Network File System (NFS)** built from scratch in C. It features a **Naming Server (NM)** that handles metadata and directory services, multiple **Storage Servers (SS)** for actual file storage and replication, and **Clients** capable of performing file operations like reading, writing (sync & async), deleting, creating, listing, streaming audio, and copying across servers. The system supports **multiple clients concurrently**, dynamic server addition, **asynchronous writes**, **replication for fault tolerance**, **error handling with custom codes**, and performance optimizations like **efficient path lookup** using tries and **LRU caching**. All components communicate using **TCP sockets**, and proper **logging** and **bookkeeping** mechanisms are integrated for debugging and monitoring. See [`Questions.md`](./Questions.md) for additional discussion and design notes.

---
