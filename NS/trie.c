#include "defs.h"

node *parent;

// Error printing
void print_error(char *str)
{
    fprintf(stderr, "Error: %s\n", str);
}

// Check if a node is null
bool Check_Node_Null(node *check_node)
{
    if (check_node == NULL)
    {
        print_error("Node is NULL.");
        return true;
    }
    return false;
}

// Create a new node
node *create_node(char *parent_path, char *segment)
{
    node *newNode = malloc(sizeof(node));
    if (!newNode)
    {
        print_error("Could not allocate memory for node");
        return NULL;
    }
    newNode->belong_to_storage_server_id = -1;
    newNode->path = NULL;
    newNode->busy = 0;
    newNode->readers = 0;
    newNode->total_path[0] = '\0';
    for (int i = 0; i < MAX_CHILDREN; i++)
    {
        newNode->next_node[i] = NULL;
    }
    // #ifdef DEBUG
    // printf("[DEBUG] Creating node with parent_path = %s, segment = %s\n", parent_path, segment);
    // #endif
    if (parent_path[0] != '\0' && segment)
    {
        snprintf(newNode->total_path, SBUFF, "%s/%s", parent_path, segment);
    }
    else if (segment) // For the root node or base segment
    {
        strcpy(newNode->total_path, (segment));
    }
    return newNode;
}

// Split a path into segments
char **split_path(char *path, int *count)
{
    char *path_copy = strdup(path);
    if (!path_copy)
    {
        print_error("Failed to allocate memory for path copy");
        return NULL;
    }

    char **segments = malloc(MAX_CHILDREN * sizeof(char *));
    if (!segments)
    {
        free(path_copy);
        print_error("Failed to allocate memory for path segments");
        return NULL;
    }

    *count = 0;
    char *token = strtok(path_copy, "/");
    while (token)
    {
        segments[(*count)++] = strdup(token);
        token = strtok(NULL, "/");
    }

    free(path_copy);
    return segments;
}

// Free path segments
void free_segments(char **segments, int count)
{
    for (int i = 0; i < count; i++)
    {
        free(segments[i]);
    }
    free(segments);
}

// Insert a path into the trie
bool insert_path(char *path, int storage_id)
{
    int segment_count;
    if (check_file_exist(path))
    {
        return false;
    }
    #ifdef DEBUG
    printf("[DEBUG] Inserting %s  just after check_file_exist\n", path);
    #endif
    char **segments = split_path(path, &segment_count);
    if (!segments)
    {
        return false;
    }
    node *tmp = parent;
    for (int i = 0; i < segment_count; i++)
    {
        bool found = false;
        for (int j = 0; j < MAX_CHILDREN; j++)
        {
            if (tmp->next_node[j] && strcmp(tmp->next_node[j]->path, segments[i]) == 0)
            {
                tmp = tmp->next_node[j];
                found = true;
                break;
            }
        }

        if (!found)
        {
            for (int j = 0; j < MAX_CHILDREN; j++)
            {
                if (!tmp->next_node[j])
                {
                    char *parent_path = strdup(tmp->total_path);
#ifdef DEBUG
                    printf("[DEBUG] Inserting %s\n", segments[i]);
#endif
                    tmp->next_node[j] = create_node(parent_path, segments[i]);
                    tmp->next_node[j]->path = strdup(segments[i]);
                    tmp->next_node[j]->belong_to_storage_server_id = storage_id;
                    tmp = tmp->next_node[j];
                    break;
                }
            }
        }
    }

    tmp->belong_to_storage_server_id = storage_id;
    free_segments(segments, segment_count);
    #ifdef DEBUG
    printf("[DEBUG] Inserted %s\n", path);
    #endif

    add_to_cache(tmp);
    return true;
}

// Get the storage ID for a path
int get_storage_id(char *path)
{
    node *temp = get_from_cache(path);
#ifdef DEBUG
    if (temp != NULL)
        printf("[DEBUG] cache_id = %d\n", temp->belong_to_storage_server_id);
    else
        printf("[DEBUG] cache_id = NULL\n");
#endif
    if (temp != NULL)
    {

        return temp->belong_to_storage_server_id;
    }
    int segment_count;
    char **segments = split_path(path, &segment_count);
    if (!segments)
    {

        return -1;
    }

    node *tmp = parent;
    for (int i = 0; i < segment_count; i++)
    {
        bool found = false;
        for (int j = 0; j < MAX_CHILDREN; j++)
        {
            if (tmp->next_node[j] && strcmp(tmp->next_node[j]->path, segments[i]) == 0)
            {
                tmp = tmp->next_node[j];
                found = true;
                break;
            }
        }
        if (!found)
        {
            free_segments(segments, segment_count);

            return -1;
        }
    }
    int storage_id = tmp->belong_to_storage_server_id;
    free_segments(segments, segment_count);
    add_to_cache(tmp);

    return storage_id;
}

// get node with given path in trie
node *get_node(char *path)
{
    node *temp = get_from_cache(path);
    if (temp != NULL)
    {
        return temp;
    }
    int segment_count;
    char **segments = split_path(path, &segment_count);
    if (!segments)
    {

        return NULL;
    }

    node *tmp = parent;

    for (int i = 0; i < segment_count; i++)
    {
        bool found = false;
        for (int j = 0; j < MAX_CHILDREN; j++)
        {
            if (tmp->next_node[j] && strcmp(tmp->next_node[j]->path, segments[i]) == 0)
            {
                tmp = tmp->next_node[j];
                found = true;
                break;
            }
        }

        if (!found)
        {
            free_segments(segments, segment_count);

            return NULL;
        }
    }

    free_segments(segments, segment_count);

    return tmp;
}

// Recursively delete a subtree
void delete_subtree(node *current)
{
    if (!current)
        return;

    for (int i = 0; i < MAX_CHILDREN; i++)
    {
        if (current->next_node[i])
        {
            delete_subtree(current->next_node[i]);
            current->next_node[i] = NULL;
        }
    }

    if (current->path)
    {
        free(current->path);
        current->path = NULL;
    }
    free(current);
}

// Delete a path from the trie
bool delete_path(char *path)
{
    int segment_count;
    char **segments = split_path(path, &segment_count);
    if (!segments)
        return false;

    node *tmp = parent;
    node *prev = NULL;
    int prev_index = -1;

    for (int i = 0; i < segment_count; i++)
    {
        bool found = false;
        for (int j = 0; j < MAX_CHILDREN; j++)
        {
            if (tmp->next_node[j] && strcmp(tmp->next_node[j]->path, segments[i]) == 0)
            {
                prev = tmp;
                prev_index = j;
                tmp = tmp->next_node[j];
                found = true;
                break;
            }
        }
        if (!found)
        {
            free_segments(segments, segment_count);
            return false;
        }
    }

    delete_subtree(tmp);
    if (prev && prev_index != -1)
    {
        prev->next_node[prev_index] = NULL;
    }

    free_segments(segments, segment_count);
    return true;
}
