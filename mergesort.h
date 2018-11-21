#ifndef _MERGESORT_H
#define _MERGESORT_H



#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <pthread.h>
#include "mergesort.h"
#define BUFFSIZE 1024



typedef struct entry{

    char** fields;
    size_t length;
    int  array_length;

}entry;

typedef struct _file_handler_args{

    char *pathname;
    char *column;
    char *output_directory;
    char *rawfilename;
    pthread_mutex_t mutex;
    entry ** global_buffer;

}file_handler_args;

typedef struct _dir_handler_args{

    char *input_directory;
    char *output_directory;
    char *column;
    file_handler_args * args;

}dir_handler_args;

typedef struct _holder{

    char *pathname;
    file_handler_args * args;

}holder;

void merging_string(entry** entries, entry** internal_buffer, int sorting_index, int low, int mid, int high);
void merging_int(entry** entries, entry** internal_buffer, int sorting_index, int low, int mid, int high);
int sanitize_content(char *token);
char* clean_string(char* string);
int removeSubstring(char *s,const char *toremove);
char* trimSpaces(char * str);

#endif
