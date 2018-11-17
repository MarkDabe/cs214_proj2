#include "mergesort.h"


void merging_string(entry** entries, entry** internal_buffer, int sorting_index, int low, int mid, int high) {



    int l1 = 0;
    int l2 = 0;
    int i = 0;

    char* temp1 = NULL;
    char* temp2 = NULL;


    for(l1 = low, l2 = mid + 1, i = low; l1 <= mid && l2 <= high; i++) {

        temp1 = clean_string(entries[l1]->fields[sorting_index]);
        temp2 = clean_string(entries[l2]->fields[sorting_index]);



        if(strcmp(temp1, temp2) <= 0){
            memcpy(internal_buffer[i], entries[l1], sizeof(entry));
            l1++;
        }
        else{
            memcpy(internal_buffer[i], entries[l2], sizeof(entry));
            l2++;
        }
        free(temp1);
        free(temp2);
    }

    while(l1 <= mid) {
        memcpy(internal_buffer[i], entries[l1], sizeof(entry));
        i++;
        l1++;
    }

    while(l2 <= high){
        memcpy(internal_buffer[i], entries[l2], sizeof(entry));
        i++;
        l2++;
    }

    for(i = low; i <= high; i++) {
        memcpy(entries[i], internal_buffer[i], sizeof(entry));
    }

}



void merging_int(entry** entries, entry** internal_buffer, int sorting_index, int low, int mid, int high) {



    int l1 = 0;
    int l2 = 0;
    int i = 0;


    for(l1 = low, l2 = mid + 1, i = low; l1 <= mid && l2 <= high; i++) {


        if(atoi(entries[l1]->fields[sorting_index]) <= atoi(entries[l2]->fields[sorting_index])){

            memcpy(internal_buffer[i], entries[l1], sizeof(entries[l1]));
            l1++;
        }
        else{
            memcpy(internal_buffer[i], entries[l2], sizeof(entries[l2]));
            l2++;
        }
    }

    while(l1 <= mid) {
        memcpy(internal_buffer[i], entries[l1], sizeof(entries[l1]));
        i++;
        l1++;
    }

    while(l2 <= high){
        memcpy(internal_buffer[i], entries[l2], sizeof(entries[l2]));
        i++;
        l2++;
    }

    for(i = low; i <= high; i++) {
        memcpy(entries[i], internal_buffer[i], sizeof(internal_buffer[i]));
    }

}



char* trimSpaces(char * str)
{
    char *end;

    while(isspace((unsigned char)*str)) {

        str++;

    }

    if(*str == 0) {
        return str;
    }

    end = str + strlen(str) - 1;

    while(end > str && isspace((unsigned char)*end)) {

        end--;

    }

    end[1] = '\0';

    return str;

}


int removeSubstring(char *s,const char *toremove)
{
    while( s=strstr(s, toremove) )
    {
        memmove(s, s + strlen(toremove), 1 + strlen(s + strlen(toremove)));
    }

    return 0;
}

char* clean_string(char* string){

    char* temp_buffer = (char *) malloc((strlen(string)+1) * sizeof(char));
    strncpy(temp_buffer, string, strlen(string) + 1);
    if(temp_buffer[0] == '"' || temp_buffer[strlen(temp_buffer) -1] == '"') {

        removeSubstring(temp_buffer, "\"");
    }

    char* temp_buffer2 = trimSpaces(temp_buffer);

    char* temp_buffer3 = (char*) malloc((strlen(temp_buffer2)+1) * sizeof(char));

    strncpy(temp_buffer3, temp_buffer2, strlen(temp_buffer2)+1);

    free(temp_buffer);

    return temp_buffer3;

}

int sanitize_content(char *token){

    removeSubstring(token, "\n");

    return 0;

}
