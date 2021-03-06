#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "multiThreadSorter_thread.h"

pthread_t TIDS[256] = {0};
pthread_mutex_t lock;
int COUNTER = 0;



int merge_numeric = -1;


// sorting function that calls merging_int or merging_string depending on data type
void sort(entry** entries, entry** internal_buffer,int sorting_index ,int low, int high) {

    int mid;


    if (low < high) {
        mid = (low + high) / 2;
        sort(entries, internal_buffer, sorting_index,low, mid);
        sort(entries, internal_buffer, sorting_index,mid + 1, high);
        if(merge_numeric == 1){
            merging_int(entries, internal_buffer, sorting_index, low, mid, high);
        }
        else {
            merging_string(entries, internal_buffer, sorting_index, low, mid, high);
        }
    } else {

        return;
    }
}
//


// function to count the number of fields needed
int countfields(char* line){
    int count = 0;
    int i =0;

    for(i = 0; line[i] != '\0'; ++i)
    {
        if(line[i] == ',')
            ++count;
    }

    return count + 1;

}
//

// function to count the number of lines in the opened file in memory
int countlines (FILE *fin)
{
    int  nlines  = 0;
    char line[BUFFSIZE];

    while(fgets(line, BUFFSIZE, fin) != NULL) {
        nlines++;
    }

    return nlines;
}
//

// add fields from a line in the opened file to an entry element in a the entry array
int add_fields(entry* array_entry,int* fields_count,  char* line, int * mapper, int map){


    int i = 0;
    char* field = NULL;

    int linecounter = 0;



    if(*fields_count == -1){
        *fields_count = countfields(line);
    }



    array_entry->length = strlen(line) + 1;
    char* temp = (char*) malloc(array_entry->length);
    strncpy(temp,line,array_entry->length);
    array_entry->fields = (char**) malloc((28) * sizeof(char*));

    while ((field = strsep(&temp, ",")) != NULL) {

        linecounter += 1;

        sanitize_content(field);

        if(strcmp(field, "") == 0){
            if(map == 1) {
                array_entry->fields[mapper[i]] = malloc(sizeof("") + 1);
                strncpy(array_entry->fields[mapper[i]], "\0", sizeof("") + 1);
            }else{
                array_entry->fields[i] = malloc(sizeof("") + 1);
                strncpy(array_entry->fields[i], "\0", sizeof("") + 1);
            }
        }
        else {

            if(map == 1) {

                array_entry->fields[mapper[i]] = (char *) malloc((strlen(field) + 1) * sizeof(char));
                strncpy(array_entry->fields[mapper[i]], field, strlen(field) + 1);
                if (array_entry->fields[i][0] == '"') {

                    size_t index = strlen(field);
                    size_t size = strlen(field) + 1;

                    while (field[strlen(field) - 1] != '"') {

                        field = strsep(&temp, ",");

                        size += strlen(field) + 1;
                        array_entry->fields[mapper[i]] = realloc(array_entry->fields[mapper[i]], size);
                        sprintf(array_entry->fields[mapper[i]] + index, ",%s", field);
                        index += strlen(field) + 1;

                    }


                }
            }else{
                array_entry->fields[i] = (char *) malloc((strlen(field) + 1) * sizeof(char));
                strncpy(array_entry->fields[i], field, strlen(field) + 1);
                if (array_entry->fields[i][0] == '"') {

                    size_t index = strlen(field);
                    size_t size = strlen(field) + 1;

                    while (field[strlen(field) - 1] != '"') {

                        field = strsep(&temp, ",");

                        size += strlen(field) + 1;
                        array_entry->fields[i] = realloc(array_entry->fields[i], size);
                        sprintf(array_entry->fields[i] + index, ",%s", field);
                        index += strlen(field) + 1;

                    }


                }

            }
        }

        i++;


    }

    int j;

    if(map == 1){
        for(i=0; i < 28; i++){
            for(j=0; j< 28; j++){
                if(mapper[i] == j){
                    break;
                }
            }
            if(j == 28){
                array_entry->fields[mapper[i]] = malloc(sizeof("") + 1);
                strncpy(array_entry->fields[mapper[i]], "\0", sizeof("") + 1);
            }
        }

        for(i=0; i < 28; i++){
            for(j=0; j< 28; j++){
                if(mapper[i] == -1){
                    array_entry->fields[i] = malloc(sizeof("") + 1);
                    strncpy(array_entry->fields[i], "", sizeof(" ") + 1);
                }
            }

        }

    }




    free(temp);

    if (*fields_count != linecounter){

        fprintf(stderr, "FILE HAS WRONG FORMAT\n");
        return -1;
    }


    return 0;

}

//


// build the array of entries through opening a file in memory and counting the number of entries needed to build the array
entry** load_array(int* entries_count, int* fields_count, char* filename){

    const char *column_names[28] = {"color","director_name","num_critic_for_reviews","duration","director_facebook_likes"
            ,"actor_3_facebook_likes","actor_2_name","actor_1_facebook_likes", "gross",
                                    "genres","actor_1_name","movie_title","num_voted_users","cast_total_facebook_likes",
                                    "actor_3_name","facenumber_in_poster","plot_keywords","movie_imdb_link","num_user_for_reviews",
                                    "language","country","content_rating","budget","title_year","actor_2_facebook_likes",
                                    "imdb_score","aspect_ratio","movie_facebook_likes"};

    int* mapper = (int*) malloc(sizeof(int)*28);
    int map = 0;

    int x = 0;
    for(x = 0; x <28; x++){
        mapper[x] = -1;
    }


    int i = 0;
    int status = 0;


    if(strstr(filename, "-sorted-")!= NULL){
        return NULL;
    }



    FILE* fptr = fopen(filename , "r");



    if(fptr == NULL){
        return NULL;
    }


    *entries_count = countlines(fptr);

    rewind(fptr);

    entry** buffer = (entry **) malloc(sizeof(entry*) * (*entries_count));


    char line_buffer[4096] ={0};

    while (fgets(line_buffer, sizeof(line_buffer), fptr) != NULL) {

        buffer[i] = (entry*) malloc(sizeof(entry));

        status = add_fields(buffer[i], fields_count,line_buffer, mapper , map);


        if(status == -1){
            fclose(fptr);
            free(buffer);
            return NULL;

        }

        if(i == 0){
            int j;
            int k;

            for(k = 0; k < 28; k++){

                if(strlen(buffer[0]->fields[k]) == 0){
                    map = 1;
                    break;
                }


                for(j = 0; j < 28; j++){

                    if(strncmp(buffer[0]->fields[k], column_names[j], strlen(column_names[j])) == 0){
                        mapper[k] = j;
                        break;
                    }

                }

                if(j >= 28){


                    fprintf(stderr, "INVALID COLUMN NAME IN FILE\n");

                    free(buffer[0]);
                    free(buffer);

                    return NULL;


                }


            }

            map = 1;
        }


        i++;
    }


    fclose(fptr);


    return buffer;
}

//

void * file_handler(void * args){


    printf("Initial PID: %d\n", getpid());

    printf("TIDS of all threads: \n");

    printf("Total number of threads: 0\n");


    holder *instance_args = args;

    char* pathname =  instance_args->pathname;
    file_handler_args *file_args = instance_args->args;


// call the function that builds the array
    int entries_count = -1;
    int fields_count = -1;


    entry** entries = load_array(&entries_count, &fields_count, pathname);

    if(entries == NULL){
        pthread_exit(NULL);
    }

    int i;


    pthread_mutex_lock(&lock);



    entry ** global_buffer = file_args->global_buffer;

    int array_length = global_buffer[0]->array_length;

    entry ** temp_global_buffer = (entry **) realloc(global_buffer,
            (size_t) (array_length + entries_count - 1) * sizeof(entry));



    if(temp_global_buffer == NULL){

        fprintf(stderr, "Error joining thread\n");

        pthread_exit(NULL);

    }

    temp_global_buffer[0]->array_length = temp_global_buffer[0]->array_length + entries_count - 1;

    file_args -> global_buffer = temp_global_buffer;

    int loop_counter;


    for(loop_counter = 0; loop_counter < entries_count - 1; loop_counter++) {

        temp_global_buffer[array_length + loop_counter] = malloc(sizeof(entry));

        memcpy(temp_global_buffer[array_length + loop_counter], entries[1+loop_counter], sizeof(entry));

    }



    pthread_mutex_unlock(&lock);


    for(i = 0; i < entries_count; i++){

        free(entries[i]);

    }

  free(entries);

  free(pathname);

  pthread_exit(NULL);


}



void* directory_handler(void * dir_args){

    pthread_t local_tids[256] = {0};
    int local_counter = 0;

    dir_handler_args * instance_dir_args = (dir_handler_args *) dir_args;

    const char* input_directory = instance_dir_args ->input_directory;
    const char* column = instance_dir_args->column;
    const char* output_directory = instance_dir_args->output_directory;
    file_handler_args * args = instance_dir_args->args;

    DIR *dp;
    DIR *dp2;
    struct dirent *ep;
    char pathname[2048] = {0};
    dp = opendir (input_directory);

    if (dp != NULL)
    {
        while (ep = readdir (dp)) {

            if (strcmp(ep->d_name, ".") == 0 || strcmp(ep->d_name, "..") == 0) {
                continue;
            }

            sprintf(pathname,"%s/%s/",input_directory, ep->d_name);


            dp2 = opendir(pathname);


            if (dp2 != NULL) {

                closedir(dp2);

                char* temp_pathname = (char*) malloc(sizeof(char) * strlen(pathname) + 1);

                strncpy(temp_pathname, pathname, strlen(pathname) +1);

                instance_dir_args ->input_directory = temp_pathname;


                pthread_create(&TIDS[COUNTER],NULL, directory_handler, dir_args);

                local_tids[local_counter] = TIDS[COUNTER];
                local_counter++;

                COUNTER++;


            }else{

                int offset = (int) strlen(ep->d_name) -4;


                   if(strcmp(ep->d_name+offset,".csv") == 0 ||
                       strcmp(ep->d_name + offset,".CSV") == 0 ) {

                        pathname[strlen(pathname) - 1] = '\0';
                        char* path_name = malloc(strlen(pathname) + 1);
                        strcpy(path_name, pathname);
                        holder* args_holder = (holder*) malloc(sizeof(holder));
                        args_holder->pathname = path_name;
                        args_holder->args = args;
                        pthread_create(&TIDS[COUNTER],NULL, file_handler, args_holder);
                        local_tids[local_counter] = TIDS[COUNTER];
                        local_counter++;
                        COUNTER++;
                   }
                }
            }

        closedir(dp);

    }
    else {
        perror("Couldn't open the directory");
    }


    printf("Initial PID: %d\n", getpid());

    printf("TIDS of all threads: ");

    int i = 0;

    while (local_tids[i] != 0) {

        printf(" %lu", (unsigned long) local_tids[i]);

        if(local_tids[i+1] != 0){
            printf(",");
        }

        i++;

    }

    printf("\n");

    printf("Total number of threads: %d\n", i - 1);


    pthread_exit(NULL);

}


void set_options(char* flag, char* option, char* allowed_flags[], char* used_flags[], int * local_counter,
                 char** column_name, char** input_directory, char** output_directory){
    int x = 0;
    int y = 0;

    for(x = 0; x < 3; x++){
        if(strcmp(flag, allowed_flags[x]) == 0){
            while(used_flags[y] != NULL){
                if(strcmp(used_flags[y], flag) == 0){
                    *local_counter = -1;
                    return;
                }
                y++;
            }

            if(strcmp(flag, "-c") == 0){
                *column_name = option;
            }else if(strcmp(flag, "-d") == 0){
                *input_directory = option;
            }else if(strcmp(flag, "-o") == 0){
                *output_directory = option;
            }

            used_flags[*local_counter] = flag;
            *local_counter +=1;
            return;
        }

    }

    if(x==3){
        *local_counter = -1;
        return;
    }


}

int main(int argc, char* argv[]) {


    char* column_name = NULL;
    char* input_directory = "./";
    char* output_directory = NULL;
    char* allowed_flags[3] = {"-c", "-o", "-d"};
    char* used_flags[3] = {0};
    int local_counter  = 0;


    // check for the number of arguments
    if( argc < 3  || argc == 4 || argc == 6  || argc > 7){
        fprintf(stderr, "INVALID NUMBER OF INPUTS\n");
        return 0;
    }
//

    if (argc >= 3){
        set_options(argv[1], argv[2],allowed_flags, used_flags, &local_counter,
                    &column_name,&input_directory,&output_directory);
    }

    if(local_counter == -1){
        fprintf(stderr, "INVALID COMMAND\n");
        return 0;
    }

    if (argc >= 5){
        set_options(argv[3], argv[4],allowed_flags, used_flags, &local_counter,
                    &column_name,&input_directory,&output_directory);
    }

    if(local_counter == -1){
        fprintf(stderr, "INVALID COMMAND\n");
        return 0;
    }

    if (argc == 7){
        set_options(argv[5], argv[6],allowed_flags, used_flags, &local_counter,
                    &column_name,&input_directory,&output_directory);
    }

    if(local_counter == -1){
        fprintf(stderr, "INVALID COMMAND\n");
        return 0;
    }


    if(column_name == NULL){
        fprintf(stderr, "MISSING COLUMN NAME\n");
        return 0;
    }

    DIR* dir = opendir(input_directory);
    if (dir) {
        closedir(dir);
    }else{

        fprintf(stderr, "INPUT DIRECTORY DOES NOT EXIST\n");
        return 0;

    }

    if(output_directory != NULL) {
        dir = opendir(output_directory);
        if (dir) {
            closedir(dir);
        } else {
            fprintf(stderr, "OUTPUT DIRECTORY DOES NOT EXIST\n");
            return 0;

        }
    }


    int i = 0;
    int j = 0;
    pthread_mutex_init(&lock, NULL);
    entry ** global_buffer = (entry **) malloc(sizeof(entry*) * 1);
    entry * header = (entry*) malloc(sizeof(entry));
    header->array_length = 1;
    header->fields = (char **) malloc(sizeof(char*)*28);
    const char *column_names[28] = {"color","director_name","num_critic_for_reviews","duration","director_facebook_likes"
                                     ,"actor_3_facebook_likes","actor_2_name","actor_1_facebook_likes", "gross",
                                    "genres","actor_1_name","movie_title","num_voted_users","cast_total_facebook_likes",
                                    "actor_3_name","facenumber_in_poster","plot_keywords","movie_imdb_link","num_user_for_reviews",
                                    "language","country","content_rating","budget","title_year","actor_2_facebook_likes",
                                    "imdb_score","aspect_ratio","movie_facebook_likes"};

    for(i = 0; i < 28; i ++){
        if(strcmp(column_name, column_names[i]) == 0){
            break;
        }
    }

    if(i >= 28 ){
        fprintf(stderr, "INVALID COLUMN NAME\n");
        return 0;
    }

    for(i = 0; i < 28; i ++){
        header->fields[i] = (char*) malloc(strlen(column_names[i]) + 1);
        strcpy(header->fields[i], column_names[i]);
    }

    global_buffer[0] = header;

    global_buffer[0]-> length = 28;

    global_buffer[0] ->array_length = 1;


    file_handler_args * args = (file_handler_args *) malloc(sizeof(file_handler_args));

    args->global_buffer = global_buffer;

    dir_handler_args * dir_args =  (dir_handler_args *) malloc(sizeof(dir_handler_args));


    dir_args->args = args;
    dir_args->input_directory = input_directory;
    dir_args->output_directory = output_directory;
    dir_args->column = column_name;


    pthread_create(&TIDS[COUNTER],NULL, directory_handler, dir_args);

    COUNTER++;

    i = 0;


    while (TIDS[i] != 0) {

        if(pthread_join(TIDS[i], NULL)) {
            fprintf(stderr, "Error joining thread\n");
            return 2;
        }

        i++;
    }


    int sorting_index = 1;


    char* string_array[13] = {"color", "director_name", "duration", "actor_2_name", "genres", "actor_1_name", "movie_title",
                            "actor_3_name", "plot_keywords", "movie_imdb_link", "language", "country", "content_rating"};

    char* numeric_array[15] = {"num_critic_for_reviews", "director_facebook_likes", "actor_3_facebook_likes", " actor_1_facebook_likes",
    "gross", "num_voted_users", "cast_total_facebook_likes", "facenumber_in_poster", "num_user_for_reviews", "budget", "title_year",
    "actor_2_facebook_likes", "imdb_score", "aspect_ratio", "movie_facebook_likes"};


    for(i = 0; i < 13; i++){
        if(strcmp(string_array[i], column_name) == 0){
            merge_numeric = 0;
            break;
            }
    }

    if(merge_numeric == -1) {
        for (i = 0; i < 15; i++) {
            if (strcmp(numeric_array[i], column_name) == 0) {
                sorting_index = i;
                break;
            }
        }
    }



//  load the internal buffer needed by mergesort
    entry** internal_buffer = (entry**) malloc(sizeof(entry*) * args->global_buffer[0]->array_length);

    i = 0;

    while(i < args->global_buffer[0]->array_length){

        internal_buffer[i] = (entry*) malloc(sizeof(entry));
        i++;
    }


// call sort on the array
    sort(args->global_buffer, internal_buffer, sorting_index, 1, args->global_buffer[0]->array_length -1);

    size_t length_output_file_name = strlen("AllFiles-sorted.csv") + strlen(column_name) + 1;

    char output_file_path[512] = {0};
    char* output_file_name =  (char*)  malloc(length_output_file_name * sizeof(char));
    sprintf(output_file_name,"/AllFiles-sorted-%s.csv", column_name);
    if(output_directory != NULL){
        strcat(output_file_path, output_directory);
    }

    strcat(output_file_path, output_file_name);

    FILE * output = fopen(output_file_path, "w");

    for(i = 0; i < args->global_buffer[0]->array_length; i++) {

        for(j= 0; j < 28 ; j++){

            fprintf(output, "%s", args->global_buffer[i]->fields[j]);
            if(j == 27){
                break;
            }
            fprintf(output, ",");
        }

        fprintf(output, "\n");

    }
    fclose(output);


    for(i = 0; i < args->global_buffer[0]->array_length; i++){
         free(internal_buffer[i]);
    }

    free(internal_buffer);
    free(output_file_name);



    int global_array_length = args->global_buffer[0]->array_length;


    for(i = 0; i < global_array_length; i++){

        for(j= 0; j < 28; j++){

            free(args->global_buffer[i]->fields[j]);
        }

        free(args->global_buffer[i]->fields);

        free(args->global_buffer[i]);
    }

    free(args->global_buffer);

    pthread_mutex_destroy(&lock);


    i = 0;

    printf("Initial PID: %d\n", getpid());

    printf("TIDS of all threads: ");

    while (TIDS[i] != 0) {

        printf(" %lu", (unsigned long) TIDS[i]);

        if(TIDS[i+1] != 0){
            printf(",");
        }

        i++;

    }

    printf("\n");

    printf("Total number of threads: %d\n", COUNTER);



    return 0;
}