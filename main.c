#include <stdio.h>
#include <stdlib.h>
#include "csv2json.h"
#include <string.h>
#include <unistd.h>

#define MAXTHS 2
#define BIN_LINE 1000000

char* strdup(const char *c){
    char* dup = malloc(strlen(c) + 1);
    if(dup != NULL){
        strcpy(dup, c);
    }
    return dup;
}


int main(int argc, char* argv[]){
    char* inputname = argv[1];
    char* outputname = argv[2];
    volatile int *finished = malloc(sizeof(int));
    *finished = 0;
    int64_t static_rows = -1;
    printf("input file: %s\n", inputname);
    printf("output file: %s\n", outputname);

    FILE* fp;
    fp = fopen(inputname, "r");

    char* line = (char*)malloc(sizeof(char) * 1024);
    memset(line, '\0', 1024);

    printf("Preparing...");
    init_head();
    init_threads(MAXTHS, &static_rows, outputname, finished, BIN_LINE);
    init_cond_mutex();
    printf("Finished\n");
    // sleep(2);
    printf("start ...\n");

    int64_t bin_line = BIN_LINE;
    int64_t prev_numlines;



    int64_t numlines = 0;
    task_ele_ptr_t item;
    printf("Start reading...");

    while(1) {
        prev_numlines = numlines;
        while(fscanf(fp, "%s\n", line) != EOF){
            item = new_task(numlines);
            item->csv_str = strdup(line);
            numlines++;
            assign_work(item);
            if(numlines % bin_line == 0){
                printf("numlines %d\n", numlines);
                break;
            }
        }

        if(prev_numlines == numlines){
            break;
        }
        printf("%d\n", prev_numlines);

        printf("Finished\n");

        pthread_mutex_lock(&output_mutex);
        static_rows = numlines;
        pthread_mutex_unlock(&output_mutex);
        pthread_cond_signal(&output_cond);
        printf("Start outputing...");
        while(!*finished) {
            printf("%d\n", ACCESS_ONCE(*finished));
            pthread_mutex_lock(&finish_mutex);
            if(!list_empty(&finish_head))
                pthread_cond_signal(&finish_cond);
            pthread_mutex_unlock(&finish_mutex);
            pthread_mutex_lock(&task_mutex);
            if(!list_empty(&task_head))
                pthread_cond_signal(&task_cond);
            pthread_mutex_unlock(&task_mutex);
            sleep(1);
        }
        *finished = 0;
        printf("Iteration over\n");
    }
    fclose(fp);

    return 0;
}