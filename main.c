#include <stdio.h>
#include <stdlib.h>
#include "csv2json.h"

#define MAXTHS 1


int main(int argc, char* argv[]){


    char* inputname = argv[1];
    outputname = argv[2];
    finished = 0;
    printf("input file: %s\n", inputname);
    printf("output file: %s\n", outputname);

    FILE* fp;
    fp = fopen(inputname, "r");

    char* line = (char*)malloc(sizeof(char) * 1024);
    memset(line, '\0', 1024);

    printf("Preparing...");
    init_head();
    init_threads(MAXTHS);
    init_cond_mutex();
    printf("Finished\n");



    int64_t numlines = 0;
    task_ele_ptr_t item;
    static_rows = -1;
    printf("static rows %d\n", static_rows);
    printf("Start reading...");
    fflush(stdout);
    while(fscanf(fp, "%s\n", line) != EOF){
        // printf("%s\n", line);
        item = new_task(numlines);
        item->csv_str = strdup(line);
        numlines++;
        // printf("%d\n", numlines);
        // memset(line, '\0', sizeof(char)*1024);
        // csv2json(item);
        assign_work(item);
    }
    // printf("Finished\n");
    while(!finished);

    // pthread_mutex_lock(&output_mutex);
    // static_rows = numlines;
    // pthread_mutex_unlock(&output_mutex);
    // pthread_cond_signal(&output_cond);
    printf("Start outputing...");




    fclose(fp);

    return 0;
}