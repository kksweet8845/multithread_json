#include <stdio.h>
#include <stdlib.h>
#include "csv2json.h"
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>

// #define MAXTHS 4
#define BIN_LINE 1000000


double diff(struct timespec start, struct timespec end) {
  struct timespec temp;
  if ((end.tv_nsec-start.tv_nsec)<0) {
    temp.tv_sec = end.tv_sec-start.tv_sec-1;
    temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
  } else {
    temp.tv_sec = end.tv_sec-start.tv_sec;
    temp.tv_nsec = end.tv_nsec-start.tv_nsec;
  }
  return temp.tv_sec + (double) temp.tv_nsec / 1000000000.0;
}



char* strdup(const char *c){
    char* dup = malloc(strlen(c) + 1);
    if(dup != NULL) {
        strcpy(dup, c);
    }
    return dup;
}


int main(int argc, char* argv[]){
    char* inputname;
    char* outputname;

    int cmd_opt;
    int MAXTHS;

    while((cmd_opt = getopt(argc, argv, "i:o:t:")) != -1){
        switch(cmd_opt){
            case 'i':
                inputname = strdup(optarg);
                break;
            case 'o':
                outputname = strdup(optarg);
                break;
            case 't':
                MAXTHS = atoi(optarg);
                break;
        }
    }


    volatile int *finished = malloc(sizeof(int));
    *finished = 0;
    volatile int *th_lines = malloc(sizeof(int) * MAXTHS);
    for(int i=0;i<MAXTHS;i++)
        th_lines[i] = i;


    int64_t static_rows = -1;
    printf("input file: %s\n", inputname);
    printf("output file: %s\n", outputname);
    printf("%d\n", MAXTHS);

    FILE* fp;
    fp = fopen(inputname, "r");

    char* line = (char*) malloc(sizeof(char) * 1024);
    memset(line, '\0', 1024);

    printf("Preparing...");
    init_head();
    init_threads(MAXTHS, &static_rows, outputname, finished, BIN_LINE, th_lines);
    init_cond_mutex();
    printf("Finished\n");
    sleep(1);
    printf("start ...\n");

    int64_t bin_line = BIN_LINE;
    int64_t prev_numlines;



    int64_t numlines = 0;
    task_ele_ptr_t item;
    printf("Start reading...");

    clock_t read_start, read_end;
    clock_t csv_start, csv_end;
    clock_t output_start, output_end;
    double read_total = 0.0f, csv_total = 0.0f, output_total = 0.0f;

    struct timespec r_s, r_e;
    struct timespec c_s, c_e;
    struct timespec o_s, o_e;


    while(1) {
        prev_numlines = numlines;
        // read_start = clock();
        clock_gettime(CLOCK_MONOTONIC, &r_s);
        while(fscanf(fp, "%s\n", line) != EOF){
            item = new_task(numlines);
            item->csv_str = strdup(line);
            numlines++;
            assign_work(item);
            if(numlines % bin_line == 0){
                // read_end = clock();
                clock_gettime(CLOCK_MONOTONIC, &r_e);
                read_total += diff(r_s, r_e);
                printf("numlines %d\n", numlines);
                // pthread_cond_signal(&task_cond);
                // csv_start = clock();
                break;
            }
        }

        if(prev_numlines == numlines){
            printf("end task_head %d\n", check_task_empty());
            pthread_mutex_lock(&output_mutex);
            static_rows = -2;
            pthread_mutex_unlock(&output_mutex);
            pthread_cond_signal(&output_cond);
            pthread_mutex_lock(&end_mutex);
            // printf("waiting\n");
            pthread_cond_wait(&end_cond, &end_mutex);
            pthread_mutex_unlock(&end_mutex);
            break;
        } else {
            pthread_cond_signal(&task_cond);
            clock_gettime(CLOCK_MONOTONIC, &c_s);
        }

        printf("Main process is waiting\n");
        int tfinished = 0;
        pthread_mutex_lock(&task_mutex);
        while(!check_task_empty() || check_finish_empty()){
            pthread_cond_wait(&task_finish_cond, &task_mutex);
            tfinished++;
            // printf("tf %d\n", tfinished);
        }
        // sleep(1);
        // csv_end = clock();
        clock_gettime(CLOCK_MONOTONIC, &c_e);
        pthread_mutex_unlock(&task_mutex);
        csv_total += diff(c_s, c_e);

        printf("%d\n", prev_numlines);
        // printf("num thread finished %d\n", tfinished);

        printf("Finished\n");

        // output_start = clock();
        clock_gettime(CLOCK_MONOTONIC, &o_s);
        printf("Start outputing...");
        // printf("finishd list %d\n", check_finish_empty());
        pthread_mutex_lock(&finish_mutex);
        // printf("finishd list %d\n", check_finish_empty());
        if(!check_finish_empty()){
            printf("Main process waiting output\n");
            pthread_mutex_lock(&output_mutex);
            static_rows = numlines;
            pthread_mutex_unlock(&output_mutex);
            pthread_cond_signal(&output_cond);
            pthread_cond_wait(&output_finish_cond, &finish_mutex);
        }
        // output_end = clock();
        clock_gettime(CLOCK_MONOTONIC, &o_e);
        pthread_mutex_unlock(&finish_mutex);
        output_total += diff(o_s, o_e);
        *finished = 0;
        printf("Iteration over\n");
    }

    printf("Read time spending       %f\n", ((double)read_total));
    printf("csv 2 json time spending %f\n", ((double)csv_total));
    printf("Output time spending     %f\n", ((double)output_total));
    printf("Total time spending      %f\n", ((double)(read_total + csv_total + output_total)));
    fclose(fp);


    return 0;
}