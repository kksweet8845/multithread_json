#include <stdio.h>
#include <stdlib.h>
#include "csv2json.h"
#include <string.h>
#include <unistd.h>
#include <getopt.h>

// #define MAXTHS 4
#define BIN_LINE 1000

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
    volatile int *th_lines = malloc(sizeof(volatile int) * MAXTHS);
    for(int i=0;i<MAXTHS;i++)
        th_lines[i] = 0;

    int64_t static_rows = -1;
    printf("input file: %s\n", inputname);
    printf("output file: %s\n", outputname);
    printf("MAXTHS : %d\n", MAXTHS);

    FILE* fp;
    FILE* ofp;
    fp = fopen(inputname, "r");
    ofp = fopen(outputname, "w");

    char* line = (char*)malloc(sizeof(char) * 1024);
    memset(line, '\0', 1024);

    printf("Preparing...");
    init_head();
    init_threads(MAXTHS, &static_rows, outputname, finished, BIN_LINE, th_lines);
    init_cond_mutex();
    printf("Finished\n");
    // sleep(2);
    printf("start ...\n");

    int64_t bin_line = BIN_LINE;
    int64_t r_line = 0;
    int64_t prev_numlines;



    int64_t numlines = 0;
    task_ele_ptr_t item;
    printf("Start reading...");

    while(1) {
        prev_numlines = numlines;
        r_line = 0;
        int sig;
        printf("before scanf\n");
        printf("task_head %d\n", list_empty(&task_head));
        while((sig = fscanf(fp, "%s\n", line)) != EOF){
            item = new_task(numlines);\
            item->csv_str = strdup(line);
            numlines++;
            assign_work(item);
            r_line++;
            if(numlines % bin_line == 0){
                printf("numlines %d\n", numlines);
                printf("sig %d %d\n", sig, numlines);
                break;
            }
        }

        // while(!list_empty(&task_head)){
        //     pthread_cond_signal(&task_cond);
        //     pthread_cond_signal(&task_cond);
        //     printf("task_head %d\n", list_empty(&task_head));
        // }

        sleep(1);
        printf("EOF ? %d\n", sig);

        int64_t total = 0;
        printf("r_line %d\n", r_line);
        while(total != r_line) {
            total = 0;
            for(int64_t i=0;i<MAXTHS;i++){
                total += th_lines[i];
            }
            if(total % 1000 == 0 && total != 0){
                for(int64_t i=0;i<MAXTHS;i++) {
                    printf("tlines %d ", th_lines[i]);
                }
                printf("\n");
            }
        }
        printf("total %d\n", total);

        if(prev_numlines == numlines){
            fprintf(ofp, "]");
            fclose(ofp);
            break;
        }
        printf("%d\n", prev_numlines);

        printf("Finished\n");

        pthread_mutex_lock(&output_mutex);
        static_rows = numlines;
        pthread_mutex_unlock(&output_mutex);
        pthread_cond_signal(&output_cond);
        printf("Start outputing...");
        // int i_i = 0;
        // while(!*finished) {
        //     printf("%d %d\n", ACCESS_ONCE(*finished), i_i++);
        //     pthread_mutex_lock(&finish_mutex);
        //     if(!list_empty(&finish_head))
        //         pthread_cond_signal(&finish_cond);
        //     pthread_mutex_unlock(&finish_mutex);
        //     // pthread_mutex_lock(&task_mutex);
        //     // if(!list_empty(&task_head))
        //     //     pthread_cond_signal(&task_cond);
        //     // pthread_mutex_unlock(&task_mutex);
        //     sleep(1);
        // }
        // *finished = 0;
        // output_json_main(bin_line, &static_rows, ofp);


        printf("Iteration over\n");

        for(int i=0;i<MAXTHS;i++)
            th_lines[i] = 0;
    }
    fclose(fp);

    return 0;
}