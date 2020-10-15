#ifndef CSV2JSON_H
#define CSV2JSON_H



#include "list.h"
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdint.h>


typedef struct TELE task_ele_t, *task_ele_ptr_t;

struct TELE {
    char* csv_str;
    char* json_str;
    int index;
    struct list_head list;
};

struct output_arg {
    int64_t* rows;
    char* outputfile;
    int* finished;
    int bin_line;
};

// struct worker_arg {

// };




/* Some static variable */
static struct list_head task_head;
static struct list_head finish_head;
pthread_cond_t task_cond, finish_cond, output_cond;
pthread_mutex_t task_mutex, finish_mutex, output_mutex;
pthread_t *ths, *output_th;



void init_head();
void init_threads(int, int64_t*, char*, int*, int);
void init_cond_mutex();
task_ele_ptr_t new_task(int);
void gen_json(int32_t* arr, char**);
void parse_csv2json(const task_ele_ptr_t);
task_ele_ptr_t csv2json(task_ele_ptr_t);
void assign_work(task_ele_ptr_t);
void* worker();
void place_priority(struct list_head*, struct list_head*);
void* output_json();















#endif