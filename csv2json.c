#include "csv2json.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>


void init_head(){
    INIT_LIST_HEAD(&task_head);
    INIT_LIST_HEAD(&finish_head);
}

void init_threads(int num_th, int64_t* rows, char* outputname, int* finished, int bin_line, int* th_lines) {
    ths = (pthread_t*) malloc(sizeof(pthread_t) * num_th);
    int* tmp;
    for(int i=0;i<num_th;i++){
        tmp = th_lines + i;
        pthread_create(ths+i, NULL, worker, (void*) tmp);
    }
    output_th = (pthread_t*) malloc(sizeof(pthread_t));

    struct output_arg* o = malloc(sizeof(struct output_arg));
    o->rows = rows;
    o->outputfile = outputname;
    o->finished = finished;
    o->bin_line = bin_line;
    pthread_create(output_th, NULL, output_json, (void*) o);
}

void init_cond_mutex() {
    pthread_cond_init(&task_cond, NULL);
    pthread_cond_init(&finish_cond, NULL);
    pthread_cond_init(&output_cond, NULL);
    pthread_cond_init(&end_cond, NULL);
    pthread_cond_init(&task_finish_cond, NULL);
    pthread_cond_init(&output_finish_cond, NULL);
    pthread_mutex_init(&task_mutex, NULL);
    pthread_mutex_init(&finish_mutex, NULL);
    pthread_mutex_init(&output_mutex, NULL);
    pthread_mutex_init(&end_mutex, NULL);
    pthread_mutex_init(&task_finish_mutex, NULL);
    pthread_mutex_init(&output_finish_mutex, NULL);
}


int check_task_empty(){
    return list_empty(&task_head);
}

int check_finish_empty() {
    return list_empty(&finish_head);
}

task_ele_ptr_t new_task(int index){
    task_ele_ptr_t task = (task_ele_ptr_t)malloc(sizeof(task_ele_t));
    task->csv_str = NULL;
    task->json_str = NULL;
    task->index = index;
    INIT_LIST_HEAD(&task->list);
    return task;
}

void gen_json(int32_t* arr, char** json){
    char* buf = malloc(sizeof(char) * 2048);
    if(buf == NULL){
        printf("return NULL\n");
        return;
    }
    memset(buf, '\0', sizeof(char) * 2048);
    sprintf(buf, "\t{\n");
    char col_str[100];
    for(int i=0;i<20;i++) {
        memset(col_str, '\0', 100);
        if(i == 19) {
            sprintf(col_str, "\t\t\"col_%d\":%d\n", i+1, arr[i]);
        }else {
            sprintf(col_str, "\t\t\"col_%d\":%d,\n", i+1, arr[i]);
        }
        buf = strcat(buf, col_str);
    }
    buf = strcat(buf, "\t}");
    *json = buf;
    fflush(stdout);
}

void parse_csv2json(const task_ele_ptr_t task){
    char* csv = task->csv_str;
    if(csv == NULL) {
        printf("csv is null\n");
        return;
    }
    char* csv_cur = csv;
    char buf[30];
    char* buf_cur = buf;
    int i = 0;
    int32_t* num_arr = malloc(sizeof(int32_t) * 20);
    int32_t* num_cur = num_arr;
    char* ptr;
    i=0;
    while(i<20) {
        memset(buf, '\0', 30);
        buf_cur = buf;
        while(*csv_cur != '|' && *csv_cur != '\0'){
            *buf_cur++ = *csv_cur++;
        }
        if(*csv_cur == '|')
            csv_cur++;
        *num_cur = strtol(buf, &ptr, 10);
        num_cur++;
        i++;
    }
    gen_json(num_arr, &task->json_str);
    return;
}

task_ele_ptr_t csv2json(task_ele_ptr_t task) {
    parse_csv2json(task);
    return task;
}

void assign_work(task_ele_ptr_t task) {
    pthread_mutex_lock(&task_mutex);
    list_add_tail(&task->list, &task_head);
    pthread_mutex_unlock(&task_mutex);
    // pthread_cond_signal(&task_cond);
}




void* worker(void* arg) {


    // pid_t tid;
    // tid = gettid();
    char outstream[20];
    memset(outstream, '\0', 20);

    int* tid = (int*) arg;
    // sprintf(outstream, "%d.stream", *tid);
    // FILE* fp = fopen(outstream, "w");

    struct list_head *cur;
    task_ele_ptr_t task;
    task_ele_ptr_t item, safe;
    struct list_head *cut_pos;
    struct list_head task_thread_head;
    INIT_LIST_HEAD(&task_thread_head);
    int err = 0;
    int i=0;
    int num_task = 0;
    int iter = 0;
    int initial = 1;
    int total = 0;
    struct timespec timeout = {.tv_nsec = 0, .tv_sec = 1};
    while(1) {
        err = pthread_mutex_lock(&task_mutex);
        while(list_empty(&task_head)) {
            if(initial == 0){
                pthread_cond_signal(&task_finish_cond);
                // fprintf(fp, "signal task finish_cond\n");
            }
            if(initial == 1) {
                initial = 0;
            }
            // fprintf(fp, "waiting\n");
            // fflush(fp);
            pthread_cond_timedwait(&task_cond, &task_mutex, &timeout);
        }
        // cur = task_head.next;
        num_task = 0;
        list_for_each(cut_pos, &task_head){
            if(++num_task == 10000){
                break;
            }
        }
        // fprintf(fp, "num_task %d\n", num_task);
        // fflush(fp);
        total += num_task;
        // printf("worker %d, num_task %d\n", *tid, total);
        if(num_task != 10000)
            list_splice_tail_init(&task_head, &task_thread_head);
        else
            list_cut_position(&task_thread_head, &task_head, cut_pos);
        // list_del_init(cur);
        // list_move_tail(cur, &task_thread_head);
        err = pthread_mutex_unlock(&task_mutex);
        if(!list_empty(&task_head)){
            pthread_cond_signal(&task_cond);
            // fprintf(fp, "signal\n");
        }
        // fprintf(fp, "before list_empty %d\n", list_empty(&task_thread_head));
        int numlines = 0;
        list_for_each_entry_safe(item, safe, &task_thread_head, list){
            // cur = &item->list;
            // list_del_init(cur);
            item = csv2json(item);
            // fprintf(fp, "index %d\n", item->index);
            numlines++;
        }
        // printf("worker %d, numlines %d, num_task %d\n", *tid, numlines, total);
        // fprintf(fp, "numlines %d\n", numlines);
        // cur = task_thread_head.next;
        // list_del_init(cur);
        // task = list_entry(cur, task_ele_t, list);
        // task = csv2json(task);
        // (*numlines)++;
        // list_del_init(cur);
        err = pthread_mutex_lock(&finish_mutex);
        list_splice_tail_init(&task_thread_head, &finish_head);
        err = pthread_mutex_unlock(&finish_mutex);
        // fprintf(fp, "list_empty %d, finish_list %d, iter %d\n", list_empty(&task_thread_head), list_empty(&finish_head), iter++);
    }
    pthread_exit(NULL);
}

void place_priority(struct list_head* node, struct list_head *head){
    task_ele_ptr_t node_entry = list_entry(node, task_ele_t, list);
    task_ele_ptr_t item;
    if(list_empty(head)){
        list_add_tail(node, head);
        return;
    }
    struct list_head *prev;
    list_for_each_entry(item, head, list) {
        printf("node_entry : %d, item : %d\n", node_entry->index, item->index);
        if(node_entry->index < item->index){
            prev = item->list.prev;
            item->list.prev = node;
            node->next = &item->list;
            node->prev = prev;
            prev->next = node;
            return;
        }
    }
    list_add_tail(node, head);
    return;
}


void* output_json(void* arg){

    struct output_arg *o = (struct output_arg*) arg;
    int64_t* rows = o->rows;
    char* outputname = o->outputfile;
    volatile int* finished = o->finished;
    int bin_line = o->bin_line;
    struct list_head *cur;
    task_ele_ptr_t task;
    struct list_head output_th_head;
    INIT_LIST_HEAD(&output_th_head);
    int64_t i = 0, index=0;
    index=0;

    task_ele_ptr_t node_entry;
    task_ele_ptr_t item;
    FILE* fp;
    FILE* s;

    task_ele_ptr_t * task_arr = malloc(sizeof(task_ele_ptr_t) * bin_line);

    fp = fopen(outputname, "w");
    // s = fopen("output.stream", "w");
    fprintf(fp, "[\n");
    int64_t pre_index = 0;
    int iter = 0;
    struct timespec timeout = {.tv_nsec = 0, .tv_sec = 1};
    while(1) {

        pthread_mutex_lock(&output_mutex);
        while(*rows == -1) {
            pthread_cond_wait(&output_cond, &output_mutex);
        }
        pthread_mutex_unlock(&output_mutex);
        // fprintf(s, "iter %d\n", iter++);

        for(i=0;i<bin_line;i++){
            task_arr[i] = NULL;
        }
        if(*rows == -2)
            break;

        while(index < *rows) {
            // printf("output>> %d, %d\n", index, *rows);
            int err = pthread_mutex_lock(&finish_mutex);
            while(list_empty(&finish_head)) {
                pthread_cond_timedwait(&finish_cond, &finish_mutex, &timeout);
            }
            list_splice_tail_init(&finish_head, &output_th_head);
            pthread_mutex_unlock(&finish_mutex);
            list_for_each_entry(item, &output_th_head, list){
                task_arr[item->index % bin_line] = item;
                index++;
            }
            INIT_LIST_HEAD(&output_th_head);
        }

        for(i=0;i < (*rows - pre_index);i++) {
            item = task_arr[i];
            fwrite(item->json_str, strlen(item->json_str)*sizeof(char), 1, fp);

            if(item->index == *rows - 1){
                fprintf(fp, "\n");
            }else {
                fprintf(fp, ",\n");
            }
            free(item->csv_str);
            free(item->json_str);
            free(item);
            task_arr[i] = NULL;
        }
        pre_index = *rows;
        *rows = -1;
        INIT_LIST_HEAD(&finish_head);
        pthread_cond_signal(&output_finish_cond);
    }
    fprintf(fp, "]");
    fclose(fp);
    free(task_arr);
    pthread_cond_signal(&end_cond);
    pthread_exit(NULL);
}