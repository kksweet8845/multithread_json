#include "csv2json.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>



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

    // struct output_arg* o = malloc(sizeof(struct output_arg));
    // o->rows = rows;
    // o->outputfile = outputname;
    // o->finished = finished;
    // o->bin_line = bin_line;
    // pthread_create(output_th, NULL, output_json, (void*) o);
}

void init_cond_mutex() {
    pthread_cond_init(&task_cond, NULL);
    pthread_cond_init(&finish_cond, NULL);
    pthread_cond_init(&output_cond, NULL);
    pthread_mutex_init(&task_mutex, NULL);
    pthread_mutex_init(&finish_mutex, NULL);
    pthread_mutex_init(&output_mutex, NULL);
}

task_ele_ptr_t new_task(int index){

    task_ele_ptr_t task = (task_ele_ptr_t)malloc(sizeof(task_ele_t));
    if(task == NULL){
        printf("Error when mallocing\n");
    }
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
    // printf("before lock\n");
    pthread_mutex_lock(&task_mutex);
    list_add_tail(&task->list, &task_head);
    pthread_mutex_unlock(&task_mutex);
    // printf("after lock\n");
    while(list_empty(&task_head)){
        printf("inside empty work\n");
    }
    // pthread_cond_signal(&task_cond);


    // task_ele_ptr_t aitem;
    // int numitem = 0;
    // list_for_each_entry(aitem, &task_head, list){
    //     numitem++;
    // }
    // printf("numitem in assign work %d\n", numitem);
    // if(numitem == 1000){
    //     printf("before signal\n");
    //     // int err = pthread_cond_signal(&task_cond);
    //     printf("err %d\n", pthread_cond_signal(&task_cond));
    //     printf("err %d\n", pthread_cond_signal(&task_cond));
    //     // err = pthread_cond_signal(&task_cond);
    //     printf("numitem %d\n", numitem);
    // }
}

void* worker(void* arg) {

    pid_t tid;
    tid = gettid();
    int* numlines = (int*) arg;

    struct list_head *cur;
    task_ele_ptr_t task;
    struct list_head task_thread_head;
    INIT_LIST_HEAD(&task_thread_head);
    int err = 0;
    int i=0;
    while(1) {
        // printf("%d %d\n", *numlines, tid);
        err = pthread_mutex_lock(&task_mutex);
        while(list_empty(&task_head)) {
            printf("list empty %d, tid %d\n", *numlines, tid);
            pthread_cond_wait(&task_cond, &task_mutex);
        }
        cur = task_head.next;
        list_del_init(cur);
        list_move_tail(cur, &task_thread_head);
        err = pthread_mutex_unlock(&task_mutex);
        cur = task_thread_head.next;
        list_del_init(cur);
        task = list_entry(cur, task_ele_t, list);
        task = csv2json(task);
        (*numlines)++;
        list_del_init(cur);
        err = pthread_mutex_lock(&finish_mutex);
        list_move_tail(cur, &finish_head);
        err = pthread_mutex_unlock(&finish_mutex);
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


void output_json_main(int bin_line, int64_t *rows, FILE* fp){

    task_ele_ptr_t item;
    int err = pthread_mutex_lock(&finish_mutex);
    fprintf(fp, "[\n");

    task_ele_ptr_t *task_arr = malloc(sizeof(task_ele_ptr_t) * bin_line);


    list_for_each_entry(item, &finish_head, list){
        task_arr[item->index % bin_line] = item;
    }

    for(int64_t i=0;i<bin_line && i < *rows;i++) {
        item = task_arr[i];
        list_del_init(&item->list);
        fwrite(item->json_str, strlen(item->json_str)*sizeof(char), 1, fp);
        free(item->csv_str);
        free(item->json_str);
        free(item);
        if(i != bin_line - 1) {
            fprintf(fp, ",\n");
        }else {
            fprintf(fp, "\n");
        }
        task_arr[i] = NULL;
    }

    pthread_mutex_unlock(&finish_mutex);

    if(!list_empty(&task_head)){
        pthread_cond_signal(&task_cond);
    }


    free(task_arr);
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

    task_ele_ptr_t *task_arr = malloc(sizeof(task_ele_ptr_t) * bin_line);

    fp = fopen(outputname, "w");
    fprintf(fp, "[\n");

    while(1){

        pthread_mutex_lock(&output_mutex);
        while(*rows == -1) {
            pthread_cond_wait(&output_cond, &output_mutex);
        }
        pthread_mutex_unlock(&output_mutex);

        for(i=0;i<bin_line;i++){
            task_arr[i] = NULL;
        }

        while(index < *rows) {
            int err = pthread_mutex_lock(&finish_mutex);
            while(list_empty(&finish_head)) {
                pthread_cond_wait(&finish_cond, &finish_mutex);
            }
            cur = finish_head.next;
            list_del_init(cur);
            pthread_mutex_unlock(&finish_mutex);
            node_entry = list_entry(cur, task_ele_t, list);
            task_arr[node_entry->index % bin_line] = node_entry;
            if(task_arr[node_entry->index % bin_line ] != NULL) index++;
            fflush(stdout);
        }

        for(i=0;i<bin_line && i < *rows;i++) {
            item = task_arr[i];
            fwrite(item->json_str, strlen(item->json_str)*sizeof(char), 1, fp);
            free(item->csv_str);
            free(item->json_str);
            free(item);
            if(i != bin_line - 1) {
                fprintf(fp, ",\n");
            }else {
                fprintf(fp, "\n");
            }
            task_arr[i] = NULL;
        }
        *finished = 1;
        *rows = -1;
    }
    fprintf(fp, "]");
    fclose(fp);
    // fclose(s);
    free(task_arr);
    pthread_exit(NULL);
}