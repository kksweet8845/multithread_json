#include "csv2json.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


void init_head(){
    INIT_LIST_HEAD(&task_head);
    INIT_LIST_HEAD(&finish_head);
}

void init_threads(int num_th, int64_t* rows, char* outputname, int* finished){
    ths = (pthread_t*) malloc(sizeof(pthread_t) * num_th);
    for(int i=0;i<num_th;i++){
        pthread_create(ths+i, NULL, worker, NULL);
    }
    output_th = (pthread_t*) malloc(sizeof(pthread_t));

    struct output_arg* o = malloc(sizeof(struct output_arg));
    o->rows = rows;
    o->outputfile = outputname;
    o->finished = finished;
    pthread_create(output_th, NULL, output_json, (void*) o);
}

void init_cond_mutex(){
    pthread_cond_init(&task_cond, NULL);
    pthread_cond_init(&finish_cond, NULL);
    pthread_cond_init(&output_cond, NULL);
    pthread_mutex_init(&task_mutex, NULL);
    pthread_mutex_init(&finish_mutex, NULL);
    pthread_mutex_init(&output_mutex, NULL);
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
    // static char* prefix = "\t{\n";
    // static char* posfix = "\t}";
    memset(buf, '\0', sizeof(char)*2048);

    sprintf(buf, "\t{\n");

    char col_str[100];

    for(int i=0;i<20;i++){
        memset(col_str, '\0', 100);
        if(i == 19){
            sprintf(col_str, "\t\t\"col_%d\":%d\n", i+1, arr[i]);
        }else{
            sprintf(col_str, "\t\t\"col_%d\":%d,\n", i+1, arr[i]);
        }

        buf = strcat(buf, col_str);
    }
    buf = strcat(buf, "\t}");
    *json = buf;
    // printf("HHHHH\n");
    fflush(stdout);
}


void parse_csv2json(const task_ele_ptr_t task){
    char* csv = task->csv_str;
    if(csv == NULL){
        printf("csv is null\n");
        return;
    }
    // printf("csv\n\n%s\n\n", csv);
    char* csv_cur = csv;
    // char* json_cur = json;
    char buf[30];
    char* buf_cur = buf;
    int i = 0;
    int32_t* num_arr = malloc(sizeof(int32_t) * 20);
    int32_t* num_cur = num_arr;
    char* ptr;
    i=0;
    while(i<20){
        memset(buf, '\0', 30);
        buf_cur = buf;
        while(*csv_cur != '|' && *csv_cur != '\0'){
            *buf_cur++ = *csv_cur++;
        }
        // printf("char %c\n", *csv_cur);
        // printf("buf %s\n", buf);
        if(*csv_cur == '|')
            csv_cur++;
        *num_cur = strtol(buf, &ptr, 10);
        // printf("i : %d, num_cur %d\n", i, *num_cur);
        fflush(stdout);
        num_cur++;
        i++;
    }
    // printf("Exit\n");
    gen_json(num_arr, &task->json_str);
    // printf("%s\n", task->json_str);
    return;
}



task_ele_ptr_t csv2json(task_ele_ptr_t task) {
    // printf("%s\n\n\n\n", task->csv_str);
    // printf("Inside %p\n", task);
    parse_csv2json(task);
    fflush(stdout);
    // printf("%s\n", task->json_str);
    // printf("Inside after %p\n", task);
    return task;
}


void assign_work(task_ele_ptr_t task){

    pthread_mutex_lock(&task_mutex);
    list_add_tail(&task->list, &task_head);
    pthread_mutex_unlock(&task_mutex);
    pthread_cond_signal(&task_cond);
}


void* worker() {

    struct list_head *cur;
    task_ele_ptr_t task;
    struct list_head task_thread_head;
    INIT_LIST_HEAD(&task_thread_head);
    while(1){
        pthread_mutex_lock(&task_mutex);
        while(list_empty(&task_head)){
            pthread_cond_wait(&task_cond, &task_mutex);
        }
        cur = task_head.next;
        list_del_init(cur);
        list_move_tail(cur, &task_thread_head);
        pthread_mutex_unlock(&task_mutex);
        cur = task_thread_head.next;
        list_del_init(cur);
        // printf("cur %p\n", cur);
        task = list_entry(cur, task_ele_t, list);
        // printf("task1 %p\n", task);
        task = csv2json(task);
        // printf("task2 %p\n", task);
        // printf("task list %p\n", &(task->list));
        // printf("%s\n", task->json_str);
        // printf("next: %p, prev: %p\n", cur->next, cur->prev);
        list_del_init(cur);
        pthread_mutex_lock(&finish_mutex);
        list_move_tail(cur, &finish_head);
        pthread_mutex_unlock(&finish_mutex);
        pthread_cond_signal(&finish_cond);
    }

    pthread_exit(NULL);
}


void place_priority(struct list_head* node, struct list_head *head){

    task_ele_ptr_t node_entry = list_entry(node, task_ele_t, list);


    task_ele_ptr_t item;

    if(list_empty(head)){
        list_add_tail(node, head);
        printf("empty\n");
        return;
    }
    struct list_head *prev;
    list_for_each_entry(item, head, list){

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
    int* finished = o->finished;

    struct list_head *cur;
    task_ele_ptr_t task;
    struct list_head output_th_head;
    INIT_LIST_HEAD(&output_th_head);
    int64_t i = 0;

    pthread_mutex_lock(&output_mutex);
    while(*rows == -1){
        pthread_cond_wait(&output_cond, &output_mutex);
    }
    pthread_mutex_unlock(&output_mutex);

    printf("rows %d\n", *rows);

    task_ele_ptr_t * task_arr = malloc(sizeof(task_ele_ptr_t) * (*rows));

    for(i=0;i<*rows;i++){
        task_arr[i] = NULL;
    }

    task_ele_ptr_t node_entry;
    i=0;
    task_ele_ptr_t item;
    FILE* fp;
    fp = fopen(outputname, "w");
    fprintf(fp, "[\n");
    while(i < *rows){
        pthread_mutex_lock(&finish_mutex);
        while(list_empty(&finish_head)){
            pthread_cond_wait(&finish_cond, &finish_mutex);
        }
        cur = finish_head.next;
        list_del_init(cur);
        pthread_mutex_unlock(&finish_mutex);
        node_entry = list_entry(cur, task_ele_t, list);
        task_arr[node_entry->index] = node_entry;
        // printf("%p %d\n", node_entry, node_entry->index);
        if(task_arr[i] != NULL) {
            item = task_arr[i];
            fwrite(item->json_str, strlen(item->json_str)*sizeof(char), 1, fp);
            free(item->csv_str);
            free(item->json_str);
            if(i != *rows - 1){
                fprintf(fp, ",\n");
            }else{
                fprintf(fp, "\n");
            }
            i++;
        }
    }

    // output file
    // task_ele_ptr_t item;
    // for(i=0;i<*rows;i++){
    //     item = task_arr[i];
    //     fwrite(item->json_str, strlen(item->json_str)*sizeof(char), 1, fp);
    //     free(item->csv_str);
    //     free(item->json_str);
    //     if(i != *rows - 1){
    //         fprintf(fp, ",\n");
    //     }else{
    //         fprintf(fp, "\n");
    //     }
    // }
    fprintf(fp, "]");
    *finished = 1;
    // list_for_each_entry(item, &output_th_head, list){

    //     printf("%s\n", item->json_str);

    //     fwrite(item->json_str, sizeof(item->json_str), 1, fp);
    //     free(item->csv_str);
    //     free(item->json_str);
    //     if(item->list.next != &output_th_head){
    //         fprintf(fp, ",\n");
    //     }else{
    //         fprintf(fp, "\n");
    //     }
    // }

    pthread_exit(NULL);

}