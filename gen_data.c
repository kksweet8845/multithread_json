#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define COL 20


int rand_n(){

    int mode = rand() % 1000 + 1;
    int r = rand() % ((int64_t)1 << 32);
    if(mode % 2 == 0)
        return r;
    else
        return -r;
}


void gen_line(char* buf){

    if(buf == NULL)
        return;
    memset(buf, '\0', sizeof(char)*1024);
    sprintf(buf, "%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d\n",
    rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),
    rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n());
    printf("%s", buf);
}

void gen_json(int *arr, FILE* fp){
    fprintf(fp, "\t{\n");
    for(int i=0;i<20;i++){
        if(i == 19) {
            fprintf(fp, "\t\t\"col_%d\":%d\n", i+1, arr[i]);
        }else {
            fprintf(fp, "\t\t\"col_%d\":%d,\n", i+1, arr[i]);
        }
    }
    fprintf(fp, "\t}");
}

int main(int argc, char* argv[]){

    char* ptr;
    printf("%d\n", argc);
    printf("%s\n", argv[1]);
    int64_t numline = strtol(argv[1], &ptr, 10);
    char* outputfile = argv[2];

    char outputpath[100];
    char verifypath[100];
    memset(outputpath, '\0', 100);
    memset(verifypath, '\0', 100);

    sprintf(outputpath, "./data/%s", outputfile);
    sprintf(verifypath, "./verify/%s", outputfile);

    FILE* fp, *vfp;
    fp = fopen(outputpath, "w");
    vfp = fopen(verifypath, "w");
    char* buf = malloc(sizeof(char)*1024);
    int* narr = malloc(sizeof(int) * 20);
    // for(int i=0;i<20;i++){
    //     narr[i] = rand_n();
    //     printf("%d ", narr[i]);
    // }
    fprintf(vfp, "[\n");
    for(int64_t i=0;i<numline;i++){
        for(int i=0;i<20;i++){
            narr[i] = rand_n();
        }
        fprintf(fp, "%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d\n",
        narr[0],narr[1],narr[2],narr[3],narr[4],narr[5],narr[6],narr[7],narr[8],narr[9],
        narr[10],narr[11],narr[12],narr[13],narr[14],narr[15],narr[16],narr[17],narr[18],narr[19]);

        gen_json(narr, vfp);
        if(i != numline - 1){
            fprintf(vfp, ",\n");
        }else {
            fprintf(vfp, "\n");
        }
    }
    fprintf(vfp, "]");

    return 0;

}