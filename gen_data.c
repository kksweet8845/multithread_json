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



int main(int argc, char* argv[]){

    char** ptr;
    int64_t numline = strtol(argv[1], ptr, 10);
    char* outputfile = argv[2];

    FILE* fp;
    fp = fopen(outputfile, "w");
    char* buf = malloc(sizeof(char)*1024);
    for(int64_t i=0;i<numline;i++){
        fprintf(fp, "%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d\n",
        rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),
        rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n(),rand_n());
    }

    return 0;

}