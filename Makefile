CC = gcc -std=c99
CCFLAGS = -Wall -g -I./include


EXEC = main

DEPS = csv2json.o

GIT_HOOKS := .git/hooks/applied

all: $(DEPS)
	rm -rf $(EXEC)
	$(CC) -o $(EXEC) $(CCFLAGS) $(EXEC).c $(DEPS) -lpthread
	rm -rf *.o


csv2json.o:
	$(CC) -c csv2json.c -I./include -lpthread

gen:
	$(CC) -o gen gen_data.c

clean:
	rm -rf $(EXEC) *.o *.stream