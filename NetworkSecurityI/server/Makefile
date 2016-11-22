CC = gcc
FLAGS = -c

all: target

target: server.o
	$(CC) server.o -o server
	
server.o: server.c
	$(CC) $(FLAGS) server.c

clean:
	rm -rf *.o target 