CC = gcc
FLAGS = -c

all: target

target: client.o
	$(CC) client.o -o client
	
client.o: client.c
	$(CC) $(FLAGS) client.c

clean:
	rm -rf *.o target 