CC = gcc
CFLAGS = -Wall -Wextra -g -DDEBUG
TARGET = program

# List of source files
SRC = main.c read.c helper.c write.c create.c list.c delete.c retreive.c copy.c stream.c

# Default target
all: $(TARGET)

# Rule to build the target executable
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(if $(TIMEOUT),-DTIMEOUT=$(TIMEOUT)) $(if $(RETRIES),-DRETRIES=$(RETRIES)) $^ -o $@

# Clean up
clean:
	rm -f $(TARGET)