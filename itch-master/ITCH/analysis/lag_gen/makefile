CC = cc
CXXFLAGS = -fPIC -Wall -Wextra -O2 -g -c
LDFLAGS  = -shared

TARGET = liblag_gen.so
SOURCES = lag_gen.c
OBJECTS = lag_gen.o

all: $(TARGET)

$(TARGET): $(OBJECTS); $(CC) $(LDFLAGS) -o $(TARGET) $(OBJECTS)

$(OBJECTS): $(SOURCES); $(CC) $(CXXFLAGS)  -o $(OBJECTS) $(SOURCES)
