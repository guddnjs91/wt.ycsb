OBJ_COMMON =  benchmark.o
OBJ_POPULATE = populate_records.o  
OBJ_YCSB = ycsb.o
OBJ_QUERY_SAMPLE = query.o

TARGET_POPULATE = populate_records
TARGET_YCSB = ycsb
TARGET_QUERY = query

C_FLAGS_DEFS =  -I../wiredtiger -I../wiredtiger/src/include 
C_FLAGS_INC =  
C_FLAGS_OPTS =  -g

LD_FLAGS_DEFS =  
LD_FLAGS_OPTS =  -lrt -ldl -lpthread
LD_FLAGS_AR =  ../wiredtiger/.libs/libwiredtiger.a 
C_FLAGS = $(C_FLAGS_DEFS) $(C_FLAGS_INC) $(C_FLAGS_OPTS)


.c.o: 
	gcc -c $(C_FLAGS) $<

all : $(TARGET_POPULATE) $(TARGET_YCSB) $(TARGET_QUERY)


$(TARGET_POPULATE) : $(OBJ_COMMON) $(OBJ_POPULATE)
	gcc $(LD_FLAGS_DEFS) -o $(TARGET_POPULATE) $(OBJ_COMMON) $(OBJ_POPULATE) $(LD_FLAGS_AR) $(LD_FLAGS_OPTS)

$(TARGET_YCSB) : $(OBJ_COMMON) $(OBJ_YCSB)
	gcc $(LD_FLAGS_DEFS) -o $(TARGET_YCSB) $(OBJ_COMMON) $(OBJ_YCSB) $(LD_FLAGS_AR) $(LD_FLAGS_OPTS)

$(TARGET_QUERY) : $(OBJ_COMMON) $(OBJ_QUERY_SAMPLE)
	gcc $(LD_FLAGS_DEFS) -o $(TARGET_QUERY) $(OBJ_COMMON) $(OBJ_QUERY_SAMPLE) $(LD_FLAGS_AR) $(LD_FLAGS_OPTS)

clean :
	rm -f *.o $(TARGET_POPULATE) $(TARGET_YCSB) $(TARGET_QUERY)
