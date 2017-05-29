#ifndef __BENCHMARK_H__
#define __BENCHMARK_H__

#define BILLION 1000000000L
#include <wiredtiger.h>
#include "wt_internal.h"
#include "extern.h"

/*
#define __CPU__PROFILE__ 1*/


struct thread_args
{
		char fname[512];
		WT_CONNECTION *conn;
		int num_records;
		int rec_size;
		int thread_id;
};

void make_rand_string( char* s, int len, WT_RAND_STATE *rnd);

#endif
