#include <stdlib.h>
#include "benchmark.h"

static char rand_string[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

void make_rand_string( char* s, int len, WT_RAND_STATE *rnd)
{
	int next_id, i;
	next_id = __wt_random(rnd)%52;
	for(i=0; i < len; i++ )
	{
		s[i] = rand_string[next_id];
	}
}
