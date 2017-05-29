#include <pthread.h>
#include "benchmark.h"

extern char *__wt_optarg;

void query_process(const char *name, WT_CONNECTION *conn, int num_records , int rec_size , int thread_id);
void wt_shutdown(WT_CONNECTION *conn);
void* query_processor(void *arg);

int exit_flag = 0;
int print_flag = 0;
int tx_cnt = 0;

int update_query_pct = 50; /*update trx pct default:50*/
struct thread_args *t_args;

int main(int argc, char* argv[] )
{
	WT_CONNECTION *conn;
	WT_SESSION *session;

	int i = 0;
	int ret=0;

	char* home;
	char config[512];
	char* config_open = "cache_size=61440MB";
	char* uri_prefix = "table:wt.%03d";

	char ch;

	int num_records = 100000;
	int num_index =42; 
	int record_size = 1000; /* ten fields, each field has 100B data*/
	int thread_num = 1;

	pthread_t *tids;

	home = "WT_DATA";

	while ((ch = __wt_getopt( argv[0], argc, argv, "r:i:s:w:u:")) != EOF)
	{
		switch (ch) 
		{
			case 'r': //num of records
				num_records =  atoi( __wt_optarg );
				break;
			case 'i': //num of indice
				num_index =  atoi( __wt_optarg );
				break;
			case 'w': //thread num
				thread_num =  atoi( __wt_optarg );
				break;
			case 'u': //update_query_pct
				update_query_pct =  atoi( __wt_optarg );
				break;

		}
	}

	printf( "num records : %d\n", num_records );
	printf( "num indice  : %d\n", num_index );
	printf( "record size : %d\n", record_size );
	printf( "thread num  : %d\n", thread_num );

	tids = (pthread_t*) malloc( sizeof(pthread_t)*thread_num );
	t_args = (struct thread_args*) malloc( sizeof(struct thread_args)*thread_num );

	snprintf(config, sizeof(config),
			"log=enabled, transaction_sync=enabled, statistics=(all),error_prefix=\"%s\",%s%s", 
			argv[0],
			config_open == NULL ? "" : ",",
			config_open == NULL ? "" : config_open);

	ret = wiredtiger_open(home, NULL, config, &conn);
	if( ret ) fprintf(stderr, "wiredtiger_open: %s\n", wiredtiger_strerror(ret));


    /*
        tx threads start!
    */
	for( i = 0 ; i < thread_num ; i++ )
	{
		memset( t_args[i].fname, 0x00, sizeof(t_args[i].fname));
		sprintf( t_args[i].fname, uri_prefix, i );
		t_args[i].conn = conn;
		t_args[i].num_records = num_records;
		t_args[i].rec_size = record_size;
		t_args[i].thread_id = i; 

		ret = __wt_thread_create( NULL, &tids[i], query_processor, (void *) &t_args[i] );
		if( ret ) perror("pthread_create:");
	}

	sleep( 60 ); /* warm-up period */

/*
#if __CPU__PROFILE__
        system("iostat -c 1 3 > top.result");
        system( "operf --system-wide --events CPU_CLK_UNHALTED:100000:0:1:1 &");
        sleep( 3 );
        system ("killall -SIGINT operf" );
        exit( 0 );
#endif*/

	print_flag = 1;
	{
		int sleep_time = 20;

		sleep( sleep_time ); /* measuring period */
		exit_flag = 1;

		printf("TOTAL TXNS: %d\n", tx_cnt);
		printf("TXNS/SEC: %d\n", tx_cnt/sleep_time);

	}

	for( i = 0 ; i < thread_num ; i++ )
	{
		__wt_thread_join(NULL, tids[i] );
	}
	wt_shutdown(conn);
}

void* query_processor(void *arg)
{
	struct thread_args _arg = *((struct thread_args*) arg);
	query_process( _arg.fname, _arg.conn, _arg.num_records, _arg.rec_size , _arg.thread_id );
}

static void begin_tx( WT_SESSION *session )
{
	int	ret = session->begin_transaction( session, "sync=1" );
	if( ret ) fprintf(stderr, "begin_transaction: %s\n", wiredtiger_strerror(ret));
}

static void end_tx( WT_SESSION *session, int ret )
{
	if ( ret == 0 )
	{
		ret = session->commit_transaction(session, NULL);
		if( ret ) fprintf(stderr, "commit_transaction: %s\n", wiredtiger_strerror(ret));
	}
	else if ( ret == WT_ROLLBACK )
	{
		ret = session->rollback_transaction(session, NULL);
		if( ret ) fprintf(stderr, "rollback_transaction: %s\n", wiredtiger_strerror(ret));
	}
	else
	{
		fprintf(stderr, "CRITICAL ERROR : %s\n", wiredtiger_strerror(ret));
	}

}

void query_process(const char *name, WT_CONNECTION *conn, int num_records , int rec_size, int thread_id )
{
	WT_SESSION *session; 
	WT_CURSOR *cursor;
	WT_ITEM key, value;
	char keybuf[64];
	char *valuebuf;
	int ret;
	int qt; //query type (0:read, 1:update)
	int update_position;
	u_int keyno;
	WT_RAND_STATE rnd;
	struct timespec begin_tx_ts, commit_tx_ts, end_tx_ts;
	uint64_t tx_diff;
	uint64_t commit_diff;

	__wt_random_init(&rnd);
	valuebuf = (char*) malloc(rec_size);


	ret = conn->open_session( conn, NULL, "isolation=read-uncommitted", &session );
	if( ret ) fprintf(stderr, "open_session: %s\n", wiredtiger_strerror(ret));

	ret = session->open_cursor( session, name, NULL, NULL, &cursor );
	if( ret ) fprintf(stderr, "open_cursor: %s\n", wiredtiger_strerror(ret));


	while( !exit_flag )
	{
		long k;

		clock_gettime(CLOCK_MONOTONIC, &begin_tx_ts); /* mark start time */
		begin_tx( session );

		keyno = __wt_random(&rnd) % num_records + 1;

		qt = __wt_random(&rnd) % 100 ;
		key.data = keybuf;
		key.size = (uint32_t) snprintf(keybuf, sizeof(keybuf), "%017u", keyno);
		cursor->set_key(cursor, &key);

		if ( qt < update_query_pct )
		{
			value.data = valuebuf;
			value.size = rec_size;
			cursor->set_value(cursor, &value);
			update_position = __wt_random(&rnd) % 10;
			make_rand_string( value.data + update_position * 100, 100 , &rnd);
	
			ret = cursor->update(cursor);
			if( ret ) fprintf(stderr, "update: %s\n", wiredtiger_strerror(ret));
		}
		else
		{
			if ((ret = cursor->search(cursor)) == 0)
				cursor->get_value(cursor, &value);
			if( ret ) fprintf(stderr, "read: %s\n", wiredtiger_strerror(ret));
		}

		clock_gettime(CLOCK_MONOTONIC, &commit_tx_ts); 
		end_tx(session, ret );
		clock_gettime(CLOCK_MONOTONIC, &end_tx_ts); /* mark the end time */


		tx_diff = (BILLION * (end_tx_ts.tv_sec - begin_tx_ts.tv_sec) + end_tx_ts.tv_nsec - begin_tx_ts.tv_nsec)/1000;
		commit_diff = (BILLION * (end_tx_ts.tv_sec - commit_tx_ts.tv_sec) + end_tx_ts.tv_nsec - commit_tx_ts.tv_nsec)/1000;

		if( print_flag )
		{
			__sync_fetch_and_add(&tx_cnt, 1 );
			if( tx_cnt % 100000 == 0 ) printf("%d\n", tx_cnt );

		}

	}

	if ((ret = session->close(session, NULL)) != 0) 
		printf( "ERROR : session.close\n");

}

void wt_shutdown(WT_CONNECTION *conn)
{
	WT_SESSION *session; 
	int ret;

	ret = conn->open_session(conn, NULL, NULL, &session);
	if( ret ) fprintf(stderr, "open_session: %s\n", wiredtiger_strerror(ret));

	ret = session->checkpoint(session, NULL);
	if( ret ) fprintf(stderr, "checkpoint: %s\n", wiredtiger_strerror(ret));

	ret = session->close(session, NULL);
	if( ret ) fprintf(stderr, "session->close: %s\n", wiredtiger_strerror(ret));
}
