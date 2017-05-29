#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <wiredtiger.h>
#include "benchmark.h"

extern char *__wt_optarg;

void file_create(const char *name, WT_CONNECTION *conn);
void populate_record(const char *name, WT_CONNECTION *conn, int num_records , int rec_size );
void wt_shutdown(WT_CONNECTION *conn);

void* worker(void *arg);


int main(int argc, char* argv[] )
{
	WT_CONNECTION *conn;
	WT_SESSION *session;

	int i = 0;
	int ret=0;

	char* home;
	char config[512];
	char* config_open = "create,cache_size=4096MB";
	char* uri_prefix = "table:wt.%03d";
	char uri[512];

	char ch;

	int num_records = 100000; /*default is 10000*/
	int num_index =42; /* num of index trees ( = tables) .. */
	int record_size = 1000; /* ten fields, each field has 100B data*/

	struct thread_args *t_args;
	pthread_t tid;

	home = "WT_DATA";
	ret = system("rm -rf WT_DATA && mkdir WT_DATA");

	while ((ch = __wt_getopt( argv[0], argc, argv, "r:i:s:")) != EOF)
	{
			switch (ch) 
			{
				case 'r': //num of records
					num_records =  atoi( __wt_optarg );
					break;
				case 'i': //num of indice
					num_index =  atoi( __wt_optarg );
					break;
				case 's': //size of record
					record_size =  atoi( __wt_optarg );
					break;
			}
	}

	printf( "num records : %d\n", num_records );
	printf( "num indice  : %d\n", num_index );
	printf( "record size : %d\n", record_size );

	t_args = (struct thread_args*) malloc( sizeof(struct thread_args));

	
	snprintf(config, sizeof(config),
					"create,log=enabled, transaction_sync=enabled, statistics=(all),error_prefix=\"%s\",%s%s", 
					argv[0],
					config_open == NULL ? "" : ",",
					config_open == NULL ? "" : config_open);

	ret = wiredtiger_open(home, NULL, config, &conn);
	if( ret ) fprintf(stderr, "wiredtiger_open: %s\n", wiredtiger_strerror(ret));

	memset( t_args->fname, 0x00, sizeof(t_args->fname)); 
	sprintf( t_args->fname, uri_prefix, 0 ); 
	t_args->conn = conn; 
	t_args->num_records = num_records; 
	t_args->rec_size = record_size; 
	t_args->thread_id = num_index; //tricky method 

	ret = __wt_thread_create( NULL, &tid, worker, (void *) &t_args[i] );
	
	__wt_thread_join(NULL, tid );

	wt_shutdown(conn);
}

void* worker(void *arg)
{
	struct thread_args _arg = *((struct thread_args*) arg);
	int i = 0;
	char* uri_prefix = "table:wt.%03d";
	char uri[512];

	int num_records = _arg.num_records;
	int num_index = _arg.thread_id;
	int record_size = _arg.rec_size;

	for( i = 0 ; i < num_index ; i++ )
	{
		printf("%d data creating..\n", i );
		memset( &uri[0], 0x00, sizeof(uri));
		sprintf( uri, uri_prefix, i );
		file_create( uri, _arg.conn );
		populate_record(uri, _arg.conn, num_records , record_size );
	}
}

void populate_record(const char *name, WT_CONNECTION *conn, int num_records , int rec_size )
{
	WT_SESSION *session; 
	WT_CURSOR *cursor;
	WT_ITEM _key, _value;
	WT_ITEM *key, *value;
	char keybuf[64]; 
	char *valuebuf;
	int ret;
	u_int keyno;
	long k;
	WT_RAND_STATE rnd;

	valuebuf = (char*) malloc(rec_size);
	
	ret = conn->open_session( conn, NULL, "isolation=read-committed", &session );
	if( ret ) fprintf(stderr, "open_session: %s\n", wiredtiger_strerror(ret));

	ret = session->open_cursor( session, name, NULL, NULL, &cursor );
	if( ret ) fprintf(stderr, "open_cursor: %s\n", wiredtiger_strerror(ret));

	ret = session->begin_transaction( session, "sync=1" );
	if( ret ) fprintf(stderr, "begin_transaction: %s\n", wiredtiger_strerror(ret));

	key = &_key; 
	value = &_value;

	__wt_random_init(&rnd);

	for( keyno = 1; keyno <= num_records; ++keyno) 
	{
		k = __wt_random(&rnd);
		memset( valuebuf, 0x00, rec_size );
		make_rand_string( valuebuf, rec_size, &rnd );

		key->data = keybuf;
		key->size = (uint32_t) snprintf(keybuf, sizeof(keybuf), "%017u", keyno);
		cursor->set_key(cursor, key);
		value->data = valuebuf;
		value->size = rec_size; 
		cursor->set_value(cursor, value);
		if ((ret = cursor->insert(cursor)) != 0) 
		{
			printf( "ERROR : cursor.insert : %d\n", keyno );
			ret = session->rollback_transaction(session, NULL);
		}
	}

	ret = session->commit_transaction(session, NULL);
	if( ret ) fprintf(stderr, "commit_transaction: %s\n", wiredtiger_strerror(ret));

	if ((ret = session->close(session, NULL)) != 0) 
		printf( "ERROR : session.close\n");

	free( valuebuf );

}

void file_create(const char *name, WT_CONNECTION *conn)
{
	char* config = "key_format=u,internal_page_max=16384,leaf_page_max=131072";
	WT_SESSION *session; 
	int ret;

	ret = conn->open_session( conn, NULL, NULL, &session );
	if( ret ) fprintf(stderr, "open_session: %s\n", wiredtiger_strerror(ret));

	ret = session->create(session, name, config);
	if( ret ) fprintf(stderr, "session->create: %s\n", wiredtiger_strerror(ret));

	ret = session->close(session, NULL);
	if( ret ) fprintf(stderr, "session->close: %s\n", wiredtiger_strerror(ret));
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
