#include <pthread.h>
#include "benchmark.h"

extern char *__wt_optarg;

static void begin_tx( WT_SESSION *session );
static void end_tx( WT_SESSION *session, int ret);
void wt_shutdown(WT_CONNECTION *conn);

int main(int argc, char* argv[] )
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    WT_ITEM key, value;
    char keybuf[64];
    char *valuebuf;
    u_int keyno;
    int ret;

    int i = 0;

    char* home;
    char config[512];
    char* config_open = "cache_size=4096MB";
    char* uri_prefix = "table:wt.%03d";

    char ch;

    int _key_no = 1;
    int _value_size = 100;

    char name[512];

    home = "WT_DATA";
    memset(name, 0x00, sizeof(name));
    sprintf(name, uri_prefix, 0);

    while ((ch = __wt_getopt( argv[0], argc, argv, "k:s:")) != EOF)
    {
        switch (ch) 
        {
            case 'k': // key number
                _key_no =  atoi( __wt_optarg );
                break;
            case 's': // value size
                _value_size =  atoi( __wt_optarg );
                break;
        }
    }


    snprintf(config, sizeof(config),
            "log=enabled, transaction_sync=enabled, statistics=(all),error_prefix=\"%s\",%s%s", 
            argv[0],
            config_open == NULL ? "" : ",",
            config_open == NULL ? "" : config_open);

    ret = wiredtiger_open(home, NULL, config, &conn);
    if( ret ) fprintf(stderr, "wiredtiger_open: %s\n", wiredtiger_strerror(ret));

    ret = conn->open_session( conn, NULL, "isolation=read-uncommitted", &session );
    if( ret ) fprintf(stderr, "open_session: %s\n", wiredtiger_strerror(ret));

    ret = session->open_cursor( session, name, NULL, NULL, &cursor );
    if( ret ) fprintf(stderr, "open_cursor: %s\n", wiredtiger_strerror(ret));

    valuebuf = (char*) malloc(_value_size);

    begin_tx( session);

    keyno = _key_no;
    key.data = keybuf;
    key.size = (uint32_t) snprintf(keybuf, sizeof(keybuf), "%017u", keyno);
    cursor->set_key(cursor, &key);

    value.data = valuebuf;
    value.size = _value_size;
    cursor->set_value(cursor, &value);

    update_value_string( value.data, _value_size);

    ret = cursor->update(cursor);
    if( ret ) fprintf(stderr, "update: %s\n", wiredtiger_strerror(ret));

    end_tx(session, ret);

    if ((ret = session->close(session, NULL)) != 0) 
        printf( "ERROR : session.close\n");

    wt_shutdown(conn);
}


void update_value_string( char* s, int len)
{
    int i;
    for(i=0; i < len; i++ )
    {   
        s[i] = '!';
    }   
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
