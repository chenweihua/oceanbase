#include "common/ob_define.h"
#include "tblog.h"
#include "common/ob_malloc.h"
#include "ob_sql_conn_acquire.h"
#include "ob_sql_mysql_adapter.h"
#include "ob_sql_cluster_select.h"
#include "ob_sql_parser.h"
#include "ob_sql_util.h"
#include "ob_sql_data_source_utility.h"

using namespace oceanbase::common;

void mysql_set_consistence(MYSQL *mysql)
{
  ObSQLMySQL *ob_mysql = (ObSQLMySQL*)mysql;
  ob_mysql->is_consistence_ = 1;
}

void mysql_unset_consistence(MYSQL *mysql)
{
  ObSQLMySQL *ob_mysql = (ObSQLMySQL*)mysql;
  ob_mysql->is_consistence_ = 0;
}

my_bool is_consistence(ObSQLMySQL *mysql)
{
  return mysql->is_consistence_;
}

my_bool need_retry(ObSQLMySQL *mysql)
{
  my_bool ret = 1;
  if (is_in_transaction(mysql))
  {
    ret = 0;
  }
  return ret;
}

void mysql_set_retry(ObSQLMySQL *mysql)
{
  mysql->retry_ = 1;
}

void mysql_unset_retry(ObSQLMySQL *mysql)
{
  mysql->retry_ = 0;
}

my_bool is_retry(ObSQLMySQL *mysql)
{
  return mysql->retry_;
}

void mysql_set_in_transaction(MYSQL *mysql)
{
  ObSQLMySQL *ob_mysql = (ObSQLMySQL*)mysql;
  ob_mysql->in_transaction_ = 1;
}

void mysql_unset_in_transaction(MYSQL *mysql)
{
  ObSQLMySQL *ob_mysql = (ObSQLMySQL*)mysql;
  ob_mysql->in_transaction_ = 0;
}

my_bool is_in_transaction(ObSQLMySQL *mysql)
{
  return mysql->in_transaction_;
}

int STDCALL mysql_server_init(int argc __attribute__((unused)),
			      char **argv __attribute__((unused)),
			      char **groups __attribute__((unused)))
{
  if (0 == g_inited) //not inited
  {
    return 1;//error
  }
  else
  {
    return (*g_func_set.real_mysql_server_init)(argc, argv, groups);
  }
}

void STDCALL mysql_server_end(void)
{
  return (*g_func_set.real_mysql_server_end)();
}

//do nothing here same as libmysql
MYSQL_PARAMETERS *STDCALL mysql_get_parameters(void)
{
  return (*g_func_set.real_mysql_get_parameters)();
}

my_bool STDCALL mysql_thread_init(void)
{
  return (*g_func_set.real_mysql_thread_init)();
}

void STDCALL mysql_thread_end(void)
{
  return (*g_func_set.real_mysql_thread_end)();
}

my_ulonglong STDCALL mysql_num_rows(MYSQL_RES *result)
{
  return (*g_func_set.real_mysql_num_rows)(result);
}

unsigned int STDCALL mysql_num_fields(MYSQL_RES *result)
{
  return (*g_func_set.real_mysql_num_fields)(result);
}

my_bool STDCALL mysql_eof(MYSQL_RES *res)
{
  return (*g_func_set.real_mysql_eof)(res);
}

MYSQL_FIELD *STDCALL mysql_fetch_field_direct(MYSQL_RES *res, unsigned int fieldnr)
{
  return (*g_func_set.real_mysql_fetch_field_direct)(res, fieldnr);
}

MYSQL_FIELD * STDCALL mysql_fetch_fields(MYSQL_RES *res)
{
  return (*g_func_set.real_mysql_fetch_field)(res);
}

MYSQL_ROW_OFFSET STDCALL mysql_row_tell(MYSQL_RES *res)
{
  return (*g_func_set.real_mysql_row_tell)(res);
}

MYSQL_FIELD_OFFSET STDCALL mysql_field_tell(MYSQL_RES *res)
{
  return (*g_func_set.real_mysql_field_tell)(res);
}

unsigned int STDCALL mysql_field_count(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_field_count);
}

my_ulonglong STDCALL mysql_affected_rows(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_affected_rows);
}

my_ulonglong STDCALL mysql_insert_id(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_insert_id);
}

unsigned int STDCALL mysql_errno(MYSQL *mysql)
{
  if (NULL != ((ObSQLMySQL*)mysql)->conn_)
  {
    CALLREAL(mysql, mysql_errno);
  }
  else
  {
    return 2006;
  }
}

const char * STDCALL mysql_error(MYSQL *mysql)
{
  if (NULL != ((ObSQLMySQL*)mysql)->conn_)
  {
    CALLREAL(mysql, mysql_error);
  }
  else
  {
    return "MySQL server has gone away";
  }
}

const char *STDCALL mysql_sqlstate(MYSQL *mysql)
{
  if (NULL != ((ObSQLMySQL*)mysql)->conn_)
  {
    CALLREAL(mysql, mysql_sqlstate);
  }
  else
  {
    return "HY000";
  }
}

unsigned int STDCALL mysql_warning_count(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_warning_count);
}

const char * STDCALL mysql_info(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_info);
}

unsigned long STDCALL mysql_thread_id(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_thread_id);
}

const char * STDCALL mysql_character_set_name(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_character_set_name);
}

int STDCALL mysql_set_character_set(MYSQL *mysql, const char *csname)
{
  CALLREALWITHJUDGE(mysql, mysql_set_character_set, csname);
}

MYSQL * STDCALL mysql_init(MYSQL *mysql)
{
  int flag = OB_SQL_SUCCESS;
  TBSYS_LOG(INFO, "mysql init called");
  if (0 == g_inited) //not inited
  {
    TBSYS_LOG(INFO, "not inited");
    return NULL;
  }
  
  if (NULL == mysql)
  {
    mysql = (MYSQL *)ob_malloc(sizeof(MYSQL), ObModIds::LIB_OBSQL);
    if (NULL == mysql)
    {
      TBSYS_LOG(ERROR, "alloc mem for MYSQL failed");
      flag = OB_SQL_ERROR;
    }
    else
    {
      TBSYS_LOG(INFO, "alloc mem for MYSQL");
      memset(mysql, 0, sizeof(MYSQL));
      (*g_func_set.real_mysql_init)(mysql);
      TBSYS_LOG(INFO, "after alloc mem for MYSQL");
      ((ObSQLMySQL*)mysql)->alloc_ = 1;
    }
  }
  else
  {
    memset(mysql,0, sizeof(MYSQL));
    (*g_func_set.real_mysql_init)(mysql);
  }

  if (OB_SQL_SUCCESS == flag)
  {
    ((ObSQLMySQL*)mysql)->magic_ = OB_SQL_MAGIC;
    //((ObSQLMySQL*)mysql)->charset = default_client_charset_info;
    TBSYS_LOG(DEBUG, "call acquire conn random in mysql init");
    if (0 == pthread_rwlock_rdlock(&g_config_rwlock)) //防止g_group_ds被修改
    {
      //随便选一条先
      ObSQLConn *conn = acquire_conn_random(&g_group_ds, (ObSQLMySQL*)mysql);
      if (NULL == conn)
      {
        TBSYS_LOG(WARN, "There are no connction in groupdatasource");
        if (1 == ((ObSQLMySQL*)mysql)->alloc_)
        {
          ob_free(mysql);
          mysql = NULL;
        }
      }
      else
      {
        ((ObSQLMySQL*)mysql)->conn_ = conn;
        ((ObSQLMySQL*)mysql)->rconn_ = conn;
        ((ObSQLMySQL*)mysql)->wconn_ = conn;
      }
      pthread_rwlock_unlock(&g_config_rwlock);
    }
    else
    {
      if (1 == ((ObSQLMySQL*)mysql)->alloc_)
      {
        ob_free(mysql);
        mysql = NULL;
      }
      TBSYS_LOG(ERROR, "pthread_rwlock_rdlock on g_config_rwlock failed");
    }
  }
  return mysql;
}

my_bool STDCALL mysql_ssl_set(MYSQL *mysql, const char *key, const char *cert, const char *ca, const char *capath, const char *cipher)
{
  CALLREALWITHJUDGE(mysql, mysql_ssl_set, key, cert, ca, capath, cipher);
}

my_bool STDCALL mysql_change_user(MYSQL *mysql, const char *user, const char *passwd, const char *db)
{
  CALLREALWITHJUDGE(mysql, mysql_change_user, user, passwd, db);
}

//do nothing if it is obsql mysql handler
MYSQL * STDCALL mysql_real_connect(MYSQL *mysql, const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long clientflag)
{
  if (OB_SQL_MAGIC == ((ObSQLMySQL*)mysql)->magic_)
  {
    UNUSED(host);
    UNUSED(user);
    UNUSED(passwd);
    UNUSED(db);
    UNUSED(port);
    UNUSED(unix_socket);
    UNUSED(clientflag);
  }
  else
  {
    mysql = (*(g_func_set.real_mysql_real_connect))(mysql, host, user, passwd, db, port, unix_socket, clientflag);
  }
  return mysql;
}

int STDCALL mysql_select_db(MYSQL *mysql, const char *db)
{
  CALLREALWITHJUDGE(mysql, mysql_select_db, db);
}

//wrapper of mysql_reql_query
int STDCALL mysql_query(MYSQL *mysql, const char *q)
{
  //if (NULL == ((ObSQLMySQL*)mysql)->conn_)
  //{
  //  return 2006;
  //}
  //else
  //{
    return mysql_real_query(mysql, q, strlen(q));
    //}
}

/*get an real MYSQL handle from obsql according to query&& consistence property*/
static MYSQL* select_connection(MYSQL *mysql, const char *q, unsigned long length, ObSQLType *stype)
{
  ObClusterInfo * cluster = NULL;
  MYSQL *real_mysql = NULL;

  //release connection acquier when mysql_init
  if (NULL != ((ObSQLMySQL*)mysql)->conn_
      && (((ObSQLMySQL*)mysql)->conn_  == ((ObSQLMySQL*)mysql)->wconn_)
      && (((ObSQLMySQL*)mysql)->conn_  == ((ObSQLMySQL*)mysql)->rconn_))
  {
    pthread_rwlock_rdlock(&g_config_rwlock);
    TBSYS_LOG(DEBUG, "release conn mysql is %p real mysql is %p  %p %p %p", mysql, ((ObSQLMySQL*)mysql)->conn_->mysql_, ((ObSQLMySQL*)mysql)->conn_,
              ((ObSQLMySQL*)mysql)->wconn_, ((ObSQLMySQL*)mysql)->rconn_);
    release_conn(((ObSQLMySQL*)mysql)->conn_);
    pthread_rwlock_unlock(&g_config_rwlock);
    ((ObSQLMySQL*)mysql)->conn_ = NULL;
    ((ObSQLMySQL*)mysql)->rconn_ = NULL;
    ((ObSQLMySQL*)mysql)->wconn_ = NULL;
  }
  TBSYS_LOG(DEBUG, "select connection msyql is %p has stmt is %d", mysql, ((ObSQLMySQL*)mysql)->has_stmt_);
  if (!((ObSQLMySQL*)mysql)->has_stmt_)
  {
    //pase sql type
    *stype = get_sql_type(q, length);
    if (OB_SQL_BEGIN_TRANSACTION == *stype)
    {
      mysql_set_in_transaction(mysql);
      mysql_set_consistence(mysql);
      TBSYS_LOG(INFO, "set consistence %p", mysql);
    }
    else if (OB_SQL_CONSISTENCE_REQUEST == *stype)
    {
      //never reach here now
      mysql_set_consistence(mysql);
      TBSYS_LOG(INFO, "set consistence %p", mysql);
    }
    else if (OB_SQL_READ != *stype)
    {
      mysql_set_consistence(mysql);
      TBSYS_LOG(INFO, "set consistence %p", mysql);
    }

    //选择主集群
    if (is_in_transaction((ObSQLMySQL*)(mysql))
        ||is_consistence((ObSQLMySQL*)(mysql)))
    {
      TBSYS_LOG(DEBUG, "query to master cluster");
      if (is_retry((ObSQLMySQL*)mysql) || NULL == (((ObSQLMySQL*)mysql)->wconn_))
      {
        if (0 == pthread_rwlock_rdlock(&g_config_rwlock))
        {
          if (NULL != (((ObSQLMySQL*)mysql)->wconn_))
          {
            release_conn(((ObSQLMySQL*)mysql)->wconn_);
          }
          //根据流量分配和请求类型，选择集群 mysql被设置成consistence 选择主集群
          cluster = select_cluster((ObSQLMySQL*)mysql);
          //每次请求都需要根据sql做一致性hash来在特定的集群里面选择新的ms
          ObSQLConn *conn = acquire_conn(cluster, q, length, (ObSQLMySQL*)mysql);
          if (NULL != conn)
          {
            ((ObSQLMySQL*)mysql)->wconn_ = conn;
            ((ObSQLMySQL*)mysql)->conn_ = conn;
          }
          else
          {
            TBSYS_LOG(ERROR, "can not acquire a connection in cluster (id is %u)", cluster->cluster_id_);
          }
          pthread_rwlock_unlock(&g_config_rwlock);
        }
        else
        {
          TBSYS_LOG(ERROR, "pthread_rwlock_rdlock failed on g_config_rwlock");
        }
      }
      else
      {
        ((ObSQLMySQL*)mysql)->conn_ = ((ObSQLMySQL*)mysql)->wconn_;
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "ordianry query");
      //release connection
      if (NULL != ((ObSQLMySQL*)mysql)->conn_)
      {
        ObSQLConn *rconn = ((ObSQLMySQL*)mysql)->rconn_;
        ObSQLConn *wconn = ((ObSQLMySQL*)mysql)->wconn_;
        pthread_rwlock_rdlock(&g_config_rwlock);
        if (NULL != rconn)
        {
          release_conn(rconn);
        }
        else if (NULL != wconn)
        {
          release_conn(wconn);
        }
        pthread_rwlock_unlock(&g_config_rwlock);
      }

      if (0 == pthread_rwlock_rdlock(&g_config_rwlock))
      {
        //根据流量分配和请求类型，选择集群
        cluster = select_cluster((ObSQLMySQL*)mysql);
        //每次请求都需要根据sql做一致性hash来在特定的集群里面选择新的ms
        ObSQLConn *conn = acquire_conn(cluster, q, length, (ObSQLMySQL*)mysql);
        if (NULL != conn)
        {
          ((ObSQLMySQL*)mysql)->rconn_ = conn;
          ((ObSQLMySQL*)mysql)->conn_ = conn;
          TBSYS_LOG(DEBUG, "conn is %p mysql is %p", conn, conn->mysql_);
          real_mysql = ((ObSQLMySQL*)mysql)->conn_->mysql_;
        }
        else
        {
          TBSYS_LOG(ERROR, "can not acquire a connection in cluster (id is %u)", cluster->cluster_id_);
        }
        pthread_rwlock_unlock(&g_config_rwlock);
      }
      else
      {
        TBSYS_LOG(ERROR, "pthread_rwlock_rdlock failed on g_config_rwlock");
      }
    }
  }
  if (NULL != (((ObSQLMySQL*)mysql)->conn_))
  {
    real_mysql = ((ObSQLMySQL*)mysql)->conn_->mysql_;
    TBSYS_LOG(DEBUG, "return real_mysql is %p", real_mysql);
  }
  TBSYS_LOG(DEBUG, "after select mysql is %p, conn is %p, wconn is %p, rconn is %p", mysql, ((ObSQLMySQL*)mysql)->conn_, ((ObSQLMySQL*)mysql)->wconn_, ((ObSQLMySQL*)mysql)->rconn_);
  return real_mysql;
}

int STDCALL mysql_send_query(MYSQL *mysql, const char *q, unsigned long length)
{
  int ret = 0;
  ObSQLType stype = OB_SQL_UNKNOWN;
  MYSQL* real_mysql = NULL;
  if (OB_SQL_MAGIC == ((ObSQLMySQL*)mysql)->magic_)
  {
    if (NULL == ((ObSQLMySQL*)mysql)->conn_)
    {
      ret = 2006;
    }
    else
    {
      real_mysql = select_connection(mysql, q, length, &stype);
      if (NULL != real_mysql)
      {
        ret = (*(g_func_set.real_mysql_send_query))(real_mysql, q, length);
      }
      else
      {
        TBSYS_LOG(WARN, "can not find a valid connection");
        ret = OB_SQL_ERROR;
      }
    }
    TBSYS_LOG(INFO, "mysql_send_query");
    if (OB_SQL_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "stype is %d", stype);
      if (OB_SQL_END_TRANSACTION == stype)
      {
        mysql_unset_in_transaction(mysql);
        mysql_unset_consistence(mysql);
        TBSYS_LOG(INFO, "unset consistence %p", mysql);
      }
      else if (OB_SQL_READ != stype)
      {
        mysql_unset_consistence(mysql);
        TBSYS_LOG(INFO, "unset consistence %p", mysql);
      }
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "query is %s with real mysql handle %p", q, mysql);
    ret = (*(g_func_set.real_mysql_send_query))(mysql, q, length);
  }
  return ret;
}

//1. select a connect
//2. fault tolerant 不做容错 由应用来处理
int STDCALL mysql_real_query(MYSQL *mysql, const char *q, unsigned long length)
{
  int ret = 0;
  TBSYS_LOG(DEBUG, "1 mysql handle is %p query is %s", mysql, q);
  ObSQLType stype = OB_SQL_UNKNOWN;
  MYSQL* real_mysql = NULL;
  if (OB_SQL_MAGIC == ((ObSQLMySQL*)mysql)->magic_)
  {
    TBSYS_LOG(DEBUG, "magic 1 mysql handle is %p query is %s", mysql, q);
    real_mysql = select_connection(mysql, q, length, &stype);
    if (NULL != real_mysql)
    {
      ret = (*(g_func_set.real_mysql_real_query))(real_mysql, q, length);
      //retry another mergeserver if necessary
      if (0 != ret && need_retry((ObSQLMySQL*)mysql))
      {
        mysql_set_retry((ObSQLMySQL*)mysql);
        TBSYS_LOG(DEBUG, "retry query mysql handle is %p, query is %s", mysql, q);
        real_mysql = select_connection(mysql, q, length, &stype);
        if (NULL != real_mysql)
        {
          ret = (*(g_func_set.real_mysql_real_query))(real_mysql, q, length);
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN, "can not find a valid connection");
      ret = OB_SQL_ERROR;
    }

    if (OB_SQL_SUCCESS == ret &&
        OB_SQL_END_TRANSACTION == stype)
    {
      mysql_unset_in_transaction(mysql);
      mysql_unset_consistence(mysql);
      TBSYS_LOG(INFO, "unset consistence %p", mysql);
    }
    if (OB_SQL_READ != stype)
    {
      mysql_unset_consistence(mysql);
      TBSYS_LOG(INFO, "unset consistence %p", mysql);
    }
    mysql_unset_retry((ObSQLMySQL *)mysql);
  }
  else
  {
    ret = (*(g_func_set.real_mysql_real_query))(mysql, q, length);
  }
  return ret;
}

MYSQL_RES * STDCALL mysql_store_result(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_store_result);
}

MYSQL_RES * STDCALL mysql_use_result(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_use_result);
}

void STDCALL mysql_get_character_set_info(MYSQL *mysql, MY_CHARSET_INFO *charset)
{
  CALLREALWITHJUDGE(mysql, mysql_get_character_set_info, charset);
}

int STDCALL mysql_shutdown(MYSQL *mysql, enum mysql_enum_shutdown_level shutdown_level)
{
  CALLREALWITHJUDGE(mysql, mysql_shutdown, shutdown_level);
}

int STDCALL mysql_dump_debug_info(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_dump_debug_info);
}

int STDCALL mysql_refresh(MYSQL *mysql, unsigned int refresh_options)
{
  CALLREALWITHJUDGE(mysql, mysql_refresh, refresh_options);
}

int STDCALL mysql_kill(MYSQL *mysql,unsigned long pid)
{
  CALLREALWITHJUDGE(mysql, mysql_kill, pid);
}

int STDCALL mysql_set_server_option(MYSQL *mysql, enum enum_mysql_set_option option)
{
  CALLREALWITHJUDGE(mysql, mysql_set_server_option, option);
}

int STDCALL mysql_ping(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_ping);
}

const char * STDCALL mysql_stat(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_stat);
}

const char * STDCALL mysql_get_server_info(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_get_server_info);
}

const char * STDCALL mysql_get_client_info(void)
{
  return OB_CLIENT_INFO;
}

unsigned long STDCALL mysql_get_client_version(void)
{
  return OB_CLIENT_VERSION;
}

const char * STDCALL mysql_get_host_info(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_get_host_info);
}

unsigned long STDCALL mysql_get_server_version(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_get_server_version);
}

unsigned int STDCALL mysql_get_proto_info(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_get_proto_info);
}

MYSQL_RES * STDCALL mysql_list_dbs(MYSQL *mysql,const char *wild)
{
  CALLREALWITHJUDGE(mysql, mysql_list_dbs, wild);  
}

MYSQL_RES * STDCALL mysql_list_tables(MYSQL *mysql,const char *wild)
{
  CALLREALWITHJUDGE(mysql, mysql_list_tables, wild);
}

MYSQL_RES * STDCALL mysql_list_processes(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_list_processes);
}

int STDCALL mysql_options(MYSQL *mysql,enum mysql_option option, const char *arg)
{
  CALLREALWITHJUDGE(mysql, mysql_options, option, arg);
}

int STDCALL mysql_options(MYSQL *mysql,enum mysql_option option, const void *arg)
{
  CALLREALWITHJUDGE(mysql, mysql_options, option, arg);
}

void STDCALL mysql_free_result(MYSQL_RES *result)
{
  (*g_func_set.real_mysql_free_result)(result);
}

void STDCALL mysql_data_seek(MYSQL_RES *result, my_ulonglong offset)
{
  (*g_func_set.real_mysql_data_seek)(result, offset);
}

MYSQL_ROW_OFFSET STDCALL mysql_row_seek(MYSQL_RES *result, MYSQL_ROW_OFFSET offset)
{
  return (*g_func_set.real_mysql_row_seek)(result, offset);
}

MYSQL_FIELD_OFFSET STDCALL mysql_field_seek(MYSQL_RES *result, MYSQL_FIELD_OFFSET offset)
{
  return (*g_func_set.real_mysql_field_seek)(result, offset);
}

MYSQL_ROW STDCALL mysql_fetch_row(MYSQL_RES *result)
{
  return (*g_func_set.real_mysql_fetch_row)(result);
}

unsigned long * STDCALL mysql_fetch_lengths(MYSQL_RES *result)
{
  return (*g_func_set.real_mysql_fetch_lengths)(result);
}

MYSQL_FIELD * STDCALL mysql_fetch_field(MYSQL_RES *result)
{
  return (*g_func_set.real_mysql_fetch_field)(result);
}

MYSQL_RES * STDCALL mysql_list_fields(MYSQL *mysql, const char *table, const char *wild)
{
  CALLREALWITHJUDGE(mysql, mysql_list_fields, table, wild);
}

unsigned long STDCALL mysql_escape_string(char *to,const char *from, unsigned long from_length)
{
  return (*g_func_set.real_mysql_escape_string)(to, from, from_length);
}

unsigned long STDCALL mysql_hex_string(char *to,const char *from, unsigned long from_length)
{
  return (*g_func_set.real_mysql_hex_string)(to, from, from_length);
}

unsigned long STDCALL mysql_real_escape_string(MYSQL *mysql, char *to,const char *from, unsigned long length)
{
  CALLREALWITHJUDGE(mysql, mysql_real_escape_string, to, from, length);
}

void STDCALL mysql_debug(const char *debug)
{
  (*g_func_set.real_mysql_debug)(debug);
}

void STDCALL myodbc_remove_escape(MYSQL *mysql,char *name)
{
  CALLREALWITHJUDGE(mysql, myodbc_remove_escape, name);
}

unsigned int STDCALL mysql_thread_safe(void)
{
  return (*g_func_set.real_mysql_thread_safe)();
}

my_bool STDCALL mysql_embedded(void)
{
  return (*g_func_set.real_mysql_embedded)();
}

my_bool STDCALL mysql_read_query_result(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_read_query_result);
}

MYSQL_STMT * STDCALL mysql_stmt_init(MYSQL *mysql)
{
  ObSQLType type = OB_SQL_UNKNOWN;
  if (NULL == ((ObSQLMySQL*)mysql)->conn_)
  {
    return (*g_func_set.real_mysql_stmt_init)(mysql);
  }
  else
  {
    if (NULL != ((ObSQLMySQL*)mysql)->conn_
      && (((ObSQLMySQL*)mysql)->conn_  == ((ObSQLMySQL*)mysql)->wconn_)
      && (((ObSQLMySQL*)mysql)->conn_  == ((ObSQLMySQL*)mysql)->rconn_))
    {
      select_connection(mysql, "select stmt", 11, &type);
    }
    ((ObSQLMySQL*)mysql)->has_stmt_ = 1;
    TBSYS_LOG(DEBUG, "stmt init user mysql is %p, mysql is %p", mysql, ((ObSQLMySQL*)mysql)->conn_->mysql_);
    CALLREAL(mysql, mysql_stmt_init);
  }
}

int STDCALL mysql_stmt_prepare(MYSQL_STMT *stmt, const char *query, unsigned long length)
{
  TBSYS_LOG(DEBUG, "stmt prepare mysql is %p query is %s", stmt->mysql, query);
  if (NULL == ((ObSQLMySQL*)stmt->mysql)->conn_)
  {
    return 2006;
  }
  CALLSTMTREAL(stmt, mysql_stmt_prepare, query, length);
}

int STDCALL mysql_stmt_execute(MYSQL_STMT *stmt)
{
  TBSYS_LOG(DEBUG, "stmt execute mysql is %p", stmt->mysql);
  CALLSTMTREAL(stmt, mysql_stmt_execute);
}

int STDCALL mysql_stmt_fetch(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_fetch);
}

int STDCALL mysql_stmt_fetch_column(MYSQL_STMT *stmt, MYSQL_BIND *bind_arg, unsigned int column, unsigned long offset)
{
  CALLSTMTREAL(stmt, mysql_stmt_fetch_column, bind_arg, column, offset);
}

int STDCALL mysql_stmt_store_result(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_store_result);
}

unsigned long STDCALL mysql_stmt_param_count(MYSQL_STMT * stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_param_count);
}

my_bool STDCALL mysql_stmt_attr_set(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, const void *attr)
{
  CALLSTMTREAL(stmt, mysql_stmt_attr_set, attr_type, attr);
}

my_bool STDCALL mysql_stmt_attr_get(MYSQL_STMT *stmt, enum enum_stmt_attr_type attr_type, void *attr)
{
  CALLSTMTREAL(stmt, mysql_stmt_attr_get, attr_type, attr);
}

my_bool STDCALL mysql_stmt_bind_param(MYSQL_STMT * stmt, MYSQL_BIND * bnd)
{
  CALLSTMTREAL(stmt, mysql_stmt_bind_param, bnd);
}

my_bool STDCALL mysql_stmt_bind_result(MYSQL_STMT * stmt, MYSQL_BIND * bnd)
{
  CALLSTMTREAL(stmt, mysql_stmt_bind_result, bnd);
}

my_bool STDCALL mysql_stmt_close(MYSQL_STMT * stmt)
{
  TBSYS_LOG(DEBUG, "stmt close mysql is %p", stmt->mysql);
  CALLSTMTREAL(stmt, mysql_stmt_close);
}

my_bool STDCALL mysql_stmt_reset(MYSQL_STMT * stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_reset);
}

my_bool STDCALL mysql_stmt_free_result(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_free_result);
}

my_bool STDCALL mysql_stmt_send_long_data(MYSQL_STMT *stmt, unsigned int param_number, const char *data, unsigned long length)
{
  CALLSTMTREAL(stmt, mysql_stmt_send_long_data, param_number, data, length);
}

MYSQL_RES *STDCALL mysql_stmt_result_metadata(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_result_metadata);
}

MYSQL_RES *STDCALL mysql_stmt_param_metadata(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_param_metadata);
}

unsigned int STDCALL mysql_stmt_errno(MYSQL_STMT * stmt)
{
  if (NULL != ((ObSQLMySQL*)stmt->mysql)->conn_)
  {
    CALLSTMTREAL(stmt, mysql_stmt_errno);
  }
  else
  {
    return 2006;
  }
}

const char *STDCALL mysql_stmt_error(MYSQL_STMT * stmt)
{
  if (NULL != ((ObSQLMySQL*)stmt->mysql)->conn_)
  {
    CALLSTMTREAL(stmt, mysql_stmt_error);
  }
  else
  {
    return "MySQL server has gone away";
  }
}

const char *STDCALL mysql_stmt_sqlstate(MYSQL_STMT * stmt)
{
  if (NULL != ((ObSQLMySQL*)stmt->mysql)->conn_)
  {
    CALLSTMTREAL(stmt, mysql_stmt_sqlstate);
  }
  else
  {
    return "HY000";
  }
}

MYSQL_ROW_OFFSET STDCALL mysql_stmt_row_seek(MYSQL_STMT *stmt, MYSQL_ROW_OFFSET offset)
{
  CALLSTMTREAL(stmt, mysql_stmt_row_seek, offset);
}

MYSQL_ROW_OFFSET STDCALL mysql_stmt_row_tell(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_row_tell);
}

void STDCALL mysql_stmt_data_seek(MYSQL_STMT *stmt, my_ulonglong offset)
{
  CALLSTMTREAL(stmt, mysql_stmt_data_seek, offset);
}

my_ulonglong STDCALL mysql_stmt_num_rows(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_num_rows);
}

my_ulonglong STDCALL mysql_stmt_affected_rows(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_affected_rows);
}

my_ulonglong STDCALL mysql_stmt_insert_id(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_insert_id);
}

unsigned int STDCALL mysql_stmt_field_count(MYSQL_STMT *stmt)
{
  CALLSTMTREAL(stmt, mysql_stmt_field_count);
}

my_bool STDCALL mysql_commit(MYSQL * mysql)
{
  //CALLREALWITHJUDGE(mysql, mysql_commit);
  return (my_bool)mysql_real_query(mysql, "commit", 6);
}

my_bool STDCALL mysql_rollback(MYSQL * mysql)
{
  //CALLREALWITHJUDGE(mysql, mysql_rollback);
  return (my_bool)mysql_real_query(mysql, "rollback", 8);
}

my_bool STDCALL mysql_autocommit(MYSQL * mysql, my_bool auto_mode)
{
  //CALLREALWITHJUDGE(mysql, mysql_autocommit, auto_mode);
  return (my_bool)mysql_real_query(mysql, auto_mode ?
                          "set autocommit=1":"set autocommit=0",
                          16);
}

my_bool STDCALL mysql_more_results(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_more_results);
}

int STDCALL mysql_next_result(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_next_result);
}

//give back to pool
void STDCALL mysql_close(MYSQL *mysql)
{
  ObSQLConn *conn = NULL;
  unsigned int eno = 0;
  if (NULL != mysql)
  {
    if (OB_SQL_MAGIC == ((ObSQLMySQL*)mysql)->magic_)
    {
      TBSYS_LOG(INFO, "ob mysql is %p", mysql);
      //TODO if mysql has some error real close it and create an new connection
      if (NULL != ((ObSQLMySQL*)mysql)->rconn_)
      {
        conn = ((ObSQLMySQL*)mysql)->rconn_;
        eno = (*g_func_set.real_mysql_errno)(conn->mysql_);
        if(eno !=0 && errno != 2006) //2006 means server gone away
        {
          TBSYS_LOG(INFO, "mysql error no is %d", (*g_func_set.real_mysql_errno)(conn->mysql_));
          pthread_rwlock_rdlock(&g_config_rwlock);
          reconnect(conn);
          pthread_rwlock_unlock(&g_config_rwlock);
        }
        else
        {
          TBSYS_LOG(INFO, "call release conn real mysql is %p in mysql close", conn->mysql_);
          pthread_rwlock_rdlock(&g_config_rwlock);
          release_conn(conn);
          pthread_rwlock_unlock(&g_config_rwlock);
        }
      }
      else if (NULL != ((ObSQLMySQL*)mysql)->wconn_)
      {
        conn = ((ObSQLMySQL*)mysql)->wconn_;
        eno = (*g_func_set.real_mysql_errno)(conn->mysql_);
        if(eno != 0 && errno != 2006)
        {
          TBSYS_LOG(INFO, "mysql error no is %d", (*g_func_set.real_mysql_errno)(conn->mysql_));
          // an error occurred close connection
          pthread_rwlock_rdlock(&g_config_rwlock);
          reconnect(conn);
          pthread_rwlock_unlock(&g_config_rwlock);
        }
        else
        {
          TBSYS_LOG(INFO, "call release conn real mysql is %p in mysql close", conn->mysql_);
          pthread_rwlock_rdlock(&g_config_rwlock);
          release_conn(conn);
          pthread_rwlock_unlock(&g_config_rwlock);
        }
      }
      ((ObSQLMySQL*)mysql)->conn_ = NULL;
      ((ObSQLMySQL*)mysql)->rconn_ = NULL;
      ((ObSQLMySQL*)mysql)->wconn_ = NULL;
      ((ObSQLMySQL*)mysql)->has_stmt_ = 0;
      if (1 == ((ObSQLMySQL*)mysql)->alloc_)
      {
        ob_free(mysql);
        mysql = NULL;
      }
    }
    else
    {
      (*g_func_set.real_mysql_close)(mysql);
    }
  }
}

const char * mysql_get_ssl_cipher(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_get_ssl_cipher);
}

void mysql_set_local_infile_default(MYSQL *mysql)
{
  CALLREALWITHJUDGE(mysql, mysql_set_local_infile_default);
}

void mysql_set_local_infile_handler(MYSQL *mysql, int (*local_infile_init)(void **, const char *, void *), int (*local_infile_read)(void *, char *, unsigned int), void (*local_infile_end)(void *), int (*local_infile_error)(void *, char*, unsigned int), void *userdata)
{
  CALLREALWITHJUDGE(mysql, mysql_set_local_infile_handler, local_infile_init, local_infile_read, local_infile_end, local_infile_error, userdata);
}

int STDCALL mysql_stmt_next_result(MYSQL_STMT *stmt)
{
  return (*g_func_set.real_mysql_stmt_next_result)(stmt);
}

struct st_mysql_client_plugin * mysql_client_find_plugin(MYSQL *mysql, const char *name, int type)
{
  CALLREALWITHJUDGE(mysql, mysql_client_find_plugin, name, type);
}

struct st_mysql_client_plugin * mysql_client_register_plugin(MYSQL *mysql, struct st_mysql_client_plugin *plugin)
{
  CALLREALWITHJUDGE(mysql, mysql_client_register_plugin, plugin);
}

struct st_mysql_client_plugin * mysql_load_plugin(MYSQL *mysql, const char *name, int type, int argc, ...)
{
  struct st_mysql_client_plugin *p;
  va_list args;
  va_start(args, argc);
  p= (*g_func_set.real_mysql_load_plugin_v)(mysql, name, type, argc, args);
  va_end(args);
  return p;
}

struct st_mysql_client_plugin * mysql_load_plugin_v(MYSQL *mysql, const char *name, int type, int argc, va_list args)
{
  CALLREALWITHJUDGE(mysql, mysql_load_plugin_v, name, type, argc, args);
}

int mysql_plugin_options(struct st_mysql_client_plugin *plugin, const char *option, const void *value)
{
  return (*g_func_set.real_mysql_plugin_options)(plugin, option, value);
}
