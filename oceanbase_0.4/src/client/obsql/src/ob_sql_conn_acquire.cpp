 #include "tblog.h"
 #include "common/ob_define.h"
 #include "common/ob_malloc.h"
 #include "ob_sql_conn_acquire.h"
 #include "ob_sql_util.h"
 #include "ob_sql_data_source_utility.h"
 #include <stdio.h>
 #include <stddef.h>
 #include <string.h>

 using namespace oceanbase::common;

 /* 连接池的互斥锁 */
 pthread_mutex_t pool_mutex;

static ObDataSource* select_ds(ObClusterInfo *cluster, const char* sql, unsigned long length, ObSQLMySQL *mysql)
 {
   ObDataSource *ds = NULL;
   if (NULL == cluster || NULL == sql)
   {
     TBSYS_LOG(WARN, "invalid argument cluster is %p, sql is %p", cluster, sql);
   }
   else
   {
     if (0 == cluster->read_strategy_)
     {
       ds = random_mergeserver_select(cluster, mysql);
     }
     else
     {
       ds = consishash_mergeserver_select(cluster, sql, length);
     }
   }
   return ds;
 }

 static ObSQLConn* select_conn(ObDataSource *server)
 {
   ObSQLConn* conn = NULL;
   if (NULL == server)
   {
     TBSYS_LOG(WARN, "invalid argument server is %p", server);
   }
   else
   {
     conn = random_conn_select(server);
   }
   return conn;
 }

 /** 从一个集群中选择一条连接
  * 1、使用一致性hash找mergeserver，然后获取一条连接
  * 2、如果上面的ms的连接用光了， 遍历集群中所有连接选一条出来
  *
  * 一致性hash选择ms 如果ms上的连接全忙则round-robbin选择一条连接
  */
ObSQLConn* acquire_conn(ObClusterInfo *cluster, const char* sql, unsigned long length, ObSQLMySQL *mysql)
 {
   ObSQLConn *real_conn = NULL;
   if (NULL == cluster || NULL == sql)
   {
     TBSYS_LOG(WARN, "invalid argument cluster is %p, sql is %p", cluster, sql);
   }
   else
   {
     pthread_mutex_lock(&pool_mutex);
     ObDataSource *server = select_ds(cluster, sql, length, mysql);
     if (NULL != server)
     {
       mysql->last_ds_ = server->server_;
       real_conn = select_conn(server);
       if (NULL == real_conn)
       {
         TBSYS_LOG(ERROR, "there are no free connection in clusetr(rootserver is %s)", get_server_str(&server->server_));
       }
       TBSYS_LOG(DEBUG, "acquire real conn is %p", real_conn);
     }
     else
     {
       TBSYS_LOG(WARN, "select ms failed");
     }
     pthread_mutex_unlock(&pool_mutex);
   }
   return real_conn;
 }

ObSQLConn* acquire_conn_random(ObGroupDataSource *gds, ObSQLMySQL* mysql)
 {
   int32_t index = 0;
   ObClusterInfo *scluster = NULL;
   ObDataSource *ds = NULL;
   ObSQLConn *real_conn = NULL;
   if (NULL == gds)
   {
     TBSYS_LOG(WARN, "invalid argument gdb is %p", gds);
   }
   else
   {
     pthread_mutex_lock(&pool_mutex);
     for(; index < gds->csize_; ++index)
     {
       scluster = gds->clusters_ + index;
       ds = random_mergeserver_select(scluster, mysql);
       if (NULL != ds)
       {
         real_conn = random_conn_select(ds);
         TBSYS_LOG(DEBUG, "thread %ld get real_conn is %p", pthread_self(), real_conn);
         if (NULL == real_conn)
         {
           TBSYS_LOG(ERROR, "there no free connection in cluster %s", get_server_str(&scluster->rs_));
         }
         else
         {
           break;
         }
       }
       else
       {
         TBSYS_LOG(WARN, "get ms failed from cluster %s", get_server_str(&scluster->rs_));
       }
     }
     pthread_mutex_unlock(&pool_mutex);
   }
   return real_conn;
 }

int reconnect(ObSQLConn *conn)
{
  int ret = OB_SQL_SUCCESS;
  if (NULL == conn)
  {
    TBSYS_LOG(ERROR, "invalid argument conn is %p", conn);
    ret = OB_SQL_ERROR;
  }
  else
  {
    ObDataSource *ds = conn->pool_;
    (*g_func_set.real_mysql_close)(conn->mysql_);
    pthread_mutex_lock(&pool_mutex);
    ob_sql_list_del(&ds->conn_list_.used_list_, conn->node_);
    TBSYS_LOG(DEBUG, "close old real mysql is %p", conn->mysql_);
    ob_free(conn->node_);
    conn->node_ = NULL;
    create_real_connection(ds);
    pthread_mutex_unlock(&pool_mutex);
    TBSYS_LOG(DEBUG, "create new connection");
  }
  return ret;
}

int release_conn(ObSQLConn *conn)
{
  int ret = OB_SQL_SUCCESS;
  pthread_mutex_lock(&pool_mutex);
  ObDataSource *ds = conn->pool_;
  ob_sql_list_del(&ds->conn_list_.used_list_, conn->node_);
  ob_sql_list_add_tail(&conn->pool_->conn_list_.free_list_, conn->node_);
  pthread_mutex_unlock(&pool_mutex);
  TBSYS_LOG(DEBUG, "release conn %p", conn);
  TBSYS_LOG(DEBUG, "free list has %d", conn->pool_->conn_list_.free_list_.size_);
  TBSYS_LOG(DEBUG, "used list has %d", conn->pool_->conn_list_.used_list_.size_);
  return ret;
}

//int move_conn_list(ObSQLConnList *dest, ObDataSource *src)
//{
//  UNUSED(dest);
//  UNUSED(src);
//  ObSQLConn *cconn = NULL;
//  ObSQLConn *nconn = NULL;
//  memset(dest, 0, sizeof(ObSQLConnList));
//  ob_sql_list_init(&dest->free_conn_list_);
//  ob_sql_list_init(&dest->used_conn_list_);
//  pthread_mutex_lock(&pool_mutex);
//  ob_sql_list_for_each_entry_safe(cconn, nconn, &src->conn_list_.used_conn_list_, conn_list_node_)
//  {
//    ob_sql_list_del(&cconn->conn_list_node_);
//    ob_sql_list_add_tail(&cconn->conn_list_node_, &dest->used_conn_list_);
//  }
//  ob_sql_list_for_each_entry_safe(cconn, nconn, &src->conn_list_.free_conn_list_, conn_list_node_)
//  {
//    ob_sql_list_del(&cconn->conn_list_node_);
//    ob_sql_list_add_tail(&cconn->conn_list_node_, &dest->free_conn_list_);
//  }
//  pthread_mutex_unlock(&pool_mutex);
//  return OB_SQL_SUCCESS;
//}

/* move connection from sinfo to dinfo */
//int move_conn(ObClusterInfo *dinfo, ObClusterInfo *sinfo)
//{
//  UNUSED(dinfo);
//  UNUSED(sinfo);
  //int idx = 0;
  //ObDataSource *dest = NULL;
  //ObDataSource *src = NULL;
  //ObSQLConn *cconn = NULL;
  //ObSQLConn *nconn = NULL;
  //pthread_mutex_lock(&pool_mutex);
  //for (; idx<sinfo->csize_; ++idx)
  //{
  //  dest = dinfo->dslist_ + idx;
  //  src = sinfo->dslist_ + idx;
  //  memset(&dest->conn_list_, 0, sizeof(ObSQLConnList));
  //  ob_sql_list_init(&dest->conn_list_.free_conn_list_);
  //  ob_sql_list_init(&dest->conn_list_.used_conn_list_);
  //  ob_sql_list_for_each_entry_safe(cconn, nconn, &src->conn_list_.used_conn_list_, conn_list_node_)
  //  {
  //    ob_sql_list_del(&cconn->conn_list_node_);
  //    ob_sql_list_add_tail(&cconn->conn_list_node_, &dest->conn_list_.used_conn_list_);
  //  }
  //  ob_sql_list_for_each_entry_safe(cconn, nconn, &src->conn_list_.free_conn_list_, conn_list_node_)
  //  {
  //    ob_sql_list_del(&cconn->conn_list_node_);
  //    ob_sql_list_add_tail(&cconn->conn_list_node_, &dest->conn_list_.free_conn_list_);
  //  }
  //}
  //pthread_mutex_unlock(&pool_mutex);
//  return OB_SQL_SUCCESS;
//}
