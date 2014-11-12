#include "tblog.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "ob_sql_conn_recycle.h"
#include "ob_sql_struct.h"
#include "ob_sql_global.h"
#include "ob_sql_list.h"
#include "ob_sql_util.h"
#include <stdio.h>
#include <stddef.h>

using namespace oceanbase::common;

static void *conn_recycle_task(void *arg)
{
  UNUSED(arg);
  while (1)
  {
    ObSQLListNode *lnode = g_delete_ms_list.head_;
    ObSQLListNode *cnode = NULL;
    ObSQLConn *conn = NULL;
    pthread_rwlock_rdlock(&g_config_rwlock);
    dump_delete_ms_conn();
    for(; lnode != NULL; lnode = lnode->next_)
      //ob_sql_list_for_each_entry_safe(slist, nlist, &g_delete_ms_list, delete_list_node_)
    {
      ObSQLConnList *slist = (ObSQLConnList*)lnode->data_;
      if (ob_sql_list_empty(slist->used_list_))
      {
        TBSYS_LOG(DEBUG, "All conn give back to pool, start recycle");
        for (cnode = slist->free_list_.head_; NULL != cnode; cnode = cnode->next_)
        {
          ob_sql_list_del(&slist->free_list_, cnode);
          conn = (ObSQLConn*)cnode->data_;
          TBSYS_LOG(DEBUG, "ds is %s real close connection is %p, real mysql is %p", get_server_str(&conn->pool_->server_), conn, conn->mysql_);
          //close real mysql connection and free conn which construct when create_real_connection
          (*(g_func_set.real_mysql_close))(conn->mysql_);
          ob_free(cnode);
          cnode = NULL;
        }
        ob_sql_list_del(&g_delete_ms_list, lnode);
        ob_free(lnode);
        lnode = NULL;
      }
      else
      {
        TBSYS_LOG(DEBUG, "used_conn_list_ is not empty");
      }
    }
    pthread_rwlock_unlock(&g_config_rwlock);
    sleep(OB_SQL_RECYCLE_INTERVAL);
  }
  return NULL;
}

int start_recycle_worker()
{
  int ret = OB_SQL_SUCCESS;
  pthread_t recycle_thread;
  ret = pthread_create(&recycle_thread, NULL, conn_recycle_task, NULL);
  if (OB_SQL_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "start recycle worker");
  }
  else
  {
    TBSYS_LOG(ERROR, "start recycle worker failed errno is %d, errstr is %s", errno, strerror(errno));
  }
  return ret;
}
