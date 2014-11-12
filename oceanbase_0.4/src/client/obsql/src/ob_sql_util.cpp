#include "ob_sql_util.h"
#include <stdio.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>
#include "tblog.h"
#include "ob_sql_global.h"

void dump_config(ObSQLGlobalConfig *config)
{
  TBSYS_LOG(INFO, "cluster size is %d", config->cluster_size_);
  int cindex = 0;
  int sindex = 0;
  int tindex = 0;
  ObSQLClusterConfig *cconfig;
  ObServerInfo server;
  for (; cindex < config->cluster_size_; ++cindex)
  {
    cconfig = config->clusters_+cindex;
    TBSYS_LOG(INFO, "cluster id is %u, role is %ld flow is %d, ms number is %d", cconfig->cluster_id_, cconfig->cluster_type_, cconfig->flow_weight_, cconfig->server_num_);
    sindex = 0;
    for (; sindex < cconfig->server_num_; ++sindex)
    {
      server = cconfig->merge_server_[sindex];
      TBSYS_LOG(INFO, "ms ip is %u, port is %u", server.ip_, server.port_);
    }
    ObSQLSelectMsTable *table = cconfig->table_;
    if (NULL != table)
    {
      tindex = 0;
      for (; tindex < table->slot_num_; ++tindex)
      {
        TBSYS_LOG(INFO, "ds is %p, hashval is %u", (table->items_+tindex)->server_, (table->items_+tindex)->hashvalue_);
      }
    }
  }
  TBSYS_LOG(INFO, "=================================");
}

void dump_table()
{
  TBSYS_LOG(INFO, "dump select table");
  int index = 0;
  for(; index < OB_SQL_SLOT_NUM; ++index)
  {
    TBSYS_LOG(INFO, "slot[%d]: cluster is %p", index, g_table->clusters_[index]);
  }
}

int get_server_ip(ObServerInfo *server, char *buffer, int32_t size)
{
  int ret = OB_SQL_ERROR;
  if (NULL != buffer && size > 0)
  {
    // ip.v4_ is network byte order
    snprintf(buffer, size, "%d.%d.%d.%d%c",
             (server->ip_ & 0xFF),
             (server->ip_ >> 8) & 0xFF,
             (server->ip_ >> 16) & 0xFF,
             (server->ip_ >> 24) & 0xFF,
             '\0');
    ret = OB_SQL_SUCCESS;
  }
  return ret;
}

uint32_t trans_ip_to_int(const char* ip)
{
  if (NULL == ip) return 0;
  uint32_t x = inet_addr(ip);
  if (x == INADDR_NONE)
  {
    struct hostent *hp = NULL;
    if ((hp = gethostbyname(ip)) == NULL)
    {
      return 0;
    }
    x = ((struct in_addr *)hp->h_addr)->s_addr;
  }
  return x;
}

const char* get_server_str(ObServerInfo *server)
{
  static __thread char buff[OB_SQL_BUFFER_NUM][OB_SQL_IP_BUFFER_SIZE];
  static int64_t i = 0;
  i++;
  memset(buff[i % OB_SQL_BUFFER_NUM], 0, OB_SQL_IP_BUFFER_SIZE);
  trans_int_to_ip(server, buff[i % OB_SQL_BUFFER_NUM], OB_SQL_IP_BUFFER_SIZE);
  return buff[ i % OB_SQL_BUFFER_NUM];
}

void trans_int_to_ip(ObServerInfo *server, char *buffer, int32_t size)
{
  if (NULL != buffer && size > 0)
  {
    if (server->port_ > 0) {
      snprintf(buffer, size, "%d.%d.%d.%d:%d",
               (server->ip_ & 0xFF),
               (server->ip_ >> 8) & 0xFF,
               (server->ip_ >> 16) & 0xFF,
               (server->ip_ >> 24) & 0xFF,
               server->port_);
    }
    else
    {
      snprintf(buffer, size, "%d.%d.%d.%d",
               (server->ip_ & 0xFF),
               (server->ip_ >> 8) & 0xFF,
               (server->ip_ >> 16) & 0xFF,
               (server->ip_ >> 24) & 0xFF);
    }
  }
}

const char* get_ip(ObServerInfo *server)
{
  static __thread char buff[OB_SQL_BUFFER_NUM][OB_SQL_IP_BUFFER_SIZE];
  static int64_t i = 0;
  i++;
  memset(buff[i % OB_SQL_BUFFER_NUM], 0, OB_SQL_IP_BUFFER_SIZE);
  snprintf(buff[i % OB_SQL_BUFFER_NUM], OB_SQL_IP_BUFFER_SIZE,
           "%d.%d.%d.%d",
           (server->ip_ & 0xFF),
           (server->ip_ >> 8) & 0xFF,
           (server->ip_ >> 16) & 0xFF,
           (server->ip_ >> 24) & 0xFF);

  return buff[ i % OB_SQL_BUFFER_NUM];
}

void insert_rs_list(uint32_t ip, uint32_t port)
{
  int index = 0;
  int exist = 0;
  for (; index < g_rsnum; ++index)
  {
    if (ip == g_rslist[index].ip_ && port == g_rslist[index].port_)
    {
      exist = 1;
    }
  }

  if (0 == exist && g_rsnum < OB_SQL_MAX_CLUSTER_NUM*2)
  {
    g_rslist[g_rsnum].ip_ = ip;
    g_rslist[g_rsnum].port_ = port;
    g_rsnum++;
  }
}

void dump_delete_ms_conn()
{
//  ObSQLConnList *slist = NULL;
//  ObSQLConnList *nlist = NULL;
//  ObSQLConn *cconn = NULL;
//  ObSQLConn *nconn = NULL;
//  TBSYS_LOG(INFO, "Dump Delete MergeSever Pool");
//  int index = 0;
//  ob_sql_list_for_each_entry_safe(slist, nlist, &g_delete_ms_list, delete_list_node_)
//  {
//    TBSYS_LOG(INFO, "list %d", index++);
//    if (!ob_sql_list_empty(&slist->used_conn_list_))
//    {
//      TBSYS_LOG(INFO,"Used connection:");
//      ob_sql_list_for_each_entry_safe(cconn, nconn, &slist->used_conn_list_, conn_list_node_)
//      {
//        TBSYS_LOG(INFO, "ds isreal mysql pointer is %p", cconn->mysql_);
//        //TBSYS_LOG(INFO, "ds is %s real mysql pointer is %p", get_server_str(&cconn->pool_->server_),cconn->mysql_);
//      }
//    }
//
//    if (!ob_sql_list_empty(&slist->free_conn_list_))
//    {
//      TBSYS_LOG(INFO, "Free connection:");
//      ob_sql_list_for_each_entry_safe(cconn, nconn, &slist->free_conn_list_, conn_list_node_)
//      {
//        TBSYS_LOG(INFO, "ds is real mysql pointer is %p", cconn->mysql_);
//        //TBSYS_LOG(INFO, "ds is %s real mysql pointer is %p", get_server_str(&cconn->pool_->server_), cconn->mysql_);
//      }
//    }
//  }
}
