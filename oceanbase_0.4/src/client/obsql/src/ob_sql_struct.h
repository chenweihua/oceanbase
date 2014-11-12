/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_struct.h is for what ...
 *
 * Version: ***: ob_sql_struct.h  Mon Dec  3 13:43:18 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_STRUCT_H_
#define OB_SQL_STRUCT_H_
#include "ob_sql_define.h"

OB_SQL_CPP_START
#include <mysql/mysql.h>
#include <stdint.h>
#include "ob_sql_list.h"
#include "ob_sql_define.h"
#include "ob_sql_server_info.h"
#include "ob_sql_cluster_info.h"
#include "ob_sql_data_source.h"

typedef enum enum_sql_type
{
  OB_SQL_UNKNOWN = 1,
  OB_SQL_BEGIN_TRANSACTION,
  OB_SQL_END_TRANSACTION,
  OB_SQL_CONSISTENCE_REQUEST,
  OB_SQL_DDL,
  OB_SQL_WRITE,
  OB_SQL_READ,
} ObSQLType;

typedef struct ob_sql_rs_list
{
  ObServerInfo entrance_[2][OB_SQL_MAX_CLUSTER_NUM*2];
  int size_[2];
  int using_;
} ObSQLRsList;

typedef struct ob_sql_config
{
  char logfile_[OB_SQL_MAX_FILENAME_LEN];
  char url_[OB_SQL_MAX_URL_LENGTH];
  char loglevel_[OB_SQL_LOG_LEVEL];
  char username_[OB_SQL_MAX_USER_NAME_LEN];
  char passwd_[OB_SQL_MAX_PASSWD_LEN];
  char ip_[OB_SQL_IP_LEN];
  uint32_t port_;
  int32_t min_conn_;
  int32_t max_conn_;
} ObSQLConfig;

typedef enum ob_sql_cluster_type
{
  MASTER = 1,
  SLAVE,
} ObSQLClusterType;

typedef struct ob_sql_select_table
{
  ObClusterInfo *master_cluster_;
  volatile uint32_t master_count_;
  volatile uint32_t cluster_index_;
  ObClusterInfo *clusters_[OB_SQL_SLOT_NUM];
} ObSQLSelectTable;

typedef struct ob_sql_hash_item
{
  uint32_t hashvalue_;
  ObDataSource* server_;       /* merge server means a connection pool */
} ObSQLHashItem;

typedef struct ob_sql_select_ms_table
{
  int64_t slot_num_;
  int64_t offset_;
  ObSQLHashItem *items_;
} ObSQLSelectMsTable;

typedef struct ob_sql_cluster_config
{
  int64_t cluster_type_;
  uint32_t cluster_id_;
  int16_t flow_weight_;
  int16_t server_num_;
  ObServerInfo server_;         /* listen ms info */
  ObServerInfo merge_server_[OB_SQL_MAX_MS_NUM];
  ObSQLSelectMsTable *table_;
  uint32_t read_strategy_;
} ObSQLClusterConfig;

typedef struct ob_sql_global_config
{
  int16_t min_conn_size_;
  int16_t max_conn_size_;
  int16_t cluster_size_;
  int16_t ms_table_inited_;
  uint32_t master_cluster_id_;
  ObSQLClusterConfig clusters_[OB_SQL_MAX_CLUSTER_NUM];
} ObSQLGlobalConfig;


/**
 * 这个数据结构用来重新解释用户持有的MYSQLhandle
 * 通过这一层转换把用户持有的对象转换到实际的MYSQL上
 */
typedef struct ob_sql_mysql
{
  ObSQLConn* wconn_;
  ObSQLConn* rconn_;
  ObSQLConn* conn_;
  my_bool is_consistence_;
  int64_t magic_;               /*  */
  my_bool alloc_;               /* mem alloc by obsql */
  my_bool in_transaction_;
  my_bool has_stmt_;
  my_bool retry_;
  ObServerInfo last_ds_;
  struct charset_info_st *charset; /* compatible with mysql mysql_init会设置这个值*/
  char buffer[sizeof(MYSQL) - 3*sizeof(ObSQLConn*) - sizeof(my_bool) - sizeof(int64_t) - sizeof(my_bool) - sizeof(my_bool) - sizeof(my_bool) - sizeof(my_bool) -sizeof(ObServerInfo) - sizeof(char*)];
} ObSQLMySQL;

#define CALLREAL(mysql, func, ...)   MYSQL *real_mysql = ((ObSQLMySQL*)mysql)->conn_->mysql_; \
  return (*(g_func_set.real_##func))(real_mysql, ##__VA_ARGS__)

#define CALLREALWITHJUDGE(mysql, func, ...)  do {                     \
    if (OB_SQL_MAGIC == ((ObSQLMySQL*)mysql)->magic_)                 \
    {                                                                 \
      MYSQL *real_mysql = ((ObSQLMySQL*)mysql)->conn_->mysql_;        \
      return (*(g_func_set.real_##func))(real_mysql, ##__VA_ARGS__);  \
    }                                                                 \
    else                                                              \
    {                                                                 \
      return (*(g_func_set.real_##func))(mysql, ##__VA_ARGS__);       \
    }                                                                 \
  }while(0)
#define CALLSTMTREAL(stmt, func, ...) return (*(g_func_set.real_##func))(stmt, ##__VA_ARGS__)

OB_SQL_CPP_END
#endif
