/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_cluster_info.h is for what ...
 *
 * Version: ***: ob_sql_cluster_info.h  Wed Jan  9 19:30:02 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_CLUSTER_INFO_H_
#define OB_SQL_CLUSTER_INFO_H_

#include "ob_sql_define.h"

OB_SQL_CPP_START
#include "ob_sql_server_info.h"
#include "ob_sql_data_source.h"
#include "ob_sql_list.h"
#include <mysql/mysql.h>
#include <pthread.h>

//一个集群中的MS list
typedef struct ob_sql_server_pool
{
  int16_t flow_weight_;         /* flow weight */
  int32_t size_;                /* ms datasource number */
  int32_t csize_;               /* current datasource number */
  uint32_t cluster_id_;
  int32_t is_master_;
  ObServerInfo rs_;             /* cluster rootserver */
  ObDataSource dslist_[OB_SQL_MAX_MS_NUM];
  uint32_t read_strategy_;
  pthread_rwlock_t rwlock_;
} ObClusterInfo;
OB_SQL_CPP_END
#endif
