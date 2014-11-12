/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_data_source_method.h is for what ...
 *
 * Version: ***: ob_sql_data_source_method.h  Fri Jan 18 11:33:05 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_DATA_SOURCE_METHOD_H_
#define OB_SQL_DATA_SOURCE_METHOD_H_

#include "ob_sql_define.h"

OB_SQL_CPP_START
#include "ob_sql_cluster_info.h"
#include "ob_sql_data_source.h"

/**
 * 在指定的集群里面创建一个datasource
 * @param conns   连接的个数
 * @param server  Ms信息
 * @param cluster 集群信息
 *
 * @return int    OB_SQL_SUCCESS if build success
 *                else return OB_SQL_ERROR
 */
int init_data_source(int32_t conns, ObServerInfo server, ObClusterInfo *cluster);

int delete_data_source(ObDataSource *ds);

int create_real_connection(ObDataSource *pool);
OB_SQL_CPP_END
#endif
