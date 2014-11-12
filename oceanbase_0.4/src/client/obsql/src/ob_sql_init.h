/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_init.h is for what ...
 *
 * Version: ***: ob_sql_init.h  Mon Nov 19 14:56:47 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *      Get merge server list and flow distribution
 */
#ifndef OB_SQL_INIT_H_
#define OB_SQL_INIT_H_

#include "ob_sql_define.h"
#include "ob_sql_global.h"

OB_SQL_CPP_START

#include <pthread.h>
#include <stdint.h>
#include "ob_sql_real_func.h"
#include "ob_sql_curl.h"
#include "ob_sql_struct.h"
#include "ob_sql_select_method_impl.h"
#include "ob_sql_cluster_select.h"
//define global var

/* 初始化obsql客户端 evn OB_SQL_CONFIG_DIR/obsql.config
1、从配置服务器读取集群信息 只包含集群(rs/ms)(ip,port)以及流量配置
2、从上面选择一个集群中获取所有配置 包括集群信息以及集群中的MergeServer列表 */
int ob_sql_init();
OB_SQL_CPP_END

#endif
