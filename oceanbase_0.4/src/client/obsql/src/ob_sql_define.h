/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_define.h is for what ...
 *
 * Version: ***: ob_sql_define.h  Wed Nov 21 10:58:59 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_DEFINE_H_
#define OB_SQL_DEFINE_H_

#ifdef __cplusplus
# define OB_SQL_CPP_START extern "C" {
# define OB_SQL_CPP_END }
#else
# define OB_SQL_CPP_START
# define OB_SQL_CPP_END
#endif

/* CONSTS */
#define OB_SQL_SUCCESS 0
#define OB_SQL_ERROR -1
#define OB_SQL_INVALID_ARGUMENT -2
#define OB_SQL_CONFIG_BUFFER_SIZE 1024*512 /* 512k */
#define OB_SQL_IP_BUFFER_SIZE 32
#define OB_SQL_BUFFER_NUM 16
#define OB_SQL_CONFIG_NUM 2
#define OB_SQL_SLOT_PER_MS 100
#define OB_SQL_MAGIC 0xF0ECAB

#define OB_SQL_CONFIG_DEFAULT_NAME "/home/admin/libobsql.conf"
#define OB_SQL_CONFIG_ENV "OB_SQL_CONFIG"
#define OB_SQL_CONFIG_LOG "logfile"
#define OB_SQL_CONFIG_URL "initurl"
#define OB_SQL_CONFIG_LOG_LEVEL "loglevel"
#define OB_SQL_CONFIG_MIN_CONN  "minconn"
#define OB_SQL_CONFIG_MAX_CONN  "maxconn"
#define OB_SQL_CONFIG_USERNAME "username"
#define OB_SQL_CONFIG_PASSWD "passwd"
#define OB_SQL_CONFIG_IP "ip"
#define OB_SQL_CONFIG_PORT "port" 

#define OB_SQL_BEG_TRANSACTION "begin"
#define OB_SQL_START_TRANSACTION "start"
#define OB_SQL_COMMIT "commit"
#define OB_SQL_ROLLBACK "rollback"
#define OB_SQL_CREATE "create"
#define OB_SQL_DROP "drop"
#define OB_SQL_SELECT "select"
//#define OB_SQL_REPLACE_OP "repalce"
//#define OB_SQL_INSERT_OP "insert"
//#define OB_SQL_UPDATE_OP "update"

#define OB_SQL_UPDATE_INTERVAL 30 /* 30 seconds */
#define OB_SQL_RECYCLE_INTERVAL 45
#define OB_SQL_MAX_FILENAME_LEN 512
#define OB_SQL_MAX_URL_LENGTH 2048
#define OB_SQL_MAX_USER_NAME_LEN 128
#define OB_SQL_MAX_PASSWD_LEN 128
#define OB_SQL_IP_LEN 64
#define OB_SQL_QUERY_STR_LENGTH 128
#define OB_SQL_LOG_LEVEL 16
#define OB_SQL_SLOT_NUM 100

/** SQL QUERY for mergeserver list */
#define OB_SQL_QUERY_CLUSTER "select cluster_id,cluster_role,cluster_flow_percent,cluster_vip,cluster_port,read_strategy from __all_cluster"
#define OB_SQL_QUERY_SERVER "select svr_ip,svr_port from __all_server where svr_type='mergeserver' and cluster_id="
#define OB_SQL_CLIENT_VERSION "select /*+ client(libobsql) client_version(4.0.1) mysql_driver(18.0) */ 'client_version'"

/*  */
#define OB_SQL_MAX_CLUSTER_NUM 5
#define OB_SQL_MAX_SLAVE_CLUSTER_NUM 16
#define OB_SQL_MAX_MS_NUM  1024

#define OB_SQL_QUERY_RETRY_TIME 3

#define OB_SQL_AHEAD_MASK 0xffff
#define OB_SQL_OFFSET_SHIFT 32

#define OB_SQL_BUCKET_PER_SERVER 100

//client info
#define OB_CLIENT_INFO  "oceanbasae V4.0 client"
#define OB_CLIENT_VERSION 400010 //means 4.0.1
#define OB_SQL_CLIENT_VERSION_51_MIN
//#define OB_SQL_CLIENT_VERSION_55_MIN 50500
#define OB_SQL_CLIENT_VERSION_55_MIN 60000
#define OB_SQL_USER "admin"
#define OB_SQL_PASS "admin"
#define OB_SQL_DB "test"
#define OB_SQL_DEFAULT_MMAP_THRESHOLD 64*1024+128
#endif
