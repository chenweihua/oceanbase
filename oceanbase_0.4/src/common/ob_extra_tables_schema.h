/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_extra_tables_schema.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *   Zhidong SUN  <xielun.szd@alipay.com>
 *
 */
#ifndef _OB_EXTRA_TABLES_SCHEMA_H
#define _OB_EXTRA_TABLES_SCHEMA_H 1

#include "ob_schema_service_impl.h"

namespace oceanbase
{
  namespace common
  {
    /////////////////////////////////////////////////////////
    // WARNING: the sys table schema can not modified      //
    /////////////////////////////////////////////////////////
    class ObExtraTablesSchema
    {
    public:
      static const int64_t TEMP_ROWKEY_LENGTH = 64;
      static const int64_t SERVER_TYPE_LENGTH = 16;
      static const int64_t SERVER_IP_LENGTH = 32;
      // core tables
      static int first_tablet_entry_schema(TableSchema& table_schema);
      static int all_all_column_schema(TableSchema& table_schema);
      static int all_join_info_schema(TableSchema& table_schema);
    public:
      // other sys tables
      static int all_sys_stat_schema(TableSchema &table_schema);
      static int all_sys_param_schema(TableSchema &table_schema);
      static int all_sys_config_schema(TableSchema &table_schema);
      static int all_sys_config_stat_schema(TableSchema &table_schema);
      static int all_user_schema(TableSchema &table_schema);
      static int all_table_privilege_schema(TableSchema &table_schema);
      static int all_trigger_event_schema(TableSchema& table_schema);
      static int all_cluster_schema(TableSchema& table_schema);
      static int all_server_schema(TableSchema& table_schema);
      static int all_client_schema(TableSchema& table_schema);
      // virtual sys tables
      static int all_server_stat_schema(TableSchema &table_schema);
      static int all_server_session_schema(TableSchema &table_schema);
      static int all_statement_schema(TableSchema &table_schema);
    private:
      ObExtraTablesSchema();
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_EXTRA_TABLES_SCHEMA_H */
