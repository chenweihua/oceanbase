/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_table.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "common/ob_privilege.h"
#include "ob_alter_table.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObAlterTable::ObAlterTable()
{
}

ObAlterTable::~ObAlterTable()
{
}

void ObAlterTable::reset()
{
  local_context_.rs_rpc_proxy_ = NULL;
  alter_schema_.table_id_ = OB_INVALID_ID;
}

void ObAlterTable::reuse()
{
  local_context_.rs_rpc_proxy_ = NULL;
  alter_schema_.table_id_ = OB_INVALID_ID;
}


int ObAlterTable::open()
{
  int ret = OB_SUCCESS;
  if (local_context_.rs_rpc_proxy_ == NULL
    || strlen(alter_schema_.table_name_) <= 0
    || alter_schema_.table_id_ == OB_INVALID_ID
    || alter_schema_.columns_.count() <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "ObAlterTable not init, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = local_context_.rs_rpc_proxy_->alter_table(alter_schema_)))
  {
    TBSYS_LOG(WARN, "failed to create table, err=%d", ret);
  }
  else
  {
    bool need_grant = false;
    for (int64_t i = 0; i < alter_schema_.columns_.count(); i++)
    {
      if (alter_schema_.columns_.at(i).type_ == AlterTableSchema::ADD_COLUMN)
      {
        need_grant = true;
        break;
      }
    }
    if (need_grant)
    {
      // 还没有赋予权限即成功,如果创建成功了，但是赋予权限失败，由DBA介入
      // same strategy of create table
      ObString user_name = my_phy_plan_->get_result_set()->get_session()->get_user_name();
      ObString table_name;
      table_name.assign_ptr(alter_schema_.table_name_, (ObString::obstr_size_t)strlen(alter_schema_.table_name_));
      local_context_.disable_privilege_check_ = true;
      int retry_times = 10; // default retry tiems
      int i = 0;
      for (; i < retry_times; ++i)
      {
        const ObSchemaManagerV2 *schema_mgr = local_context_.merger_schema_mgr_->get_user_schema(0);
        if (NULL == schema_mgr)
        {
          TBSYS_LOG(WARN, "%s 's schema is not available,retry", alter_schema_.table_name_);
          usleep(10 * 1000); // 10ms
        }
        else
        {
          if (schema_mgr->get_table_schema(alter_schema_.table_name_) != NULL)
          {
            // new table 's schema still not available
            TBSYS_LOG(INFO, "get alter table %s 's schema success", alter_schema_.table_name_);
            local_context_.schema_manager_ = schema_mgr;
            break;
          }
          else
          {
            TBSYS_LOG(WARN, "%s 's schema is not available,retry", alter_schema_.table_name_);
            if (local_context_.merger_schema_mgr_->release_schema(schema_mgr) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "release schema failed");
            }
          }
        }
      }

      if (OB_UNLIKELY(i >= retry_times))
      {
        ret = OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE;
        //报警，让DBA使用admin账户来处理，给新建的表手工加权限
        TBSYS_LOG(ERROR, "User: %.*s alter table %s success, "
                         "but grant all privileges, grant option on %s to '%.*s' failed, ret=%d",
                         user_name.length(), user_name.ptr(),
                         alter_schema_.table_name_, alter_schema_.table_name_,
                         user_name.length(), user_name.ptr(), ret);
      }
      else
      {
        int err = OB_SUCCESS;
        char grant_buff[256];
        int64_t pos = 0;
        ObString grant_stmt;
        ObResultSet local_result;
        databuff_printf(grant_buff, 256, pos, "GRANT ALL PRIVILEGES, GRANT OPTION ON %.*s to '%.*s'",
                        table_name.length(), table_name.ptr(), user_name.length(), user_name.ptr());
        grant_stmt.assign_ptr(grant_buff, (ObString::obstr_size_t)pos);
        if (pos >= 255)
        {
          //overflow
          err = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "privilege buffer overflow, ret=%d", err);
        }
        else if (OB_SUCCESS != (err = local_result.init()))
        {
          TBSYS_LOG(WARN, "init result set failed,ret=%d", err);
        }
        else if (OB_SUCCESS != (err = ObSql::direct_execute(grant_stmt, local_result, local_context_)))
        {
          TBSYS_LOG(WARN, "grant privilege to created table failed, sql=%.*s, ret=%d",
              grant_stmt.length(), grant_stmt.ptr(), err);
        }
        else if (OB_SUCCESS != (err = local_result.open()))
        {
          TBSYS_LOG(WARN, "open result set failed,ret=%d", err);
        }
        else if (OB_SUCCESS != (err = local_result.close()))
        {
          TBSYS_LOG(WARN, "close result set failed,ret=%d", err);
        }
        else
        {
          local_result.reset();
        }
      }
      local_context_.disable_privilege_check_ = false;
      if (local_context_.schema_manager_ != NULL)
      {
        int err = local_context_.merger_schema_mgr_->release_schema(local_context_.schema_manager_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "release schema failed,ret=%d", err);
        }
        else
        {
          local_context_.schema_manager_ = NULL;
        }
      }
    }
  }
  return ret;
}

int ObAlterTable::close()
{
  return OB_SUCCESS;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObAlterTable, PHY_ALTER_TABLE);
  }
}

int64_t ObAlterTable::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "AlterTable(table_name=%s, ", alter_schema_.table_name_);
  databuff_printf(buf, buf_len, pos, "table_id=%lu, ", alter_schema_.table_id_);
  databuff_printf(buf, buf_len, pos, "columns=[");
  for (int64_t i = 0; i < alter_schema_.columns_.count(); ++i)
  {
    const AlterTableSchema::AlterColumnSchema& alt_col = alter_schema_.columns_.at(i);
    databuff_printf(buf, buf_len, pos, "(column_name=%s, ", alt_col.column_.column_name_);
    databuff_printf(buf, buf_len, pos, "column_id_=%lu, ", alt_col.column_.column_id_);
    switch (alt_col.type_)
    {
      case AlterTableSchema::ADD_COLUMN:
        databuff_printf(buf, buf_len, pos, "action_type=ADD_COLUMN, ");
        databuff_printf(buf, buf_len, pos, "column_group_id=%lu, ", alt_col.column_.column_group_id_);
        databuff_printf(buf, buf_len, pos, "rowkey_id=%ld, ", alt_col.column_.rowkey_id_);
        databuff_printf(buf, buf_len, pos, "join_table_id=%lu, ", alt_col.column_.join_table_id_);
        databuff_printf(buf, buf_len, pos, "join_column_id=%lu, ", alt_col.column_.join_column_id_);
        databuff_printf(buf, buf_len, pos, "data_type=%d, ", alt_col.column_.data_type_);
        databuff_printf(buf, buf_len, pos, "data_length_=%ld, ", alt_col.column_.data_length_);
        databuff_printf(buf, buf_len, pos, "data_precision=%ld, ", alt_col.column_.data_precision_);
        databuff_printf(buf, buf_len, pos, "nullable=%s, ", alt_col.column_.nullable_ ? "TRUE" : "FALSE");
        databuff_printf(buf, buf_len, pos, "length_in_rowkey=%ld, ", alt_col.column_.length_in_rowkey_);
        databuff_printf(buf, buf_len, pos, "gm_create=%ld, ", alt_col.column_.gm_create_);
        databuff_printf(buf, buf_len, pos, "gm_modify=%ld)", alt_col.column_.gm_modify_);
        break;
      case AlterTableSchema::DEL_COLUMN:
        databuff_printf(buf, buf_len, pos, "action_type=DEL_COLUMN, ");
        break;
      case AlterTableSchema::MOD_COLUMN:
        databuff_printf(buf, buf_len, pos, "action_type=MOD_COLUMN, ");
        databuff_printf(buf, buf_len, pos, "nullable=%s, ", alt_col.column_.nullable_ ? "TRUE" : "FALSE");
        break;
      default:
        break;
    }
    if (i != alter_schema_.columns_.count())
      databuff_printf(buf, buf_len, pos, ", ");
  } // end for
  databuff_printf(buf, buf_len, pos, "])\n");
  return pos;
}
