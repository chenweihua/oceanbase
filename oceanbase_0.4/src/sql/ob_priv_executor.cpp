/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@alipay.com>
 */

#include "common/ob_row.h"
#include "common/ob_privilege_type.h"
#include "common/ob_define.h"
#include "common/ob_encrypted_helper.h"
#include "common/ob_strings.h"
#include "common/ob_trigger_msg.h"
#include "common/ob_array.h"
#include "common/ob_obj_cast.h"
#include "common/ob_string.h"
#include "common/ob_inner_table_operator.h"
#include "sql/ob_priv_executor.h"
#include "sql/ob_sql.h"
#include "sql/ob_grant_stmt.h"
#include "sql/ob_create_user_stmt.h"
#include "sql/ob_drop_user_stmt.h"
#include "sql/ob_revoke_stmt.h"
#include "sql/ob_lock_user_stmt.h"
#include "sql/ob_set_password_stmt.h"
#include "sql/ob_rename_user_stmt.h"

using namespace oceanbase;
using namespace oceanbase::sql;

void ObPrivExecutor::reset()
{
  stmt_ = NULL;
  context_ = NULL;
  result_set_out_ = NULL;
  page_arena_.free();
}

void ObPrivExecutor::reuse()
{
  stmt_ = NULL;
  context_ = NULL;
  result_set_out_ = NULL;
  page_arena_.reuse();
}

int ObPrivExecutor::start_transaction()
{
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("START TRANSACTION");
  ret = execute_stmt_no_return_rows(start_thx);
  return ret;
}
int ObPrivExecutor::commit()
{
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("COMMIT");
  ret = execute_stmt_no_return_rows(start_thx);
  if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = insert_trigger())))
  {
    TBSYS_LOG(ERROR, "insert trigger  failed,ret=%d", ret);
  }
  return ret;
}
int ObPrivExecutor::rollback()
{
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("ROLLBACK");
  ret = execute_stmt_no_return_rows(start_thx);
  return ret;
}
int ObPrivExecutor::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  UNUSED(row_desc);
  return OB_NOT_SUPPORTED;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObPrivExecutor, PHY_PRIV_EXECUTOR);
  }
}
int64_t ObPrivExecutor::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObPrivExecutor(stmt_type=%d)\n", stmt_->get_stmt_type());
  return pos;
}
ObPrivExecutor::ObPrivExecutor()
:context_(NULL), result_set_out_(NULL), page_arena_(4 * 1024)
{
}
ObPrivExecutor::~ObPrivExecutor()
{
}
void ObPrivExecutor::set_stmt(const ObBasicStmt *stmt)
{
  stmt_ = stmt;
}
void ObPrivExecutor::set_context(ObSqlContext *context)
{
  context_ = context;
  result_set_out_ = context_->session_info_->get_current_result_set();
}
int ObPrivExecutor::get_next_row(const ObRow *&row)
{
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}
int ObPrivExecutor::open()
{
  int ret = OB_SUCCESS;
  context_->disable_privilege_check_ = true;
  context_->schema_manager_ = context_->merger_schema_mgr_->get_user_schema(0);
  if (context_->schema_manager_ == NULL)
  {
    ret = OB_SCHEMA_ERROR;
    TBSYS_LOG(WARN, "get schema error, ret=%d", ret);
  }
  else
  {
    switch(stmt_->get_stmt_type())
    {
      case ObBasicStmt::T_CREATE_USER:
        ret = do_create_user(stmt_);
        break;
      case ObBasicStmt::T_DROP_USER:
        ret = do_drop_user(stmt_);
        break;
      case ObBasicStmt::T_SET_PASSWORD:
        ret = do_set_password(stmt_);
        break;
      case ObBasicStmt::T_LOCK_USER:
        ret = do_lock_user(stmt_);
        break;
      case ObBasicStmt::T_GRANT:
        ret = do_grant_privilege(stmt_);
        break;
      case ObBasicStmt::T_REVOKE:
        ret = do_revoke_privilege(stmt_);
        break;
      case ObBasicStmt::T_RENAME_USER:
        ret = do_rename_user(stmt_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(DEBUG, "not a privilege-related sql, ret=%d", ret);
        break;
    };
    int err = context_->merger_schema_mgr_->release_schema(context_->schema_manager_);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "release schema failed,ret=%d", err);
    }
    context_->disable_privilege_check_ = false;
  }
  return ret;
}
int ObPrivExecutor::close()
{
  return OB_SUCCESS;
}
int ObPrivExecutor::get_all_columns_by_user_name(const ObString &user_name, UserInfo &user_info)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char select_users_buff[512];
  ObString select_user;
  // 13个整数列
  databuff_printf(select_users_buff, 512, pos, "SELECT user_id,priv_all,priv_alter,priv_create,priv_create_user,\
      priv_delete, priv_drop, priv_grant_option,priv_insert, priv_update, priv_select, priv_replace,is_locked,pass_word,info FROM \
      __all_user WHERE user_name='%.*s'", user_name.length(), user_name.ptr());
  if (pos >= 511)
  {
    // overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
  }
  else
  {
    select_user.assign_ptr(select_users_buff, static_cast<ObString::obstr_size_t>(pos));
    ObResultSet result2;
    context_->session_info_->set_current_result_set(&result2);
    if (OB_SUCCESS != (ret = result2.init()))
    {
      TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_user, result2, *context_)))
    {
      TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_user.length(), select_user.ptr(), ret);
      result_set_out_->set_message(result2.get_message());
    }
    else if (OB_SUCCESS != (ret = result2.open()))
    {
      TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_user.length(), select_user.ptr(), ret);
    }
    else
    {
      OB_ASSERT(result2.is_with_rows() == true);
      const ObRow* row = NULL;
      ret = result2.get_next_row(row);
      if (OB_ITER_END == ret)
      {
        ret = OB_ERR_USER_NOT_EXIST;
        result_set_out_->set_message("user not exist");
        TBSYS_LOG(WARN, "user not exist");
      }
      else if (OB_SUCCESS == ret)
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        // 13个整数列
        int field_count = 13;
        int i = 0;
        for (i = 0;i < field_count; ++i)
        {
          int64_t field_value = 0;
          if (OB_SUCCESS != (ret = row->raw_get_cell(i, pcell, table_id, column_id)))
          {
            TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
            break;
          }
          else if (pcell->get_type() != ObIntType)
          {
            if (pcell->get_type() == ObNullType)
            {
            }
            else
            {
              ret = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN, "type is not expected, type=%d", pcell->get_type());
              break;
            }
          }
          else if (OB_SUCCESS != (ret = pcell->get_int(field_value)))
          {
            TBSYS_LOG(WARN, "get value from cell failed, ret=%d", ret);
            break;
          }
          else if (OB_SUCCESS != (ret = user_info.field_values_.push_back(field_value)))
          {
            TBSYS_LOG(WARN, "push field value to array failed, ret=%d", ret);
            break;
          }
        }
        // 得到两个string字段，password和comment
        if (OB_SUCCESS == ret)
        {
          ObString tmp;
          if (OB_SUCCESS != (ret = row->raw_get_cell(i, pcell, table_id, column_id)))
          {
            TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
          }
          else if (pcell->get_type() != ObVarcharType)
          {
            if (pcell->get_type() == ObNullType)
            {
              user_info.password_ = ObString::make_string("");
            }
            else
            {
              ret = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN, "type is not expected, type=%d, ret=%d", pcell->get_type(), ret);
            }
          }
          else if (OB_SUCCESS != (ret = pcell->get_varchar(tmp)))
          {
            TBSYS_LOG(WARN, "get value from cell failed, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = ob_write_string(page_arena_, tmp, user_info.password_)))
          {
            TBSYS_LOG(WARN, "ob_write_string failed, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = row->raw_get_cell(i + 1, pcell, table_id, column_id)))
          {
            TBSYS_LOG(WARN, "raw get cell failed, ret=%d", ret);
          }
          else if (pcell->get_type() != ObVarcharType)
          {
            if (pcell->get_type() == ObNullType)
            {
              user_info.comment_ = ObString::make_string("");
            }
            else
            {
              ret = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN, "type is not expected, type=%d, ret=%d", pcell->get_type(), ret);
            }
          }
          else if (OB_SUCCESS != (ret = pcell->get_varchar(tmp)))
          {
            TBSYS_LOG(WARN, "get value from cell failed, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = ob_write_string(page_arena_, tmp, user_info.comment_)))
          {
            TBSYS_LOG(WARN, "ob_write_string failed, ret=%d", ret);
          }
        }
      }
      int err = result2.close();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
      }
      result2.reset();
    }
    context_->session_info_->set_current_result_set(result_set_out_);
  }
  return ret;
}
void ObPrivExecutor::construct_replace_stmt(char *buf, int buf_size, int64_t &pos, int64_t user_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges)
{
  ObString replace_prefix = ObString::make_string("REPLACE INTO __all_table_privilege(user_id, table_id,");
  databuff_printf(buf, buf_size, pos, "%.*s", replace_prefix.length(), replace_prefix.ptr());
  int i = 0;
  for (i = 0;i < privileges->count();++i)
  {
    ObPrivilegeType privilege = privileges->at(i);
    if (privilege == OB_PRIV_ALL)
    {
      databuff_printf(buf, buf_size, pos, "priv_all,");
    }
    else if (privilege == OB_PRIV_ALTER)
    {
      databuff_printf(buf, buf_size, pos, "priv_alter,");
    }
    else if (privilege == OB_PRIV_CREATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_create,");
    }
    else if (privilege == OB_PRIV_CREATE_USER)
    {
      databuff_printf(buf, buf_size, pos, "priv_create_user,");
    }
    else if (privilege == OB_PRIV_DELETE)
    {
      databuff_printf(buf, buf_size, pos, "priv_delete,");
    }
    else if (privilege == OB_PRIV_DROP)
    {
      databuff_printf(buf, buf_size, pos, "priv_drop,");
    }
    else if (privilege == OB_PRIV_GRANT_OPTION)
    {
      databuff_printf(buf, buf_size, pos, "priv_grant_option,");
    }
    else if (privilege == OB_PRIV_INSERT)
    {
      databuff_printf(buf, buf_size, pos, "priv_insert,");
    }
    else if (privilege == OB_PRIV_UPDATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_update,");
    }
    else if (privilege == OB_PRIV_SELECT)
    {
      databuff_printf(buf, buf_size, pos, "priv_select,");
    }
    else if (privilege == OB_PRIV_REPLACE)
    {
      databuff_printf(buf, buf_size, pos, "priv_replace,");
    }
  }
  pos = pos - 1;
  databuff_printf(buf, buf_size, pos, ") VALUES (%ld, %lu,", user_id, table_id);
  for (i = 0;i < privileges->count();++i)
  {
    databuff_printf(buf, buf_size, pos, "1,");
  }
  pos = pos - 1;
  databuff_printf(buf, buf_size, pos, ")");
}
/**
 * @synopsis  construct_update_expressions
 *
 * @param buf
 * @param buf_size
 * @param pos 从pos开始写数据
 * @param privileges 不能为NULL并且必须至少有一个元素
 * @param is_grant 如果true，则构造grant语句中的update，如果false，则构造revoke语句中的update
 * @returns
 */
void ObPrivExecutor::construct_update_expressions(char *buf, int buf_size, int64_t &pos, const ObArray<ObPrivilegeType> *privileges, bool is_grant)
{
  int ret = OB_SUCCESS;
  int64_t priv_count = privileges->count();
  // 如果GRANT OPTION以外的权限被revoke，all权限位需要被重置为0
  bool flag = false;
  int64_t i = 0;
  for (i = 0;i < priv_count;++i)
  {
    ObPrivilegeType privilege = privileges->at(i);
    if (privilege == OB_PRIV_ALL)
    {
      if (is_grant)
      {
        databuff_printf(buf, buf_size, pos, "priv_all=1,");
      }
      else
      {
        databuff_printf(buf, buf_size, pos, "priv_alter=0,");
        databuff_printf(buf, buf_size, pos, "priv_create=0,");
        databuff_printf(buf, buf_size, pos, "priv_create_user=0,");
        databuff_printf(buf, buf_size, pos, "priv_delete=0,");
        databuff_printf(buf, buf_size, pos, "priv_drop=0,");
        databuff_printf(buf, buf_size, pos, "priv_grant_option=0,");
        databuff_printf(buf, buf_size, pos, "priv_insert=0,");
        databuff_printf(buf, buf_size, pos, "priv_update=0,");
        databuff_printf(buf, buf_size, pos, "priv_select=0,");
        databuff_printf(buf, buf_size, pos, "priv_replace=0,");
        flag = true;
      }
    }
    else if (privilege == OB_PRIV_ALTER)
    {
      databuff_printf(buf, buf_size, pos, "priv_alter=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_CREATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_create=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_CREATE_USER)
    {
      databuff_printf(buf, buf_size, pos, "priv_create_user=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_DELETE)
    {
      databuff_printf(buf, buf_size, pos, "priv_delete=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_DROP)
    {
      databuff_printf(buf, buf_size, pos, "priv_drop=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_GRANT_OPTION)
    {
      databuff_printf(buf, buf_size, pos, "priv_grant_option=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_INSERT)
    {
      databuff_printf(buf, buf_size, pos, "priv_insert=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_UPDATE)
    {
      databuff_printf(buf, buf_size, pos, "priv_update=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_SELECT)
    {
      databuff_printf(buf, buf_size, pos, "priv_select=%d,", is_grant ? 1 : 0);
      flag = true;
    }
    else if (privilege == OB_PRIV_REPLACE)
    {
      databuff_printf(buf, buf_size, pos, "priv_replace=%d,", is_grant ? 1 : 0);
      flag = true;
    }
  }
  // revoke语句使用
  if (true == flag && !is_grant)
  {
    databuff_printf(buf, buf_size, pos, "priv_all=0,");
  }
  if (pos >= buf_size - 1)
  {
    // overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
  }
  else
  {
    //回退最后一个逗号
    pos = pos - 1;
  }
}
int ObPrivExecutor::insert_trigger()
{
  int ret = OB_SUCCESS;
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  ObString sql;
  ObServer server;
  server.set_ipv4_addr(tbsys::CNetUtil::getLocalAddr(NULL), 0);
  char buf[OB_MAX_SQL_LENGTH] = "";
  sql.assign(buf, sizeof(buf));
  ret = ObInnerTableOperator::update_all_trigger_event(sql, timestamp, server, UPDATE_PRIVILEGE_TIMESTAMP_TRIGGER, 0);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get update all trigger event sql failed:ret[%d]", ret);
  }
  else
  {
    ret = execute_stmt_no_return_rows(sql);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "execute_stmt_no_return_rows failed:sql[%.*s], ret[%d]", sql.length(), sql.ptr(), ret);
    }
  }
  return ret;
}
int ObPrivExecutor::revoke_all_priv_from_table_privs_by_user_name(const ObString &user_name)
{
  int ret = OB_SUCCESS;
  int64_t user_id = -1;
  ObArray<uint64_t> table_ids;
  if (OB_SUCCESS != (ret = get_user_id_by_user_name(user_name, user_id)))
  {
    TBSYS_LOG(ERROR, "get user id by user name failed, user_name=%.*s, ret=%d", user_name.length(), user_name.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = get_table_ids_by_user_id(user_id, table_ids)))
  {
    TBSYS_LOG(ERROR, "get table ids from user id failed, user_id=%ld, ret=%d", user_id, ret);
  }
  else
  {
    int i = 0;
    for (i = 0;i < table_ids.count();++i)
    {
      uint64_t tid = table_ids.at(i);
      if (OB_SUCCESS != (ret = reset_table_privilege(user_id, tid, NULL)))
      {
        TBSYS_LOG(ERROR, "revoke privilege from __all_table_privilege failed, user_id=%lu, table_id=%lu, ret=%d", user_id, tid, ret);
        break;
      }
    }
  }
  return ret;
}
int ObPrivExecutor::revoke_all_priv_from_users_by_user_name(const ObString &user_name)
{
  int ret = OB_SUCCESS;
  char update_users_buff[512];
  ObString update_users;
  int64_t pos = 0;
  databuff_printf(update_users_buff, 512, pos, "UPDATE __all_user SET priv_all=0, priv_alter=0,priv_create=0,priv_create_user=0,priv_delete=0,priv_drop=0,priv_grant_option=0,priv_insert=0, priv_update=0,priv_select=0,priv_replace=0 WHERE user_name=");
  databuff_printf(update_users_buff, 512, pos, "'%.*s'", user_name.length(), user_name.ptr());
  if (pos >= 511)
  {
    // overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
  }
  else
  {
    update_users.assign_ptr(update_users_buff, static_cast<ObString::obstr_size_t>(pos));
    ret = execute_stmt_no_return_rows(update_users);
  }
  return ret;
}
int ObPrivExecutor::reset_table_privilege(int64_t user_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObString update_priv;
  char update_table_buff[512];
  // indicates revoke all privileges grant_option
  if (NULL == privileges)
  {
    databuff_printf(update_table_buff, 512, pos, "UPDATE __all_table_privilege SET priv_all=0, priv_alter=0,priv_create=0,priv_create_user=0,priv_delete=0,priv_drop=0,priv_grant_option=0,priv_insert=0, priv_update=0,priv_select=0,priv_replace=0 WHERE user_id=%ld and table_id=%lu", user_id, table_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      update_priv.assign_ptr(update_table_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  else
  {
    databuff_printf(update_table_buff, 512, pos, "UPDATE __all_table_privilege SET ");
    construct_update_expressions(update_table_buff, 512, pos, privileges, false);
    databuff_printf(update_table_buff, 512, pos, "WHERE user_id=%lu and table_id=%lu", user_id, table_id);
    if (pos >= 511)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
    }
    else
    {
      update_priv.assign_ptr(update_table_buff, static_cast<ObString::obstr_size_t>(pos));
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = execute_stmt_no_return_rows(update_priv);
  }
  return ret;
}
int ObPrivExecutor::get_table_ids_by_user_id(int64_t user_id, ObArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  char select_table_id_buff[512];
  int cnt = snprintf(select_table_id_buff, 512, "SELECT table_id from __all_table_privilege where user_id=%ld", user_id);
  ObString select_table_id;
  select_table_id.assign_ptr(select_table_id_buff, cnt);
  ObResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_table_id, tmp_result, *context_)))
  {
    context_->session_info_->get_current_result_set()->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_table_id.length(), select_table_id.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_table_id.length(), select_table_id.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == true);
    const ObRow* row = NULL;
    while (true)
    {
      ret = tmp_result.get_next_row(row);
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ret = row->raw_get_cell(0, pcell, table_id, column_id);

        if (OB_SUCCESS == ret)
        {
          if (pcell->get_type() == ObIntType)
          {
            int64_t tid = -1;
            if (OB_SUCCESS != (ret = pcell->get_int(tid)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
            else
            {
              table_id = static_cast<uint64_t>(tid);
              if (OB_SUCCESS != (ret = table_ids.push_back(table_id)))
              {
                TBSYS_LOG(WARN, "push table id=%lu to array failed, ret=%d", table_id, ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(table_id) failed, ret=%d", ret);
        }
      }
    }// while
  }
  return ret;
}
int ObPrivExecutor::execute_update_user(const ObString &update_user)
{
  int ret = OB_SUCCESS;
  ObResultSet tmp_result;
  context_->session_info_->set_current_result_set(&tmp_result);
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(update_user, tmp_result, *context_)))
  {
    result_set_out_->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", update_user.length(), update_user.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", update_user.length(), update_user.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == false);
    int64_t affected_rows = tmp_result.get_affected_rows();
    if (affected_rows == 0)
    {
      TBSYS_LOG(WARN, "user not exist, sql=%.*s", update_user.length(), update_user.ptr());
      result_set_out_->set_message("user not exist");
      ret = OB_ERR_USER_NOT_EXIST;
    }
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  context_->session_info_->set_current_result_set(&tmp_result);
  return ret;
}
int ObPrivExecutor::execute_delete_user(const ObString &delete_user)
{
  int ret = OB_SUCCESS;
  ObResultSet tmp_result;
  context_->session_info_->set_current_result_set(&tmp_result);
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(delete_user, tmp_result, *context_)))
  {
    result_set_out_->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", delete_user.length(), delete_user.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", delete_user.length(), delete_user.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == false);
    int64_t affected_rows = tmp_result.get_affected_rows();
    if (affected_rows == 0)
    {
      TBSYS_LOG(WARN, "delete user failed, sql=%.*s", delete_user.length(), delete_user.ptr());
      result_set_out_->set_message("delete user failed");
      ret = OB_ERR_USER_NOT_EXIST;
    }
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  return ret;
}
int ObPrivExecutor::execute_stmt_no_return_rows(const ObString &stmt)
{
  int ret = OB_SUCCESS;
  ObResultSet tmp_result;
  context_->session_info_->set_current_result_set(&tmp_result);
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(stmt, tmp_result, *context_)))
  {
    result_set_out_->set_message(tmp_result.get_message());
    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == false);
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  return ret;
}
int ObPrivExecutor::get_user_id_by_user_name(const ObString &user_name, int64_t &user_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObString select_user_id_prefix = ObString::make_string("select user_id from __all_user where user_name = '");
  char select_user_id_buff[512];
  ObString select_user_id;
  databuff_printf(select_user_id_buff, 512, pos, "select user_id from __all_user where user_name = '%.*s'", user_name.length(), user_name.ptr());
  if (pos >= 511)
  {
    // overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
  }
  else
  {
    select_user_id.assign_ptr(select_user_id_buff, static_cast<ObString::obstr_size_t>(pos));
    ObResultSet result2;
    context_->session_info_->set_current_result_set(&result2);
    if (OB_SUCCESS != (ret = result2.init()))
    {
      TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_user_id, result2, *context_)))
    {
      TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
      result_set_out_->set_message(result2.get_message());
    }
    else if (OB_SUCCESS != (ret = result2.open()))
    {
      TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
    }
    else
    {
      OB_ASSERT(result2.is_with_rows() == true);
      const ObRow* row = NULL;
      ret = result2.get_next_row(row);
      if (OB_SUCCESS != ret)
      {
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(WARN, "user_name: %.*s not exists, ret=%d", user_name.length(), user_name.ptr(), ret);
          ret = OB_ERR_USER_NOT_EXIST;
          result_set_out_->set_message("user not exists");
        }
        else
        {
          TBSYS_LOG(WARN, "next row from ObResultSet failed,ret=%d", ret);
        }
      }
      else
      {
        const ObObj *pcell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ret = row->raw_get_cell(0, pcell, table_id, column_id);

        if (OB_SUCCESS == ret)
        {
          if (pcell->get_type() == ObIntType)
          {
            if (OB_SUCCESS != (ret = pcell->get_int(user_id)))
            {
              TBSYS_LOG(WARN, "failed to get int from ObObj, ret=%d", ret);
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "got type of %d cell from __all_user row, expected type=%d", pcell->get_type(), ObIntType);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "raw get cell(user_id) from __all_user failed, ret=%d", ret);
        }
      }
      int err = result2.close();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
      }
      result2.reset();
    }
    context_->session_info_->set_current_result_set(result_set_out_);
  }
  return ret;
}
int ObPrivExecutor::do_revoke_privilege(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObRevokeStmt *revoke_stmt = dynamic_cast<const ObRevokeStmt*>(stmt);
  if (OB_UNLIKELY(NULL == revoke_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast revoke stmt failed, ret=%d", ret);
  }
  else
  {
    // start transaction
    // user_name->user_id:   select user_id from __users for update where user_name = 'user1';
    // update __users表
    // 根据user_id 查__table_privileges表，得到所有table_id
    // 根据user_id和table_id update __table_privileges表
    // commit
    // insert trigger table

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    else
    {
      ObString user_name;
      const ObStrings *users = revoke_stmt->get_users();
      int64_t user_count = users->count();
      int64_t user_id = -1;
      int i = 0;
      for (i = 0;i < user_count;++i)
      {
        // step2: username->user_id select user_id from __users for update where user_name = 'user1'
        user_id = -1;
        pos = 0;
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(ERROR, "get user from grant stmt failed, ret=%d", ret);
          break;
        }
        else
        {
          // user_name->user_id
          ret = get_user_id_by_user_name(user_name, user_id);
        }
        // so far, we got user_id
        // step3 : update __users and  __table_privileges
        if (OB_SUCCESS == ret)
        {
          char update_table_priv_buff[512];
          ObString update_table_priv;
          update_table_priv.assign_ptr(update_table_priv_buff, 512);

          char update_priv_buff[512];
          ObString update_priv;

          uint64_t table_id = revoke_stmt->get_table_id();
          // revoke xx,xx on * from user
          // revoke xx, xx on table_name from user
          if ((OB_NOT_EXIST_TABLE_TID == table_id) || (table_id != OB_INVALID_ID))
          {
            // step 3.1: 在__users表中清除全局权限
            const ObArray<ObPrivilegeType> *privileges = revoke_stmt->get_privileges();
            databuff_printf(update_priv_buff, 512, pos, "UPDATE __all_user SET ");
            construct_update_expressions(update_priv_buff, 512, pos, privileges, false);
            databuff_printf(update_priv_buff, 512, pos, " WHERE user_name='%.*s'", user_name.length(), user_name.ptr());
            if (pos >= 511)
            {
              // overflow
              ret = OB_BUF_NOT_ENOUGH;
              TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
            }
            else
            {
              //update __users表
              update_priv.assign_ptr(update_priv_buff, static_cast<ObString::obstr_size_t>(pos));
              ret = execute_stmt_no_return_rows(update_priv);
            }
            if (OB_SUCCESS == ret)
            {
              // revoke xx,xx on * from user
              if (OB_NOT_EXIST_TABLE_TID == table_id)
              {
                // select table_id from __table_privileges where user_id =
                // 从__table_privileges表中reset局部权限，将与user_name这个用户相关的所有的记录的这几个权限全部清 0
                ObArray<uint64_t> table_ids;
                ret = get_table_ids_by_user_id(user_id, table_ids);
                if (OB_SUCCESS == ret)
                {
                  char replace_table_priv_buff[512];
                  ObString replace_table_priv;
                  replace_table_priv.assign_buffer(replace_table_priv_buff, 512);
                  for (int i = 0;i < table_ids.count();++i)
                  {
                    uint64_t tid = table_ids.at(i);
                    if (OB_SUCCESS != (ret = reset_table_privilege(user_id, tid, privileges)))
                    {
                      TBSYS_LOG(ERROR, "revoke privilege from __all_table_privilege failed, user_id=%lu, table_id=%lu, ret=%d", user_id, tid, ret);
                      break;
                    }
                  }
                }
              }
              //revoke xx, xx on table_name from user
              else
              {
                if (OB_SUCCESS != (ret = reset_table_privilege(user_id, table_id, privileges)))
                {
                  TBSYS_LOG(ERROR, "revoke privilege from __all_table_privilege failed, user_id=%ld, table_id=%lu, ret=%d", user_id, table_id, ret);
                }
              }
            }
          }
          // revoke ALL PRIVILEGES, GRANT OPTION from user, only support this syntax
          // e.g. revoke select from user is not a valid syntax
          else if (OB_INVALID_ID == table_id)
          {
            if (OB_SUCCESS != (ret = revoke_all_priv_from_users_by_user_name(user_name)))
            {
              TBSYS_LOG(ERROR, "revoke all users priv from username=%.*s failed, ret=%d", user_name.length(), user_name.ptr(), ret);
            }
            else if (OB_SUCCESS != (ret = revoke_all_priv_from_table_privs_by_user_name(user_name)))
            {
              TBSYS_LOG(ERROR, "revoke all table privs from username=%.*s failed, ret=%d", user_name.length(), user_name.ptr(), ret);
            }
          }
        }
      }// for (i = 0;i < user_count;++i)
    }
    // step 4: commit or rollback
    if (OB_SUCCESS == ret)
    {
      ret = commit();
      if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = insert_trigger())))
      {
        TBSYS_LOG(ERROR, "insert trigger  failed,ret=%d", ret);
      }
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
int ObPrivExecutor::do_grant_privilege(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObGrantStmt *grant_stmt = dynamic_cast<const ObGrantStmt*>(stmt);
  if (OB_UNLIKELY(NULL == grant_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast grant stmt failed, ret=%d", ret);
  }
  else
  {
    // start transaction
    // user_name->user_id:   select user_id from __users for update where user_name = 'user1';
    //
    // 看看有没有 on *  1. 如果有*，直接修改__users表 2. 如果没有* , 直接修改__table_privileges表
    // commit

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    else
    {
      char select_user_id_buff[512];
      ObString user_name;
      ObString select_user_id;
      ObString select_user_id_prefix = ObString::make_string("select user_id from __all_user where user_name = '");
      const ObStrings *users = grant_stmt->get_users();
      int64_t user_count = users->count();
      int64_t user_id = -1;
      int i = 0;
      for (i = 0;i < user_count;++i)
      {
        // step2: username->user_id select user_id from __users for update where user_name = 'user1'
        user_id = -1;
        pos = 0;
        select_user_id.assign_buffer(select_user_id_buff, 512);
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(ERROR, "get user from grant stmt failed, ret=%d", ret);
          break;
        }
        else
        {
          // user_name->user_id
          ret = get_user_id_by_user_name(user_name, user_id);
        }
        // so far, we got user_id, used by __table_privileges
        // step3 : update __users or __table_privileges
        if (OB_SUCCESS == ret)
        {
          char update_priv_buff[512];
          ObString update_priv;
          //update_priv.assign_buffer(update_priv_buff, 512);
          uint64_t table_id = grant_stmt->get_table_id();
          // grant xx,xx on * to user
          if (OB_NOT_EXIST_TABLE_TID == table_id)
          {
            ObString update_priv_suffix = ObString::make_string(" WHERE user_name='");
            const ObArray<ObPrivilegeType> *privileges = grant_stmt->get_privileges();
            databuff_printf(update_priv_buff, 512, pos, "UPDATE __all_user SET ");
            construct_update_expressions(update_priv_buff, 512, pos, privileges, true);
            databuff_printf(update_priv_buff, 512, pos, " WHERE user_name='%.*s'", user_name.length(), user_name.ptr());
          }// grant xx,xx on * to user
          // grant xx, xx on table_name to user
          else
          {
            // only one table id
            const ObArray<ObPrivilegeType> *privileges = grant_stmt->get_privileges();
            construct_replace_stmt(update_priv_buff, 512, pos, user_id, table_id, privileges);

          }// grant xx, xx on table_name to user
          if (pos >= 511)
          {
            // overflow
            ret = OB_BUF_NOT_ENOUGH;
            TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
          }
          else
          {
            // execute this statement
            update_priv.assign_ptr(update_priv_buff, static_cast<ObString::obstr_size_t>(pos));
            ret = execute_stmt_no_return_rows(update_priv);
          }
        }
      }// for (i = 0;i < user_count;++i)
    }
    // step 4: commit or rollback
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}

int ObPrivExecutor::do_drop_user(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObDropUserStmt *drop_user_stmt = dynamic_cast<const ObDropUserStmt*>(stmt);
  if (OB_UNLIKELY(NULL == drop_user_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast drop user stmt failed, ret=%d", ret);
  }
  else
  {
    // START TRANSACTION
    // delete from __users where user_name = 'user1'
    // commit;
    // insert trigger

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    // step 3: delete user
    else
    {
      const ObStrings *users = drop_user_stmt->get_users();
      int64_t user_count = users->count();
      ObString user_name;
      int i = 0;
      for (i = 0;i < user_count; ++i)
      {
        pos = 0;
        char delete_user_buff[512];
        ObString delete_user;
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(ERROR, "get user from drop user stmt failed, ret=%d", ret);
          break;
        }
        else if (user_name == OB_ADMIN_USER_NAME)
        {
          ret = OB_ERR_NO_PRIVILEGE;
          result_set_out_->set_message("no privilege");
          TBSYS_LOG(ERROR, "drop admin failed, can't drop admin");
          break;
        }
        else
        {
          databuff_printf(delete_user_buff, 512, pos, "delete from __all_user where user_name = '%.*s'", user_name.length(), user_name.ptr());
          if (pos >= 511)
          {
            // overflow
            ret = OB_BUF_NOT_ENOUGH;
            TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
          }
          else
          {
            delete_user.assign_ptr(delete_user_buff, static_cast<ObString::obstr_size_t>(pos));
            ret = execute_delete_user(delete_user);
          }
        }
      }
    }
    // step 4: commit or rollback
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
int ObPrivExecutor::do_rename_user(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  const ObRenameUserStmt *rename_user_stmt = dynamic_cast<const ObRenameUserStmt*>(stmt);
  const common::ObStrings* rename_infos = rename_user_stmt->get_rename_infos();
  int64_t pos = 0;
  // step 1: start transaction
  // step 2: select * from __users where user_name =
  // step 3: delete from __users where user_name =
  // step 4: insert into __users() values()
  // step 5: commit
  // step 6: insert trigger table

  // step 1: start transaction and insert trigger
  if (OB_SUCCESS != (ret = start_transaction()))
  {
    TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
  }
  else
  {
    OB_ASSERT(rename_infos->count() % 2 == 0);
    ObString from_user;
    ObString to_user;
    for (int i = 0;i < rename_infos->count(); i = i + 2)
    {
      UserInfo user_info;
      pos = 0;
      if (OB_SUCCESS != (ret = rename_infos->get_string(i, from_user)))
      {
      }
      else if (OB_SUCCESS != (ret = rename_infos->get_string(i + 1, to_user)))
      {
      }
      else
      {
        ret = get_all_columns_by_user_name(from_user, user_info);
      }
      // delete
      if (OB_SUCCESS == ret)
      {
        char delete_user_buff[128];
        ObString delete_user;
        databuff_printf(delete_user_buff, 128, pos, "DELETE FROM __all_user WHERE user_name='%.*s'", from_user.length(), from_user.ptr());
        if (pos >= 127)
        {
          // overflow
          ret = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
        }
        else
        {
          delete_user.assign_ptr(delete_user_buff, static_cast<ObString::obstr_size_t>(pos));
          ret = execute_delete_user(delete_user);
        }
      }
      // insert
      if (OB_SUCCESS == ret)
      {
        pos = 0;
        char insert_user_buff[1024];
        ObString insert_user;
        databuff_printf(insert_user_buff, 1024, pos, "INSERT INTO __all_user(user_id,priv_all,priv_alter,priv_create,priv_create_user,\
                  priv_delete,priv_drop,priv_grant_option,priv_insert,priv_update,priv_select,priv_replace,is_locked,user_name,pass_word,info) \
                  VALUES(%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,'%.*s','%.*s', '%.*s')", user_info.field_values_.at(0), user_info.field_values_.at(1),
                    user_info.field_values_.at(2), user_info.field_values_.at(3),user_info.field_values_.at(4),
                    user_info.field_values_.at(5), user_info.field_values_.at(6),user_info.field_values_.at(7),
                    user_info.field_values_.at(8), user_info.field_values_.at(9),user_info.field_values_.at(10),
                    user_info.field_values_.at(11),user_info.field_values_.at(12),to_user.length(), to_user.ptr(),
                    user_info.password_.length(), user_info.password_.ptr(),
                    user_info.comment_.length(), user_info.comment_.ptr());
        if (pos >= 639)
        {
          // overflow
          ret = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
        }
        else
        {
          insert_user.assign_ptr(insert_user_buff, static_cast<ObString::obstr_size_t>(pos));
          TBSYS_LOG(INFO, "insert_user=%.*s", insert_user.length(), insert_user.ptr());
          ret = execute_stmt_no_return_rows(insert_user);
        }
      }
    }// for each from user and to user
  }
  if (OB_SUCCESS == ret)
  {
    ret = commit();
  }
  else
  {
    // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
    // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
    int err = rollback();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
    }
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
int ObPrivExecutor::do_set_password(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char encrypted_pass_buff[SCRAMBLE_LENGTH * 2 + 1];
  ObString encrypted_pass;
  encrypted_pass.assign_ptr(encrypted_pass_buff, SCRAMBLE_LENGTH * 2 + 1);
  ObString empty_user;
  ObString user;
  ObString pwd;
  const ObSetPasswordStmt *set_password_stmt = dynamic_cast<const ObSetPasswordStmt*>(stmt);
  if (OB_UNLIKELY(NULL == set_password_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", ret);
  }
  else
  {
    const common::ObStrings *user_pass = set_password_stmt->get_user_password();
    OB_ASSERT(user_pass->count() == 2);
    if (OB_SUCCESS != (ret = user_pass->get_string(0, user)))
    {
    }
    else if (OB_SUCCESS != (ret = user_pass->get_string(1, pwd)))
    {
    }
    else
    {
      if (user == empty_user)
      {
        user = context_->session_info_->get_user_name();
      }
      if (pwd.length() == 0)
      {
        ret = OB_ERR_PASSWORD_EMPTY;
        result_set_out_->set_message("password must not be empty");
        TBSYS_LOG(WARN, "password must not be empty");
      }
      else
      {
        ObEncryptedHelper::encrypt(encrypted_pass, pwd);
        encrypted_pass.assign_ptr(encrypted_pass_buff, SCRAMBLE_LENGTH * 2);
      }
      // got user and password
    }
    // step 1: start transaction
    // step 2: update __users
    // step 3: commit
    // step 4: insert trigger table
    if (OB_SUCCESS == ret)
    {
      // step 1: start transaction and insert trigger
      if (OB_SUCCESS != (ret = start_transaction()))
      {
        TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
      }
      else
      {
        char update_user_buff[512];
        ObString update_user;
        databuff_printf(update_user_buff, 512, pos, "UPDATE __all_user SET pass_word='%.*s' WHERE user_name='%.*s'",
            encrypted_pass.length(), encrypted_pass.ptr(), user.length(), user.ptr());
        if (pos >= 511)
        {
          // overflow
          ret = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
        }
        else
        {
          update_user.assign_ptr(update_user_buff, static_cast<ObString::obstr_size_t>(pos));
          ret = execute_update_user(update_user);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = commit();
      }
      else
      {
        // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
        // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
        int err = rollback();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
        }
        //if (OB_ERR_USER_NOT_EXIST == ret)
        //{
          //ret = OB_USER_NOT_EXIST;
        //}
      }
      //int64_t affected_rows = context_->session_info_->get_current_result_set()->get_affected_rows();
      //if (affected_rows == 0)
      //{
        //context_->session_info_->get_current_result_set()->set_message("user not exist");
        //TBSYS_LOG(WARN, "user %.*s not exists, ret=%d", user.length(), user.ptr(), ret);
      //}
    }
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
int ObPrivExecutor::do_lock_user(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObLockUserStmt *lock_user_stmt = dynamic_cast<const ObLockUserStmt*>(stmt);
  if (OB_UNLIKELY(NULL == lock_user_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast lock user stmt failed, ret=%d", ret);
  }
  else
  {
    // step1: start transaction
    // step2: update __users
    // step3: commit
    // step4: insert trigger table

    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    else
    {
      const common::ObStrings* user = lock_user_stmt->get_user();
      OB_ASSERT(user->count() == 1);
      ObString user_name;
      if (OB_SUCCESS != (ret = user->get_string(0, user_name)))
      {
        TBSYS_LOG(WARN, "get user from lock user stmt failed, ret=%d", ret);
      }
      else
      {
        // step 2
        char update_user_buff[128];
        ObString update_user;
        databuff_printf(update_user_buff, 128, pos, "UPDATE __all_user SET is_locked=%d WHERE user_name='%.*s'",
            lock_user_stmt->is_locked(), user_name.length(), user_name.ptr());
        if (pos >= 127)
        {
          // overflow
          ret = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
        }
        else
        {
          update_user.assign_ptr(update_user_buff, static_cast<ObString::obstr_size_t>(pos));
          ret = execute_stmt_no_return_rows(update_user);
        }
      }
    }
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
int ObPrivExecutor::do_create_user(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  // hold return value of result.close()
  int err = OB_SUCCESS;
  const ObCreateUserStmt *create_user_stmt = dynamic_cast<const ObCreateUserStmt*>(stmt);
  if (OB_UNLIKELY(NULL == create_user_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "dynamic cast create user stmt failed, ret=%d", ret);
  }
  else
  {

     // step 1: START TRANSACTION;
     // step 2: insert trigger table
     // step 3: select value1 from __all_sys_stat where cluster_role = 0 and cluster_id = 0 and server_type = 0 and server_role = 0 and server_ipv4 = 0 and
     //         server_ipv6_high = 0 and server_ipv6_low = 0 and server_port = 0 and table_id = 0 and name = 'ob_max_user_id' for update
     // step 4: 对明文密码进行加密
     // step 5: insert into __users
     //         insert into __users
     // step 6: UPDATE __all_sys_stat set value1 = value1 + 用户个数 where cluster_role = 0 and cluster_id = 0 and server_type = 0 and server_role = 0
     //         and server_ipv4 = 0 and server_ipv6_high = 0 and server_ipv6_low = 0 and server_port = 0 and table_id = 0 and name = 'ob_max_user_id'
     // step 7: COMMIT;

    int64_t current_user_id = -1;
    // step 1: start transaction and insert trigger
    if (OB_SUCCESS != (ret = start_transaction()))
    {
      TBSYS_LOG(WARN, "start transaction failed,ret=%d", ret);
    }
    // step2: get user id
    else
    {
      ObString select_user_id = ObString::make_string("select value from __all_sys_stat where cluster_id = 0 and name = 'ob_max_user_id'");
      ObResultSet result2;
      context_->session_info_->set_current_result_set(&result2);
      if (OB_SUCCESS != (ret = result2.init()))
      {
        TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ObSql::direct_execute(select_user_id, result2, *context_)))
      {
        TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
        result_set_out_->set_message(result2.get_message());
      }
      else if (OB_SUCCESS != (ret = result2.open()))
      {
        TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
      }
      else
      {
        OB_ASSERT(result2.is_with_rows() == true);
        const ObRow* row = NULL;
        ret = result2.get_next_row(row);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "next row from ObResultSet failed, sql=%.*s ret=%d", select_user_id.length(), select_user_id.ptr(), ret);
        }
        else
        {
          ObExprObj in;
          ObExprObj out;
          const ObObj *pcell = NULL;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          ret = row->raw_get_cell(0, pcell, table_id, column_id);
          if (OB_SUCCESS == ret)
          {
            in.assign(*pcell);
            ObObjCastParams params;
            if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObVarcharType][ObIntType](params, in, out)))
            {
              TBSYS_LOG(ERROR, "varchar cast to int failed, ret=%d", ret);
            }
            else
            {
              current_user_id = out.get_int();
            }
          }
          else
          {
            TBSYS_LOG(WARN, "raw get cell(ob_max_user_id) from row failed, ret=%d", ret);
          }
        }
        err = result2.close();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
        }
        result2.reset();
      }
      context_->session_info_->set_current_result_set(result_set_out_);
    }
    // step 3: insert __users
    // INSERT INTO __users(is_locked,priv_all,priv_alter,priv_create,priv_create_user,priv_delete,priv_drop,priv_grant_option,priv_insert,
    // priv_update,priv_select,priv_replace,user_id,user_name,pass_word) values(0,0,0,0,0,0,0,0,0,0,0,0,value1+1,'user1',
    // '经过加密后的user1的password'
    if (OB_SUCCESS == ret)
    {
      const ObStrings * users = create_user_stmt->get_users();
      int64_t count = users->count();
      OB_ASSERT(count % 2 == 0);
      //int64_t user_count = count / 2;
      int i = 0;
      ObString insert_user_prefix = ObString::make_string("INSERT INTO __all_user(info,is_locked,priv_all,priv_alter,priv_create,priv_create_user,priv_delete,\
               priv_drop,priv_grant_option,priv_insert,priv_update,priv_select,priv_replace,user_id,user_name,pass_word) values('', 0,0,0,0,0,0,0,0,0,0,0,0,");
      ObString user_name;
      ObString pass_word;
      ObString empty_pwd = ObString::make_string("");
      for (i = 0;i < count; i = i + 2)
      {
        pos = 0;
        char insert_user_buff[512];
        ObString insert_user;
        insert_user.assign_buffer(insert_user_buff, 512);
        if (OB_SUCCESS != (ret = users->get_string(i, user_name)))
        {
          TBSYS_LOG(WARN, "get user from create user stmt failed, ret=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = users->get_string(i + 1, pass_word)))
        {
          TBSYS_LOG(WARN, "get password from create user stmt failed, ret=%d", ret);
          break;
        }
        else if (pass_word == empty_pwd)
        {
          ret = OB_ERR_PASSWORD_EMPTY;
          result_set_out_->set_message("password must not be empty");
          TBSYS_LOG(WARN, "password must not be empty");
          break;
        }
        else
        {
          char scrambled[SCRAMBLE_LENGTH * 2 + 1];
          memset(scrambled, 0, sizeof(scrambled));
          ObString stored_pwd;
          stored_pwd.assign_ptr(scrambled, SCRAMBLE_LENGTH * 2 + 1);
          ObEncryptedHelper::encrypt(stored_pwd, pass_word);
          stored_pwd.assign_ptr(scrambled, SCRAMBLE_LENGTH * 2);
          databuff_printf(insert_user_buff, 512, pos, "INSERT INTO __all_user(info,is_locked,priv_all,priv_alter,priv_create,priv_create_user,priv_delete,\
            priv_drop,priv_grant_option,priv_insert,priv_update,priv_select,priv_replace,user_id,user_name,pass_word) \
            VALUES('', 0,0,0,0,0,0,0,0,0,0,0,0,%ld, '%.*s', '%.*s')", current_user_id + i / 2 + 1,
              user_name.length(), user_name.ptr(), stored_pwd.length(), stored_pwd.ptr());
          if (pos >= 511)
          {
            // overflow
            ret = OB_BUF_NOT_ENOUGH;
            TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
          }
          else
          {
            insert_user.assign_ptr(insert_user_buff, static_cast<ObString::obstr_size_t>(pos));
            ret = execute_stmt_no_return_rows(insert_user);
            if (OB_SUCCESS != ret)
            {
              ret = OB_ERR_USER_EXIST;
              result_set_out_->set_message("user exists");
              TBSYS_LOG(WARN, "create user failed, user:%.*s exists",user_name.length(), user_name.ptr());
              break;
            }
          }
        }
      }// for
      current_user_id += count/2;
    }
    // step 4: update __all_sys_stat
    if (OB_SUCCESS == ret)
    {
      pos = 0;
      char update_max_user_id_buff[512];
      ObString update_max_user_id;
      databuff_printf(update_max_user_id_buff, 512, pos, "UPDATE __all_sys_stat set value = '%ld' \
          where cluster_id = 0 and name = 'ob_max_user_id'", current_user_id);
      if (pos >= 511)
      {
        // overflow
        ret = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
      }
      else
      {
        update_max_user_id.assign_ptr(update_max_user_id_buff, static_cast<ObString::obstr_size_t>(pos));
        ret = execute_stmt_no_return_rows(update_max_user_id);
      }
    }
    if (OB_SUCCESS == ret)
    {
      ret = commit();
    }
    else
    {
      // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
      // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
      int err = rollback();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "rollback failed,ret=%d", err);
      }
    }
  }
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
