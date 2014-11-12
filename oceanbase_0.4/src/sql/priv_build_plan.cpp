/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * priv_build_plan.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "priv_build_plan.h"
#include "ob_logical_plan.h"
#include "parse_malloc.h"
#include "ob_create_user_stmt.h"
#include "ob_drop_user_stmt.h"
#include "ob_set_password_stmt.h"
#include "ob_rename_user_stmt.h"
#include "ob_lock_user_stmt.h"
#include "ob_grant_stmt.h"
#include "ob_schema_checker.h"
#include "ob_revoke_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

int resolve_create_user_stmt(ResultPlan* result_plan,
                             ParseNode* node,
                             uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_CREATE_USER && node->num_child_ > 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObCreateUserStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    for (int i = 0; i < node->num_child_; ++i)
    {
      ParseNode *user_pass = node->children_[i];
      OB_ASSERT(2 == user_pass->num_child_);
      ObString user = ObString::make_string(user_pass->children_[0]->str_value_);
      ObString password = ObString::make_string(user_pass->children_[1]->str_value_);
      if (OB_SUCCESS != (ret = stmt->add_user(user, password)))
      {
        PARSER_LOG("Failed to add user to CreateUserStmt");
        break;
      }
    } // end for
  }
  return ret;
}

int resolve_drop_user_stmt(ResultPlan* result_plan,
                           ParseNode* node,
                           uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_DROP_USER && node->num_child_ > 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObDropUserStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    for (int i = 0; i < node->num_child_; ++i)
    {
      ObString user = ObString::make_string(node->children_[i]->str_value_);
      if (OB_SUCCESS != (ret = stmt->add_user(user)))
      {
        PARSER_LOG("Failed to add user to DropUserStmt");
        break;
      }
    }
  }
  return ret;
}

int resolve_set_password_stmt(ResultPlan* result_plan,
                              ParseNode* node,
                              uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_SET_PASSWORD && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObSetPasswordStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObString user;
    if (NULL != node->children_[0])
    {
      user.assign_ptr(const_cast<char*>(node->children_[0]->str_value_),
                      static_cast<int32_t>(strlen(node->children_[0]->str_value_)));
    }
    ObString password = ObString::make_string(node->children_[1]->str_value_);
    if (OB_SUCCESS != (ret = stmt->set_user_password(user, password)))
    {
      PARSER_LOG("Failed to set UserPasswordStmt");
    }
  }
  return ret;
}

int resolve_rename_user_stmt(ResultPlan* result_plan,
                             ParseNode* node,
                             uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_RENAME_USER && node->num_child_ > 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObRenameUserStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    for (int i = 0; i < node->num_child_; ++i)
    {
      ParseNode *rename_info = node->children_[i];
      OB_ASSERT(2 == rename_info->num_child_ && T_RENAME_INFO == rename_info->type_);

      ObString from_user = ObString::make_string(rename_info->children_[0]->str_value_);
      ObString to_user = ObString::make_string(rename_info->children_[1]->str_value_);
      if (OB_SUCCESS != (ret = stmt->add_rename_info(from_user, to_user)))
      {
        PARSER_LOG("Failed to add user to RenameUserStmt");
        break;
      }
    } // end for
  }
  return ret;
}

int resolve_lock_user_stmt(ResultPlan* result_plan,
                           ParseNode* node,
                           uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_LOCK_USER && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObLockUserStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObString user = ObString::make_string(node->children_[0]->str_value_);
    bool locked = (0 != node->children_[1]->value_);
    if (OB_SUCCESS != (ret = stmt->set_lock_info(user, locked)))
    {
      PARSER_LOG("Failed to set lock info for LockUserStmt");
    }
  }
  return ret;
}

static int get_table_id(ResultPlan* result_plan, const ObString &table_name, uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != result_plan);
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  ObSchemaChecker* schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);

  if (logical_plan == NULL)
  {
    ret = OB_ERR_UNEXPECTED;
    PARSER_LOG("unexpected branch, logical_plan is NULL");
  }
  else if (NULL == schema_checker)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    PARSER_LOG("unexpected branch, schema_checker is NULL");
  }
  else if (OB_INVALID_ID == (table_id = schema_checker->get_table_id(table_name)))
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    PARSER_LOG("Table `%.*s' does not exist", table_name.length(), table_name.ptr());
  }
  return ret;
}

int resolve_grant_stmt(ResultPlan* result_plan,
                       ParseNode* node,
                       uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_GRANT && node->num_child_ == 3);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObGrantStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ParseNode *privileges_node = node->children_[0];
    ParseNode *priv_level = node->children_[1];
    ParseNode *users_node = node->children_[2];
    OB_ASSERT(NULL != privileges_node);
    OB_ASSERT(NULL != priv_level);
    OB_ASSERT(NULL != users_node);
    OB_ASSERT(privileges_node->num_child_ > 0);
    for (int i = 0; i < privileges_node->num_child_; ++i)
    {
      OB_ASSERT(T_PRIV_TYPE == privileges_node->children_[i]->type_);
      if (OB_SUCCESS != (ret = stmt->add_priv(static_cast<ObPrivilegeType>(privileges_node->children_[i]->value_))))
      {
        PARSER_LOG("Failed to add privilege");
        break;
      }
    }
    if (OB_SUCCESS == ret)
    {
      if (0 == priv_level->num_child_)
      {
        if (OB_SUCCESS != (ret = stmt->set_table_id(OB_NOT_EXIST_TABLE_TID)))
        {
          PARSER_LOG("Failed to set table id");
        }
      }
      else
      {
        OB_ASSERT(1 == priv_level->num_child_);
        // table name -> table id
        uint64_t table_id = OB_INVALID_ID;
        if (OB_SUCCESS != (ret = get_table_id(result_plan,
                                              ObString::make_string(priv_level->children_[0]->str_value_),
                                              table_id)))
        {
        }
        else
        {
          if (OB_SUCCESS != (ret = stmt->set_table_id(table_id)))
          {
            PARSER_LOG("Failed to set table id");
          }
        }
      }
    } // end if
    if (OB_SUCCESS == ret)
    {
      OB_ASSERT(users_node->num_child_ > 0);
      for (int i = 0; i < users_node->num_child_; ++i)
      {
        if (OB_SUCCESS != (ret = stmt->add_user(ObString::make_string(users_node->children_[i]->str_value_))))
        {
          PARSER_LOG("Failed to add user");
          break;
        }
      }
    }
  }
  return ret;
}

int resolve_revoke_stmt(ResultPlan* result_plan,
                        ParseNode* node,
                        uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_REVOKE && node->num_child_ == 3);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObRevokeStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ParseNode *privileges_node = node->children_[0];
    ParseNode *priv_level = node->children_[1];
    ParseNode *users_node = node->children_[2];
    OB_ASSERT(NULL != privileges_node);
    OB_ASSERT(NULL != users_node);
    OB_ASSERT(privileges_node->num_child_ > 0);
    for (int i = 0; i < privileges_node->num_child_; ++i)
    {
      OB_ASSERT(T_PRIV_TYPE == privileges_node->children_[i]->type_);
      if (OB_SUCCESS != (ret = stmt->add_priv(static_cast<ObPrivilegeType>(privileges_node->children_[i]->value_))))
      {
        PARSER_LOG("Failed to add privilege");
        break;
      }
    }
    if (OB_SUCCESS == ret)
    {
      uint64_t table_id = OB_INVALID_ID;
      if (NULL == priv_level)
      {
      }
      else if (0 == priv_level->num_child_)
      {
        table_id = OB_NOT_EXIST_TABLE_TID;
      }
      else
      {
        OB_ASSERT(1 == priv_level->num_child_);
        // table name -> table id
        if (OB_SUCCESS != (ret = get_table_id(result_plan,
                                              ObString::make_string(priv_level->children_[0]->str_value_),
                                              table_id)))
        {
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = stmt->set_table_id(table_id)))
        {
          PARSER_LOG("Failed to set table id");
        }
      }
    } // end if
    if (OB_SUCCESS == ret)
    {
      OB_ASSERT(users_node->num_child_ > 0);
      for (int i = 0; i < users_node->num_child_; ++i)
      {
        if (OB_SUCCESS != (ret = stmt->add_user(ObString::make_string(users_node->children_[i]->str_value_))))
        {
          PARSER_LOG("Failed to add user");
          break;
        }
      }
    }
  }
  return ret;
}
