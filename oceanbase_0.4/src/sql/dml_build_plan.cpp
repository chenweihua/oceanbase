/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * dml_build_plan.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "dml_build_plan.h"
#include "ob_raw_expr.h"
#include "common/ob_bit_set.h"
#include "ob_select_stmt.h"
#include "ob_multi_logic_plan.h"
#include "ob_insert_stmt.h"
#include "ob_delete_stmt.h"
#include "ob_update_stmt.h"
#include "ob_schema_checker.h"
#include "ob_type_convertor.h"
#include "ob_sql_session_info.h"
#include "parse_malloc.h"
#include "common/ob_define.h"
#include "common/ob_array.h"
#include "common/ob_string_buf.h"
#include "common/utility.h"
#include "common/ob_hint.h"
#include <stdint.h>

using namespace oceanbase::common;
using namespace oceanbase::sql;

extern const char* get_type_name(int type);

int resolve_expr(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    ObSqlRawExpr *sql_expr,
    ObRawExpr*& expr,
    int32_t expr_scope_type = T_NONE_LIMIT,
    bool sub_query_results_scalar = true);
int resolve_agg_func(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr);
int resolve_joined_table(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node,
    JoinedTable& joined_table);
int resolve_from_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_star(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_select_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_where_clause(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node);
int resolve_group_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_having_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_order_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_limit_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_for_update_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node);
int resolve_insert_columns(
    ResultPlan * result_plan,
    ObInsertStmt* insert_stmt,
    ParseNode* node);
int resolve_insert_values(
    ResultPlan * result_plan,
    ObInsertStmt* insert_stmt,
    ParseNode* node);
int resolve_hints(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node);
int resolve_when_clause(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node);
int resolve_when_func(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr);
ObSqlRawExpr* create_middle_sql_raw_expr(
    ResultPlan& result_plan,
    ParseNode& node,
    uint64_t& expr_id);

static int add_all_rowkey_columns_to_stmt(ResultPlan* result_plan, uint64_t table_id, ObStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* rowkey_column_schema = NULL;
  ObRowkeyInfo rowkey_info;
  int64_t rowkey_idx = 0;
  uint64_t rowkey_column_id = 0;


  if (NULL == stmt || NULL == result_plan)
  {
    TBSYS_LOG(WARN, "invalid argument. stmt=%p, result_plan=%p", stmt, result_plan);
    ret = OB_INVALID_ARGUMENT;
  }

  if (ret == OB_SUCCESS)
  {
    if (NULL == (schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_)))
    {
      ret = OB_ERR_SCHEMA_UNSET;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Schema(s) are not set");
    }
    else if (NULL == (table_schema = schema_checker->get_table_schema(table_id)))
    {
      ret = OB_ERR_TABLE_UNKNOWN;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Table schema not found");
    }
    else
    {
      // fill rowkey columns to statement, which must be returned in order to delete row
      rowkey_info = table_schema->get_rowkey_info();
      for (rowkey_idx = 0; rowkey_idx < rowkey_info.get_size(); rowkey_idx++)
      {
        if (OB_SUCCESS != (ret = rowkey_info.get_column_id(rowkey_idx, rowkey_column_id)))
        {
          TBSYS_LOG(WARN, "fail to get table %lu column %ld. ret=%d", table_id, rowkey_idx, ret);
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "BUG: Unexpected primary columns.");
          break;
        }
        else if (NULL == (rowkey_column_schema = schema_checker->get_column_schema(table_id, rowkey_column_id)))
        {
          ret = OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(WARN, "fail to get table %lu column %lu", table_id, rowkey_column_id);
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "BUG: Primary key column schema not found");
          break;
        }
        else
        {
          ObString column_name;
          column_name.assign(
              const_cast<char *>(rowkey_column_schema->get_name()),
              static_cast<ObString::obstr_size_t>(strlen(rowkey_column_schema->get_name())));
          ret = stmt->add_column_item(*result_plan, column_name);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fail to add column item '%s' to table %lu", rowkey_column_schema->get_name(), table_id);
            break;
          }
        }
      }/* end for */
    }
  }
  return ret;
}
int resolve_independ_expr(
  ResultPlan * result_plan,
  ObStmt* stmt,
  ParseNode* node,
  uint64_t& expr_id,
  int32_t expr_scope_type)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    ObRawExpr* expr = NULL;
    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    ObSqlRawExpr* sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr), result_plan->name_pool_);
    if (sql_expr == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
        "Can not malloc space for ObSqlRawExpr");
    }
    if (ret == OB_SUCCESS)
    {
      sql_expr = new(sql_expr) ObSqlRawExpr();
      ret = logical_plan->add_expr(sql_expr);
      if (ret != OB_SUCCESS)
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Add ObSqlRawExpr error");
    }
    if (ret == OB_SUCCESS)
    {
      expr_id = logical_plan->generate_expr_id();
      sql_expr->set_expr_id(expr_id);
      ret = resolve_expr(result_plan, stmt, node, sql_expr, expr, expr_scope_type);
    }
    if (ret == OB_SUCCESS)
    {
      if (expr->get_expr_type() == T_REF_COLUMN)
      {
        ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(expr);
        sql_expr->set_table_id(col_expr->get_first_ref_id());
        sql_expr->set_column_id(col_expr->get_second_ref_id());
      }
      else
      {
        sql_expr->set_table_id(OB_INVALID_ID);
        sql_expr->set_column_id(logical_plan->generate_column_id());
      }
      sql_expr->set_expr(expr);
    }
  }
  return ret;
}

int resolve_and_exprs(
  ResultPlan * result_plan,
  ObStmt* stmt,
  ParseNode* node,
  ObVector<uint64_t>& and_exprs,
  int32_t expr_scope_type)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    if (node->type_ != T_OP_AND)
    {
      uint64_t expr_id = OB_INVALID_ID;
      ret = resolve_independ_expr(result_plan, stmt, node, expr_id, expr_scope_type);
      if (ret == OB_SUCCESS)
      {
        ret = and_exprs.push_back(expr_id);
        if (ret != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Add 'AND' expression error");
      }
    }
    else
    {
      ret = resolve_and_exprs(result_plan, stmt, node->children_[0], and_exprs, expr_scope_type);
      if (ret == OB_SUCCESS)
        ret = resolve_and_exprs(result_plan, stmt, node->children_[1], and_exprs, expr_scope_type);
    }
  }
  return ret;
}

#define CREATE_RAW_EXPR(expr, type_name, result_plan)    \
({    \
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_); \
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);  \
  expr = (type_name*)parse_malloc(sizeof(type_name), name_pool);   \
  if (expr != NULL) \
  { \
    expr = new(expr) type_name();   \
    if (OB_SUCCESS != logical_plan->add_raw_expr(expr))    \
    { \
      expr = NULL;  /* no memory leak, bulk dealloc */ \
    } \
  } \
  if (expr == NULL)  \
  { \
    result_plan->err_stat_.err_code_ = OB_ERR_PARSER_MALLOC_FAILED; \
    TBSYS_LOG(WARN, "out of memory"); \
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,  \
        "Fail to malloc new raw expression"); \
  } \
  expr; \
})

int resolve_expr(
  ResultPlan * result_plan,
  ObStmt* stmt,
  ParseNode* node,
  ObSqlRawExpr *sql_expr,
  ObRawExpr*& expr,
  int32_t expr_scope_type,
  bool sub_query_results_scalar)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  expr = NULL;
  if (node == NULL)
    return ret;

  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);

  switch (node->type_)
  {
    case T_BINARY:
    {
      ObString str;
      ObString str_val;
      str_val.assign_ptr(const_cast<char*>(node->str_value_), static_cast<int32_t>(node->value_));
      if (OB_SUCCESS != (ret = ob_write_string(*name_pool, str_val, str)))
      {
        TBSYS_LOG(WARN, "out of memory");
        break;
      }
      ObObj val;
      val.set_varchar(str);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(node->type_);
      c_expr->set_result_type(ObVarcharType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_STRING:
    case T_SYSTEM_VARIABLE:
    case T_TEMP_VARIABLE:
    {
      ObString str;
      if (OB_SUCCESS != (ret = ob_write_string(*name_pool, ObString::make_string(node->str_value_), str)))
      {
        TBSYS_LOG(WARN, "out of memory");
        break;
      }
      ObObj val;
      val.set_varchar(str);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(node->type_);
      c_expr->set_result_type(ObVarcharType);
      c_expr->set_value(val);
      expr = c_expr;
      if (node->type_ == T_TEMP_VARIABLE)
      {
        TBSYS_LOG(INFO, "resolve tmp variable, name=%.*s", str.length(), str.ptr());
      }
      break;
    }
    case T_FLOAT:
    {
      ObObj val;
      val.set_float(static_cast<float>(atof(node->str_value_)));
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_FLOAT);
      c_expr->set_result_type(ObFloatType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_DOUBLE:
    {
      ObObj val;
      val.set_double(atof(node->str_value_));
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_DOUBLE);
      c_expr->set_result_type(ObDoubleType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_DECIMAL: // set as string
    {
      ObString str;
      if (OB_SUCCESS != (ret = ob_write_string(*name_pool, ObString::make_string(node->str_value_), str)))
      {
        TBSYS_LOG(WARN, "out of memory");
        break;
      }
      ObObj val;
      val.set_varchar(str);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_DECIMAL);
      c_expr->set_result_type(ObDecimalType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_INT:
    {
      ObObj val;
      val.set_int(node->value_);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_INT);
      c_expr->set_result_type(ObIntType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_BOOL:
    {
      ObObj val;
      val.set_bool(node->value_ == 1 ? true : false);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_BOOL);
      c_expr->set_result_type(ObBoolType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_DATE:
    {
      ObObj val;
      val.set_precise_datetime(node->value_);
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_DATE);
      c_expr->set_result_type(ObPreciseDateTimeType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_NULL:
    {
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_NULL);
      c_expr->set_result_type(ObNullType);
      expr = c_expr;
      break;
    }
    case T_QUESTIONMARK:
    {
      ObObj val;
      val.set_int(logical_plan->inc_question_mark());
      ObConstRawExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObConstRawExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_QUESTIONMARK);
      c_expr->set_result_type(ObIntType);
      c_expr->set_value(val);
      expr = c_expr;
      break;
    }
    case T_CUR_TIME_UPS:
      logical_plan->set_cur_time_fun_ups(); // same as T_CUR_TIME, except run cur time on ups
    case T_CUR_TIME:
    {
      logical_plan->set_cur_time_fun(); // do nothing if called after set_cur_time_fun_ups()
      ObCurTimeExpr *c_expr = NULL;
      if (CREATE_RAW_EXPR(c_expr, ObCurTimeExpr, result_plan) == NULL)
        break;
      c_expr->set_expr_type(T_CUR_TIME);
      c_expr->set_result_type(ObPreciseDateTimeType);
      expr = c_expr;
      break;
    }
    case T_OP_NAME_FIELD:
    {
      OB_ASSERT(node->children_[0]->type_ == T_IDENT);
      // star has been expand before
      // T_IDENT.* can't has alias name here, which is illeagal
      if (node->children_[1]->type_ != T_IDENT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "%s.* is illeagal", node->children_[0]->str_value_);
        break;
      }

      const char* table_str = node->children_[0]->str_value_;
      const char* column_str = node->children_[1]->str_value_;
      if (expr_scope_type == T_INSERT_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Illegal usage %s.%s", table_str, column_str);
        break;
      }
      else if (expr_scope_type == T_WHEN_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Column name %s.%s cannot be used in WHEN clause", table_str, column_str);
        break;
      }

      ObString table_name;
      ObString column_name;
      table_name.assign_ptr((char*)table_str, static_cast<int32_t>(strlen(table_str)));
      column_name.assign_ptr((char*)column_str, static_cast<int32_t>(strlen(column_str)));

      // Column name with table name, it can't be alias name, so we don't need to check select item list
      if (expr_scope_type == T_HAVING_LIMIT)
      {
        OB_ASSERT(stmt->get_stmt_type() == ObStmt::T_SELECT);
        ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
        TableItem* table_item;
        if ((select_stmt->get_table_item(table_name, &table_item)) == OB_INVALID_ID)
        {
          ret = OB_ERR_TABLE_UNKNOWN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Unknown table %s in having clause", table_str);
          break;
        }
        ret = select_stmt->check_having_ident(*result_plan, column_name, table_item, expr);
        // table_set is of no use in having clause, because all tables have been joined to one table
        // when having condition is calculated
        //sql_expr->get_tables_set().add_member(select_stmt->get_table_bit_index(table_item->table_id_));
      }
      else
      {
        ColumnItem *column_item = stmt->get_column_item(&table_name, column_name);
        if (!column_item)
        {
          ret = stmt->add_column_item(*result_plan, column_name, &table_name, &column_item);
          if (ret != OB_SUCCESS)
          {
            break;
          }
        }
        ObBinaryRefRawExpr *b_expr = NULL;
        if (CREATE_RAW_EXPR(b_expr, ObBinaryRefRawExpr, result_plan) == NULL)
          break;
        b_expr->set_expr_type(T_REF_COLUMN);
        b_expr->set_result_type(column_item->data_type_);
        b_expr->set_first_ref_id(column_item->table_id_);
        b_expr->set_second_ref_id(column_item->column_id_);
        expr = b_expr;
        sql_expr->get_tables_set().add_member(stmt->get_table_bit_index(column_item->table_id_));
      }
      break;
    }
    case T_IDENT:
    {
      if (expr_scope_type == T_INSERT_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unknown value %s", node->str_value_);
        break;
      }
      else if (expr_scope_type == T_VARIABLE_VALUE_LIMIT)
      {
        /* TBD */
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unknown value %s", node->str_value_);
        break;
      }
      else if (expr_scope_type == T_WHEN_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Column name %s cannot be used in WHEN clause", node->str_value_);
        break;
      }

      ObString column_name;
      column_name.assign_ptr(
          (char*)(node->str_value_),
          static_cast<int32_t>(strlen(node->str_value_))
          );

      if (expr_scope_type == T_HAVING_LIMIT)
      {
        OB_ASSERT(stmt->get_stmt_type() == ObStmt::T_SELECT);
        ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
        ret = select_stmt->check_having_ident(*result_plan, column_name, NULL, expr);
        // table_set is of no use in having clause, because all tables have been joined to one table
        // when having condition is calculated
        // sql_expr->get_tables_set().add_member(select_stmt->get_table_bit_index(table_item->table_id_));
      }
      else
      {
        // the checking rule is follow mysql, although not reasonable
        // 1. select user_id user_id, item_id user_id from order_list where user_id>0;
        //     syntax correct, you can try
        // 2. select item_id as user_id, user_id from order_list  where user_id>0;
        //     real order_list.user_id is used, so real column first.
        // 3. select item_id as user_id from order_list  where user_id>0;
        //     real order_list.user_id is used, so real column first.
        if (expr == NULL)
        {
          ColumnItem *column_item = stmt->get_column_item(NULL, column_name);
          if (column_item)
          {
            ObBinaryRefRawExpr *b_expr = NULL;
            if (CREATE_RAW_EXPR(b_expr, ObBinaryRefRawExpr, result_plan) == NULL)
              break;
            b_expr->set_expr_type(T_REF_COLUMN);
            b_expr->set_result_type(column_item->data_type_);
            b_expr->set_first_ref_id(column_item->table_id_);
            b_expr->set_second_ref_id(column_item->column_id_);
            expr = b_expr;
            sql_expr->get_tables_set().add_member(stmt->get_table_bit_index(column_item->table_id_));
          }
        }
        if (expr == NULL)
        {
          ColumnItem *column_item = NULL;
          ret = stmt->add_column_item(*result_plan, column_name, NULL, &column_item);
          if (ret == OB_SUCCESS)
          {
            ObBinaryRefRawExpr *b_expr = NULL;
            if (CREATE_RAW_EXPR(b_expr, ObBinaryRefRawExpr, result_plan) == NULL)
              break;
            b_expr->set_expr_type(T_REF_COLUMN);
            b_expr->set_result_type(column_item->data_type_);
            b_expr->set_first_ref_id(column_item->table_id_);
            b_expr->set_second_ref_id(column_item->column_id_);
            expr = b_expr;
            sql_expr->get_tables_set().add_member(stmt->get_table_bit_index(column_item->table_id_));
          }
          else if (ret == OB_ERR_COLUMN_UNKNOWN)
          {
            ret = OB_SUCCESS;
          }
          else
          {
            break;
          }
        }
        if (!expr && stmt->get_stmt_type() == ObStmt::T_SELECT)
        {
          ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
          uint64_t expr_id = select_stmt->get_alias_expr_id(column_name);
          if (expr_id != OB_INVALID_ID)
          {
            ObSqlRawExpr* alias_expr = logical_plan->get_expr(expr_id);
            if (alias_expr == NULL)
            {
              ret = OB_ERR_ILLEGAL_ID;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                  "Wrong expr_id %lu", expr_id);
              break;
            }
            if (alias_expr->is_contain_aggr()
              && (expr_scope_type == T_INSERT_LIMIT
              || expr_scope_type == T_UPDATE_LIMIT
              || expr_scope_type == T_AGG_LIMIT
              || expr_scope_type == T_WHERE_LIMIT
              || expr_scope_type == T_GROUP_LIMIT))
            {
              ret = OB_ERR_PARSER_SYNTAX;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                  "Invalid use of alias which contains group function");
              break;
            }
            else
            {
              ObBinaryRefRawExpr *b_expr = NULL;
              if (CREATE_RAW_EXPR(b_expr, ObBinaryRefRawExpr, result_plan) == NULL)
                break;
              b_expr->set_expr_type(T_REF_COLUMN);
              b_expr->set_result_type(alias_expr->get_result_type());
              b_expr->set_first_ref_id(alias_expr->get_table_id());
              b_expr->set_second_ref_id(alias_expr->get_column_id());
              expr = b_expr;
              sql_expr->get_tables_set().add_members(alias_expr->get_tables_set());
              sql_expr->set_contain_alias(true);
            }
          }
        }
        if (expr == NULL)
        {
          ret = OB_ERR_COLUMN_UNKNOWN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Unkown column name %.*s", column_name.length(), column_name.ptr());
        }
      }
      break;
    }
    case T_OP_EXISTS:
      if (expr_scope_type == T_INSERT_LIMIT || expr_scope_type == T_UPDATE)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "EXISTS expression can not appear in INSERT/UPDATE statement");
        break;
      }
    case T_OP_POS:
    case T_OP_NEG:
    case T_OP_NOT:
    {
      ObRawExpr* sub_expr = NULL;
      ret = resolve_expr(result_plan, stmt, node->children_[0], sql_expr, sub_expr, expr_scope_type, true);
      if (ret != OB_SUCCESS)
        break;
      if (node->type_ == T_OP_POS)
      {
        expr = sub_expr;
      }
      // T_OP_EXISTS can not has child of type T_OP_EXISTS/T_OP_POS/T_OP_NEG/T_OP_NOT,
      // so we can do this way
      else if (node->type_ == sub_expr->get_expr_type())
      {
        expr = (dynamic_cast<ObUnaryOpRawExpr*>(sub_expr))->get_op_expr();
      }
      // only INT/FLOAT/DOUBLE are in consideration
      else if (node->type_ == T_OP_NEG && sub_expr->is_const()
        && (sub_expr->get_expr_type() == T_INT
        || sub_expr->get_expr_type() == T_FLOAT
        || sub_expr->get_expr_type() == T_DOUBLE))
      {
        ObConstRawExpr *const_expr = dynamic_cast<ObConstRawExpr*>(sub_expr);
        if (const_expr == NULL)
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Wrong internal status of const expression");
          break;
        }
        switch (sub_expr->get_expr_type())
        {
          case T_INT:
          {
            int64_t val = OB_INVALID_ID;
            if ((ret = const_expr->get_value().get_int(val)) == OB_SUCCESS)
            {
              ObObj new_val;
              new_val.set_int(-val);
              const_expr->set_value(new_val);
            }
            break;
          }
          case T_FLOAT:
          {
            float val = static_cast<float>(OB_INVALID_ID);
            if ((ret = const_expr->get_value().get_float(val)) == OB_SUCCESS)
            {
              ObObj new_val;
              new_val.set_float(-val);
              const_expr->set_value(new_val);
            }
            break;
          }
          case T_DOUBLE:
          {
            double val = static_cast<double>(OB_INVALID_ID);
            if ((ret = const_expr->get_value().get_double(val)) == OB_SUCCESS)
            {
              ObObj new_val;
              new_val.set_double(-val);
              const_expr->set_value(new_val);
            }
            break;
          }
          default:
          {
            /* won't be here */
            ret = OB_ERR_PARSER_SYNTAX;
          }
        }
        if (ret == OB_SUCCESS)
        {
          expr = sub_expr;
        }
        else
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Wrong internal status of const expression");
          break;
        }
      }
      else
      {
        ObUnaryOpRawExpr *u_expr = NULL;
        if (CREATE_RAW_EXPR(u_expr, ObUnaryOpRawExpr, result_plan) == NULL)
          break;
        u_expr->set_expr_type(node->type_);
        if (node->type_ == T_OP_POS)
        {
          u_expr->set_result_type(sub_expr->get_result_type());
        }
        else if (node->type_ == T_OP_NEG)
        {
          ObObj in_type;
          in_type.set_type(sub_expr->get_result_type());
          u_expr->set_result_type(ObExprObj::type_negate(in_type).get_type());
        }
        else if (node->type_ == T_OP_EXISTS || node->type_ == T_OP_NOT)
        {
          u_expr->set_result_type(ObBoolType);
        }
        else
        {
          /* won't be here */
          u_expr->set_result_type(ObMinType);
        }
        u_expr->set_op_expr(sub_expr);
        expr = u_expr;
      }
      break;
    }
    case T_OP_ADD:
    case T_OP_MINUS:
    case T_OP_MUL:
    case T_OP_DIV:
    case T_OP_REM:
    case T_OP_POW:
    case T_OP_MOD:
    case T_OP_LE:
    case T_OP_LT:
    case T_OP_EQ:
    case T_OP_GE:
    case T_OP_GT:
    case T_OP_NE:
    case T_OP_LIKE:
    case T_OP_NOT_LIKE:
    case T_OP_AND:
    case T_OP_OR:
    case T_OP_IS:
    case T_OP_IS_NOT:
    case T_OP_CNN:
    {
      ObRawExpr* sub_expr1 = NULL;
      ret = resolve_expr(result_plan, stmt, node->children_[0], sql_expr, sub_expr1, expr_scope_type, true);
      if (ret != OB_SUCCESS)
        break;
      ObRawExpr* sub_expr2 = NULL;
      ret = resolve_expr(result_plan, stmt, node->children_[1], sql_expr, sub_expr2, expr_scope_type, true);
      if (ret != OB_SUCCESS)
        break;
      ObBinaryOpRawExpr *b_expr = NULL;
      if (CREATE_RAW_EXPR(b_expr, ObBinaryOpRawExpr, result_plan) == NULL)
        break;
      b_expr->set_expr_type(node->type_);
      ObObj in_type1;
      in_type1.set_type(sub_expr1->get_result_type());
      ObObj in_type2;
      in_type2.set_type(sub_expr2->get_result_type());
      if (node->type_ == T_OP_ADD)
      {
        b_expr->set_result_type(ObExprObj::type_add(in_type1, in_type2).get_type());
      }
      else if (node->type_ == T_OP_MINUS)
      {
        b_expr->set_result_type(ObExprObj::type_sub(in_type1, in_type2).get_type());
      }
      else if (node->type_ == T_OP_MUL)
      {
        b_expr->set_result_type(ObExprObj::type_mul(in_type1, in_type2).get_type());
      }
      else if (node->type_ == T_OP_DIV)
      {
        if (in_type1.get_type() == ObDoubleType || in_type2.get_type() == ObDoubleType)
          b_expr->set_result_type(ObExprObj::type_div(in_type1, in_type2, true).get_type());
        else
          b_expr->set_result_type(ObExprObj::type_div(in_type1, in_type2, false).get_type());
      }
      else if (node->type_ == T_OP_REM || node->type_ == T_OP_MOD)
      {
        b_expr->set_result_type(ObExprObj::type_mod(in_type1, in_type2).get_type());
      }
      else if (node->type_ == T_OP_POW)
      {
        b_expr->set_result_type(sub_expr1->get_result_type());
      }
      else if (node->type_ == T_OP_LE || node->type_ == T_OP_LT || node->type_ == T_OP_EQ
        || node->type_ == T_OP_GE || node->type_ == T_OP_GT || node->type_ == T_OP_NE
        || node->type_ == T_OP_LIKE || node->type_ == T_OP_NOT_LIKE || node->type_ == T_OP_AND
        || node->type_ == T_OP_OR || node->type_ == T_OP_IS || node->type_ == T_OP_IS_NOT)
      {
        b_expr->set_result_type(ObBoolType);
      }
      else if (node->type_ == T_OP_CNN)
      {
        b_expr->set_result_type(ObVarcharType);
      }
      else
      {
        /* won't be here */
        b_expr->set_result_type(ObMinType);
      }
      b_expr->set_op_exprs(sub_expr1, sub_expr2);
      expr = b_expr;
      break;
    }
    case T_OP_BTW:
      /* pass through */
    case T_OP_NOT_BTW:
    {
      ObRawExpr* sub_expr1 = NULL;
      ObRawExpr* sub_expr2 = NULL;
      ObRawExpr* sub_expr3 = NULL;
      ret = resolve_expr(result_plan, stmt, node->children_[0], sql_expr, sub_expr1, expr_scope_type);
      if (ret != OB_SUCCESS)
        break;
      ret = resolve_expr(result_plan, stmt, node->children_[1], sql_expr, sub_expr2, expr_scope_type);
      if (ret != OB_SUCCESS)
        break;
      ret = resolve_expr(result_plan, stmt, node->children_[2], sql_expr, sub_expr3, expr_scope_type);
      if (ret != OB_SUCCESS)
        break;

      ObTripleOpRawExpr *t_expr = NULL;
      if (CREATE_RAW_EXPR(t_expr, ObTripleOpRawExpr, result_plan) == NULL)
        break;
      t_expr->set_expr_type(node->type_);
      t_expr->set_result_type(ObBoolType);
      t_expr->set_op_exprs(sub_expr1, sub_expr2, sub_expr3);
      expr = t_expr;
      break;
    }
    case T_OP_IN:
      // get through
    case T_OP_NOT_IN:
    {
      ObRawExpr* sub_expr1 = NULL;
      if (node->children_[0]->type_ == T_SELECT)
        ret = resolve_expr(
                  result_plan,
                  stmt,
                  node->children_[0],
                  sql_expr, sub_expr1,
                  expr_scope_type,
                  false
                  );
      else
        ret = resolve_expr(
                  result_plan,
                  stmt,
                  node->children_[0],
                  sql_expr,
                  sub_expr1,
                  expr_scope_type,
                  true);
      if (ret != OB_SUCCESS)
        break;
      ObRawExpr* sub_expr2 = NULL;
      ret = resolve_expr(
               result_plan,
               stmt,
               node->children_[1],
               sql_expr,
               sub_expr2,
               expr_scope_type,
               false
               );
      if (ret != OB_SUCCESS)
        break;
      ObBinaryOpRawExpr *in_expr = NULL;
      if (CREATE_RAW_EXPR(in_expr, ObBinaryOpRawExpr, result_plan) == NULL)
        break;
      in_expr->set_expr_type(node->type_ == T_OP_IN ? T_OP_IN : T_OP_NOT_IN);
      in_expr->set_result_type(ObBoolType);
      in_expr->set_op_exprs(sub_expr1, sub_expr2);

      /* 1. get the the column num of left operand */
      int32_t num_left_param = 1;
      switch (in_expr->get_first_op_expr()->get_expr_type())
      {
        case T_OP_ROW :
        {
          ObMultiOpRawExpr *left_expr = dynamic_cast<ObMultiOpRawExpr *>(in_expr->get_first_op_expr());
          num_left_param = left_expr->get_expr_size();
          break;
        }
        case T_REF_QUERY :
        {
          ObUnaryRefRawExpr *left_expr = dynamic_cast<ObUnaryRefRawExpr *>(in_expr->get_first_op_expr());
          ObSelectStmt *sub_select = dynamic_cast<ObSelectStmt *>(logical_plan->get_query(left_expr->get_ref_id()));
          if (!sub_select)
          {
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Sub-query of In operator is not select statment");
            break;
          }
          num_left_param = sub_select->get_select_item_size();
          break;
        }
        default:
          num_left_param = 1;
          break;
      }

      /* 2. get the the column num of right operand(s) */
      int32_t num_right_param = 0;
      switch (in_expr->get_second_op_expr()->get_expr_type())
      {
        case T_OP_ROW:
        {
          ObMultiOpRawExpr *row_expr = dynamic_cast<ObMultiOpRawExpr *>(in_expr->get_second_op_expr());
          int32_t num = row_expr->get_expr_size();
          ObRawExpr *sub_expr = NULL;
          for (int32_t i = 0; i < num; i++)
          {
            sub_expr = row_expr->get_op_expr(i);
            switch (sub_expr->get_expr_type())
            {
              case T_OP_ROW:
              {
                num_right_param = (dynamic_cast<ObMultiOpRawExpr *>(sub_expr))->get_expr_size();
                break;
              }
              case T_REF_QUERY:
              {
                uint64_t query_id = (dynamic_cast<ObUnaryRefRawExpr *>(sub_expr))->get_ref_id();
                ObSelectStmt *sub_query = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(query_id));
                if (sub_query)
                  num_right_param = sub_query->get_select_item_size();
                else
                  num_right_param = 0;
                break;
              }
              default:
                num_right_param = 1;
                break;
            }
            if (num_left_param != num_right_param)
            {
              break;
            }
          }
          break;
        }
        case T_REF_QUERY:
        {
          uint64_t query_id = (dynamic_cast<ObUnaryRefRawExpr *>(in_expr->get_second_op_expr()))->get_ref_id();
          ObSelectStmt *sub_query = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(query_id));
          if (sub_query)
            num_right_param = sub_query->get_select_item_size();
          else
            num_right_param = 0;
          break;
        }
        default:
          /* won't be here */
          OB_ASSERT(0);
          break;
      }

      /* 3. to check if the nums of two sides are equal */
      if (num_left_param != num_right_param)
      {
        ret = OB_ERR_COLUMN_SIZE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "In operands contain different column(s)");
        break;
      }

      expr = in_expr;
      break;
    }
    case T_CASE:
    {
      ObCaseOpRawExpr *case_expr = NULL;
      ObObjType tmp_type = ObMinType;
      if (CREATE_RAW_EXPR(case_expr, ObCaseOpRawExpr, result_plan) == NULL)
        break;
      if (node->children_[0])
      {
        ObRawExpr *arg_expr = NULL;
        ret = resolve_expr(result_plan, stmt, node->children_[0], sql_expr, arg_expr, expr_scope_type);
        if(ret != OB_SUCCESS)
        {
          break;
        }
        case_expr->set_arg_op_expr(arg_expr);
        case_expr->set_expr_type(T_OP_ARG_CASE);
      }
      else
      {
        case_expr->set_expr_type(T_OP_CASE);
      }

      OB_ASSERT(node->children_[1]->type_ == T_WHEN_LIST);
      ParseNode *when_node;
      ObRawExpr   *when_expr = NULL;
      ObRawExpr   *then_expr = NULL;
      for (int32_t i = 0; ret == OB_SUCCESS && i < node->children_[1]->num_child_; i++)
      {
        when_node = node->children_[1]->children_[i];
        ret = resolve_expr(result_plan, stmt, when_node->children_[0], sql_expr, when_expr, expr_scope_type);
        if(ret != OB_SUCCESS)
        {
          break;
        }
        ret = resolve_expr(result_plan, stmt, when_node->children_[1], sql_expr, then_expr, expr_scope_type);
        if(ret != OB_SUCCESS)
        {
          break;
        }
        ret = case_expr->add_when_op_expr(when_expr);
        if (ret != OB_SUCCESS)
        {
          break;
        }
        ret = case_expr->add_then_op_expr(then_expr);
        if (ret != OB_SUCCESS)
        {
          break;
        }
        const ObObjType then_type = then_expr->get_result_type();
        if (then_type == ObNullType)
        {
          continue;
        }
        else if (then_type > ObMinType && then_type < ObMaxType
          && (then_type == tmp_type || tmp_type == ObMinType))
        {
          tmp_type = then_type;
        }
        else
        {
          ret = OB_ERR_ILLEGAL_TYPE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Return types of then clause are not compatible");
          break;
        }
      }
      if (ret != OB_SUCCESS)
      {
        break;
      }
      // @bug FIXME
      if (tmp_type == ObMinType) tmp_type = ObVarcharType;

      case_expr->set_result_type(tmp_type);
      if (case_expr->get_when_expr_size() != case_expr->get_then_expr_size())
      {
        ret = OB_ERR_COLUMN_SIZE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Error size of when expressions");
        break;
      }
      if (node->children_[2])
      {
        ObRawExpr *default_expr = NULL;
        ret = resolve_expr(result_plan, stmt, node->children_[2], sql_expr, default_expr, expr_scope_type);
        if (ret != OB_SUCCESS)
        {
          break;
        }
        case_expr->set_default_op_expr(default_expr);
      }
      expr = case_expr;
      break;
    }
    case T_EXPR_LIST:
    {
      ObMultiOpRawExpr *multi_expr = NULL;
      if (CREATE_RAW_EXPR(multi_expr, ObMultiOpRawExpr, result_plan) == NULL)
        break;
      multi_expr->set_expr_type(T_OP_ROW);
      // not mathematic expression, result type is of no use.
      // should be ObRowType
      multi_expr->set_result_type(ObMinType);

      ObRawExpr *sub_query = NULL;
      uint64_t num = node->num_child_;
      for (uint64_t i = 0; ret == OB_SUCCESS && i < num; i++)
      {
        if (node->children_[i]->type_ == T_SELECT && !sub_query_results_scalar)
          ret = resolve_expr(
              result_plan,
              stmt,
              node->children_[i],
              sql_expr,
              sub_query,
              expr_scope_type,
              false);
        else
          ret = resolve_expr(
              result_plan,
              stmt,
              node->children_[i],
              sql_expr,
              sub_query,
              expr_scope_type,
              true);
        if (ret != OB_SUCCESS)
        {
          break;
        }
        ret = multi_expr->add_op_expr(sub_query);
      }
      if (ret == OB_SUCCESS)
        expr = multi_expr;
      break;
    }
    case T_SELECT:
    {
      if (expr_scope_type == T_INSERT_LIMIT
        || expr_scope_type == T_UPDATE_LIMIT
        || expr_scope_type == T_AGG_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Sub-query is illeagal in INSERT/UPDATE statement or AGGREGATION function");
        break;
      }

      uint64_t query_id = OB_INVALID_ID;
      if ((ret = resolve_select_stmt(result_plan, node, query_id)) != OB_SUCCESS)
        break;
      if (sub_query_results_scalar)
      {
        ObBasicStmt *sub_stmt = logical_plan->get_query(query_id);
        ObSelectStmt *sub_select = dynamic_cast<ObSelectStmt*>(sub_stmt);
        if (sub_select->get_select_item_size() != 1)
        {
          ret = OB_ERR_COLUMN_SIZE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Operand should contain 1 column(s)");
          break;
        }
      }
      ObUnaryRefRawExpr *sub_query_expr = NULL;
      if (CREATE_RAW_EXPR(sub_query_expr, ObUnaryRefRawExpr, result_plan) == NULL)
        break;
      sub_query_expr->set_expr_type(T_REF_QUERY);
      // not mathematic expression, result type is of no use.
      // should be ObRowType
      sub_query_expr->set_result_type(ObMinType);
      sub_query_expr->set_ref_id(query_id);
      expr = sub_query_expr;
      break;
    }
    case T_INSERT:
    case T_DELETE:
    case T_UPDATE:
    {
      uint64_t query_id = OB_INVALID_ID;
      ObUnaryRefRawExpr *sub_query_expr = NULL;
      if (expr_scope_type != T_WHEN_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "INSERT/DELTE/UPDATE statement is illeagal when out of when function");
        break;
      }
      switch (node->type_)
      {
        case T_INSERT:
          ret = resolve_insert_stmt(result_plan, node, query_id);
          break;
        case T_DELETE:
          ret = resolve_delete_stmt(result_plan, node, query_id);
          break;
        case T_UPDATE:
          ret = resolve_update_stmt(result_plan, node, query_id);
          break;
        default:
          ret = OB_ERR_ILLEGAL_TYPE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unknown statement type in when function");
          break;
      }
      if (ret != OB_SUCCESS)
      {
        break;
      }
      else if (CREATE_RAW_EXPR(sub_query_expr, ObUnaryRefRawExpr, result_plan) == NULL)
      {
        break;
      }
      else
      {
        sub_query_expr->set_expr_type(T_REF_QUERY);
        // not mathematic expression, result type is of no use.
        // should be ObRowType
        sub_query_expr->set_result_type(ObMinType);
        sub_query_expr->set_ref_id(query_id);
        expr = sub_query_expr;
      }
      break;
    }
    case T_FUN_COUNT:
    case T_FUN_MAX:
    case T_FUN_MIN:
    case T_FUN_SUM:
    case T_FUN_AVG:
    {
      if (expr_scope_type == T_INSERT_LIMIT
        || expr_scope_type == T_UPDATE_LIMIT
        || expr_scope_type == T_AGG_LIMIT
        || expr_scope_type == T_WHERE_LIMIT
        || expr_scope_type == T_GROUP_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Invalid use of group function");
        break;
      }
      ObSelectStmt* select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
      ObSqlRawExpr *ret_sql_expr = NULL;
      if ((ret = resolve_agg_func(result_plan, select_stmt, node, ret_sql_expr)) != OB_SUCCESS)
        break;
      ObBinaryRefRawExpr *col_expr = NULL;
      if (CREATE_RAW_EXPR(col_expr, ObBinaryRefRawExpr, result_plan) == NULL)
        break;
      col_expr->set_expr_type(T_REF_COLUMN);
      col_expr->set_result_type(ret_sql_expr->get_result_type());

      col_expr->set_first_ref_id(ret_sql_expr->get_table_id());
      col_expr->set_second_ref_id(ret_sql_expr->get_column_id());
      // add invalid table bit index, avoid aggregate function expressions are used as filter
      sql_expr->get_tables_set().add_member(0);
      sql_expr->set_contain_aggr(true);
      expr = col_expr;
      break;
    }
    case T_FUN_SYS:
    {
      ObSysFunRawExpr *func_expr = NULL;
      if (CREATE_RAW_EXPR(func_expr, ObSysFunRawExpr, result_plan) == NULL)
        break;
      func_expr->set_expr_type(T_FUN_SYS);
      ObString func_name;
      ret = ob_write_string(*logical_plan->get_name_pool(), ObString::make_string(node->children_[0]->str_value_), func_name);
      if (ret != OB_SUCCESS)
      {
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        TBSYS_LOG(WARN, "out of memory");
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Malloc function name failed");
        break;
      }
      func_expr->set_func_name(func_name);
      if (node->num_child_ > 1)
      {
        OB_ASSERT(node->children_[1]->type_ == T_EXPR_LIST);
        ObRawExpr *para_expr = NULL;
        int32_t num = node->children_[1]->num_child_;
        for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
        {
          ret = resolve_expr(
                    result_plan,
                    stmt,
                    node->children_[1]->children_[i],
                    sql_expr,
                    para_expr);
          if (ret != OB_SUCCESS)
            break;
          if (OB_SUCCESS != (ret = func_expr->add_param_expr(para_expr)))
            break;
        }
      }
      if (ret == OB_SUCCESS)
      {
        int32_t param_num = 0;
        if ((ret = oceanbase::sql::ObPostfixExpression::get_sys_func_param_num(func_name, param_num)) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown function '%.*s', ret=%d", func_name.length(), func_name.ptr(), ret);
        }
        else
        {
          switch(param_num)
          {
            case TWO_OR_THREE:
            {
              if (func_expr->get_param_size() < 2 || func_expr->get_param_size() > 3)
              {
                ret = OB_ERR_PARAM_SIZE;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Param num of function '%.*s' can not be less than 2 or more than 3, ret=%d",
                   func_name.length(), func_name.ptr(), ret);
              }
              break;
            }
            case OCCUR_AS_PAIR:
            {
              /* Won't be here */
              /* No function of this type now */
              ret = OB_ERR_PARAM_SIZE;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Wrong num of function param(s), function='%.*s', num=%d, ret=%d",
                   func_name.length(), func_name.ptr(), OCCUR_AS_PAIR, ret);
              break;
            }
            case MORE_THAN_ZERO:
            {
              if (func_expr->get_param_size() <= 0)
              {
                ret = OB_ERR_PARAM_SIZE;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Param num of function '%.*s' can not be empty, ret=%d", func_name.length(), func_name.ptr(), ret);
              }
              break;
            }
            default:
            {
              if (func_expr->get_param_size() != param_num)
              {
                ret = OB_ERR_PARAM_SIZE;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Param num of function '%.*s' must be %d, ret=%d", func_name.length(), func_name.ptr(), param_num, ret);
              }
              break;
            }
          }
        }
      }
      if (ret == OB_SUCCESS)
      {
        if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_LENGTH), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObIntType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_SUBSTR), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_CAST), func_name.ptr(), func_name.length()))
        {
          int32_t num = node->children_[1]->num_child_;
          if (num == 2)
          {
            ObObj obj;
            int64_t item_type;
            ObConstRawExpr *param_expr = dynamic_cast<ObConstRawExpr *>(func_expr->get_param_expr(1));
            if (NULL != param_expr)
            {
              obj = param_expr->get_value();
              if (OB_SUCCESS == (ret = obj.get_int(item_type)))
              {
                ObObjType dest_type = convert_item_type_to_obj_type(static_cast<ObItemType>(item_type));
                func_expr->set_result_type(dest_type);
              }
              else
              {
                TBSYS_LOG(WARN, "fail to get int val. obj.get_type()=%d", obj.get_type());
                break;
              }
            }
            else
            {
              TBSYS_LOG(WARN, "fail to get param expression");
              break;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "CAST function must only take 2 params");
          }
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_CUR_USER), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_TRIM), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_LOWER), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_UPPER), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_COALESCE), func_name.ptr(), func_name.length()))
        {
          // always cast to varchar as it is an all-mighty type
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_GREATEST), func_name.ptr(), func_name.length()))
        {
          // always cast to varchar as it is an all-mighty type
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_LEAST), func_name.ptr(), func_name.length()))
        {
          // always cast to varchar as it is an all-mighty type
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_HEX), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_UNHEX), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_IP_TO_INT), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObIntType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_INT_TO_IP), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_GREATEST), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else if (0 == strncasecmp(oceanbase::sql::ObPostfixExpression::get_sys_func_name(SYS_FUNC_LEAST), func_name.ptr(), func_name.length()))
        {
          func_expr->set_result_type(ObVarcharType);
        }
        else
        {
          ret = OB_ERR_UNKNOWN_SYS_FUNC;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "system function `%.*s' not supported", func_name.length(), func_name.ptr());
        }
      }
      if (ret == OB_SUCCESS)
      {
        expr = func_expr;
      }
      break;
    }
    case T_ROW_COUNT:
    {
      if (expr_scope_type != T_WHEN_LIMIT)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Invalid use of row_count function, it can be in WHEN clause only");
        break;
      }
      ObSqlRawExpr *ret_sql_expr = NULL;
      ObBinaryRefRawExpr *col_expr = NULL;
      if ((ret = resolve_when_func(result_plan, stmt, node, ret_sql_expr)) != OB_SUCCESS)
      {
        break;
      }
      else if (CREATE_RAW_EXPR(col_expr, ObBinaryRefRawExpr, result_plan) == NULL)
      {
        break;
      }
      col_expr->set_expr_type(T_REF_COLUMN);
      col_expr->set_result_type(ObIntType);
      col_expr->set_first_ref_id(ret_sql_expr->get_table_id());
      col_expr->set_second_ref_id(ret_sql_expr->get_column_id());
      expr = col_expr;
      break;
    }
    default:
      ret = OB_ERR_PARSER_SYNTAX;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Wrong type in expression");
      break;
  }

  return ret;
}

ObSqlRawExpr* create_middle_sql_raw_expr(
    ResultPlan& result_plan,
    ParseNode& node,
    uint64_t& expr_id)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSqlRawExpr* sql_expr = NULL;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan.plan_tree_);

  if (logical_plan == NULL)
  {
    TBSYS_LOG(WARN, "Logical Plan is empty");
  }
  else if ((sql_expr = (ObSqlRawExpr*)parse_malloc(
                            sizeof(ObSqlRawExpr),
                            result_plan.name_pool_)) == NULL)
  {
    ret = OB_ERR_PARSER_MALLOC_FAILED;
    TBSYS_LOG(WARN, "out of memory");
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "Can not malloc space for ObSqlRawExpr");
  }
  else
  {
    sql_expr = new(sql_expr) ObSqlRawExpr();
    if ((ret = logical_plan->add_expr(sql_expr)) != OB_SUCCESS)
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
               "Add ObSqlRawExpr error");
    }
  }
  if (ret == OB_SUCCESS)
  {
    expr_id = logical_plan->generate_expr_id();
    sql_expr->set_expr_id(expr_id);
    sql_expr->set_table_id(OB_INVALID_ID);
    switch(node.type_)
    {
      case T_FUN_COUNT:
      case T_FUN_MAX:
      case T_FUN_MIN:
      case T_FUN_SUM:
      case T_FUN_AVG:
        sql_expr->set_column_id(logical_plan->generate_range_column_id());
        break;
      case T_ROW_COUNT:
        sql_expr->set_column_id(logical_plan->generate_column_id());
        break;
      default:
        ret = OB_ERR_ILLEGAL_TYPE;
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
               "Unknown function type");
        break;
    }
  }
  if (ret != OB_SUCCESS)
  {
    sql_expr = NULL;
  }
  return sql_expr;
}

int resolve_when_func(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t expr_id = OB_INVALID_ID;
  ObSqlRawExpr* sql_expr = NULL;
  if (node != NULL)
  {
    if ((sql_expr = create_middle_sql_raw_expr(
                        *result_plan,
                        *node,
                        expr_id)) == NULL)
    {
      TBSYS_LOG(WARN, "Create middle sql raw expr failed");
    }
    else if ((ret = stmt->add_when_func(expr_id)) != OB_SUCCESS)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Add when function error");
    }
    else
    {
      ObRawExpr *sub_expr = NULL;
      ObUnaryOpRawExpr *row_count_expr = NULL;
      if ((ret = resolve_expr(
                    result_plan,
                    stmt,
                    node->children_[0],
                    sql_expr,
                    sub_expr,
                    T_WHEN_LIMIT,
                    false)) != OB_SUCCESS)
      {
      }
      else if (CREATE_RAW_EXPR(row_count_expr, ObUnaryOpRawExpr, result_plan) == NULL)
      {
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        TBSYS_LOG(WARN, "Create row_count expression failed");
      }
      else
      {
        row_count_expr->set_expr_type(node->type_);
        row_count_expr->set_result_type(ObIntType);
        row_count_expr->set_op_expr(sub_expr);
        sql_expr->set_expr(row_count_expr);
        ret_sql_expr = sql_expr;
      }
    }
  }
  else
  {
    ret = OB_ERR_PARSER_SYNTAX;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
        "Wrong usage of When function");
  }
  return ret;
}

int resolve_agg_func(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node,
    ObSqlRawExpr*& ret_sql_expr)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t expr_id = OB_INVALID_ID;
  ObSqlRawExpr* sql_expr = NULL;
  if (node != NULL)
  {
    if ((sql_expr = create_middle_sql_raw_expr(
                        *result_plan,
                        *node,
                        expr_id)) == NULL)
    {
      TBSYS_LOG(WARN, "Create middle sql raw expr failed");
    }
    else if ((ret = select_stmt->add_agg_func(expr_id)) != OB_SUCCESS)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Add aggregate function error");
    }

    // When '*', do not set parameter
    ObRawExpr* sub_expr = NULL;
    if (ret == OB_SUCCESS)
    {
      if (node->type_ != T_FUN_COUNT || node->num_child_ > 1)
        ret = resolve_expr(result_plan, select_stmt, node->children_[1], sql_expr, sub_expr, T_AGG_LIMIT);
    }

    ObAggFunRawExpr *agg_expr = NULL;
    if (ret == OB_SUCCESS && CREATE_RAW_EXPR(agg_expr, ObAggFunRawExpr, result_plan) != NULL)
    {
      agg_expr->set_param_expr(sub_expr);
      if (node->children_[0] && node->children_[0]->type_ == T_DISTINCT)
        agg_expr->set_param_distinct();
      agg_expr->set_expr_type(node->type_);
      if (node->type_ == T_FUN_COUNT)
        agg_expr->set_expr_type(T_FUN_COUNT);
      else if (node->type_ == T_FUN_MAX)
        agg_expr->set_expr_type(T_FUN_MAX);
      else if (node->type_ == T_FUN_MIN)
        agg_expr->set_expr_type(T_FUN_MIN);
      else if (node->type_ == T_FUN_SUM)
        agg_expr->set_expr_type(T_FUN_SUM);
      else if (node->type_ == T_FUN_AVG)
        agg_expr->set_expr_type(T_FUN_AVG);
      else
      {
        /* Won't be here */

      }
      if (node->type_ == T_FUN_COUNT)
      {
        agg_expr->set_result_type(ObIntType);
      }
      else if (node->type_ == T_FUN_MAX || node->type_ == T_FUN_MIN || node->type_ == T_FUN_SUM)
      {
        agg_expr->set_result_type(sub_expr->get_result_type());
      }
      else if (node->type_ == T_FUN_AVG)
      {
        ObObj in_type1;
        ObObj in_type2;
        in_type1.set_type(sub_expr->get_result_type());
        in_type2.set_type(ObIntType);
        if (in_type1.get_type() == ObDoubleType)
          agg_expr->set_result_type(ObExprObj::type_div(in_type1, in_type2, true).get_type());
        else
          agg_expr->set_result_type(ObExprObj::type_div(in_type1, in_type2, false).get_type());
      }
      else
      {
        /* won't be here */
        agg_expr->set_result_type(ObMinType);
        OB_ASSERT(false);
      }

      sql_expr->set_expr(agg_expr);
      sql_expr->set_contain_aggr(true);
      // add invalid table bit index, avoid aggregate function expressions are used as filters
      sql_expr->get_tables_set().add_member(0);
    }
  }
  else
  {
    ret = OB_ERR_PARSER_SYNTAX;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
        "Wrong usage of aggregate function");
  }

  if (ret == OB_SUCCESS)
    ret_sql_expr = sql_expr;
  return ret;
}

int resolve_joined_table(
  ResultPlan * result_plan,
  ObSelectStmt* select_stmt,
  ParseNode* node,
  JoinedTable& joined_table)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node->type_ == T_JOINED_TABLE);

  uint64_t tid = OB_INVALID_ID;
  uint64_t expr_id = OB_INVALID_ID;
  ParseNode* table_node = NULL;

  /* resolve table */
  for (uint64_t i = 1; ret == OB_SUCCESS && i < 3; i++)
  {
    table_node = node->children_[i];
    switch (table_node->type_)
    {
      case T_IDENT:
      case T_SELECT:
      case T_ALIAS:
        ret = resolve_table(result_plan, select_stmt, table_node, tid);
        if (ret == OB_SUCCESS && (ret = joined_table.add_table_id(tid)) != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Add table_id to outer joined table failed");
        break;
      case T_JOINED_TABLE:
        ret = resolve_joined_table(result_plan, select_stmt, table_node, joined_table);
        break;
      default:
        /* won't be here */
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unknown table type in outer join");
        break;
    }
  }

  /* resolve join type */
  if (ret == OB_SUCCESS)
  {
    switch (node->children_[0]->type_)
    {
      case T_JOIN_FULL:
        ret = joined_table.add_join_type(JoinedTable::T_FULL);
        break;
      case T_JOIN_LEFT:
        ret = joined_table.add_join_type(JoinedTable::T_LEFT);
        break;
      case T_JOIN_RIGHT:
        ret = joined_table.add_join_type(JoinedTable::T_RIGHT);
        break;
      case T_JOIN_INNER:
        ret = joined_table.add_join_type(JoinedTable::T_INNER);
        break;
      default:
        /* won't be here */
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unknown outer join type");
        break;
    }
  }

  /* resolve expression */
  if (ret == OB_SUCCESS)
  {
    ret = resolve_independ_expr(result_plan, select_stmt, node->children_[3], expr_id);
  }
  if (ret == OB_SUCCESS)
  {
    if ((ret = joined_table.add_expr_id(expr_id)) != OB_SUCCESS)
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Add outer join condition error");
  }

  return ret;
}

int resolve_table(
  ResultPlan * result_plan,
  ObStmt* stmt,
  ParseNode* node,
  uint64_t& table_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    table_id = OB_INVALID_ID;
    ParseNode* table_node = node;
    ParseNode* alias_node = NULL;
    if (node->type_ == T_ALIAS)
    {
      OB_ASSERT(node->num_child_ == 2);
      OB_ASSERT(node->children_[0]);
      OB_ASSERT(node->children_[1]);

      table_node = node->children_[0];
      alias_node = node->children_[1];
    }

    switch (table_node->type_)
    {
      case T_IDENT:
      {
        ObString table_name;
        ObString alias_name;
        table_name.assign_ptr(
            (char*)(table_node->str_value_),
            static_cast<int32_t>(strlen(table_node->str_value_))
            );
        if (alias_node)
        {
          alias_name.assign_ptr(
              (char*)(alias_node->str_value_),
              static_cast<int32_t>(strlen(alias_node->str_value_))
              );
          ret = stmt->add_table_item(*result_plan, table_name, alias_name, table_id, TableItem::ALIAS_TABLE);
        }
        else
          ret = stmt->add_table_item(*result_plan, table_name, alias_name, table_id, TableItem::BASE_TABLE);
        break;
      }
      case T_SELECT:
      {
        /* It must be select statement.
              * For other statements, if the target is a view, it need to be expanded before this step
              */
        OB_ASSERT(stmt->get_stmt_type() == ObStmt::T_SELECT);
        ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
        if (alias_node == NULL)
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "generated table must have alias name");
          break;
        }

        uint64_t query_id = OB_INVALID_ID;
        ret = resolve_select_stmt(result_plan, table_node, query_id);
        if (ret == OB_SUCCESS)
        {
          ObString table_name;
          ObString alias_name;
          table_name.assign_ptr(
              (char*)(alias_node->str_value_),
              static_cast<int32_t>(strlen(alias_node->str_value_))
              );
          ret = select_stmt->add_table_item(
                                *result_plan,
                                table_name,
                                alias_name,
                                table_id,
                                TableItem::GENERATED_TABLE,
                                query_id
                                );
        }
        break;
      }
      case T_JOINED_TABLE:
      {
        /* only select statement has this type */
        OB_ASSERT(stmt->get_stmt_type() == ObStmt::T_SELECT);
        ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
        table_id = select_stmt->generate_joined_tid();
        JoinedTable* joined_table = (JoinedTable*)parse_malloc(sizeof(JoinedTable), result_plan->name_pool_);
        if (joined_table == NULL)
        {
          ret = OB_ERR_PARSER_MALLOC_FAILED;
          TBSYS_LOG(WARN, "out of memory");
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Can not malloc space for JoinedTable");
          break;
        }
        joined_table = new(joined_table) JoinedTable;
        joined_table->set_joined_tid(table_id);
        ret = resolve_joined_table(result_plan, select_stmt, table_node, *joined_table);
        if (ret != OB_SUCCESS)
          break;
        ret = select_stmt->add_joined_table(joined_table);
        if (ret != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Can not add JoinedTable");
        break;
      }
      default:
        /* won't be here */
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Unknown table type");
        break;
    }
  }
  else
  {
    ret = OB_ERR_PARSER_SYNTAX;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
        "No table in from clause");
  }

  return ret;
}

int resolve_from_clause(
  ResultPlan * result_plan,
  ObSelectStmt* select_stmt,
  ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_FROM_LIST);
    OB_ASSERT(node->num_child_ >= 1);

    uint64_t tid = OB_INVALID_ID;
    for(int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      ParseNode* child_node = node->children_[i];
      ret = resolve_table(result_plan, select_stmt, child_node, tid);
      if (ret != OB_SUCCESS)
        break;

      if (child_node->type_ == T_JOINED_TABLE)
        ret = select_stmt->add_from_item(tid, true);
      else
        ret = select_stmt->add_from_item(tid);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Add from table failed");
        break;
      }
    }
  }
  return ret;
}

int resolve_table_columns(
  ResultPlan * result_plan,
  ObStmt* stmt,
  TableItem& table_item,
  int64_t num_columns)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ColumnItem *column_item = NULL;
  ColumnItem new_column_item;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
        "Wrong invocation of ObStmt::add_table_item, logical_plan must exist!!!");
  }

  ObSchemaChecker* schema_checker = NULL;
  if (ret == OB_SUCCESS)
  {
    schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
    if (schema_checker == NULL)
    {
      ret = OB_ERR_SCHEMA_UNSET;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Schema(s) are not set");
    }
  }

  if (ret == OB_SUCCESS)
  {
    if (table_item.type_ == TableItem::GENERATED_TABLE)
    {
      ObSelectStmt* sub_select = static_cast<ObSelectStmt*>(logical_plan->get_query(table_item.ref_id_));
      if (sub_select == NULL)
      {
        ret = OB_ERR_ILLEGAL_ID;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not get sub-query whose id = %lu", table_item.ref_id_);
      }
      else
      {
        int32_t num = sub_select->get_select_item_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < num && (num_columns <= 0 || i < num_columns); i++)
        {
          const SelectItem& select_item = sub_select->get_select_item(i);
          column_item = stmt->get_column_item_by_id(table_item.table_id_, OB_APP_MIN_COLUMN_ID + i);
          if (column_item == NULL)
          {
            new_column_item.column_id_ = OB_APP_MIN_COLUMN_ID + i;
            if ((ret = ob_write_string(*stmt->get_name_pool(),
                                       select_item.alias_name_,
                                       new_column_item.column_name_)) != OB_SUCCESS)
            {
              ret = OB_ERR_PARSER_MALLOC_FAILED;
              TBSYS_LOG(WARN, "out of memory");
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Can not malloc space for column name");
              break;
            }
            new_column_item.table_id_ = table_item.table_id_;
            new_column_item.query_id_ = 0; // no use now, because we don't support correlated subquery
            new_column_item.is_name_unique_ = false;
            new_column_item.is_group_based_ = false;
            new_column_item.data_type_ = select_item.type_;
            ret = stmt->add_column_item(new_column_item);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Add column error");
              break;
            }
            column_item = &new_column_item;
          }

          if (stmt->get_stmt_type() == ObStmt::T_SELECT && num_columns <= 0)
          {
            ObBinaryRefRawExpr* expr = NULL;
            if (CREATE_RAW_EXPR(expr, ObBinaryRefRawExpr, result_plan) == NULL)
              break;
            expr->set_expr_type(T_REF_COLUMN);
            expr->set_first_ref_id(column_item->table_id_);
            expr->set_second_ref_id(column_item->column_id_);
            expr->set_result_type(column_item->data_type_);
            ObSqlRawExpr* sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr), result_plan->name_pool_);
            if (sql_expr == NULL)
            {
              ret = OB_ERR_PARSER_MALLOC_FAILED;
              TBSYS_LOG(WARN, "out of memory");
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Can not malloc space for ObSqlRawExpr");
              break;
            }
            sql_expr = new(sql_expr) ObSqlRawExpr();
            sql_expr->set_expr_id(logical_plan->generate_expr_id());
            sql_expr->set_table_id(OB_INVALID_ID);
            sql_expr->set_column_id(logical_plan->generate_column_id());
            sql_expr->set_expr(expr);
            ObBitSet<> tables_set;
            tables_set.add_member(stmt->get_table_bit_index(table_item.table_id_));
            sql_expr->set_tables_set(tables_set);
            ret = logical_plan->add_expr(sql_expr);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Can not add ObSqlRawExpr to logical plan");
              break;
            }

            ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
            ret = select_stmt->add_select_item(
                                  sql_expr->get_expr_id(),
                                  false,
                                  column_item->column_name_,
                                  select_item.expr_name_,
                                  select_item.type_);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Can not add select item");
              break;
            }
          }
        }
      }
    }
    else
    {
      const ObColumnSchemaV2* column = NULL;
      int32_t column_size = 0;
      column = schema_checker->get_table_columns(table_item.ref_id_, column_size);
      if (NULL != column && column_size > 0)
      {
        if (table_item.ref_id_ == OB_TABLES_SHOW_TID) // @FIXME !!!
        {
          column_size = 1;
        }
        for (int32_t i = 0; ret == OB_SUCCESS && i < column_size && (num_columns <= 0 || i < num_columns); i++)
        {
          new_column_item.column_id_ = column[i].get_id();
          column_item = stmt->get_column_item_by_id(table_item.table_id_, new_column_item.column_id_);
          if (column_item == NULL)
          {
            ret = ob_write_string(*stmt->get_name_pool(),
                                  ObString::make_string(column[i].get_name()),
                                  new_column_item.column_name_);
            if (ret != OB_SUCCESS)
            {
              ret = OB_ERR_PARSER_MALLOC_FAILED;
              TBSYS_LOG(WARN, "out of memory");
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Can not malloc space for column name");
              break;
            }
            new_column_item.table_id_ = table_item.table_id_;
            new_column_item.query_id_ = 0; // no use now, because we don't support correlated subquery
            new_column_item.is_name_unique_ = false;
            new_column_item.is_group_based_ = false;
            new_column_item.data_type_ = column[i].get_type();
            ret = stmt->add_column_item(new_column_item);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Add column error");
              break;
            }
            column_item = &new_column_item;
          }

          if (stmt->get_stmt_type() == ObStmt::T_SELECT && num_columns <= 0)
          {
            ObBinaryRefRawExpr* expr = NULL;
            if (CREATE_RAW_EXPR(expr, ObBinaryRefRawExpr, result_plan) == NULL)
              break;
            expr->set_expr_type(T_REF_COLUMN);
            expr->set_first_ref_id(column_item->table_id_);
            expr->set_second_ref_id(column_item->column_id_);
            expr->set_result_type(column_item->data_type_);
            ObSqlRawExpr* sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr), result_plan->name_pool_);
            if (sql_expr == NULL)
            {
              ret = OB_ERR_PARSER_MALLOC_FAILED;
              TBSYS_LOG(WARN, "out of memory");
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Can not malloc space for ObSqlRawExpr");
              break;
            }
            sql_expr = new(sql_expr) ObSqlRawExpr();
            sql_expr->set_expr_id(logical_plan->generate_expr_id());
            sql_expr->set_table_id(OB_INVALID_ID);
            sql_expr->set_column_id(logical_plan->generate_column_id());
            sql_expr->set_expr(expr);
            ObBitSet<> tables_set;
            tables_set.add_member(stmt->get_table_bit_index(table_item.table_id_));
            sql_expr->set_tables_set(tables_set);
            ret = logical_plan->add_expr(sql_expr);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Can not add ObSqlRawExpr to logical plan");
              break;
            }

            ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
            ret = select_stmt->add_select_item(
                                  sql_expr->get_expr_id(),
                                  false,
                                  column_item->column_name_,
                                  column_item->column_name_,
                                  column_item->data_type_);
            if (ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                "Can not add select item");
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int resolve_star(
  ResultPlan * result_plan,
  ObSelectStmt* select_stmt,
  ParseNode* node)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(select_stmt);
  OB_ASSERT(node);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;

  if (node->type_ == T_STAR)
  {
    int32_t num = select_stmt->get_table_size();
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
      TableItem& table_item = select_stmt->get_table_item(i);
      ret = resolve_table_columns(result_plan, select_stmt, table_item);
    }
  }
  else if (node->type_ == T_OP_NAME_FIELD)
  {
    OB_ASSERT(node->children_[0]->type_ == T_IDENT);
    OB_ASSERT(node->children_[1]->type_ == T_STAR);

    TableItem* table_item;
    ParseNode* table_node = node->children_[0];
    ObString table_name;
    table_name.assign_ptr(
        (char*)(table_node->str_value_),
        static_cast<int32_t>(strlen(table_node->str_value_))
        );
    if ((select_stmt->get_table_item(table_name, &table_item)) == OB_INVALID_ID)
    {
      ret = OB_ERR_TABLE_UNKNOWN;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Unknown table %s", table_node->str_value_);
    }
    if (ret == OB_SUCCESS)
      ret = resolve_table_columns(result_plan, select_stmt, *table_item);
  }
  else
  {
    /* won't be here */
  }

  return ret;
}

int resolve_select_clause(
  ResultPlan * result_plan,
  ObSelectStmt* select_stmt,
  ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node->type_ == T_PROJECT_LIST);
  OB_ASSERT(node->num_child_ >= 1);

  ParseNode* project_node = NULL;
  ParseNode* alias_node = NULL;
  ObString   alias_name;
  ObString   expr_name;
  bool       is_bald_star = false;
  bool       is_real_alias;
  for (int32_t i = 0; ret == OB_SUCCESS &&i < node->num_child_; i++)
  {
    is_real_alias = false;
    expr_name.assign_ptr(
        (char*)(node->children_[i]->str_value_),
        static_cast<int32_t>(strlen(node->children_[i]->str_value_))
        );
    project_node = node->children_[i]->children_[0];
    if (project_node->type_ == T_STAR
      || (project_node->type_ == T_OP_NAME_FIELD
      && project_node->children_[1]->type_ == T_STAR))
    {
      if (project_node->type_ == T_STAR)
      {
        if (is_bald_star)
        {
          ret = OB_ERR_STAR_DUPLICATE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Wrong usage of '*'");
          break;
        }
        else
          is_bald_star = true;
      }

      ret = resolve_star(result_plan, select_stmt, project_node);
      continue;
    }

    if (project_node->type_ == T_ALIAS)
    {
      OB_ASSERT(project_node->num_child_ == 2);
      expr_name.assign_ptr(
                   const_cast<char*>(project_node->str_value_),
                   static_cast<int32_t>(strlen(project_node->str_value_))
                   );
      alias_node = project_node->children_[1];
      project_node = project_node->children_[0];
      is_real_alias = true;

      /* check if the alias name is legal */
      OB_ASSERT(alias_node->type_ == T_IDENT);
      alias_name.assign_ptr(
          (char*)(alias_node->str_value_),
          static_cast<int32_t>(strlen(alias_node->str_value_))
          );
      // Same as mysql, we do not check alias name
      // if (!(select_stmt->check_alias_name(logical_plan, sAlias)))
      // {
      //   TBSYS_LOG(ERROR, "alias name %.s is ambiguous", alias_node->str_value_);
      //   return false;
      // }
    }
    /* it is not real alias name, we just record them for convenience */
    else
    {
      if (project_node->type_ == T_IDENT)
        alias_node = project_node;
      else if (project_node->type_ == T_OP_NAME_FIELD)
      {
        expr_name.assign_ptr(
                     const_cast<char*>(project_node->str_value_),
                     static_cast<int32_t>(strlen(project_node->str_value_))
                     );
        alias_node = project_node->children_[1];
        OB_ASSERT(alias_node->type_ == T_IDENT);
      }

      /* original column name of based table, it has been checked in expression resolve */
      if (alias_node)
        alias_name.assign_ptr(
            (char*)(alias_node->str_value_),
            static_cast<int32_t>(strlen(alias_node->str_value_))
            );
    }

    if (project_node->type_ == T_EXPR_LIST && project_node->num_child_ != 1)
    {
      ret = OB_ERR_RESOLVE_SQL;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Operand should contain 1 column(s)");
      break;
    }

    uint64_t expr_id = OB_INVALID_ID;
    if ((ret = resolve_independ_expr(result_plan, select_stmt, project_node, expr_id)) != OB_SUCCESS)
      break;

    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    ObSqlRawExpr *select_expr = NULL;
    if ((select_expr = logical_plan->get_expr(expr_id)) == NULL)
    {
      ret = OB_ERR_ILLEGAL_ID;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Wrong expr_id");
      break;
    }

    /* if IDENT, we need to assign new id for it to avoid same (table_id, column_id) in ObRowDesc */
    /* 1. select price + off new_price, new_price from tbl; */
    /* 2. select price, price from tbl; */
    /* new_price from 1 and two price from 2 will have new column id after top project operator,
     * so, before this project any operator use none-aliased base column must get its real table id
     * and column id not the ids of the expression
     */
    if (project_node->type_ == T_IDENT || project_node->type_ == T_OP_NAME_FIELD)
    {
      select_expr->set_table_id(OB_INVALID_ID);
      select_expr->set_column_id(logical_plan->generate_column_id());
    }

    /* get table name and column name here*/
    const ObObjType type = select_expr->get_result_type();
    ret = select_stmt->add_select_item(expr_id, is_real_alias, alias_name, expr_name, type);
    if (ret != OB_SUCCESS)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Add select item error");
      break;
    }

    alias_node = NULL;
    alias_name.assign_ptr(NULL, 0);
  }

  return ret;
}

int resolve_where_clause(
  ResultPlan * result_plan,
  ObStmt* stmt,
  ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    ret = resolve_and_exprs(
              result_plan,
              stmt,
              node,
              stmt->get_where_exprs(),
              T_WHERE_LIMIT
              );
  }
  return ret;
}

int resolve_group_clause(
  ResultPlan * result_plan,
  ObSelectStmt* select_stmt,
  ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;

  /*****************************************************************************
   * The non-aggregate expression of select clause must be expression of group items,
   * but we don't check it here, which is in accordance with mysql.
   * Although there are different values of one group, but the executor only pick the first one
   * E.g.
   * select c1, c2, sum(c3)
   * from tbl
   * group by c1;
   * c2 in select clause is leagal, which is not in standard.
   *****************************************************************************/

  if (ret == OB_SUCCESS && node != NULL)
  {
    OB_ASSERT(node->type_ == T_EXPR_LIST);
    OB_ASSERT(node->num_child_ >= 1);
    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    uint64_t expr_id;
    ParseNode* group_node;
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      group_node = node->children_[i];
      if (group_node->type_ == T_INT && group_node->value_ >= 0)
      {
        int32_t pos = static_cast<int32_t>(group_node->value_);
        if (pos <= 0 || pos > select_stmt->get_select_item_size())
        {
          ret = OB_ERR_WRONG_POS;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Unknown column '%d' in 'group clause'", pos);
          break;
        }
        expr_id = select_stmt->get_select_item(pos - 1).expr_id_;
        ObSqlRawExpr *sql_expr = logical_plan->get_expr(expr_id);
        if (!sql_expr)
        {
          ret = OB_ERR_ILLEGAL_ID;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Can not find expression, expr_id = %lu", expr_id);
          break;
        }
        if (sql_expr->is_contain_aggr())
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Invalid use of expression which contains group function");
          break;
        }
      }
      else
      {
        ret = resolve_independ_expr(
                  result_plan,
                  select_stmt,
                  group_node,
                  expr_id,
                  T_GROUP_LIMIT
                  );
      }
      if (ret == OB_SUCCESS)
      {
        if ((ret = select_stmt->add_group_expr(expr_id)) != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Add group expression error");
      }
    }
  }
  return ret;
}

int resolve_having_clause(
  ResultPlan * result_plan,
  ObSelectStmt* select_stmt,
  ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {

    ret = resolve_and_exprs(
              result_plan,
              select_stmt,
              node,
              select_stmt->get_having_exprs(),
              T_HAVING_LIMIT
              );
  }
  return ret;
}

int resolve_order_clause(
  ResultPlan * result_plan,
  ObSelectStmt* select_stmt,
  ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_SORT_LIST);

    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      ParseNode* sort_node = node->children_[i];
      OB_ASSERT(sort_node->type_ == T_SORT_KEY);

      OrderItem order_item;
      order_item.order_type_ = OrderItem::ASC;
      if (sort_node->children_[1]->type_ == T_SORT_ASC)
        order_item.order_type_ = OrderItem::ASC;
      else if (sort_node->children_[1]->type_ == T_SORT_DESC)
        order_item.order_type_ = OrderItem::DESC;
      else
      {
        OB_ASSERT(false); /* Won't be here */
      }

      if (sort_node->children_[0]->type_ == T_INT && sort_node->children_[0]->value_ >= 0)
      {
        int32_t pos = static_cast<int32_t>(sort_node->children_[0]->value_);
        if (pos <= 0 || pos > select_stmt->get_select_item_size())
        {
          ret = OB_ERR_WRONG_POS;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Unknown column '%d' in 'group clause'", pos);
          break;
        }
        order_item.expr_id_ = select_stmt->get_select_item(pos - 1).expr_id_;
      }
      else
      {
        ret = resolve_independ_expr(result_plan, select_stmt, sort_node->children_[0], order_item.expr_id_);
      }
      if (ret == OB_SUCCESS)
      {
        if ((ret = select_stmt->add_order_item(order_item)) != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Add order expression error");
      }
    }
  }
  return ret;
}

int resolve_limit_clause(
  ResultPlan * result_plan,
  ObSelectStmt* select_stmt,
  ParseNode* node)
{

  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(result_plan != NULL);
    OB_ASSERT(node->type_ == T_LIMIT_CLAUSE);

    ParseNode* limit_node = node->children_[0];
    ParseNode* offset_node = node->children_[1];
    OB_ASSERT(limit_node != NULL || offset_node != NULL);
    uint64_t limit_count = OB_INVALID_ID;
    uint64_t limit_offset = OB_INVALID_ID;

    // resolve the question mark with less value first
    if (limit_node != NULL && limit_node->type_ == T_QUESTIONMARK
      && offset_node != NULL && offset_node->type_ == T_QUESTIONMARK
      && limit_node->value_ > offset_node->value_)
    {
      if ((ret = resolve_independ_expr(result_plan, select_stmt, offset_node, limit_offset)) != OB_SUCCESS
        || (ret = resolve_independ_expr(result_plan, select_stmt, limit_node, limit_count)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Resolve limit/offset error, ret=%d", ret);
      }
    }
    else
    {
      if (ret == OB_SUCCESS && limit_node != NULL)
      {
        if (limit_node->type_ != T_INT && limit_node->type_ != T_QUESTIONMARK)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Wrong type of limit value");
        }
        else if ((ret = resolve_independ_expr(result_plan, select_stmt, limit_node, limit_count)) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Resolve limit error, ret=%d", ret);
        }
      }
      if (ret == OB_SUCCESS && offset_node != NULL)
      {
        if (offset_node->type_ != T_INT && offset_node->type_ != T_QUESTIONMARK)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Wrong type of limit value");
        }
        else if ((ret = resolve_independ_expr(result_plan, select_stmt, offset_node, limit_offset)) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Resolve offset error, ret=%d", ret);
        }
      }
    }
    if (ret == OB_SUCCESS)
    {
      select_stmt->set_limit_offset(limit_count, limit_offset);
    }
  }
  return ret;
}

int resolve_for_update_clause(
    ResultPlan * result_plan,
    ObSelectStmt* select_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_BOOL);
    if (node->value_ == 1)
      select_stmt->set_for_update(true);
  }
  return ret;
}

int resolve_select_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->num_child_ >= 15);
  query_id = OB_INVALID_ID;

  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObLogicalPlan* logical_plan = NULL;
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  ObSelectStmt* select_stmt = NULL;
  if (ret == OB_SUCCESS)
  {
    select_stmt = (ObSelectStmt*)parse_malloc(sizeof(ObSelectStmt), result_plan->name_pool_);
    if (select_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObSelectStmt");
    }
  }

  if (ret == OB_SUCCESS)
  {
    select_stmt = new(select_stmt) ObSelectStmt(name_pool);
    query_id = logical_plan->generate_query_id();
    select_stmt->set_query_id(query_id);
    ret = logical_plan->add_query(select_stmt);
    if (ret != OB_SUCCESS)
    {
       snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObSelectStmt to logical plan");
    }
  }

  /* -----------------------------------------------------------------
     * The later resolve may need some infomation resolved by the former one,
     * so please follow the resolving orders:
     *
     * 1. set clause
     * 2. from clause
     * 3. select clause
     * 4. where clause
     * 5. group by clause
     * 6. having clause
     * 7. order by clause
     * 8. limit clause
     * -----------------------------------------------------------------
     */

  /* resolve set clause */
  if (node->children_[6] != NULL)
  {
    OB_ASSERT(node->children_[8] != NULL);
    OB_ASSERT(node->children_[9] != NULL);

    if (node->children_[12] && node->children_[12]->value_ == 1)
    {
      ret = OB_ERR_ILLEGAL_ID;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Select for update statement can not be processed in set query");
    }

    // assign set type
    if (ret == OB_SUCCESS)
    {
      switch (node->children_[6]->type_)
      {
        case T_SET_UNION:
          select_stmt->assign_set_op(ObSelectStmt::UNION);
          break;
        case T_SET_INTERSECT:
          select_stmt->assign_set_op(ObSelectStmt::INTERSECT);
          break;
        case T_SET_EXCEPT:
          select_stmt->assign_set_op(ObSelectStmt::EXCEPT);
          break;
        default:
          ret = OB_ERR_OPERATOR_UNKNOWN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "unknown set operator of set clause");
          break;
      }
    }

    // assign first query
    uint64_t sub_query_id = OB_INVALID_ID;
    if (ret == OB_SUCCESS)
    {
      if (node->children_[7] == NULL || node->children_[7]->type_ == T_DISTINCT)
      {
        select_stmt->assign_set_distinct();
      }
      else
      {
        select_stmt->assign_set_all();
      }
      ret = resolve_select_stmt(result_plan, node->children_[8], sub_query_id);
      if (ret == OB_SUCCESS)
        select_stmt->assign_left_query_id(sub_query_id);
    }
    // assign second query
    if (ret == OB_SUCCESS)
    {
      ret = resolve_select_stmt(result_plan, node->children_[9], sub_query_id);
      if (ret == OB_SUCCESS)
        select_stmt->assign_right_query_id(sub_query_id);
    }

    // check if columns number ars match
    if (ret == OB_SUCCESS)
    {
      ObSelectStmt* left_select = logical_plan->get_select_query(select_stmt->get_left_query_id());
      ObSelectStmt* right_select = logical_plan->get_select_query(select_stmt->get_right_query_id());
      if (!left_select || !right_select)
      {
        ret = OB_ERR_ILLEGAL_ID;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "resolve set clause error");
      }
      else if(left_select->get_select_item_size() != right_select->get_select_item_size())
      {
        ret = OB_ERR_COLUMN_SIZE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "The used SELECT statements have a different number of columns");
      }
      else
        ret = select_stmt->copy_select_items(left_select);
    }
  }
  else
  {
    /* normal select */
    select_stmt->assign_set_op(ObSelectStmt::NONE);

    if (node->children_[0] == NULL || node->children_[0]->type_ == T_ALL)
    {
      ret = OB_ERR_ILLEGAL_ID;
      select_stmt->assign_all();
    }
    else
    {
      select_stmt->assign_distinct();
    }

    /* resolve from clause */
    if ((ret = resolve_from_clause(result_plan, select_stmt, node->children_[2])) == OB_SUCCESS)
    {
    }
    if (ret == OB_SUCCESS && node->children_[12] && node->children_[12]->value_ == 1)
    {
      if (select_stmt->get_table_size() > 1)
      {
        ret = OB_ERR_ILLEGAL_ID;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Select for update statement can not process more than one table");
      }
      else if (select_stmt->get_table_size() > 0)
      {
        TableItem& table_item = select_stmt->get_table_item(0);
        uint64_t table_id = table_item.table_id_;
        ret = add_all_rowkey_columns_to_stmt(result_plan, table_id, select_stmt);
      }
    }
    /* resolve select clause */
    /* resolve where clause */
    /* resolve group by clause */
    /* resolve having clause */
    if (ret == OB_SUCCESS
      && (ret = resolve_select_clause(result_plan, select_stmt, node->children_[1]))
          == OB_SUCCESS
      && (ret = resolve_where_clause(result_plan, select_stmt, node->children_[3]))
          == OB_SUCCESS
      && (ret = resolve_group_clause(result_plan, select_stmt, node->children_[4]))
          == OB_SUCCESS
      && (ret = resolve_having_clause(result_plan, select_stmt, node->children_[5]))
          == OB_SUCCESS
      )
    {
      ;
    }
  }

  /* resolve order by clause */
  /* resolve limit clause */
  if (ret == OB_SUCCESS
    && (ret = resolve_order_clause(result_plan, select_stmt, node->children_[10]))
        == OB_SUCCESS
    && (ret = resolve_limit_clause(result_plan, select_stmt, node->children_[11]))
        == OB_SUCCESS
    && (ret = resolve_for_update_clause(result_plan, select_stmt, node->children_[12]))
        == OB_SUCCESS
    )
  {
    ;
  }

  // In some cases, some table(s) may have none column mentioned,
  // considerating the optimization, not all columns needed, we only need to scan out one of its columns
  // Example:
  // 1. select count(*) from t1;
  // 2. select t1.c1 from t1, t2;
  if (ret == OB_SUCCESS)
  {
    if (select_stmt->get_select_item_size() <= 0 && select_stmt->get_table_size() <= 0)
    {
      ret = OB_ERR_RESOLVE_SQL;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "No tables used");
    }
    for (int32_t i = 0; ret == OB_SUCCESS && i < select_stmt->get_table_size(); i++)
    {
      TableItem& table_item = select_stmt->get_table_item(i);
      if (!table_item.has_scan_columns_)
        ret = resolve_table_columns(result_plan, select_stmt, table_item, 1);
    }
  }

  if (ret == OB_SUCCESS && node->children_[13])
  {
    ret = resolve_hints(result_plan, select_stmt, node->children_[13]);
  }
  if (ret == OB_SUCCESS && node->children_[14])
  {
    ret = resolve_when_clause(result_plan, select_stmt, node->children_[14]);
  }

  return ret;
}

int resolve_hints(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    ObQueryHint& query_hint = stmt->get_query_hint();
    OB_ASSERT(node->type_ == T_HINT_OPTION_LIST);
    for (int32_t i = 0; i < node->num_child_; i++)
    {
      ParseNode* hint_node = node->children_[i];
      if (!hint_node)
        continue;
      switch (hint_node->type_)
      {
        case T_READ_STATIC:
          query_hint.read_consistency_ = STATIC;
          break;
        case T_HOTSPOT:
          query_hint.hotspot_= true;
          break;
        case T_READ_CONSISTENCY:
          if (hint_node->value_ == 1)
          {
            query_hint.read_consistency_ = STATIC;
          }
          else if (hint_node->value_ == 2)
          {
            query_hint.read_consistency_ = FROZEN;
          }
          else if (hint_node->value_ == 3)
          {
            query_hint.read_consistency_ = WEAK;
          }
          else if (hint_node->value_ == 4)
          {
            query_hint.read_consistency_ = STRONG;
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "unknown hint value, ret=%d", ret);
          }
          break;
        default:
          ret = OB_ERR_HINT_UNKNOWN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Unknown hint '%s'", get_type_name(hint_node->type_));
          break;
      }
    }
  }
  return ret;
}

int resolve_delete_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  OB_ASSERT(node && node->type_ == T_DELETE && node->num_child_ >= 3);
  query_id = OB_INVALID_ID;

  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    ObDeleteStmt* delete_stmt = (ObDeleteStmt*)parse_malloc(sizeof(ObDeleteStmt), result_plan->name_pool_);
    if (delete_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObDeleteStmt");
    }
    else
    {
      delete_stmt = new(delete_stmt) ObDeleteStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      delete_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(delete_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObDeleteStmt to logical plan");
      }
      else
      {
        ParseNode* table_node = node->children_[0];
        if (table_node->type_ != T_IDENT)
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Only single base table is supported for delete");
        }
        if (ret == OB_SUCCESS)
        {
          ret = resolve_table(result_plan, delete_stmt, table_node, table_id);
        }
        if (ret == OB_SUCCESS)
        {
          ret = add_all_rowkey_columns_to_stmt(result_plan, table_id, delete_stmt);
        }
        if (ret == OB_SUCCESS)
        {
          delete_stmt->set_delete_table(table_id);
          ret = resolve_where_clause(result_plan, delete_stmt, node->children_[1]);
        }
        if (ret == OB_SUCCESS && node->children_[2])
        {
          ret = resolve_when_clause(result_plan, delete_stmt, node->children_[2]);
        }
      }
    }
  }
  return ret;
}

int resolve_insert_columns(
  ResultPlan * result_plan,
  ObInsertStmt* insert_stmt,
  ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    OB_ASSERT(node->type_ == T_COLUMN_LIST);
    ColumnItem* column_item = NULL;
    ParseNode* column_node = NULL;
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      column_node = node->children_[i];
      OB_ASSERT(column_node->type_ == T_IDENT);

      ObString column_name;
      column_name.assign_ptr(
          (char*)(column_node->str_value_),
          static_cast<int32_t>(strlen(column_node->str_value_))
          );
      column_item = insert_stmt->get_column_item(NULL, column_name);
      if (column_item == NULL)
      {
        if ((ret = insert_stmt->add_column_item(*result_plan, column_name)) != OB_SUCCESS)
          break;
      }
      else
      {
        ret = OB_ERR_COLUMN_DUPLICATE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Column %s are duplicate", column_node->str_value_);
        break;
      }
    }
  }
  else
  {
    if (insert_stmt->get_table_size() != 1)
    {
      ret = OB_ERR_PARSER_SYNTAX;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Insert statement only support one table");
    }
    if (ret == OB_SUCCESS)
    {
      TableItem& table_item = insert_stmt->get_table_item(0);
      if (table_item.type_ != TableItem::BASE_TABLE)
      {
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Only base table can be inserted");
      }
      else
        ret = resolve_table_columns(result_plan, insert_stmt, table_item);
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int32_t i=0;OB_SUCCESS == ret && i<insert_stmt->get_column_size();i++)
    {
      const ColumnItem* column_item = insert_stmt->get_column_item(i);
      if (NULL != column_item && column_item->table_id_ != OB_INVALID_ID)
      {
        ObSchemaChecker* schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
        if (schema_checker == NULL)
        {
          ret = OB_ERR_SCHEMA_UNSET;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                  "Schema(s) are not set");
          break;
        }

        if (schema_checker->is_join_column(column_item->table_id_, column_item->column_id_))
        {
          ret = OB_ERR_INSERT_INNER_JOIN_COLUMN;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Cannot insert inner join column: %.*s", column_item->column_name_.length(), column_item->column_name_.ptr());
          break;
        }
      }
    }
  }
  return ret;
}

int resolve_insert_values(
  ResultPlan * result_plan,
  ObInsertStmt* insert_stmt,
  ParseNode* node)
{
  OB_ASSERT(node->type_ == T_VALUE_LIST);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;

  insert_stmt->set_values_size(node->num_child_);
  ObArray<uint64_t> value_row;
  for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
  {
    ParseNode* vector_node = node->children_[i];
    uint64_t expr_id;
    for (int32_t j = 0; ret == OB_SUCCESS && j < vector_node->num_child_; j++)
    {
      ret = resolve_independ_expr(result_plan, insert_stmt, vector_node->children_[j],
                                  expr_id, T_INSERT_LIMIT);
      if (ret == OB_SUCCESS && (ret = value_row.push_back(expr_id)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            "Can not add expr_id to ObArray");
      }
    }
    if (ret == OB_SUCCESS &&
      insert_stmt->get_column_size() != value_row.count())
    {
      ret = OB_ERR_COLUMN_SIZE;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Column count doesn't match value count");
    }
    if (ret == OB_SUCCESS)
    {
      if ((ret = insert_stmt->add_value_row(value_row)) != OB_SUCCESS)
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Add value-row to ObInsertStmt error");
    }
    value_row.clear();
  }

  return ret;
}

int resolve_insert_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  OB_ASSERT(node && node->type_ == T_INSERT && node->num_child_ >= 6);
  query_id = OB_INVALID_ID;

  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {

    ObInsertStmt* insert_stmt = (ObInsertStmt*)parse_malloc(sizeof(ObInsertStmt), result_plan->name_pool_);
    if (insert_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObInsertStmt");
    }
    else
    {
      insert_stmt = new(insert_stmt) ObInsertStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      insert_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(insert_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObInsertStmt to logical plan");
      }
      else
      {
        ParseNode* table_node = node->children_[0];
        if (table_node->type_ != T_IDENT)
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Only single base table is supported for insert");
        }
        if (ret == OB_SUCCESS)
          ret = resolve_table(result_plan, insert_stmt, table_node, table_id);
        if (ret == OB_SUCCESS)
        {
          insert_stmt->set_insert_table(table_id);
          ret = resolve_insert_columns(result_plan, insert_stmt, node->children_[1]);
        }
        if (ret == OB_SUCCESS)
        {
          // value list
          if (node->children_[2])
          {
            ret = resolve_insert_values(result_plan, insert_stmt, node->children_[2]);
          }
          else
          {
            // value from sub-query(insert into table select ..)
            OB_ASSERT(node->children_[3] && node->children_[3]->type_ == T_SELECT);
            uint64_t ref_id = OB_INVALID_ID;
            ret = resolve_select_stmt(result_plan, node->children_[3], ref_id);
            if (ret == OB_SUCCESS)
            {
              insert_stmt->set_insert_query(ref_id);
              ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
              if (select_stmt == NULL)
              {
                ret = OB_ERR_ILLEGAL_ID;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                    "Invalid query id of sub-query");
              }
              if (ret == OB_SUCCESS &&
                insert_stmt->get_column_size() != select_stmt->get_select_item_size())
              {
                ret = OB_ERR_COLUMN_SIZE;
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                    "select values are not match insert columns");
              }
            }
          }
        }
        if (ret == OB_SUCCESS)
        {
          OB_ASSERT(node->children_[4] && node->children_[4]->type_ == T_BOOL);
          if (node->children_[4]->value_ == 1)
            insert_stmt->set_replace(true);
          else
            insert_stmt->set_replace(false);
        }
        if (ret == OB_SUCCESS && node->children_[5])
        {
          ret = resolve_when_clause(result_plan, insert_stmt, node->children_[5]);
        }
      }
    }
  }
  return ret;
}

int resolve_update_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  OB_ASSERT(node && node->type_ == T_UPDATE && node->num_child_ >= 5);
  query_id = OB_INVALID_ID;

  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    ObUpdateStmt* update_stmt = (ObUpdateStmt*)parse_malloc(sizeof(ObUpdateStmt), result_plan->name_pool_);
    if (update_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      TBSYS_LOG(WARN, "out of memory");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc ObUpdateStmt");
    }
    else
    {
      update_stmt = new(update_stmt) ObUpdateStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      update_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(update_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not add ObUpdateStmt to logical plan");
      }
      else
      {
        ParseNode* table_node = node->children_[0];
        if (table_node->type_ != T_IDENT)
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
              "Only single base table is supported for Update");
        }
        if (ret == OB_SUCCESS)
        {
          ret = resolve_table(result_plan, update_stmt, table_node, table_id);
        }
        if (ret == OB_SUCCESS)
        {
          ret = add_all_rowkey_columns_to_stmt(result_plan, table_id, update_stmt);
        }
        if (ret == OB_SUCCESS)
        {
          update_stmt->set_update_table(table_id);
          ParseNode* assign_list = node->children_[1];
          OB_ASSERT(assign_list && assign_list->type_ == T_ASSIGN_LIST);
          uint64_t ref_id;
          ColumnItem *column_item = NULL;
          for (int32_t i = 0; ret == OB_SUCCESS && i < assign_list->num_child_; i++)
          {
            ParseNode* assgin_node = assign_list->children_[i];
            OB_ASSERT(assgin_node && assgin_node->type_ == T_ASSIGN_ITEM && assgin_node->num_child_ >= 2);

            /* resolve target column */
            ParseNode* column_node = assgin_node->children_[0];
            OB_ASSERT(column_node && column_node->type_ == T_IDENT);
            ObString column_name;
            column_name.assign_ptr(
                (char*)(column_node->str_value_),
                static_cast<int32_t>(strlen(column_node->str_value_))
                );
            column_item = update_stmt->get_column_item(NULL, column_name);
            if (column_item == NULL)
            {
              ret = update_stmt->add_column_item(*result_plan, column_name, NULL, &column_item);
            }
            if (ret == OB_SUCCESS)
            {
              ret = update_stmt->add_update_column(column_item->column_id_);
            }
            /* resolve new value expression */
            if (ret == OB_SUCCESS)
            {
              ParseNode* expr = assgin_node->children_[1];
              ret = resolve_independ_expr(result_plan, update_stmt, expr, ref_id, T_UPDATE_LIMIT);
            }
            if (ret == OB_SUCCESS)
            {
              if ((ret = update_stmt->add_update_expr(ref_id)) != OB_SUCCESS)
              {
                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                    "Add update value error");
              }
            }
          }
        }
        if (ret == OB_SUCCESS)
          ret = resolve_where_clause(result_plan, update_stmt, node->children_[2]);
        if (ret == OB_SUCCESS && node->children_[3])
          ret = resolve_when_clause(result_plan, update_stmt, node->children_[3]);
        if (ret == OB_SUCCESS && node->children_[4])
          ret = resolve_hints(result_plan, update_stmt, node->children_[4]);
      }
    }
  }
  return ret;
}

int resolve_when_clause(
    ResultPlan * result_plan,
    ObStmt* stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (node)
  {
    if ((ret = resolve_and_exprs(
                    result_plan,
                    stmt,
                    node,
                    stmt->get_when_exprs(),
                    T_WHEN_LIMIT
                    )) == OB_SUCCESS)
    {
      ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
      stmt->set_when_number(logical_plan->generate_when_number());
    }
  }
  return ret;
}
