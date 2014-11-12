/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_execute.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_execute.h"
#include "ob_result_set.h"
#include "ob_physical_plan.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObExecute::ObExecute()
  :stmt_id_(OB_INVALID_ID)
{
}

ObExecute::~ObExecute()
{
}

void ObExecute::reset()
{
  stmt_id_ = OB_INVALID_ID;
  param_names_.clear();
  ObSingleChildPhyOperator::reset();
}

void ObExecute::reuse()
{
  stmt_id_ = OB_INVALID_ID;
  param_names_.clear();
  ObSingleChildPhyOperator::reuse();
}

int ObExecute::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ObResultSet *result_set = NULL;
    ObPhysicalPlan *physical_plan = NULL;
    ObPhyOperator *main_query = NULL;
    if ((result_set = my_phy_plan_->get_result_set()->get_session()->get_plan(stmt_id_)) == NULL
      || (physical_plan = result_set->get_physical_plan()) == NULL
      || (main_query = physical_plan->get_main_query()) == NULL)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "Stored session plan can not be fount or not correct");
    }
    else
    {
      ret = main_query->get_row_desc(row_desc);
    }
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObExecute::open()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == stmt_id_)
  {
    ret = OB_ERR_GEN_PLAN;
    TBSYS_LOG(WARN, "Prepare statement is not initiated, stmt_id=%lu", stmt_id_);
  }
  else
  {
    // get stored executing plan and params
    ret = fill_execute_items();
  }
  return ret;
}

int ObExecute::close()
{
  return ObSingleChildPhyOperator::close();
}

int ObExecute::fill_execute_items()
{
  int ret = OB_SUCCESS;

  // get stored executing plan
  ObResultSet *result_set = NULL;
  ObPhysicalPlan *physical_plan = NULL;
  ObPhyOperator *main_query = NULL;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  if ((result_set = session->get_plan(stmt_id_)) == NULL
    || (physical_plan = result_set->get_physical_plan()) == NULL
    || (main_query = physical_plan->get_main_query()) == NULL)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "Stored session plan can not be fount or not correct, result_set=%p main_query=%p phy_plan=%p",
              result_set, main_query, physical_plan);
  }
  else if ((ret = set_child(0, *main_query)) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "Find stored executing plan failed");
  }

  // fill running params
  if (ret == OB_SUCCESS)
  {
    ObIArray<ObObj*>& param_values = result_set->get_params();
    if (param_values.count() != param_names_.count())
    {
      ret = OB_ERR_WRONG_DYNAMIC_PARAM;
      TBSYS_LOG(USER_ERROR, "Incorrect arguments number to EXECUTE, need %ld arguments", param_values.count());
    }
    for (int64_t i = 0; ret == OB_SUCCESS && i < param_names_.count(); i++)
    {
      ObObj val;
      ObString param_name = param_names_.at(i);
      if ((ret = session->get_variable_value(param_name, val)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Get variable %.*s faild. ret=%d", param_name.length(), param_name.ptr(), ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "execute using variables, i=%ld val=%s stmt_id=%ld",
                  i, to_cstring(val), stmt_id_);
        ObObj *stored_val = param_values.at(i);
        *stored_val = val;
      }
    }
  }

  if (ret == OB_SUCCESS && (ret = result_set->open()) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "failed to open result set, err=%d", ret);
  }
  return ret;
}

int ObExecute::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;

  if (NULL == child_op_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "child_op_ must not NULL");
  }
  else
  {
    ret = child_op_->get_next_row(row);
  }
   return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObExecute, PHY_EXECUTE);
  }
}

int64_t ObExecute::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Execute(stmt_id_=<%lu>, \n", stmt_id_);
  if (param_names_.count() > 0)
  {
    for (int64_t i = 0; i < param_names_.count(); i++)
    {
      if (i == 0)
        databuff_printf(buf, buf_len, pos, "Using=<%.*s>", param_names_.at(i).length(), param_names_.at(i).ptr());
      else
        databuff_printf(buf, buf_len, pos, ", <%.*s>", param_names_.at(i).length(), param_names_.at(i).ptr());
    }
    databuff_printf(buf, buf_len, pos, ")\n");
  }
  return pos;
}
