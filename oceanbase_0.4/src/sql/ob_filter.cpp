/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_filter.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_filter.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

#define destroy_sql_expression_dlist(expr_list)\
  dlist_for_each_del(p, expr_list)\
  {\
    ObSqlExpression::free(dynamic_cast<ObSqlExpression*>(p));\
  }

ObFilter::ObFilter()
{
}

ObFilter::~ObFilter()
{
  destroy_sql_expression_dlist(filters_);
}

void ObFilter::reset()
{
  destroy_sql_expression_dlist(filters_);
  ObSingleChildPhyOperator::reset();
}

void ObFilter::reuse()
{
  filters_.clear();
  ObSingleChildPhyOperator::reuse();
}

int ObFilter::add_filter(ObSqlExpression* expr)
{
  int ret = OB_SUCCESS;
  expr->set_owner_op(this);
  if (!filters_.add_last(expr))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "failed to add column");
  }
  return ret;
}

int ObFilter::open()
{
  return ObSingleChildPhyOperator::open();
}

int ObFilter::close()
{
  return ObSingleChildPhyOperator::close();
}

int ObFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObFilter::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObRow *input_row = NULL;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    const ObObj *result = NULL;
    bool did_output = true;
    while(OB_SUCCESS == ret
          && OB_SUCCESS == (ret = child_op_->get_next_row(input_row)))
    {
      did_output = true;
      dlist_for_each(ObSqlExpression, p, filters_)
      {
        if (OB_SUCCESS != (ret = p->calc(*input_row, result)))
        {
          TBSYS_LOG(WARN, "failed to calc expression, err=%d", ret);
          break;
        }
        else if (!result->is_true())
        {
          did_output = false;
          break;
        }
      } // end for
      if (did_output)
      {
        row = input_row;
        break;
      }
    } // end while
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObFilter, PHY_FILTER);
  }
}

int64_t ObFilter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Filter(filters=[");
  dlist_for_each_const(ObSqlExpression, p, filters_)
  {
    pos += p->to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, ",");
  }
  databuff_printf(buf, buf_len, pos, "])\n");
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}


DEFINE_SERIALIZE(ObFilter)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_int(filters_.get_size());
  if (0 >= filters_.get_size())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "no column for output");
  }
  else if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to serialize filter expr count. ret=%d", ret);
  }
  else
  {
    dlist_for_each_const(ObSqlExpression, p, filters_)
    {
      if (OB_SUCCESS != (ret = p->serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "filter expr serialize fail. ret=%d", ret);
        break;
      }
    } // end for
  }
  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObFilter)
{
  int64_t size = 0;
  ObObj obj;
  obj.set_int(filters_.get_size());
  size += obj.get_serialize_size();
  dlist_for_each_const(ObSqlExpression, p, filters_)
  {
    size += p->get_serialize_size();
  }
  return size;
}

DEFINE_DESERIALIZE(ObFilter)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t expr_count = 0, i = 0;
  destroy_sql_expression_dlist(filters_);
  if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to deserialize expr count. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = obj.get_int(expr_count)))
  {
    TBSYS_LOG(WARN, "fail to get expr_count. ret=%d", ret);
  }
  else
  {
    for (i = 0; i < expr_count; i++)
    {
      ObSqlExpression *expr = ObSqlExpression::alloc();
      if (NULL == expr)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "no memory");
        break;
      }
      else if (OB_SUCCESS != (ret = expr->deserialize(buf, data_len, pos)))
      {
        ObSqlExpression::free(expr);
        TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d i=%ld count=%ld", ret, i, expr_count);
        break;
      }
      else if (OB_SUCCESS != (ret = add_filter(expr)))
      {
        TBSYS_LOG(WARN, "fail to add expression to filter.ret=%d, buf=%p, data_len=%ld, pos=%ld", ret, buf, data_len, pos);
        break;
      }
    }
  }
  return ret;
}

PHY_OPERATOR_ASSIGN(ObFilter)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObFilter);
  reset();
  dlist_for_each_const(ObSqlExpression, p, o_ptr->filters_)
  {
    ObSqlExpression *expr = ObSqlExpression::alloc();
    if (NULL == expr)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "no memory");
      break;
    }
    else
    {
      *expr = *p;
      expr->set_owner_op(this);
      if (!filters_.add_last(expr))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "Add expression to ObFilter failed");
        break;
      }
    }
  }
  return ret;
}

ObPhyOperatorType ObFilter::get_type() const
{
  return PHY_FILTER;
}
