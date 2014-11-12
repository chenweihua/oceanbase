/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_except.cpp
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */

#include "ob_merge_except.h"
#include "common/utility.h"
#include "common/ob_expr_obj.h"
#include "common/ob_row_util.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObMergeExcept::ObMergeExcept()
  :cur_first_query_row_(NULL),cur_second_query_row_(NULL),
  left_ret_(OB_SUCCESS), right_ret_(OB_SUCCESS),
  last_cmp_(-1), got_first_row_(false), left_last_row_buf_(NULL),
  right_last_row_buf_(NULL)

{
}

ObMergeExcept::~ObMergeExcept()
{
}

void ObMergeExcept::reset()
{
  get_next_row_func_ = NULL;
  //left_last_row_.reset(false, ObRow::DEFAULT_NULL);
  //right_last_row_.reset(false, ObRow::DEFAULT_NULL);
  cur_first_query_row_ = NULL;
  cur_second_query_row_ = NULL;
  left_ret_ = OB_SUCCESS;
  right_ret_ = OB_SUCCESS;
  last_cmp_ = -1;
  got_first_row_ = false;
  left_last_row_buf_ = NULL;
  right_last_row_buf_ = NULL;
  ObSetOperator::reset();
}

void ObMergeExcept::reuse()
{
  get_next_row_func_ = NULL;
  //left_last_row_.reset(false, ObRow::DEFAULT_NULL);
  //right_last_row_.reset(false, ObRow::DEFAULT_NULL);
  cur_first_query_row_ = NULL;
  cur_second_query_row_ = NULL;
  left_ret_ = OB_SUCCESS;
  right_ret_ = OB_SUCCESS;
  last_cmp_ = -1;
  got_first_row_ = false;
  left_last_row_buf_ = NULL;
  right_last_row_buf_ = NULL;
  ObSetOperator::reuse();
}

int ObMergeExcept::set_distinct(bool is_distinct)
{
  int ret = OB_SUCCESS;
  ObSetOperator::set_distinct(is_distinct);
  if (is_distinct)
  {
    get_next_row_func_ = &ObMergeExcept::distinct_get_next_row;
  }
  else
  {
    get_next_row_func_ = &ObMergeExcept::all_get_next_row;
  }
  return ret;
}

int ObMergeExcept::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObDoubleChildrenPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open double child operators, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_row_desc()))
  {
    TBSYS_LOG(WARN, "failed to construct row description, ret=%d", ret);
  }
  else if (NULL == (left_last_row_buf_ = (char*)ob_malloc(OB_ROW_BUF_SIZE, 0)))
  {
    TBSYS_LOG(ERROR, "failed to ob_malloc %lu bytes memory", OB_ROW_BUF_SIZE);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL == (right_last_row_buf_ = (char*)ob_malloc(OB_ROW_BUF_SIZE, 0)))
  {
    TBSYS_LOG(ERROR, "failed to ob_malloc %lu bytes memory", OB_ROW_BUF_SIZE);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    got_first_row_ = false;
    left_ret_ = left_op_->get_next_row(cur_first_query_row_);
    right_ret_ = right_op_->get_next_row(cur_second_query_row_);
  }
  return ret;
}
int ObMergeExcept::close()
{
  int ret = OB_SUCCESS;
  if (NULL != right_last_row_buf_)
  {
    ob_free(right_last_row_buf_);
    right_last_row_buf_ = NULL;
  }
  if (NULL != left_last_row_buf_)
  {
    ob_free(left_last_row_buf_);
    left_last_row_buf_ = NULL;
  }
  if (OB_SUCCESS != (ret = ObDoubleChildrenPhyOperator::close()))
  {
    TBSYS_LOG(WARN, "failed to close child op,ret=%d", ret);
  }
  row_desc_ = NULL;
  cur_first_query_row_ = NULL;
  cur_second_query_row_ = NULL;
  left_ret_ = OB_SUCCESS;
  right_ret_ = OB_SUCCESS;
  last_cmp_ = -1;
  got_first_row_ = false;
  return ret;
}
int ObMergeExcept::get_next_row(const ObRow *&row)
{
  OB_ASSERT(get_next_row_func_);
  return (this->*(this->ObMergeExcept::get_next_row_func_))(row);
}
int ObMergeExcept::do_distinct(ObPhyOperator *op, const ObRow *other, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  while (true)
  {
    ret = op->get_next_row(row);
    if (OB_SUCCESS == ret)
    {
      ret = compare(row, other, cmp);
      if (OB_SUCCESS == ret)
      {
        if (cmp != 0)
        {
          break;
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "failed to compare two row, ret=%d", ret);
        break;
      }
    }
    else if (OB_ITER_END == ret)
    {
      TBSYS_LOG(DEBUG, "reach the end of op when do distinct");
      break;
    }
    else
    {
      TBSYS_LOG(WARN, "failed to get_next_row,ret=%d",ret);
      break;
    }
  }
  return ret;
}

int ObMergeExcept::distinct_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObString left_last_compact_row;
  left_last_compact_row.assign(left_last_row_buf_, OB_ROW_BUF_SIZE);
  ObString right_last_compact_row;
  right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);

  int cmp = 0;
  if (got_first_row_)
  {
    left_ret_ = do_distinct(left_op_, &left_last_row_, cur_first_query_row_);
    if (left_ret_ != OB_SUCCESS && left_ret_ != OB_ITER_END)
    {
      ret = left_ret_;
      TBSYS_LOG(WARN, "failed to do_distinct on left_op, ret=%d", left_ret_);
    }
  }
  if (OB_SUCCESS == ret)
  {
    while (true)
    {
      if (OB_SUCCESS == left_ret_ && OB_SUCCESS == right_ret_)
      {
        ret = compare(cur_first_query_row_, cur_second_query_row_, cmp);
        if (OB_SUCCESS == ret)
        {
          if (cmp < 0)
          {
            //output
            row = cur_first_query_row_;
            got_first_row_ = true;
            left_last_compact_row.assign(left_last_row_buf_, OB_ROW_BUF_SIZE);
            // save cur_first_query_row_ to left_last_row_
            if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_first_query_row_, left_last_compact_row, left_last_row_)))
            {
              TBSYS_LOG(WARN, "failed to save cur_first_query_row_ to last row, ret=%d",ret);
            }
            break;
          }
          else if (cmp == 0)
          {
            left_last_compact_row.assign(left_last_row_buf_, OB_ROW_BUF_SIZE);
            right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);
            // save cur_first_query_row_ to left_last_row_
            if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_first_query_row_, left_last_compact_row, left_last_row_)))
            {
              TBSYS_LOG(WARN, "failed to save cur_first_query_row_ to left last row, ret=%d",ret);
              break;
            }
            // save cur_second_query_row_ to right_last_row_
            else if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_second_query_row_, right_last_compact_row, right_last_row_)))
            {
              TBSYS_LOG(WARN, "failed to save cur_second_query_row_ to right last row, ret=%d",ret);
              break;
            }
            else
            {
              left_ret_ = do_distinct(left_op_, &left_last_row_, cur_first_query_row_);
              right_ret_ = do_distinct(right_op_, &right_last_row_, cur_second_query_row_);
              continue;
            }

          }
          else
          {
            right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);
            // save cur_second_query_row_ to right_last_row_
            if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_second_query_row_, right_last_compact_row, right_last_row_)))
            {
              TBSYS_LOG(WARN, "failed to save cur_second_query_row_ to right last row, ret=%d",ret);
              break;
            }
            else
            {
              right_ret_ = do_distinct(right_op_, &right_last_row_, cur_second_query_row_);
              continue;
            }

          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to compare two row, ret=%d", ret);
          break;
        }

      }
      else if (OB_SUCCESS == left_ret_ && OB_ITER_END == right_ret_)
      {
        //output
        row = cur_first_query_row_;
        got_first_row_ = true;
        left_last_compact_row.assign(left_last_row_buf_, OB_ROW_BUF_SIZE);
        // save cur_first_query_row_ to left_last_row_
        if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_first_query_row_, left_last_compact_row, left_last_row_)))
        {
          TBSYS_LOG(WARN, "failed to save cur_first_query_row_ to last row, ret=%d",ret);
        }
        break;
      }
      else if (OB_ITER_END == left_ret_ && (OB_SUCCESS == right_ret_ || OB_ITER_END == right_ret_))
      {
        ret = OB_ITER_END;
        break;
      }
      else// unexpected branch
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "failed to do distinct, ret=%d,left_ret_=%d, right_ret_=%d", ret, left_ret_, right_ret_);
        break;
      }
    }//while
  }
  else
  {
    TBSYS_LOG(WARN, "failed to do distinct on left_op, ret=%d", ret);
  }
  return ret;
}
int ObMergeExcept::all_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObString right_last_compact_row;
  right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);

  int cmp = 0;
  if (got_first_row_)
  {
    left_ret_ = left_op_->get_next_row(cur_first_query_row_);
    if (left_ret_ != OB_SUCCESS && left_ret_ != OB_ITER_END)
    {
      ret = left_ret_;
      TBSYS_LOG(WARN, "failed to get_next_row on left_op, ret=%d", left_ret_);
    }
  }
  if (OB_SUCCESS == ret)
  {
    while (true)
    {
      if (OB_SUCCESS == left_ret_ && OB_SUCCESS == right_ret_)
      {
        ret = compare(cur_first_query_row_, cur_second_query_row_, cmp);
        if (OB_SUCCESS == ret)
        {
          if (cmp < 0)
          {
            //output
            row = cur_first_query_row_;
            got_first_row_ = true;
            break;
          }
          else if (cmp == 0)
          {
            left_ret_ = left_op_->get_next_row(cur_first_query_row_);
            right_ret_ = right_op_->get_next_row(cur_second_query_row_);
            continue;
          }
          else
          {
            right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);
            // save cur_second_query_row_ to right_last_row_
            if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_second_query_row_, right_last_compact_row, right_last_row_)))
            {
              TBSYS_LOG(WARN, "failed to save cur_second_query_row_ to right last row, ret=%d",ret);
              break;
            }
            else
            {
              right_ret_ = do_distinct(right_op_, &right_last_row_, cur_second_query_row_);
              continue;
            }

          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to compare two row, ret=%d", ret);
          break;
        }

      }
      else if (OB_SUCCESS == left_ret_ && OB_ITER_END == right_ret_)
      {
        //output
        row = cur_first_query_row_;
        got_first_row_ = true;
        break;
      }
      else if (OB_ITER_END == left_ret_ && (OB_SUCCESS == right_ret_ || OB_ITER_END == right_ret_))
      {
        ret = OB_ITER_END;
        break;
      }
      else// unexpected branch
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "failed to get next row, ret=%d, left_ret_=%d, right_ret_=%d", ret, left_ret_, right_ret_);
        break;
      }
    }//while
  }
  else
  {
    TBSYS_LOG(WARN, "failed to get next row  on left_op, ret=%d", ret);
  }
  return ret;
}
int ObMergeExcept::cons_row_desc()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *left_row_desc = NULL;
  const ObRowDesc *right_row_desc = NULL;
  if (OB_SUCCESS != (ret = left_op_->get_row_desc(left_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get row desc of left op, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = right_op_->get_row_desc(right_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get row desc of right op, ret=%d", ret);
  }
  else
  {
    row_desc_ = left_row_desc;
  }
  return ret;
}
int ObMergeExcept::compare(const ObRow *row1, const ObRow *row2, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  int64_t column_num = row1->get_column_num();
  int64_t i = 0;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  for (;i < column_num ; ++i)
  {
    ObExprObj expr_obj1;
    ObExprObj expr_obj2;
    if (OB_SUCCESS != (ret = row1->raw_get_cell(i, cell1, table_id, column_id)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d",ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = row2->raw_get_cell(i, cell2, table_id, column_id)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d",ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else
    {
      expr_obj1.assign(*cell1);
      expr_obj2.assign(*cell2);
      ret = expr_obj1.compare(expr_obj2, cmp);
      if (OB_SUCCESS == ret)
      {
        if (cmp != 0)
        {
          break;
        }
      }
      else // (OB_RESULT_UNKNOWN == ret)
      {
        cmp = -1;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int ObMergeExcept::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (NULL == row_desc_)
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else if (OB_UNLIKELY(row_desc_->get_column_num() <= 0))
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = row_desc_;
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObMergeExcept, PHY_MERGE_EXCEPT);
  }
}

int64_t ObMergeExcept::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MergeExcept()\n");
  if (NULL != left_op_)
  {
    databuff_printf(buf, buf_len, pos, "ExceptLeftChild=\n");
    pos += left_op_->to_string(buf+pos, buf_len-pos);
  }
  if (NULL != right_op_)
  {
    databuff_printf(buf, buf_len, pos, "ExceptRightChild=\n");
    pos += right_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObMergeExcept)
{
  int ret = OB_SUCCESS;
  reset();
  ObSetOperator::assign(other);
  if (distinct_)
  {
    get_next_row_func_ = &ObMergeExcept::distinct_get_next_row;
  }
  else
  {
    get_next_row_func_ = &ObMergeExcept::all_get_next_row;
  }
  return ret;
}
