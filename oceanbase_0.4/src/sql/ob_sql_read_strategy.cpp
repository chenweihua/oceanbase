/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_read_strategy.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_sql_read_strategy.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObSqlReadStrategy::ObSqlReadStrategy()
  :simple_in_filter_list_(common::OB_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_READ_STRATEGY)),
   simple_cond_filter_list_(common::OB_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_READ_STRATEGY)),
   rowkey_info_(NULL)
{
  memset(start_key_mem_hold_, 0, sizeof(start_key_mem_hold_));
  memset(end_key_mem_hold_, 0, sizeof(end_key_mem_hold_));
}

ObSqlReadStrategy::~ObSqlReadStrategy()
{
  this->destroy();
}

void ObSqlReadStrategy::reset()
{
  simple_in_filter_list_.clear();
  simple_cond_filter_list_.clear();
  rowkey_info_ = NULL;
  memset(start_key_mem_hold_, 0, sizeof(start_key_mem_hold_));
  memset(end_key_mem_hold_, 0, sizeof(end_key_mem_hold_));
}

int ObSqlReadStrategy::find_single_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found)
{
  static const bool single_row_only = true;
  bool found_start = false;
  bool found_end = false;
  int ret = find_closed_column_range(real_val, idx, column_id, found_start, found_end, single_row_only);
  found = (found_start && found_end);
  return ret;
}

int ObSqlReadStrategy::find_scan_range(ObNewRange &range, bool &found, bool single_row_only)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  uint64_t column_id = OB_INVALID_ID;
  bool found_start = false;
  bool found_end = false;
  OB_ASSERT(NULL != rowkey_info_);
  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    start_key_objs_[idx].set_min_value();
    end_key_objs_[idx].set_max_value();
  }

  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    if (OB_SUCCESS != (ret = rowkey_info_->get_column_id(idx, column_id)))
    {
      TBSYS_LOG(WARN, "fail to get column id ret=%d, idx=%ld, column_id=%ld", ret, idx, column_id);
      break;
    }
    else
    {
      if (OB_SUCCESS != (ret = find_closed_column_range(true, idx, column_id, found_start, found_end, single_row_only)))
      {
        TBSYS_LOG(WARN, "fail to find closed column range for column %lu", column_id);
        break;
      }
    }
    if (!found_start || !found_end)
    {
      break; // no more search
    }
  }
  range.start_key_.assign(start_key_objs_, rowkey_info_->get_size());
  range.end_key_.assign(end_key_objs_, rowkey_info_->get_size());

  if (0 == idx && (!found_start) && (!found_end))
  {
    found = false;
  }
  return ret;
}

int ObSqlReadStrategy::find_closed_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found_start, bool &found_end, bool single_row_only)
{
  int ret = OB_SUCCESS;
  int i = 0;
  uint64_t cond_cid = OB_INVALID_ID;
  int64_t cond_op = T_MIN_OP;
  ObObj cond_val;
  ObObj cond_start;
  ObObj cond_end;
  const ObRowkeyColumn *column = NULL;
  found_end = false;
  found_start = false;
  for (i = 0; i < simple_cond_filter_list_.count(); i++)
  {
    if (simple_cond_filter_list_.at(i).is_simple_condition(real_val, cond_cid, cond_op, cond_val))
    {
      if ((cond_cid == column_id) &&
          (NULL != (column = rowkey_info_->get_column(idx))))
      {
        ObObjType target_type = column->type_;
        ObObj expected_type;
        ObObj promoted_obj;
        const ObObj *p_promoted_obj = NULL;
        ObObjType source_type = cond_val.get_type();
        expected_type.set_type(target_type);
        ObString string;
        char *varchar_buff = NULL;
        if (target_type == ObVarcharType && source_type != ObVarcharType)
        {
          if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
          }
          else
          {
            string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
            promoted_obj.set_varchar(string);
            if (OB_SUCCESS != (ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj)))
            {
                TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
                ob_free(varchar_buff);
                varchar_buff = NULL;
                break;
            }
            else
            {
              switch (cond_op)
              {
                case T_OP_LT:
                case T_OP_LE:
                  if (end_key_objs_[idx].is_max_value())
                  {
                    end_key_objs_[idx] = *p_promoted_obj;
                    end_key_mem_hold_[idx] = varchar_buff;
                    found_end = true;
                  }
                  else if (*p_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                    else
                    {
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                  }
                  else
                  {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                  break;
                case T_OP_GT:
                case T_OP_GE:
                  if (start_key_objs_[idx].is_min_value())
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    found_start = true;
                    start_key_mem_hold_[idx] = varchar_buff;
                  }
                  else if (*p_promoted_obj > start_key_objs_[idx])
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    if (start_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(start_key_mem_hold_[idx]);
                      start_key_mem_hold_[idx] = varchar_buff;
                    }
                    else
                    {
                      start_key_mem_hold_[idx] = varchar_buff;
                    }
                  }
                  else
                  {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                  break;
                case T_OP_EQ:
                case T_OP_IS:
                  if (start_key_objs_[idx].is_min_value() && end_key_objs_[idx].is_max_value())
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    end_key_objs_[idx] = *p_promoted_obj;
                    // when free, we compare this two address, if equals, then release once
                    start_key_mem_hold_[idx] = varchar_buff;
                    end_key_mem_hold_[idx] = varchar_buff;
                    found_start = true;
                    found_end = true;
                  }
                  else if (start_key_objs_[idx] == end_key_objs_[idx])
                  {
                    if (*p_promoted_obj != start_key_objs_[idx])
                    {
                      TBSYS_LOG(WARN, "two different equal condition on the sanme column, column_id=%lu", column_id);
                    }
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                  else
                  {
                    // actually, if the eq condition is not between the previous range, we also can set range using eq condition,in this case,
                    // the scan range is actually a single-get, filter will filter-out the record,
                    // so, here , we set range to a single-get scan uniformly,contact to lide.wd@taobao.com
                    //if (*p_promoted_obj >= start_key_objs_[idx] && *p_promoted_obj <= end_key_objs_[idx])
                    //{
                      start_key_objs_[idx] = *p_promoted_obj;
                      end_key_objs_[idx] = *p_promoted_obj;
                      if (start_key_mem_hold_[idx] != NULL)
                      {
                        ob_free(start_key_mem_hold_[idx]);
                      }
                      start_key_mem_hold_[idx] = varchar_buff;
                      if (end_key_mem_hold_[idx] != NULL)
                      {
                        ob_free(end_key_mem_hold_[idx]);
                      }
                      end_key_mem_hold_[idx] = varchar_buff;
                    //}
                  }
                  break;
                default:
                  ob_free(varchar_buff);
                  varchar_buff = NULL;
                  TBSYS_LOG(WARN, "unexpected cond op: %ld", cond_op);
                  ret = OB_ERR_UNEXPECTED;
                  break;
              }
            }
          }
        }
        else
        {
          if (OB_SUCCESS != (ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj)))
          {
            TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
            break;
          }
          else
          {
            switch (cond_op)
            {
              case T_OP_LT:
              case T_OP_LE:
                if (end_key_objs_[idx].is_max_value())
                {
                  end_key_objs_[idx] = *p_promoted_obj;
                  found_end = true;
                }
                else
                {
                  if (*p_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                    }
                  }
                }
                break;
              case T_OP_GT:
              case T_OP_GE:
                if (start_key_objs_[idx].is_min_value())
                {
                  start_key_objs_[idx] = *p_promoted_obj;
                  found_start = true;
                }
                else
                {
                  if (*p_promoted_obj > start_key_objs_[idx])
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    if (start_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(start_key_mem_hold_[idx]);
                    }
                  }
                }
                break;
              case T_OP_EQ:
              case T_OP_IS:
                if (start_key_objs_[idx].is_min_value() && end_key_objs_[idx].is_max_value())
                {
                  start_key_objs_[idx] = *p_promoted_obj;
                  end_key_objs_[idx] = *p_promoted_obj;
                  found_start = true;
                  found_end = true;
                }
                else if (start_key_objs_[idx] == end_key_objs_[idx])
                {
                  if (*p_promoted_obj != start_key_objs_[idx])
                  {
                    TBSYS_LOG(WARN, "two different equal condition on the same column, column_id=%lu"
                        " start_key_objs_[idx]=%s, *p_promoted_obj=%s",
                        column_id, to_cstring(start_key_objs_[idx]), to_cstring(*p_promoted_obj));
                  }
                }
                else
                {
                  // actually, if the eq condition is not between the previous range, we also can set range using eq condition,in this case,
                  // the scan range is actually a single-get, filter will filter-out the record,
                  // so, here , we set range to a single-get scan uniformly,contact to lide.wd@taobao.com
                  //if (*p_promoted_obj >= start_key_objs_[idx] && *p_promoted_obj <= end_key_objs_[idx])
                  //{
                    start_key_objs_[idx] = *p_promoted_obj;
                    end_key_objs_[idx] = *p_promoted_obj;
                    if (start_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(start_key_mem_hold_[idx]);
                    }
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                    }
                  //}
                }
                break;
              default:
                TBSYS_LOG(WARN, "unexpected cond op: %ld", cond_op);
                ret = OB_ERR_UNEXPECTED;
                break;
            }
          }
        }
        if (single_row_only && cond_op != T_OP_EQ)
        {
          found_end = found_start = false;
        }
      }
    }
    else if ((!single_row_only) && simple_cond_filter_list_.at(i).is_simple_between(real_val, cond_cid, cond_op, cond_start, cond_end))
    {
      if (cond_cid == column_id)
      {
        OB_ASSERT(T_OP_BTW == cond_op);
        column = rowkey_info_->get_column(idx);
        ObObjType target_type;
        if (column == NULL)
        {
          TBSYS_LOG(WARN, "get column from rowkey_info failed, column = NULL");
        }
        else
        {
          target_type = column->type_;
          ObObj expected_type;
          expected_type.set_type(target_type);
          ObObj start_promoted_obj;
          ObString start_string;
          ObString end_string;
          char *varchar_buff = NULL;
          const ObObj *p_start_promoted_obj = NULL;
          ObObj end_promoted_obj;
          const ObObj *p_end_promoted_obj = NULL;
          ObObjType start_source_type = cond_start.get_type();
          ObObjType end_source_type = cond_end.get_type();
          if (target_type == ObVarcharType && start_source_type != ObVarcharType)
          {
            if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
            }
            else
            {
              start_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
              start_promoted_obj.set_varchar(start_string);
              if (OB_SUCCESS != (ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj)))
              {
                TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
                ob_free(varchar_buff);
                varchar_buff = NULL;
                break;
              }
              else
              {
                if (start_key_objs_[idx].is_min_value())
                {
                  start_key_objs_[idx] = *p_start_promoted_obj;
                  found_start = true;
                  start_key_mem_hold_[idx] = varchar_buff;
                }
                else if (*p_start_promoted_obj > start_key_objs_[idx])
                {
                  start_key_objs_[idx] = *p_start_promoted_obj;
                  if (start_key_mem_hold_[idx] != NULL)
                  {
                    ob_free(start_key_mem_hold_[idx]);
                    start_key_mem_hold_[idx] = varchar_buff;
                  }
                  else
                  {
                    start_key_mem_hold_[idx] = varchar_buff;
                  }
                }
                else
                {
                  ob_free(varchar_buff);
                  varchar_buff = NULL;
                }
              }
            }
          }
          else
          {
            if (OB_SUCCESS != (ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj)))
            {
              TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
            }
            else
            {
              if (start_key_objs_[idx].is_min_value())
              {
                start_key_objs_[idx] = *p_start_promoted_obj;
                found_start = true;
              }
              else
              {
                if (*p_start_promoted_obj > start_key_objs_[idx])
                {
                  start_key_objs_[idx] = *p_start_promoted_obj;
                  if (start_key_mem_hold_[idx] != NULL)
                  {
                    ob_free(start_key_mem_hold_[idx]);
                    start_key_mem_hold_[idx] = NULL;
                  }
                }
              }
            }
          }
          varchar_buff = NULL;
          if (OB_SUCCESS == ret)
          {
            if (target_type == ObVarcharType && end_source_type != ObVarcharType)
            {
              if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
              }
              else
              {
                end_key_mem_hold_[idx] = varchar_buff;
                end_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                end_promoted_obj.set_varchar(end_string);
                if (OB_SUCCESS != (ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj)))
                {
                  TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
                  ob_free(varchar_buff);
                  varchar_buff = NULL;
                  break;
                }
                else
                {
                  if (end_key_objs_[idx].is_max_value())
                  {
                    end_key_objs_[idx] = *p_end_promoted_obj;
                    found_end = true;
                    end_key_mem_hold_[idx] = varchar_buff;
                  }
                  else if (*p_end_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_end_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                    else
                    {
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                  }
                  else
                  {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                }
              }
            }
            else
            {
              if (OB_SUCCESS != (ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj)))
              {
                TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
              }
              else
              {
                if (end_key_objs_[idx].is_max_value())
                {
                  end_key_objs_[idx] = *p_end_promoted_obj;
                  found_end = true;
                }
                else
                {
                  if (*p_end_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_end_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                      end_key_mem_hold_[idx] = NULL;
                    }
                  }
                }
              }
            }
          }
				}
      }
    }

    if (ret != OB_SUCCESS)
    {
      break;
    }
    //if (found_start && found_end)
    //{
      /* we can break earlier here */
      //break;
    //}
  }
  return ret;
}

int ObSqlReadStrategy::add_filter(const ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;
  uint64_t cid = OB_INVALID_ID;
  int64_t op = T_INVALID;
  ObObj val1;
  ObObj val2;

  if (expr.is_simple_condition(false, cid, op, val1))
  {
    // TBSYS_LOG(DEBUG, "simple condition [%s]", to_cstring(expr));
    if (OB_SUCCESS != (ret = simple_cond_filter_list_.push_back(expr)))
    {
      TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
    }
  }
  else if (expr.is_simple_between(false, cid, op, val1, val2))
  {
    // TBSYS_LOG(DEBUG, "simple between condition [%s]", to_cstring(expr));
    if (OB_SUCCESS != (ret = simple_cond_filter_list_.push_back(expr)))
    {
      TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
    }
  }
  else
  {
    ObArray<ObRowkey> rowkey_array;
    common::PageArena<ObObj,common::ModulePageAllocator> rowkey_objs_allocator(
        PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_READ_STATEGY));
    if (true == expr.is_simple_in_expr(false, *rowkey_info_, rowkey_array, rowkey_objs_allocator))
    {
      // TBSYS_LOG(DEBUG, "simple in expr [%s]", to_cstring(expr));
      if (OB_SUCCESS != (ret = simple_in_filter_list_.push_back(expr)))
      {
        TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObSqlReadStrategy::find_rowkeys_from_equal_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &objs_allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  uint64_t column_id = OB_INVALID_ID;
  bool found = false;
  UNUSED(objs_allocator);
  OB_ASSERT(rowkey_info_->get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    start_key_objs_[idx].set_min_value();
    end_key_objs_[idx].set_max_value();
  }
  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    if (OB_SUCCESS != (ret = rowkey_info_->get_column_id(idx, column_id)))
    {
      TBSYS_LOG(WARN, "fail to get column id. idx=%ld, ret=%d", idx, ret);
      break;
    }
    else if (OB_SUCCESS != (ret = find_single_column_range(real_val, idx, column_id, found)))
    {
      TBSYS_LOG(WARN, "fail to find closed range for column %lu", column_id);
      break;
    }
    else if (!found)
    {
      break; // no more search
    }
  }/* end for */
  if (OB_SUCCESS == ret && found && idx == rowkey_info_->get_size())
  {
    ObRowkey rowkey;
    rowkey.assign(start_key_objs_, rowkey_info_->get_size());
    if (OB_SUCCESS != (ret = rowkey_array.push_back(rowkey)))
    {
      TBSYS_LOG(WARN, "fail to push rowkey to list. rowkey=%s, ret=%d", to_cstring(rowkey), ret);
    }
  }
  return ret;
}

int ObSqlReadStrategy::find_rowkeys_from_in_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, common::PageArena<ObObj,common::ModulePageAllocator> &objs_allocator)
{
  int ret = OB_SUCCESS;
  bool is_in_expr_with_rowkey = false;
  int i = 0;
  if (simple_in_filter_list_.count() > 1)
  {
    TBSYS_LOG(DEBUG, "simple in filter count[%ld]", simple_in_filter_list_.count());
    ret = OB_SUCCESS;
  }
  else
  {
    for (i = 0; i < simple_in_filter_list_.count(); i++)
    {
      // assume rowkey in sequence and all rowkey columns present
      if (false == (is_in_expr_with_rowkey = simple_in_filter_list_.at(i).is_simple_in_expr(real_val, *rowkey_info_, rowkey_array, objs_allocator)))
      {
        TBSYS_LOG(WARN, "fail to get rowkey(s) from in expression. ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "simple in expr rowkey_array count = %ld", rowkey_array.count());
      }
    }
  }
  // cast rowkey if needed
  if (OB_SUCCESS == ret && true == is_in_expr_with_rowkey)
  {
    char *in_rowkey_buf = NULL;
    int64_t total_used_buf_len = 0;
    int64_t used_buf_len = 0;
    int64_t rowkey_idx = 0;
    for (rowkey_idx = 0; rowkey_idx < rowkey_array.count(); rowkey_idx++)
    {
      bool need_buf = false;
      if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(*rowkey_info_, rowkey_array.at(rowkey_idx), need_buf)))
      {
        TBSYS_LOG(WARN, "err=%d", ret);
      }
      else if (need_buf)
      {
        if (NULL == in_rowkey_buf)
        {
          in_rowkey_buf = (char*)objs_allocator.alloc(OB_MAX_ROW_LENGTH);
        }
        if (NULL == in_rowkey_buf)
        {
          TBSYS_LOG(ERROR, "no memory");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if (OB_MAX_ROW_LENGTH <= total_used_buf_len)
        {
          TBSYS_LOG(WARN, "rowkey has too much varchar. len=%ld", total_used_buf_len);
        }
        else if (OB_SUCCESS != (ret = ob_cast_rowkey(*rowkey_info_, rowkey_array.at(rowkey_idx),
                in_rowkey_buf, OB_MAX_ROW_LENGTH - total_used_buf_len, used_buf_len)))
        {
          TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
        }
        else
        {
          total_used_buf_len = used_buf_len;
          in_rowkey_buf += used_buf_len;
        }
      }
    }
  }

  return ret;
}

int ObSqlReadStrategy::get_read_method(ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &rowkey_objs_allocator, int32_t &read_method)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = find_rowkeys_from_in_expr(false, rowkey_array, rowkey_objs_allocator)))
    {
      TBSYS_LOG(WARN, "fail to find rowkeys in IN operator. ret=%d", ret);
    }
    else if (rowkey_array.count() > 0)
    {
      read_method = USE_GET;
    }
    else if (OB_SUCCESS != (ret = find_rowkeys_from_equal_expr(false, rowkey_array, rowkey_objs_allocator)))
    {
      TBSYS_LOG(WARN, "fail to find rowkeys in equal where operator. ret=%d", ret);
    }
    else if (rowkey_array.count() == 1)
    {
      read_method = USE_GET;
    }
    else
    {
      read_method = USE_SCAN;
    }
  }
  return ret;
}

void ObSqlReadStrategy::destroy()
{
  for (int i = 0 ;i < OB_MAX_ROWKEY_COLUMN_NUMBER; ++i)
  {
    if (start_key_mem_hold_[i] != NULL && end_key_mem_hold_[i] != NULL && start_key_mem_hold_[i] == end_key_mem_hold_[i])
    {
      // release only once on the same memory block
      ob_free(start_key_mem_hold_[i]);
      start_key_mem_hold_[i] = NULL;
      end_key_mem_hold_[i] = NULL;
    }
    else
    {
      if (start_key_mem_hold_[i] != NULL)
      {
        ob_free(start_key_mem_hold_[i]);
        start_key_mem_hold_[i] = NULL;
      }
      if (end_key_mem_hold_[i] != NULL)
      {
        ob_free(end_key_mem_hold_[i]);
        end_key_mem_hold_[i] = NULL;
      }
    }
  }
}

int ObSqlReadStrategy::assign(const ObSqlReadStrategy *other, ObPhyOperator *owner_op)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObSqlReadStrategy);
  reset();
  for (int64_t i = 0; ret == OB_SUCCESS && i < o_ptr->simple_in_filter_list_.count(); i++)
  {
    if ((ret = simple_in_filter_list_.push_back(o_ptr->simple_in_filter_list_.at(i))) == OB_SUCCESS)
    {
      if (owner_op)
      {
        simple_in_filter_list_.at(i).set_owner_op(owner_op);
      }
    }
    else
    {
      break;
    }
  }
  for (int64_t i = 0; ret == OB_SUCCESS && i < o_ptr->simple_cond_filter_list_.count(); i++)
  {
    if ((ret = simple_cond_filter_list_.push_back(o_ptr->simple_cond_filter_list_.at(i))) == OB_SUCCESS)
    {
      if (owner_op)
      {
        simple_cond_filter_list_.at(i).set_owner_op(owner_op);
      }
    }
    else
    {
      break;
    }
  }
  return ret;
}

int64_t ObSqlReadStrategy::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ReadStrategy(in_filter=");
  pos += simple_in_filter_list_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ", cond_filter=");
  pos += simple_cond_filter_list_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")\n");
  return pos;
}
