/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * /home/jianming.cjq/ss_g/src/sql/ob_multiple_get_merge.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_multiple_get_merge.h"
#include "common/ob_row_fuse.h"

using namespace oceanbase;
using namespace sql;

void ObMultipleGetMerge::reset()
{
  ObMultipleMerge::reset();
}

void ObMultipleGetMerge::reuse()
{
  ObMultipleMerge::reuse();
}

int ObMultipleGetMerge::open()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *row_desc = NULL;

  for (int32_t i = 0; OB_SUCCESS == ret && i < child_num_; i ++)
  {
    if (OB_SUCCESS != (ret = child_array_[i]->open()))
    {
      TBSYS_LOG(WARN, "fail to open child i[%d]:ret[%d]", i, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
    }
    else
    {
      cur_row_.set_row_desc(*row_desc);
    }
  }

  return ret;
}

int ObMultipleGetMerge::close()
{
  int ret = OB_SUCCESS;

  for (int32_t i = 0; i < child_num_; i ++)
  {
    if (OB_SUCCESS != (ret = child_array_[i]->close()))
    {
      TBSYS_LOG(WARN, "fail to close child i[%d]:ret[%d]", i, ret);
    }
  }

  return ret;
}

int ObMultipleGetMerge::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *tmp_row = NULL;
  bool is_row_empty = true;
  if (child_num_ <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "has no child");
  }
  while (OB_SUCCESS == ret)
  {
    is_row_empty = true;
    cur_row_.reset(false, is_ups_row_ ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);
    for (int32_t i = 0; OB_SUCCESS == ret && i < child_num_; i++)
    {
      ret = child_array_[i]->get_next_row(tmp_row);
      if (OB_SUCCESS != ret)
      {
        if (OB_ITER_END == ret)
        {
          if ( 0 != i)
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "should not be iter end[%d]", i);
          }
          else
          {
            int err = OB_SUCCESS;
            for (int32_t k = 1; OB_SUCCESS == err && k < child_num_; k ++)
            {
              err = child_array_[k]->get_next_row(tmp_row);
              if (OB_ITER_END != err)
              {
                err = OB_ERR_UNEXPECTED;
                ret = err;
                TBSYS_LOG(WARN, "should be iter end[%d], i=%d", k, i);
              }
            }
          }
        }
        else
        {
          if (!IS_SQL_ERR(ret))
          {
            TBSYS_LOG(WARN, "fail to get next row:ret[%d]", ret);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "multiple get merge child[%d] row[%s]", i, to_cstring(*tmp_row));
        if (OB_SUCCESS != (ret = common::ObRowFuse::fuse_row(*tmp_row, cur_row_, is_row_empty, is_ups_row_)))
        {
          TBSYS_LOG(WARN, "fail to fuse row:ret[%d]", ret);
        }
        else if (0 == i)
        {
          if (OB_SUCCESS != (ret = copy_rowkey(*tmp_row, cur_row_, false)))
          {
            TBSYS_LOG(WARN, "fail to copy rowkey:ret[%d]", ret);
          }
        }
      }

    }
    if (OB_SUCCESS == ret)
    {
      if (is_ups_row_ || !is_row_empty)
      {
        break;
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    row = &cur_row_;
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObMultipleGetMerge, PHY_MULTIPLE_GET_MERGE);
  }
}

int64_t ObMultipleGetMerge::to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MuitipleGetMerge(children_num=%d)\n",
                  child_num_);
  for (int32_t i = 0; i < child_num_; ++i)
  {
    databuff_printf(buf, buf_len, pos, "Child%d:\n", i);
    if (NULL != child_array_[i])
    {
      pos += child_array_[i]->to_string(buf+pos, buf_len-pos);
    }
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObMultipleGetMerge)
{
  int ret = OB_SUCCESS;
  cur_row_.reset(false, ObRow::DEFAULT_NULL);
  ret = ObMultipleMerge::assign(other);
  return ret;
}

