/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_values.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_values.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObValues::ObValues()
{
}

ObValues::~ObValues()
{
}

void ObValues::reset()
{
  row_desc_.reset();
  //curr_row_.reset(false, ObRow::DEFAULT_NULL);
  row_store_.clear();
  ObSingleChildPhyOperator::reset();
}

void ObValues::reuse()
{
  row_desc_.reset();
  //curr_row_.reset(false, ObRow::DEFAULT_NULL);
  row_store_.clear();
  ObSingleChildPhyOperator::reset();
}

int ObValues::set_row_desc(const common::ObRowDesc &row_desc)
{
  TBSYS_LOG(DEBUG, "DEBUG ObValues set row desc %s", to_cstring(row_desc));
  row_desc_ = row_desc;
  return OB_SUCCESS;
}

int ObValues::add_values(const common::ObRow &value)
{
  const ObRowStore::StoredRow *stored_row = NULL;
  return row_store_.add_row(value, stored_row);
}

int ObValues::open()
{
  int ret = OB_SUCCESS;
  curr_row_.set_row_desc(row_desc_);
  if (NULL != child_op_)
  {
    if (OB_SUCCESS != (ret = load_data()))
    {
      TBSYS_LOG(WARN, "failed to load data from child op, err=%d", ret);
    }
  }
  return ret;
}

int ObValues::close()
{
  row_store_.clear();
  return OB_SUCCESS;
}

int ObValues::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.get_next_row(curr_row_)))
  {
    if (OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "failed to get next row from row store, err=%d", ret);
    }
  }
  else
  {
    row = &curr_row_;
  }
  return ret;
}

int ObValues::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = &row_desc_;
  return OB_SUCCESS;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObValues, PHY_VALUES);
  }
}

int64_t ObValues::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Values(row_store=%s)\n", to_cstring(row_store_));
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObValues)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObValues);
  reset();
  ObRowStore *store_ptr = const_cast<ObRowStore*>(&o_ptr->row_store_);
  if ((ret = row_desc_.assign(o_ptr->row_desc_)) == OB_SUCCESS)
  {
    ObRow row;
    int64_t cur_size_counter;
    store_ptr->reset_iterator();
    while ((ret = store_ptr->get_next_row(row)) == OB_SUCCESS)
    {
      if ((ret = row_store_.add_row(row, cur_size_counter)) != OB_SUCCESS)
      {
        break;
      }
    }
    if (ret == OB_ITER_END)
    {
      ret = OB_SUCCESS;
    }
    store_ptr->reset_iterator();
  }
  return ret;
}

DEFINE_SERIALIZE(ObValues)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS != (ret = row_desc_.serialize(buf, buf_len, tmp_pos)))
  {
    TBSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, tmp_pos);
  }
  else if (OB_SUCCESS != (ret = row_store_.serialize(buf, buf_len, tmp_pos)))
  {
    TBSYS_LOG(WARN, "serialize row_store fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, tmp_pos);
  }
  else
  {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObValues)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS != (ret = row_desc_.deserialize(buf, data_len, tmp_pos)))
  {
    TBSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p data_len=%ld pos=%ld", ret, buf, data_len, tmp_pos);
  }
  else if (OB_SUCCESS != (ret = row_store_.deserialize(buf, data_len, tmp_pos)))
  {
    TBSYS_LOG(WARN, "serialize row_store fail ret=%d buf=%p data_len=%ld pos=%ld", ret, buf, data_len, tmp_pos);
  }
  else
  {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObValues)
{
  return (row_desc_.get_serialize_size() + row_store_.get_serialize_size());
}

int ObValues::load_data()
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  const ObRow *row = NULL;
  const ObRowDesc *row_desc = NULL;
  const ObRowStore::StoredRow *stored_row = NULL;

  if (OB_SUCCESS != (ret = child_op_->open()))
  {
    TBSYS_LOG(WARN, "fail to open rpc scan:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = child_op_->get_row_desc(row_desc)))
    {
      TBSYS_LOG(WARN, "fail to get row_desc:ret[%d]", ret);
    }
    else
    {
      row_desc_ = *row_desc;
    }
  }

  while (OB_SUCCESS == ret)
  {
    ret = child_op_->get_next_row(row);
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get next row from rpc scan");
    }
    else
    {
      TBSYS_LOG(DEBUG, "load data from child, row=%s", to_cstring(*row));
      if (OB_SUCCESS != (ret = row_store_.add_row(*row, stored_row)))
      {
        TBSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
      }
    }
  }
  if (OB_SUCCESS != (err = child_op_->close()))
  {
    TBSYS_LOG(WARN, "fail to close rpc scan:err[%d]", err);
    if (OB_SUCCESS == ret)
    {
      ret = err;
    }
  }
  return ret;
}
