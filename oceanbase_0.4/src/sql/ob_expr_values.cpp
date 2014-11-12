/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_values.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_expr_values.h"
#include "ob_duplicate_indicator.h"
#include "common/utility.h"
#include "common/ob_obj_cast.h"
#include "common/hash/ob_hashmap.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
ObExprValues::ObExprValues()
  :values_(OB_TC_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_ARRAY)),
   from_deserialize_(false),
   check_rowkey_duplicat_(false),
   do_eval_when_serialize_(false)
{
}

ObExprValues::~ObExprValues()
{
}

void ObExprValues::reset()
{
  row_desc_.reset();
  row_desc_ext_.reset();
  values_.clear();
  row_store_.clear();
  //row_.reset(false, ObRow::DEFAULT_NULL);
  from_deserialize_ = false;
  check_rowkey_duplicat_ = false;
  do_eval_when_serialize_ = false;
}

void ObExprValues::reuse()
{
  row_desc_.reset();
  row_desc_ext_.reset();
  values_.clear();
  row_store_.clear();
  //row_.reset(false, ObRow::DEFAULT_NULL);
  from_deserialize_ = false;
  check_rowkey_duplicat_ = false;
  do_eval_when_serialize_ = false;
}

int ObExprValues::set_row_desc(const common::ObRowDesc &row_desc, const common::ObRowDescExt &row_desc_ext)
{
  row_desc_ = row_desc;
  row_desc_ext_ = row_desc_ext;
  return OB_SUCCESS;
}

int ObExprValues::add_value(const ObSqlExpression &v)
{
  int ret = OB_SUCCESS;
  if ((ret = values_.push_back(v)) == OB_SUCCESS)
  {
    values_.at(values_.count() - 1).set_owner_op(this);
  }
  return ret;
}

int ObExprValues::open()
{
  int ret = OB_SUCCESS;
  if (from_deserialize_)
  {
    row_store_.reset_iterator();
    row_.set_row_desc(row_desc_);
    // pass
  }
  else if (0 >= row_desc_.get_column_num()
      || 0 >= row_desc_ext_.get_column_num())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "row_desc not init");
  }
  else if (0 >= values_.count())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "values not init");
  }
  else
  {
    row_.set_row_desc(row_desc_);
    row_store_.reuse();
    if (OB_SUCCESS != (ret = eval()))
    {
      TBSYS_LOG(WARN, "failed to eval exprs, err=%d", ret);
    }
  }
  return ret;
}

int ObExprValues::close()
{
  if (from_deserialize_)
  {
    row_store_.reset_iterator();
  }
  else
  {
    row_store_.reuse();
  }
  return OB_SUCCESS;
}

int ObExprValues::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.get_next_row(row_)))
  {
    if (OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "failed to get next row from row store, err=%d", ret);
    }
  }
  else
  {
    row = &row_;
  }
  return ret;
}

int ObExprValues::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = &row_desc_;
  return OB_SUCCESS;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObExprValues, PHY_EXPR_VALUES);
  }
}

int64_t ObExprValues::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ExprValues(values_num=%ld, values=",
                  values_.count());
  pos += values_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ", row_desc=");
  pos += row_desc_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")\n");
  return pos;
}

int ObExprValues::eval()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(0 < values_.count());
  OB_ASSERT(0 < row_desc_.get_column_num());
  OB_ASSERT(0 == (values_.count() % row_desc_.get_column_num()));
  ModuleArena buf(OB_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
  char* varchar_buff = NULL;
  if (NULL == (varchar_buff = buf.alloc(OB_MAX_VARCHAR_LENGTH)))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "No memory");
  }
  else
  {
    const ObRowStore::StoredRow *stored_row = NULL;
    ObDuplicateIndicator *indicator = NULL;
    int64_t col_num = row_desc_.get_column_num();
    int64_t row_num = values_.count() / col_num;
    // RowKey duplication checking doesn't need while 1 row only
    if (check_rowkey_duplicat_ && row_num > 1)
    {
      void *ind_buf = NULL;
      if (row_desc_.get_rowkey_cell_count() <= 0)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "RowKey is empty, ret=%d", ret);
      }
      else if ((ind_buf = buf.alloc(sizeof(ObDuplicateIndicator))) == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "Malloc ObDuplicateIndicator failed, ret=%d", ret);
      }
      else
      {
        indicator = new (ind_buf) ObDuplicateIndicator();
        if ((ret = indicator->init(row_num)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Init ObDuplicateIndicator failed, ret=%d", ret);
        }
      }
    }
    for (int64_t i = 0; OB_SUCCESS == ret && i < values_.count(); i+=col_num) // for each row
    {
      ObRow val_row;
      val_row.set_row_desc(row_desc_);
      ObString varchar;
      ObObj casted_cell;
      for (int64_t j = 0; OB_SUCCESS == ret && j < col_num; ++j)
      {
        varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
        casted_cell.set_varchar(varchar); // reuse the varchar buffer
        const ObObj *single_value = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ObObj tmp_value;
        ObObj data_type;
        ObSqlExpression &val_expr = values_.at(i+j);
        if ((ret = val_expr.calc(val_row, single_value)) != OB_SUCCESS) // the expr should be a const expr here
        {
          TBSYS_LOG(WARN, "Calculate value result failed, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = row_desc_ext_.get_by_idx(j, table_id, column_id, data_type)))
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "Failed to get column, err=%d", ret);
        }
        /*
        else if (0 < row_desc_.get_rowkey_cell_count()
                 && j < row_desc_.get_rowkey_cell_count()
                 && single_value->is_null())
        {
          TBSYS_LOG(USER_ERROR, "primary key can not be null");
          ret = OB_ERR_INSERT_NULL_ROWKEY;
        }
        */
        else if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
        {
          TBSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = ob_write_obj(buf, *single_value, tmp_value)))
        {
          TBSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
        }
        else if ((ret = val_row.set_cell(table_id, column_id, tmp_value)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Add value to ObRow failed");
        }
        else
        {
          //TBSYS_LOG(DEBUG, "i=%ld j=%ld cell=%s", i, j, to_cstring(tmp_value));
        }
      } // end for
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if (OB_SUCCESS != (ret = row_store_.add_row(val_row, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
        }
        else if (indicator)
        {
          const ObRowkey *rowkey = NULL;
          bool is_dup = false;
          if ((ret = val_row.get_rowkey(rowkey)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Get RowKey failed, err=%d", ret);
          }
          else if ((ret = indicator->have_seen(*rowkey, is_dup)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
          }
          else if (is_dup)
          {
            ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
            TBSYS_LOG(USER_ERROR, "Duplicate entry \'%s\' for key \'PRIMARY\'", to_cstring(*rowkey));
          }
          TBSYS_LOG(INFO, "check rowkey isdup is %c rowkey=%s", is_dup?'Y':'N', to_cstring(*rowkey));
        }
      }
    }   // end for
    if (indicator)
    {
      indicator->~ObDuplicateIndicator();
    }
  }
  return ret;
}

PHY_OPERATOR_ASSIGN(ObExprValues)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObExprValues);
  reset();
  row_desc_ = o_ptr->row_desc_;
  row_desc_ext_ = o_ptr->row_desc_ext_;

  values_.reserve(o_ptr->values_.count());
  for (int64_t i = 0; i < o_ptr->values_.count(); i++)
  {
    if ((ret = this->values_.push_back(o_ptr->values_.at(i))) == OB_SUCCESS)
    {
      this->values_.at(i).set_owner_op(this);
    }
    else
    {
      break;
    }
  }
  do_eval_when_serialize_ = o_ptr->do_eval_when_serialize_;
  check_rowkey_duplicat_ = o_ptr->check_rowkey_duplicat_;
  // Does not need to assign row_store_, because this function is used by MS only before opening
  return ret;
}

DEFINE_SERIALIZE(ObExprValues)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (do_eval_when_serialize_)
  {
    if (OB_SUCCESS != (ret = (const_cast<ObExprValues*>(this))->open()))
    {
      TBSYS_LOG(WARN, "failed to open expr_values, err=%d", ret);
    }
  }

  if (OB_LIKELY(OB_SUCCESS == ret))
  {
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
  }
  if (do_eval_when_serialize_)
  {
    (const_cast<ObExprValues*>(this))->close();
  }
  return ret;
}

DEFINE_DESERIALIZE(ObExprValues)
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
    from_deserialize_ = true;
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObExprValues)
{
  return (row_desc_.get_serialize_size() + row_store_.get_serialize_size());
}
