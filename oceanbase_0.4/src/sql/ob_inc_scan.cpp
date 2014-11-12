/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "common/ob_define.h"
#include "ob_inc_scan.h"
#include "common/utility.h"
#include "common/ob_trace_log.h"
#include "common/ob_packet.h"


namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObIncScan, PHY_INC_SCAN);
  }
}
namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    ObGetParamPool& get_get_param_pool()
    {
      static ObGetParamPool get_param_pool;
      return get_param_pool;
    }

    ObScanParamPool& get_scan_param_pool()
    {
      static ObScanParamPool scan_param_pool;
      return scan_param_pool;
    }

    ObIncScan::ObIncScan()
      : lock_flag_(LF_NONE), scan_type_(ST_NONE),
        get_param_guard_(get_get_param_pool()),
        scan_param_guard_(get_scan_param_pool()),
        get_param_(NULL),
        scan_param_(NULL),
        values_subquery_id_(common::OB_INVALID_ID),
        cons_get_param_with_rowkey_(false),
        hotspot_(false)
    {
    }

    ObIncScan::~ObIncScan()
    {
    }

    void ObIncScan::reset()
    {
      lock_flag_ = LF_NONE;
      scan_type_ = ST_NONE;
      get_param_ = NULL;
      scan_param_ = NULL;
      values_subquery_id_ = common::OB_INVALID_ID;
      cons_get_param_with_rowkey_ = false;
    }

    void ObIncScan::reuse()
    {
      lock_flag_ = LF_NONE;
      scan_type_ = ST_NONE;
      get_param_ = NULL;
      scan_param_ = NULL;
      values_subquery_id_ = common::OB_INVALID_ID;
      cons_get_param_with_rowkey_ = false;
    }

    ObGetParam* ObIncScan::get_get_param()
    {
      ObGetParam* get_param = (get_param_?: get_param_ = get_get_param_pool().get(get_param_guard_));
      if (NULL != get_param)
      {
        get_param->set_deep_copy_args(true);
      }
      return get_param;
    }

    ObScanParam* ObIncScan::get_scan_param()
    {
      return scan_param_?: scan_param_ = get_scan_param_pool().get(scan_param_guard_);
    }

    int ObIncScan::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, lock_flag_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_bool(buf, buf_len, new_pos, hotspot_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, scan_type_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (ST_MGET == scan_type_)
      {
        if (OB_UNLIKELY(common::OB_INVALID_ID == values_subquery_id_))
        {
          err = OB_NOT_INIT;
          TBSYS_LOG(ERROR, "values is invalid");
        }
        else if (NULL == get_param_)
        {
          if (NULL == const_cast<ObIncScan*>(this)->get_get_param())
          {
            TBSYS_LOG(WARN, "failed to get get_param");
            err = OB_ALLOCATE_MEMORY_FAILED;
          }
        }
        if (OB_LIKELY(OB_SUCCESS == err))
        {
          get_param_->reset(true);
          if (OB_SUCCESS != (err = const_cast<ObIncScan*>(this)->create_get_param_from_values(get_param_)))
          {
            TBSYS_LOG(WARN, "failed to create get param, err=%d", err);
          }
          else
          {
            ObVersionRange version_range;
            version_range.border_flag_.set_inclusive_start();
            version_range.border_flag_.set_max_value();
            version_range.start_version_ = my_phy_plan_->get_curr_frozen_version() + 1;
            get_param_->set_version_range(version_range);
            TBSYS_LOG(DEBUG, "get_param=%s", to_cstring(*get_param_));
            FILL_TRACE_LOG("inc_version=%s", to_cstring(version_range));
            if (OB_SUCCESS != (err = get_param_->serialize(buf, buf_len, new_pos)))
            {
              TBSYS_LOG(ERROR, "get_param.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
            }
            else
            {
              get_param_->reset(true);
            }
          }
        }
      }
      else if (ST_SCAN == scan_type_)
      {
        if (NULL == scan_param_)
        {
          err = OB_NOT_INIT;
          TBSYS_LOG(ERROR, "scan_param == NULL");
        }
        else if (OB_SUCCESS != (err = scan_param_->serialize(buf, buf_len, new_pos)))
        {
          TBSYS_LOG(ERROR, "scan_param.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        pos = new_pos;
      }
      return err;
    }

    int ObIncScan::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&lock_flag_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_bool(buf, data_len, new_pos, &hotspot_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&scan_type_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (ST_MGET == scan_type_)
      {
        if (NULL == get_get_param())
        {
          err = OB_MEM_OVERFLOW;
          TBSYS_LOG(ERROR, "get_param == NULL");
        }
        else if (OB_SUCCESS != (err = get_param_->deserialize(buf, data_len, new_pos)))
        {
          TBSYS_LOG(ERROR, "get_param.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
        }
      }
      else if (ST_SCAN == scan_type_)
      {
        if (NULL == get_scan_param())
        {
          err = OB_MEM_OVERFLOW;
          TBSYS_LOG(ERROR, "get_param == NULL");
        }
        else if (OB_SUCCESS != (err = scan_param_->deserialize(buf, data_len, new_pos)))
        {
          TBSYS_LOG(ERROR, "scan_param.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObIncScan::get_serialize_size() const
    {
      int64_t size = serialization::encoded_length_i32(lock_flag_) + serialization::encoded_length_i32(scan_type_);
      if (ST_MGET == scan_type_)
      {
        if (NULL != get_param_)
        {
          size += get_param_->get_serialize_size();
        }
      }
      else if (ST_SCAN == scan_type_)
      {
        if (NULL != scan_param_)
        {
          size += scan_param_->get_serialize_size();
        }
      }
      return size;
    }

    int ObIncScan::create_get_param_from_values(common::ObGetParam* get_param)
    {
      int ret = OB_SUCCESS;
      ObExprValues *values = NULL;
      if (common::OB_INVALID_ID == values_subquery_id_)
      {
        TBSYS_LOG(WARN, "values is invalid");
        ret = OB_NOT_INIT;
      }
      else if (NULL == (values = dynamic_cast<ObExprValues*>(my_phy_plan_->get_phy_query_by_id(values_subquery_id_))))
      {
        TBSYS_LOG(ERROR, "subquery not exist");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (ret = values->open()))
      {
        TBSYS_LOG(WARN, "failed to open values, err=%d", ret);
      }
      else
      {
        const ObRow *row = NULL;
        const ObRowkey *rowkey = NULL;
        ObCellInfo cell_info;
        const common::ObObj *cell = NULL;
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        uint64_t request_sign = 0;
        while (OB_SUCCESS == ret)
        {
          ret = values->get_next_row(row);
          if (OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
            break;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
            break;
          }
          else if (OB_SUCCESS != (ret = row->get_rowkey(rowkey)))
          {
            TBSYS_LOG(WARN, "failed to get rowkey, err=%d", ret);
            break;
          }
          else
          {
            int64_t cell_num = cons_get_param_with_rowkey_ ? rowkey->length() : row->get_column_num();
            for (int64_t i = 0; i < cell_num; ++i)
            {
              if (OB_SUCCESS != (ret = row->raw_get_cell(i, cell, tid, cid)))
              {
                TBSYS_LOG(WARN, "failed to get cell, err=%d i=%ld", ret, i);
                break;
              }
              else
              {
                cell_info.row_key_ = *rowkey;
                cell_info.table_id_ = tid;
                cell_info.column_id_ = cid;
                if (OB_SUCCESS != (ret = get_param->add_cell(cell_info)))
                {
                  TBSYS_LOG(WARN, "failed to add cell into get param, err=%d", ret);
                  break;
                }
              }
            } // end for
            if (0 == request_sign
                && hotspot_)
            {
              request_sign = rowkey->murmurhash2((uint32_t)tid);
            }
          }
        } // end while
        if (hotspot_)
        {
          ObPacket::tsi_req_sign() = request_sign;
        }
      }
      return ret;
    }

    PHY_OPERATOR_ASSIGN(ObIncScan)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObIncScan);
      lock_flag_ = o_ptr->lock_flag_;
      scan_type_ = o_ptr->scan_type_;
      scan_param_guard_.reset();
      get_param_guard_.reset();
      get_param_ = NULL;
      scan_param_ = NULL;
      values_subquery_id_ = o_ptr->values_subquery_id_;
      cons_get_param_with_rowkey_ = o_ptr->cons_get_param_with_rowkey_;
      hotspot_ = o_ptr->hotspot_;
      return ret;
    }
  }; // end namespace sql
}; // end namespace oceanbase
