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
#include "ob_transaction.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    // Transaction Isolcation Levels: READ-UNCOMMITTED READ-COMMITTED REPEATABLE-READ SERIALIZABLE
    int ObTransReq::set_isolation_by_name(const ObString &isolation)
    {
      int ret = OB_SUCCESS;
      if (OB_LIKELY(isolation == ObString::make_string("READ-COMMITTED")))
      {
        isolation_ = READ_COMMITED;
      }
      else if (isolation == ObString::make_string("READ-UNCOMMITTED"))
      {
        isolation_ = NO_LOCK;
      }
      else if (isolation == ObString::make_string("REPEATABLE-READ"))
      {
        isolation_ = REPEATABLE_READ;
      }
      else if (isolation == ObString::make_string("SERIALIZABLE"))
      {
        isolation_ = SERIALIZABLE;
      }
      else
      {
        TBSYS_LOG(WARN, "Unknown isolation level `%.*s'", isolation.length(), isolation.ptr());
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }

    int64_t ObTransReq::to_string(char* buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos,
                      "TransReq(type=%d,isolation=%x,start_time=%ld,timeout=%ld,idletime=%ld)",
                      type_, isolation_, start_time_, timeout_, idle_time_);
      return pos;
    }

    int ObTransReq::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, type_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, isolation_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, start_time_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, timeout_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, idle_time_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObTransReq::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, &type_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, &isolation_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &start_time_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &timeout_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &idle_time_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObTransReq::get_serialize_size(void) const
    {
      return serialization::encoded_length_i32(type_)
              + serialization::encoded_length_i32(isolation_)
              + serialization::encoded_length_i64(start_time_)
              + serialization::encoded_length_i64(timeout_)
              + serialization::encoded_length_i64(idle_time_);
    }

    void ObTransID::reset()
    {
      descriptor_ = INVALID_SESSION_ID;
      ups_.reset();
      start_time_us_ = 0;
    }

    bool ObTransID::is_valid() const
    {
      return INVALID_SESSION_ID != descriptor_;
    }

    int64_t ObTransID::to_string(char* buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "TransID(sd=%u,ups=%s,start=%ld)",
                      descriptor_, ups_.to_cstring(), start_time_us_);
      return pos;
    }

    int ObTransID::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, descriptor_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, start_time_us_)))
      {
        TBSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.serialize(buf, buf_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObTransID::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&descriptor_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &start_time_us_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.deserialize(buf, data_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObTransID::get_serialize_size(void) const
    {
      return serialization::encoded_length_i32(descriptor_)
        + serialization::encoded_length_i64(start_time_us_)
        + ups_.get_serialize_size();
    }

    int64_t ObEndTransReq::to_string(char* buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "EndTransReq(%s,rollback=%s)",
                      to_cstring(trans_id_), STR_BOOL(rollback_));
      return pos;
    }

    int ObEndTransReq::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = trans_id_.serialize(buf, buf_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_bool(buf, buf_len, new_pos, rollback_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObEndTransReq::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = trans_id_.deserialize(buf, data_len, new_pos)))
      {
        TBSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_bool(buf, data_len, new_pos, &rollback_)))
      {
        TBSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObEndTransReq::get_serialize_size(void) const
    {
      return trans_id_.get_serialize_size() + serialization::encoded_length_bool(rollback_);
    }
  }; // end namespace common
}; // end namespace oceanbase
