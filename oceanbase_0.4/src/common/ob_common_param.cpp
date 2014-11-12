#include "ob_object.h"
#include "ob_action_flag.h"
#include "ob_common_param.h"
#include "ob_schema.h"
#include "ob_rowkey_helper.h"

namespace oceanbase
{
  namespace common
  {
    bool ObCellInfo::operator == (const ObCellInfo & other) const
    {
      return ((table_name_ == other.table_name_) && (table_id_ == other.table_id_)
          && (row_key_ == other.row_key_) && (column_name_ == other.column_name_)
          && (column_id_ == other.column_id_) && (value_ == other.value_));
    }

    ObReadParam::ObReadParam()
    {
      reset();
    }

    void ObReadParam::reset()
    {
      is_read_master_ = 1;
      is_result_cached_ = 0;
      version_range_.start_version_ = 0;
      version_range_.end_version_ = 0;
    }

    ObReadParam::~ObReadParam()
    {
    }

    void ObReadParam::set_is_read_consistency(const bool consistency)
    {
      is_read_master_ = consistency;
    }

    bool ObReadParam::get_is_read_consistency()const
    {
      return (is_read_master_ > 0);
    }

    void ObReadParam::set_is_result_cached(const bool cached)
    {
      is_result_cached_ = cached;
    }

    bool ObReadParam::get_is_result_cached()const
    {
      return (is_result_cached_ > 0);
    }

    void ObReadParam::set_version_range(const ObVersionRange & range)
    {
      version_range_ = range;
    }

    ObVersionRange ObReadParam::get_version_range(void) const
    {
      return version_range_;
    }

    int ObReadParam::serialize_reserve_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      // serialize RESERVER PARAM FIELD
      obj.set_ext(ObActionFlag::RESERVE_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      if (ret == OB_SUCCESS)
      {
        obj.set_int(get_is_read_consistency());
        ret = obj.serialize(buf, buf_len, pos);
      }
      return ret;
    }

    int ObReadParam::deserialize_reserve_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      ObObj obj;
      int64_t int_value = 0;
      int ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          //is read master
          set_is_read_consistency(int_value);
        }
      }
      return ret;
    }

    int64_t ObReadParam::get_reserve_param_serialize_size(void) const
    {
      ObObj obj;
      // reserve for read master
      obj.set_ext(ObActionFlag::RESERVE_PARAM_FIELD);
      int64_t total_size = obj.get_serialize_size();
      obj.set_int(get_is_read_consistency());
      total_size += obj.get_serialize_size();
      return total_size;
    }

    DEFINE_SERIALIZE(ObReadParam)
    {
      ObObj obj;
      // is cache
      obj.set_int(ObReadParam::get_is_result_cached());
      int ret = obj.serialize(buf, buf_len, pos);
      // scan version range
      if (ret == OB_SUCCESS)
      {
        ObVersionRange version_range = ObReadParam::get_version_range();;
        obj.set_int(version_range.border_flag_.get_data());
        ret = obj.serialize(buf, buf_len, pos);
        if (ret == OB_SUCCESS)
        {
          obj.set_int(version_range.start_version_);
          ret = obj.serialize(buf, buf_len, pos);
        }

        if (ret == OB_SUCCESS)
        {
          obj.set_int(version_range.end_version_);
          ret = obj.serialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObReadParam)
    {
      ObObj obj;
      int ret = OB_SUCCESS;
      int64_t int_value = 0;
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          //is cached
          set_is_result_cached(int_value);
        }
      }

      // version range
      if (OB_SUCCESS == ret)
      {
        // border flag
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.border_flag_.set_data(static_cast<int8_t>(int_value));
          }
        }
      }

      // start version
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.start_version_ = int_value;
          }
        }
      }

      // end version
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.end_version_ = int_value;
          }
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObReadParam)
    {
      ObObj obj;
      // is cache
      obj.set_int(get_is_result_cached());
      int64_t total_size = obj.get_serialize_size();

      // scan version range
      obj.set_int(version_range_.border_flag_.get_data());
      total_size += obj.get_serialize_size();
      obj.set_int(version_range_.start_version_);
      total_size += obj.get_serialize_size();
      obj.set_int(version_range_.end_version_);
      total_size += obj.get_serialize_size();

      return total_size;
    }


    //-------------------------------------------------------------------------------
    int set_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_ext(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, int64_t& value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
          && ObExtendType == obj.get_type())
      {
        ret = obj.get_ext(value);
      }
      return ret;
    }

    int set_int_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_int(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_int_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, int64_t & int_value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
          && ObIntType == obj.get_type())
      {
        ret = obj.get_int(int_value);
      }
      return ret;
    }

    int set_str_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const ObString &value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_varchar(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_str_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, ObString & str_value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
          && ObVarcharType == obj.get_type())
      {
        ret = obj.get_varchar(str_value);
      }
      return ret;
    }


    int set_rowkey_obj_array(char* buf, const int64_t buf_len, int64_t & pos, const ObObj* array, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == (ret = set_int_obj_value(buf, buf_len, pos, size)))
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = array[i].serialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    int get_rowkey_obj_array(const char* buf, const int64_t buf_len, int64_t & pos, ObObj* array, int64_t& size)
    {
      int ret = OB_SUCCESS;
      size = 0;
      if (OB_SUCCESS == (ret = get_int_obj_value(buf, buf_len, pos, size)))
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = array[i].deserialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    int64_t get_rowkey_obj_array_size(const ObObj* array, const int64_t size)
    {
      int64_t total_size = 0;
      ObObj obj;
      obj.set_int(size);
      total_size += obj.get_serialize_size();

      for (int64_t i = 0; i < size; ++i)
      {
        total_size += array[i].get_serialize_size();
      }
      return total_size;
    }

    int get_rowkey_compatible(const char* buf, const int64_t buf_len, int64_t & pos,
        const ObRowkeyInfo& info, ObObj* array, int64_t& size, bool& is_binary_rowkey)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      int64_t obj_count = 0;
      ObString str_value;

      is_binary_rowkey = false;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos)) )
      {
        if (ObIntType == obj.get_type() && (OB_SUCCESS == (ret = obj.get_int(obj_count))))
        {
          // new rowkey format.
          for (int64_t i = 0; i < obj_count && OB_SUCCESS == ret; ++i)
          {
            if (i >= size)
            {
              ret = OB_SIZE_OVERFLOW;
            }
            else
            {
              ret = array[i].deserialize(buf, buf_len, pos);
            }
          }

          if (OB_SUCCESS == ret) size = obj_count;
        }
        else if (ObVarcharType == obj.get_type() && OB_SUCCESS == (ret = obj.get_varchar(str_value)))
        {
          is_binary_rowkey = true;
          // old fashion , binary rowkey stream
          if (size < info.get_size())
          {
            TBSYS_LOG(WARN, "input size=%ld not enough, need rowkey obj size=%ld", size, info.get_size());
            ret = OB_SIZE_OVERFLOW;
          }
          else if (str_value.length() == 0)
          {
            // allow empty binary rowkey , incase min, max range.
            size = 0;
          }
          else if (str_value.length() < info.get_binary_rowkey_length())
          {
            TBSYS_LOG(WARN, "binary rowkey length=%d < need rowkey length=%ld",
                str_value.length(), info.get_binary_rowkey_length());
            ret = OB_SIZE_OVERFLOW;
          }
          else
          {
            size = info.get_size();
            ret = ObRowkeyHelper::binary_rowkey_to_obj_array(info, str_value, array, size);
          }
        }
      }

      return ret;
    }

    int get_rowkey_info_from_sm(const ObSchemaManagerV2* schema_mgr,
        const uint64_t table_id, const ObString& table_name, ObRowkeyInfo& rowkey_info)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* tbl = NULL;
      if (NULL == schema_mgr)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (table_id > 0 && table_id != OB_INVALID_ID)
      {
        tbl = schema_mgr->get_table_schema(table_id);
      }
      else if (NULL != table_name.ptr())
      {
        tbl = schema_mgr->get_table_schema(table_name);
      }

      if (NULL == tbl)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        rowkey_info = tbl->get_rowkey_info();
      }
      return ret;
    }

  } /* common */
} /* oceanbase */
