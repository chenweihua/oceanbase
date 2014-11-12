#ifndef OCEANBASE_COMMON_OB_OBJECT_H_
#define OCEANBASE_COMMON_OB_OBJECT_H_

#include "ob_define.h"
#include "ob_string.h"
#include "tbsys.h"
#include "ob_action_flag.h"
#include "ob_obj_type.h"
#include "ob_number.h"

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class ObjTest;
    }
  }
  namespace common
  {
    struct ObObjMeta
    {
      uint32_t type_:8;
      uint32_t dec_vscale_:6;
      uint32_t op_flag_:2;
      uint32_t dec_precision_:7;
      uint32_t dec_scale_:6;
      uint32_t dec_nwords_:3; // the actual nwords is dec_nwords_ + 1, so the range of nwords is [1, 8]
    };

    union _ObjValue
    {
      int64_t int_val;
      float float_val;
      double double_val;
      ObDateTime second_val;
      ObPreciseDateTime microsecond_val;
      ObCreateTime      create_time_val;
      ObModifyTime      modify_time_val;
      bool bool_val;
    };

    class ObBatchChecksum;
    class ObExprObj;
    class ObObj
    {
      public:
        // min, max extend value
        static const int64_t MIN_OBJECT_VALUE         = UINT64_MAX-2;
        static const int64_t MAX_OBJECT_VALUE         = UINT64_MAX-1;
      public:
        ObObj();

        ObObj(const ObObjType type, const int8_t flag, const int32_t val_len, const int64_t value);
        /// set add flag
        int set_add(const bool is_add = false);
        bool get_add()const;
        bool get_add_fast() const;
        /*
         *   设置类型
         */
        void set_type(const ObObjType& type);
        /*
         *   设置列值
         */
        void set_int(const int64_t value,const bool is_add = false);
        void set_float(const float value,const bool is_add = false);
        void set_double(const double value,const bool is_add = false);
        void set_ext(const int64_t value);
        /*
         *   设置日期类型，如果date_time为OB_SYS_DATE，表示设置为服务器当前时间
         */
        void set_datetime(const ObDateTime& value,const bool is_add = false);
        void set_precise_datetime(const ObPreciseDateTime& value,const bool is_add = false);
        void set_varchar(const ObString& value);
        void set_seq();
        void set_modifytime(const ObModifyTime& value);
        void set_createtime(const ObCreateTime& value);
        void set_bool(const bool value);

        /*
         *   设置列值为空
         */
        void set_null();
        bool is_null() const;
        void set_min_value();
        void set_max_value();

        /// @fn apply mutation to this obj
        int apply(const ObObj &mutation);


        void reset();
        void reset_op_flag();
        bool is_valid_type() const;
        bool is_min_value() const;
        bool is_max_value() const;

        void dump(const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;
        void print_value(FILE* fd);
        int64_t to_string(char* buffer, const int64_t length) const;
        //
        NEED_SERIALIZE_AND_DESERIALIZE;
        /*
         *   获取列值，用户根据已知的数据类型调用相应函数，如果类型不符则返回失败
         */
        int get_int(int64_t& value,bool& is_add) const;
        int get_int(int64_t& value) const;

        int get_float(float& value,bool& is_add) const;
        int get_float(float& value) const;

        int get_double(double& value,bool& is_add) const;
        int get_double(double& value) const;

        int get_ext(int64_t& value) const;
        int64_t get_ext() const;

        int get_datetime(ObDateTime& value,bool& is_add) const;
        int get_datetime(ObDateTime& value) const;

        int get_precise_datetime(ObPreciseDateTime& value,bool& is_add ) const;
        int get_precise_datetime(ObPreciseDateTime& value) const;

        int get_modifytime(ObModifyTime& value) const;
        int get_createtime(ObCreateTime& value) const;
        /*
         *   获取varchar类型数据，直接使用ObObj内部缓冲区
         */
        int get_varchar(ObString& value) const;
        int get_bool(bool &value) const;

        int set_decimal(const ObNumber &num, int8_t precision = 38, int8_t scale = 0, bool is_add = false);
        int get_decimal(ObNumber &num) const;
        int get_decimal(ObNumber &num, bool &is_add) const;
        /*
         *   获取数据类型
         */
        ObObjType get_type(void) const;

        int32_t get_val_len() const;
        void set_val_len(const int32_t val_len);
        bool need_deep_copy()const;
        bool is_true() const;
        static const char* get_sql_type(ObObjType type);
        /*
         *   计算obj内数据的校验和
         */
        int64_t checksum(const int64_t current) const;
        void checksum(ObBatchChecksum &bc) const;

        uint32_t murmurhash2(const uint32_t hash) const;
        uint64_t murmurhash64A(const uint64_t hash) const;

        int64_t hash() const;   // for ob_hashtable.h

        bool operator<(const ObObj &that_obj) const;
        bool operator>(const ObObj &that_obj) const;
        bool operator<=(const ObObj &that_obj) const;
        bool operator>=(const ObObj &that_obj) const;
        bool operator==(const ObObj &that_obj) const;
        bool operator!=(const ObObj &that_obj) const;
        int compare(const ObObj &other) const;
        int  compare_same_type(const ObObj &other) const;

        int get_timestamp(int64_t & timestamp) const;

        const void *get_data_ptr() const
        {
          const void *ret = NULL;
          if (ObVarcharType == get_type())
          {
            ret = const_cast<char*>(value_.varchar_val);
          }
          else
          {
            ret = &value_;
          }
          return ret;
        };

        int64_t get_data_length() const
        {
          int64_t ret = sizeof(value_);
          if (ObVarcharType == get_type())
          {
            ret = val_len_;
          }
          return ret;
        };
      private:
        friend class tests::common::ObjTest;
        friend class ObCompactCellWriter;
        friend class ObCompactCellIterator;
        friend class ObExprObj;
        bool is_datetime() const;
        bool can_compare(const ObObj & other) const;
        void set_flag(bool is_add);
      private:
        static const uint8_t INVALID_OP_FLAG = 0x0;
        static const uint8_t ADD = 0x1;
        static const uint8_t META_VSCALE_MASK = 0x3F;
        static const uint8_t META_OP_FLAG_MASK = 0x3;
        static const uint8_t META_PREC_MASK = 0x7F;
        static const uint8_t META_SCALE_MASK = 0x3F;
        static const uint8_t META_NWORDS_MASK = 0x7;
        ObObjMeta meta_;
      int32_t val_len_;
      union          // value实际内容
      {
        int64_t int_val;
        int64_t ext_val;
        float float_val;
        double double_val;
        ObDateTime time_val;
        ObPreciseDateTime precisetime_val;
        ObModifyTime modifytime_val;
        ObCreateTime createtime_val;
        const char *varchar_val;
        bool bool_val;
        uint32_t *dec_words_;
      } value_;
    };

    inline ObObj::ObObj()
    {
      reset();
    }

    inline ObObj::ObObj(const ObObjType type, const int8_t flag, const int32_t val_len, const int64_t value)
    {
      meta_.type_ = static_cast<int8_t>(type);
      meta_.op_flag_ = flag & 0x3;
      meta_.dec_vscale_ = 0;
      meta_.dec_precision_ = 0;
      meta_.dec_nwords_ = 0;
      meta_.dec_scale_ = 0;
      val_len_ = val_len;
      memcpy(&value_, &value, sizeof(value_));
    }

    inline void ObObj::reset()
    {
      memset(this, 0, sizeof(ObObj));
    }

    inline void ObObj::set_flag(bool is_add)
    {
      uint8_t flag = is_add ? ADD : INVALID_OP_FLAG;
      meta_.op_flag_ = flag & META_OP_FLAG_MASK;
    }

    inline void ObObj::reset_op_flag()
    {
      if (ObExtendType == get_type() && ObActionFlag::OP_NOP == value_.ext_val)
      {
        meta_.type_ = ObNullType;
      }
      meta_.op_flag_ = INVALID_OP_FLAG;
    }

    inline bool ObObj::get_add()const
    {
      bool ret = false;
      switch (get_type())
      {
        case ObIntType:
        case ObDateTimeType:
        case ObPreciseDateTimeType:
          ret = (meta_.op_flag_ == ADD);
          break;
        default:
          ;/// do nothing
      }
      return ret;
    }

    inline bool ObObj::get_add_fast() const
    {
      return (meta_.op_flag_ == ADD);
    }

    inline int ObObj::set_add(const bool is_add /*=false*/)
    {
      int ret = OB_SUCCESS;
      switch (get_type())
      {
        case ObIntType:
        case ObDateTimeType:
        case ObPreciseDateTimeType:
        case ObDecimalType:
        case ObFloatType:
        case ObDoubleType:
          set_flag(is_add);
          break;
        default:
          TBSYS_LOG(ERROR, "check obj type failed:type[%d]", get_type());
          ret = OB_ERROR;
      }
      return ret;
    }

    inline void ObObj::set_type(const ObObjType& type)
    {
      meta_.type_ = static_cast<unsigned char>(type);
    }

    inline void ObObj::set_int(const int64_t value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObIntType;
      value_.int_val = value;
    }

    inline void ObObj::set_float(const float value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObFloatType;
      value_.float_val = value;
    }

    inline void ObObj::set_double(const double value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObDoubleType;
      value_.double_val = value;
    }

    inline void ObObj::set_datetime(const ObDateTime& value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObDateTimeType;
      value_.time_val= value;
    }

    inline void ObObj::set_precise_datetime(const ObPreciseDateTime& value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObPreciseDateTimeType;
      value_.precisetime_val = value;
    }

    inline void ObObj::set_varchar(const ObString& value)
    {
      meta_.type_ = ObVarcharType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.varchar_val = value.ptr();
      val_len_ = value.length();
    }

    inline void ObObj::set_seq()
    {
      meta_.type_ = ObSeqType;
      meta_.op_flag_ = INVALID_OP_FLAG;
    }

    inline void ObObj::set_modifytime(const ObModifyTime& value)
    {
      meta_.type_ = ObModifyTimeType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.modifytime_val = value;
    }

    inline void ObObj::set_createtime(const ObCreateTime& value)
    {
      meta_.type_ = ObCreateTimeType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.createtime_val = value;
    }

    inline void ObObj::set_bool(const bool value)
    {
      meta_.type_ = ObBoolType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.bool_val = value;
    }

    inline void ObObj::set_null()
    {
      meta_.type_ = ObNullType;
      meta_.op_flag_ = INVALID_OP_FLAG;
    }

    inline bool ObObj::is_null() const
    {
      return meta_.type_ == ObNullType;
    }

    inline void ObObj::set_ext(const int64_t value)
    {
      meta_.type_ = ObExtendType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.ext_val = value;
    }

    inline void ObObj::set_min_value()
    {
      set_ext(MIN_OBJECT_VALUE);
    }

    inline void ObObj::set_max_value()
    {
      set_ext(MAX_OBJECT_VALUE);
    }

    inline bool ObObj::is_valid_type() const
    {
      ObObjType type = get_type();
      bool ret = true;
      if (type <= ObMinType || type >= ObMaxType)
      {
        ret = false;
      }
      return ret;
    }

    inline bool ObObj::is_min_value() const
    {
      return meta_.type_ == ObExtendType && value_.ext_val == MIN_OBJECT_VALUE;
    }

    inline bool ObObj::is_max_value() const
    {
      return meta_.type_ == ObExtendType && value_.ext_val == MAX_OBJECT_VALUE;
    }

    inline int ObObj::get_int(int64_t& value,bool& is_add) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObIntType)
      {
        is_add = (ADD == meta_.op_flag_);
        value = value_.int_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_int(int64_t& value) const
    {
      bool add = false;
      return get_int(value,add);
    }


    inline bool ObObj::need_deep_copy()const
    {
      return (meta_.type_ == ObVarcharType);
    }

    inline int ObObj::get_datetime(ObDateTime& value,bool& is_add) const
    {
      int ret = OB_OBJ_TYPE_ERROR;
      if (ObDateTimeType == meta_.type_)
      {
        value = value_.time_val;
        is_add = (meta_.op_flag_ == ADD);
        ret = OB_SUCCESS;
      }
      return ret;
    }

    inline int ObObj::get_datetime(ObDateTime& value) const
    {
      bool add = false;
      return get_datetime(value,add);
    }

    inline int ObObj::get_precise_datetime(ObPreciseDateTime& value,bool& is_add) const
    {
      int ret = OB_OBJ_TYPE_ERROR;
      if (ObPreciseDateTimeType == meta_.type_)
      {
        value = value_.precisetime_val;
        is_add = (meta_.op_flag_ == ADD);
        ret = OB_SUCCESS;
      }
      return ret;
    }

    inline int ObObj::get_precise_datetime(ObPreciseDateTime& value) const
    {
      bool add = false;
      return get_precise_datetime(value,add);
    }

    inline int ObObj::get_varchar(ObString& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObVarcharType)
      {
        value.assign_ptr(const_cast<char *>(value_.varchar_val), val_len_);
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_modifytime(ObModifyTime& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (ObModifyTimeType == meta_.type_)
      {
        value = value_.modifytime_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_createtime(ObCreateTime& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (ObCreateTimeType == meta_.type_)
      {
        value = value_.createtime_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_ext(int64_t& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (ObExtendType == meta_.type_)
      {
        value = value_.ext_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int64_t ObObj::get_ext() const
    {
      int64_t res = 0;
      if (ObExtendType == meta_.type_)
      {
        res = value_.ext_val;
      }
      return res;
    }

    inline ObObjType ObObj::get_type(void) const
    {
      return static_cast<ObObjType>(meta_.type_);
    }

    inline int32_t ObObj::get_val_len() const
    {
      return val_len_;
    }

    inline void ObObj::set_val_len(const int32_t val_len)
    {
      val_len_ = val_len;
    }

    inline int ObObj::get_float(float& value,bool& is_add) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObFloatType)
      {
        value = value_.float_val;
        is_add = (meta_.op_flag_ == ADD);
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_float(float& value) const
    {
      bool add = false;
      return get_float(value,add);
    }

    inline int ObObj::get_double(double& value,bool& is_add) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObDoubleType)
      {
        value = value_.double_val;
        is_add = (meta_.op_flag_ == ADD);
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_double(double& value) const
    {
      bool add = false;
      return get_double(value,add);
    }

    inline int ObObj::get_bool(bool &value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (get_type() == ObBoolType)
      {
        value = value_.bool_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int64_t ObObj::hash() const
    {
      return this->murmurhash2(0);
    }

    inline bool ObObj::is_datetime() const
    {
      return ((meta_.type_ == ObDateTimeType)
          || (meta_.type_ == ObPreciseDateTimeType)
          || (meta_.type_ == ObCreateTimeType)
          || (meta_.type_ == ObModifyTimeType));
    }

    inline bool ObObj::can_compare(const ObObj & other) const
    {
      bool ret = false;
      if ((get_type() == ObNullType) || (other.get_type() == ObNullType)
          || is_min_value() || is_max_value()
          || other.is_min_value() || other.is_max_value()
          || (get_type() == other.get_type()) || (is_datetime() && other.is_datetime()))
      {
        ret = true;
      }
      return ret;
    }

    inline int ObObj::get_timestamp(int64_t & timestamp) const
    {
      int ret = OB_SUCCESS;
      switch(meta_.type_)
      {
        case ObDateTimeType:
          timestamp = value_.time_val * 1000 * 1000L;
          break;
        case ObPreciseDateTimeType:
          timestamp = value_.precisetime_val;
          break;
        case ObModifyTimeType:
          timestamp = value_.modifytime_val;
          break;
        case ObCreateTimeType:
          timestamp = value_.createtime_val;
          break;
        default:
          TBSYS_LOG(ERROR, "unexpected branch");
          ret = OB_OBJ_TYPE_ERROR;
      }
      return ret;
    }

    template <typename AllocatorT>
    int ob_write_obj(AllocatorT &allocator, const ObObj &src, ObObj &dst)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(ObVarcharType != src.get_type()))
      {
        dst = src;
      }
      else
      {
        ObString str;
        ObString str_clone;
        src.get_varchar(str);
        if (OB_SUCCESS == (ret = ob_write_string(allocator, str, str_clone)))
        {
          dst.set_varchar(str_clone);
        }
      }
      return ret;
    }
  }
}

#endif //
