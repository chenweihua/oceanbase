#include "ob_mysql_row.h"
#include "ob_mysql_global.h"
#include "ob_mysql_util.h"
#include "ob_mysql_dtoa.h"

namespace oceanbase {
  namespace obmysql {
    ObMySQLRow::ObMySQLRow():row_(NULL), type_(TEXT), bitmap_(NULL), bitmap_bytes_(0)
    {

    }
    int ObMySQLRow::serialize(char *buf, const int64_t len, int64_t &pos) const
    {
      int64_t cell_index = 0;
      const common::ObObj *cell;
      uint64_t table_id = 0;
      uint64_t column_id = 0;
      int64_t pos_bk = pos;
      int ret = OB_SUCCESS;
      int64_t column_num = row_->get_column_num();
      //for binary protocol
      if (BINARY == type_)
      {
        //http://dev.mysql.com/doc/internals/en/prepared-statements.html#null-bitmap
        //one byte header alwasy 0x00
        memset(buf + pos, 0, 1);
        pos ++;
        //NULL-bitmap-bytes = (num-fields + 7 + offset) / 8
        //offset in binary row response is 2
        bitmap_ = buf + pos;
        bitmap_bytes_ = (column_num + 7 + 2) / 8;
        memset(bitmap_, 0, bitmap_bytes_);
        pos += bitmap_bytes_;
      }
      for (cell_index = 0; cell_index < column_num; cell_index++)
      {
        if (OB_SUCCESS != (ret = row_->raw_get_cell(cell_index, cell, table_id, column_id)))
        {
          TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = cell_str(*cell, buf, len, pos, cell_index)))
        {
          if (OB_LIKELY(OB_SIZE_OVERFLOW == ret || OB_BUF_NOT_ENOUGH == ret))
          {
            //do nothing
          }
          else
          {
            TBSYS_LOG(WARN, "failed to serialize cell, err=%d", ret);
          }
          break;
        }
      }

      if (OB_SUCCESS != ret)
      {
        pos = pos_bk;
      }
      return ret;
    }

    int ObMySQLRow::cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos, int64_t cell_index) const
    {
      int ret = OB_SUCCESS;
      switch (obj.get_type())
      {
        case ObMinType:
        case ObMaxType:
        case ObExtendType:
        case ObSeqType:
          TBSYS_LOG(WARN, "invalid ob type=%d", obj.get_type());
          // not goto here.
          ret = OB_ERROR;
          break;
        case ObNullType:
          ret = null_cell_str(obj, buf, len, pos, cell_index);
          break;
        case ObBoolType:
          ret = bool_cell_str(obj, buf, len, pos);
          break;
        case ObIntType:
          ret = int_cell_str(obj, buf, len, pos);
          break;
        case ObDateTimeType:
        case ObPreciseDateTimeType:
        case ObCreateTimeType:
        case ObModifyTimeType:
          ret = datetime_cell_str(obj, buf, len, pos);
          break;
        case ObFloatType:
        case ObDoubleType:
          ret = float_cell_str(obj, buf, len, pos);
          break;
        case ObDecimalType:
          ret = decimal_cell_str(obj, buf, len, pos);
          break;
        case ObVarcharType:
          ret = varchar_cell_str(obj, buf, len, pos);
          break;
        default:
          TBSYS_LOG(WARN, "invalid ob type=%d", obj.get_type());
          ret = OB_ERROR;
          break;
      }
      return ret;
    }

    int ObMySQLRow::bool_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const
    {
      bool bool_val = false;
      int ret = OB_SUCCESS;
      uint64_t length = 0;
      // MYSQL_TYPE_TINY format
      if (len - pos < 2)
      {
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        ret = obj.get_bool(bool_val);
        OB_ASSERT(OB_SUCCESS == ret);
        if (TEXT == type_)
        {
          /* skip 1 byte to store length */
          length = snprintf(buf + pos + 1, len - pos - 1, "%d", bool_val ? 1 : 0);
          ObMySQLUtil::store_length(buf, len, length, pos);
          pos += length;
        }
        else if (BINARY == type_)
        {
          int8_t value = bool_val? 1 : 0;
          ret = ObMySQLUtil::store_int1(buf, len, value, pos);
        }
      }
      return ret;
    }

    int ObMySQLRow::int_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const
    {
      int64_t int_val = 0;
      int ret = OB_SUCCESS;
      uint64_t length = 0;
      // MYSQL_TYPE_LONGLONG format
      if (len - pos < 29)
      {
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        ret = obj.get_int(int_val);
        OB_ASSERT(OB_SUCCESS == ret);
        if (TEXT == type_)
        {
          /* skip 1 byte to store length */
          length = snprintf(buf + pos + 1, len - pos - 1, "%ld", int_val);
          ObMySQLUtil::store_length(buf, len, length, pos);
          pos += length;
        }
        else if (BINARY == type_)
        {
          ret = ObMySQLUtil::store_int8(buf, len, int_val, pos);
        }
      }
      return ret;
    }

    int ObMySQLRow::null_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos, int64_t cell_index) const
    {
      int ret = OB_SUCCESS;
      UNUSED(obj);
      if (len - pos <= 0)
      {
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        if (BINARY == type_)
        {
          ObMySQLUtil::update_null_bitmap(bitmap_, cell_index);
        }
        else
        {
          ret = ObMySQLUtil::store_null(buf, len, pos);
        }
      }
      return ret;
    }

    int ObMySQLRow::decimal_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      uint64_t length = 0;
      ObNumber num;

      if (OB_SUCCESS == (ret = obj.get_decimal(num)))
      {
        /* skip 1 byte to store length */
        length = num.to_string(buf + pos + 1, len - pos - 1);
        ObMySQLUtil::store_length(buf, len, length, pos);
        pos += length;
      }
      return ret;
    }

    int ObMySQLRow::datetime_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const
    {
      int64_t datetime;
      time_t time;
      struct tm tms;
      int ret = OB_SUCCESS;
      int32_t microsecond = 0;
      uint64_t length = 0;
      size_t r = 0;
      uint8_t timelen = 0;
      ret = obj.get_timestamp(datetime);
      // that's precise datetime which has millisecond.
      if (OB_SUCCESS == ret)
      {
        microsecond = static_cast<int32_t>(datetime % 1000000);
        time = datetime / 1000000;

        localtime_r(&time, &tms);
        if (type_ == BINARY)
        {
          if (0 == tms.tm_year && 0 == tms.tm_mon
              && 0 == tms.tm_mday && 0 == tms.tm_hour
              && 0 == tms.tm_min && 0 == tms.tm_sec
              && 0 == microsecond)
          {
            timelen = 0;
            ObMySQLUtil::store_int1(buf, len, timelen, pos);
          }
          else if(0 == tms.tm_hour && 0 == tms.tm_min
                  && 0 == tms.tm_sec && 0 == microsecond)
          {
            timelen = 4;
            ObMySQLUtil::store_int1(buf, len, timelen, pos);
            ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(tms.tm_year + 1900), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_mon + 1), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_mday), pos);
          }
          else if (0 == microsecond)
          {
            timelen = 7;
            ObMySQLUtil::store_int1(buf, len, timelen, pos);
            ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(tms.tm_year + 1900), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_mon + 1), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_mday), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_hour), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_min), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_sec), pos);
          }
          else
          {
            timelen = 11;
            ObMySQLUtil::store_int1(buf, len, timelen, pos);
            ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(tms.tm_year + 1900), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_mon + 1), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_mday), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_hour), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_min), pos);
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(tms.tm_sec), pos);
            ObMySQLUtil::store_int4(buf, len, microsecond, pos);
          }
        }
        else
        {
          /* skip 1 byte to store length */
          if (len - pos <= 1 || 0 == (r = strftime(buf + pos + 1, len - pos - 1, "%F %T", &tms)))
          {
            ret = OB_SIZE_OVERFLOW;
          }
          else
          {
            length = r;
            if (0 != microsecond)
            {
              /* skip 1 byte to store length */
              if (len - pos - length <= 1)
              {
                ret = OB_SIZE_OVERFLOW;
              }
              else
              {
                r = snprintf(buf + pos + length + 1, len - pos - length - 1, ".%d", microsecond);
                if (r >= len - pos - length - 1)
                {
                  ret = OB_SIZE_OVERFLOW;
                }
                else if (0 == r)
                {
                  ret = OB_ERROR;
                  TBSYS_LOG(ERROR, "snprintf microsecond(%d) to buffer failed", microsecond);
                }
              }
              if (OB_SUCCESS == ret)
              {
                length += r;
              }
            }
          }

          if (OB_SUCCESS == ret && static_cast<int64_t>(length + 1) <= len - pos)
          {
            ObMySQLUtil::store_length(buf, len, length, pos);
            pos += length;
          }
          else                    /* not enough space to hold the string */
          {
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }
      return ret;
    }

    int ObMySQLRow::varchar_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const
    {
      ObString str;
      int ret = OB_SUCCESS;
      uint64_t length;
      int64_t pos_bk = pos;

      if (obj.get_type() != ObVarcharType)
      {
        ret = OB_OBJ_TYPE_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_varchar(str);
        if (OB_SUCCESS == ret && str.length() < len - pos)
        {
          if (str.length() < 0)   // ensure no neg signed int to unsigned cast.
            return OB_ERROR;

          length = static_cast<uint64_t>(str.length());
          if ((ret = ObMySQLUtil::store_length(buf, len, length, pos)) == OB_SUCCESS)
          {
            if (len - pos >= str.length())
            {
              memcpy(buf + pos, str.ptr(), length);
              pos += length;
            }
            else
            {
              pos = pos_bk;
              ret = OB_SIZE_OVERFLOW;
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "serialize data len failed len = %lu", length);
          }
        }
        else
        {
          ret = OB_SIZE_OVERFLOW;
        }
      }
      return ret;
    }

    int ObMySQLRow::float_cell_str(const ObObj &obj, char *buf, const int64_t len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      float value_f = 0.0f;
      double value_d = 0.0;
      static const int flt_len =  FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE;
      static const int dbl_len =  DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE;
      uint64_t length = 0;

      if (obj.get_type() != ObFloatType && obj.get_type() != ObDoubleType)
      {
        ret = OB_OBJ_TYPE_ERROR;
      }
      if (OB_SUCCESS == ret && obj.get_type() == ObFloatType)
      {
        ret = obj.get_float(value_f);
        if (BINARY == type_)
        {
          TBSYS_LOG(DEBUG, "float_cell_str %f", value_f);
          if (OB_SUCCESS == ret && len - pos > static_cast<int64_t>(sizeof(value_f)))
          {
            memcpy(buf + pos, &value_f, sizeof(value_f));
            pos += sizeof(value_f);
          }
          else
          {
            ret = OB_SIZE_OVERFLOW;
            //TBSYS_LOG(WARN, "Not enough space, err=%d", ret);
          }
        }
        else
        {
          if (OB_SUCCESS == ret && len - pos > flt_len)
          {
            /* skip 1 byte to store length */
            length = static_cast<size_t>(my_gcvt(value_f, MY_GCVT_ARG_FLOAT,
                                                 flt_len - 1, buf + pos + 1,
                                                 NULL));
          }
          else
          {
            ret = OB_SIZE_OVERFLOW;
            //TBSYS_LOG(WARN, "Not enough space, err=%d", ret);
          }
        }
      }
      else if (OB_SUCCESS == ret && obj.get_type() == ObDoubleType)
      {
        ret = obj.get_double(value_d);
        if (BINARY == type_)
        {
          if (OB_SUCCESS == ret && len - pos > static_cast<int64_t>(sizeof(value_d)))
          {
            memcpy(buf + pos, &value_d, sizeof(value_d));
            pos += sizeof(value_d);
          }
          else
          {
            ret = OB_SIZE_OVERFLOW;
            //TBSYS_LOG(WARN, "Not enough space, err=%d", ret);
          }
        }
        else
        {
          if (OB_SUCCESS == ret && len - pos > dbl_len)
          {
            /* skip 1 byte to store length */
            length = static_cast<size_t>(my_gcvt(value_d, MY_GCVT_ARG_DOUBLE,
                                                 dbl_len - 1, buf + pos + 1,
                                                 NULL));
          }
          else
          {
            ret = OB_SIZE_OVERFLOW;
            //TBSYS_LOG(WARN, "Not enough space, err=%d", ret);
          }
        }
      }

      if (TEXT == type_)
      {
        if (OB_SUCCESS == ret && length > 0)
        {
          if (length < 251 && static_cast<int64_t>(length + 1) <= len - pos)
          {
            ObMySQLUtil::store_length(buf, len, length, pos);
            pos += length;
          }
          else if (length >= 251 && static_cast<int64_t>(length + 3) <= len - pos)
          {
            /* we need 3 btyes to hold length of double (may be) */
            memmove(buf + 3, buf + 1, length);
            ObMySQLUtil::store_length(buf, len, length, pos);
            pos += length;
          }
          else
          {
            ret = OB_SIZE_OVERFLOW;
            //TBSYS_LOG(WARN, "Not enough space, err=%d", ret);
          }
        }
        else
        {
          ret = OB_SIZE_OVERFLOW;
          //TBSYS_LOG(WARN, "Not enough space, err=%d", ret);
        }
      }
      return ret;
    }
  } // end of namespace obmysql
} // end of namespace oceanbase
