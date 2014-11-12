#ifndef OCEANBASE_COMMON_COMMON_PARAM_H_
#define OCEANBASE_COMMON_COMMON_PARAM_H_

#include "ob_define.h"
#include "ob_object.h"
#include "ob_range.h"
#include "ob_rowkey.h"

namespace oceanbase
{
  namespace common
  {
    // CellInfo only with table_id + column_id + rowkey + vlaue
    struct ObInnerCellInfo
    {
      uint64_t table_id_;
      ObRowkey row_key_;
      uint64_t column_id_;
      ObObj value_;
      ObInnerCellInfo():table_id_(OB_INVALID_ID), row_key_(), column_id_(OB_INVALID_ID), value_() {}
    };
    // whole CellInfo with table_id || table_name + column_id || column_name + rowkey + value
    struct ObCellInfo
    {
      ObCellInfo() : table_name_(), table_id_(OB_INVALID_ID),
        row_key_(), column_id_(OB_INVALID_ID),
        column_name_(), value_() {}

      ObString table_name_;
      uint64_t table_id_;
      ObRowkey row_key_;
      uint64_t column_id_;
      ObString column_name_;
      ObObj value_;
      bool operator == (const ObCellInfo & other) const;
      void reset()
      {
        table_name_.assign(NULL, 0);
        table_id_ = OB_INVALID_ID;
        row_key_.assign(NULL, 0);
        column_id_ = OB_INVALID_ID;
        column_name_.assign(NULL, 0);
        value_.reset();
      }
    };

    /// @class ObReadParam  OB read parameter, API should not concern these parameters,
    ///   and mergeserver will directly ignore these parameters
    class ObReadParam
    {
    public:
      ObReadParam();
      virtual ~ObReadParam();

      /// @fn get data whose timestamp is newer or as new as the given timestamp,
      ///   -# when reading cs, if not setted, the result is decided by the server;
      ///   -# when reading ups, this parameter must be setted
      void set_version_range(const ObVersionRange & range);
      ObVersionRange get_version_range(void) const;

      /// @fn when reading cs, indicating whether the result (including intermediate result,
      /// like sstable block readed from sstable) of this operation should be cached.
      ///
      /// ups just ignores this parameter
      void set_is_result_cached(const bool cached);
      bool get_is_result_cached()const;

      void set_is_read_consistency(const bool cons);
      bool get_is_read_consistency()const;

      void reset(void);

      /// serailize or deserialization
      VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;

    protected:
      // RESERVE_PARAM_FIELD
      int serialize_reserve_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_reserve_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_reserve_param_serialize_size(void) const;

    protected:
      int8_t is_read_master_;
      int8_t is_result_cached_;
      ObVersionRange version_range_;
    };

    struct ScanFlag
    {
#define SF_BIT_READ_MODE            2
#define SF_BIT_DIRECTION            2
#define SF_BIT_NOT_EXIT_COL_RET_NOP 1
#define SF_BIT_DAILY_MERGE_SCAN     1
#define SF_BIT_FULL_ROW_SCAN        1
#define SF_BIT_ROWKEY_COLUMN_COUNT  16
#define SF_BIT_RESERVED             41
      static const uint64_t SF_MASK_READ_MODE             = (0x1UL<<SF_BIT_READ_MODE)             - 1;
      static const uint64_t SF_MASK_DIRECTION             = (0x1UL<<SF_BIT_DIRECTION)             - 1;
      static const uint64_t SF_MASK_NOT_EXIT_COL_RET_NOP  = (0x1UL<<SF_BIT_NOT_EXIT_COL_RET_NOP)  - 1;
      static const uint64_t SF_MASK_DAILY_MERGE_SCAN      = (0x1UL<<SF_BIT_DAILY_MERGE_SCAN)      - 1;
      static const uint64_t SF_MASK_FULL_ROW_SCAN         = (0x1UL<<SF_BIT_FULL_ROW_SCAN)         - 1;
      static const uint64_t SF_MASK_ROWKEY_COLUMN_COUNT   = (0x1UL<<SF_BIT_ROWKEY_COLUMN_COUNT)   - 1;
      static const uint64_t SF_MASK_RESERVED              = (0x1UL<<SF_BIT_RESERVED)              - 1;
      enum Direction
      {
        FORWARD = 0,
        BACKWARD = 1,
      };

      enum SyncMode 
      {
        SYNCREAD = 0,
        ASYNCREAD = 1,
      };

      ScanFlag()
        :  read_mode_(SYNCREAD), direction_(FORWARD), not_exit_col_ret_nop_(0),
        daily_merge_scan_(0), full_row_scan_(0), rowkey_column_count_(0), reserved_(0)
      {
      }
      ScanFlag(
          const SyncMode mode, const Direction dir, 
          const bool nop, const bool merge, 
          const bool full, const int64_t count)
      {
        flag_ = 0;
        read_mode_            = mode  & SF_MASK_READ_MODE;
        direction_            = dir   & SF_MASK_DIRECTION;
        not_exit_col_ret_nop_ = nop   & SF_MASK_NOT_EXIT_COL_RET_NOP;
        daily_merge_scan_     = merge & SF_MASK_DAILY_MERGE_SCAN;
        full_row_scan_        = full  & SF_MASK_FULL_ROW_SCAN;
        rowkey_column_count_  = count & SF_MASK_ROWKEY_COLUMN_COUNT;
        //flag_ |= (mode & 0x3);
        //flag_ |= ((dir & 0x3) << 2);
        //flag_ |= ((nop & 0x1) << 4);
        //flag_ |= ((merge & 0x1) << 5);
        //flag_ |= ((full & 0x1) << 6);
        //flag_ |= ((count & 0xFFFF) << 7);
      }
      union
      {
        int64_t flag_;
        struct
        {
          uint64_t read_mode_             : SF_BIT_READ_MODE;
          uint64_t direction_             : SF_BIT_DIRECTION;
          uint64_t not_exit_col_ret_nop_  : SF_BIT_NOT_EXIT_COL_RET_NOP;
          uint64_t daily_merge_scan_      : SF_BIT_DAILY_MERGE_SCAN;
          uint64_t full_row_scan_         : SF_BIT_FULL_ROW_SCAN;
          uint64_t rowkey_column_count_   : SF_BIT_ROWKEY_COLUMN_COUNT;
          uint64_t reserved_              : SF_BIT_RESERVED;
        };
      };
    };

    class ObRowkeyInfo;
    int set_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value);
    int set_int_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value);
    int get_int_obj_value(const char* buf, const int64_t data_len, int64_t & pos, int64_t & int_value);
    int set_str_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const ObString &value);
    int get_str_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, ObString & str_value);
    int set_rowkey_obj_array(char* buf, const int64_t buf_len, int64_t & pos, const ObObj* array, const int64_t size);
    int get_rowkey_obj_array(const char* buf, const int64_t buf_len, int64_t & pos, ObObj* array, int64_t& size);
    int64_t get_rowkey_obj_array_size(const ObObj* array, const int64_t size);
    int get_rowkey_compatible(const char* buf, const int64_t buf_len, int64_t & pos,
        const ObRowkeyInfo& info, ObObj* array, int64_t& size, bool &is_binary_rowkey) ;
    template <typename Allocator>
    int get_rowkey_compatible(const char* buf, const int64_t buf_len, int64_t & pos,
        const ObRowkeyInfo& info, Allocator& allocator, ObRowkey& rowkey, bool &is_binary_rowkey)
    {
      int ret = OB_SUCCESS;
      ObRowkey tmp_rowkey;
      ObObj tmp_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
      int64_t rowkey_size = OB_MAX_ROWKEY_COLUMN_NUMBER;
      if (OB_SUCCESS == ret)
      {
        ret = get_rowkey_compatible(buf, buf_len, pos, info,
            tmp_obj_array, rowkey_size, is_binary_rowkey);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize rowkey error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, buf_len, pos);
        }
        else
        {
          tmp_rowkey.assign(tmp_obj_array, rowkey_size);
          ret = tmp_rowkey.deep_copy(rowkey, allocator);
        }
      }
      return ret;
    }
    class ObSchemaManagerV2;
    int get_rowkey_info_from_sm(const ObSchemaManagerV2* schema_mgr,
        const uint64_t table_id, const ObString& table_name, ObRowkeyInfo& rowkey_info);
  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_COMMON_PARAM_H_ */
