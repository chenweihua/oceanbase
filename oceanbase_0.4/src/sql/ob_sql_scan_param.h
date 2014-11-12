#ifndef OCEANBASE_SQL_SCAN_PARAM_H_
#define OCEANBASE_SQL_SCAN_PARAM_H_

#include "common/ob_define.h"
#include "common/ob_array_helper.h"
#include "common/ob_object.h"
#include "common/ob_range2.h"
#include "common/ob_simple_filter.h"
#include "common/ob_common_param.h"
#include "common/ob_composite_column.h"
#include "ob_sql_read_param.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObSqlScanParamTest_serialize_test_Test;

namespace oceanbase
{
  namespace sql
  {
    class ObSqlScanParam : public ObSqlReadParam
    {
    public:
      ObSqlScanParam();
      virtual ~ObSqlScanParam();

    public:
      int set_range(const ObNewRange& range, bool deep_copy_args = false);
      inline const ObNewRange* const get_range() const
      {
        return &range_;
      }
      inline ObStringBuf* get_buffer_pool()
      {
        return &buffer_pool_;
      }
      void reset(void);
      void reset_local(void);
      inline void set_read_mode(const ScanFlag::SyncMode mode) { scan_flag_.read_mode_ = mode & ScanFlag::SF_MASK_READ_MODE; }
      inline ScanFlag::SyncMode get_read_mode() const { return static_cast<ScanFlag::SyncMode>(scan_flag_.read_mode_); }

      inline void set_scan_direction(const ScanFlag::Direction dir) { scan_flag_.direction_ = dir & 0x3; }
      inline ScanFlag::Direction get_scan_direction() const { return static_cast<ScanFlag::Direction>(scan_flag_.direction_); }

      inline void set_not_exit_col_ret_nop(const bool nop) { scan_flag_.not_exit_col_ret_nop_ = nop; }
      inline bool is_not_exit_col_ret_nop() const { return scan_flag_.not_exit_col_ret_nop_; }

      inline void set_daily_merge_scan(const bool full) { scan_flag_.daily_merge_scan_ = full; }
      inline bool is_daily_merge_scan() const { return scan_flag_.daily_merge_scan_; }

      inline void set_full_row_scan(const bool full) { scan_flag_.full_row_scan_ = full; }
      inline bool is_full_row_scan() const { return scan_flag_.full_row_scan_; }

      inline void set_rowkey_column_count(const int16_t count) { scan_flag_.rowkey_column_count_ = count; }
      inline int16_t get_rowkey_column_count() const { return scan_flag_.rowkey_column_count_; }

      inline void set_scan_flag(const ScanFlag flag) { scan_flag_ = flag; }
      inline ScanFlag get_scan_flag() const { return scan_flag_; }
      virtual int assign(const ObSqlReadParam* other);

      NEED_SERIALIZE_AND_DESERIALIZE;

      // dump scan param info, basic version
      void dump(void) const;
      // to_string
      int64_t to_string(char *buf, const int64_t buf_len) const;
    private:
      // BASIC_PARAM_FIELD
      int serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_basic_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_basic_param_serialize_size(void) const;

      // END_PARAM_FIELD
      int64_t get_end_param_serialize_size(void) const;

    private:
      friend class ::ObSqlScanParamTest_serialize_test_Test;
      bool deep_copy_args_;
      ObStringBuf buffer_pool_;
      ObNewRange range_;
      //反序列化的时候，需要利用这两个数组
      ObObj start_rowkey_obj_array_[OB_MAX_ROWKEY_COLUMN_NUMBER];
      ObObj end_rowkey_obj_array_[OB_MAX_ROWKEY_COLUMN_NUMBER];
      ScanFlag scan_flag_;
    };
  } /* sql */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_SQL_SCAN_PARAM_H_ */
