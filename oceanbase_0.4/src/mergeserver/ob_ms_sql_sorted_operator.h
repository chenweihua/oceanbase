/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_merger_sorted_operator.h for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MS_SQL_SORTED_OPERATOR_H_
#define MERGESERVER_OB_MS_SQL_SORTED_OPERATOR_H_
#include "common/ob_row_iterator.h"
#include "common/ob_row.h"
#include "ob_ms_request.h"
#include "common/ob_se_array.h"
namespace oceanbase
{
  namespace sql
  {
    class ObSqlScanParam;
  }
  namespace common
  {
    class ObNewScanner;
    class ObNewRange;
    class ObCellArray;
    class ObRowkey;
  }
  namespace mergeserver
  {
    class ObMsSqlSortedOperator : public oceanbase::common::ObRowIterator
    {
    public:
      ObMsSqlSortedOperator();
      virtual ~ObMsSqlSortedOperator();

      /// initialize
      int set_param(const sql::ObSqlScanParam & scan_param);
      /// add a subscanrequest's result
      int add_sharding_result(common::ObNewScanner & sharding_res, const common::ObNewRange & query_range, bool &is_finish, ObStringBuf &rowkey_buffer);
      /// finish processing result, like orderby grouped result

      int64_t get_mem_size_used()const
      {
        return total_mem_size_used_;
      }

    public:
      // row interface
      int get_next_row(common::ObRow &row);

      void reset();

      int64_t get_sharding_result_count()const { return sharding_result_arr_.count(); }
      int64_t get_cur_sharding_result_idx()const { return cur_sharding_result_idx_; }
      //inline int64_t get_seamless_result_count() { return seamless_result_count_; }

    private:
      static const int64_t FULL_SCANNER_RESERVED_BYTE_COUNT  = 200;
      void sort(bool &is_finish, oceanbase::common::ObNewScanner * last_sharding_res = NULL);
      struct sharding_result_t
      {
        common::ObNewScanner *sharding_res_;
        const common::ObNewRange   *sharding_query_range_;
        const sql::ObSqlScanParam *param_;
        int64_t                   fullfilled_item_num_;
        common::ObRowkey last_row_key_;

        void init(common::ObNewScanner & sharding_res, const common::ObNewRange & query_range, const sql::ObSqlScanParam &param,
          common::ObRowkey & last_proces_rowkey, const int64_t fullfilled_item_num);
        bool operator<(const sharding_result_t & other)const;
      };
      static const int64_t COMMON_SHARDING_RESULT_COUNT = 4;
      ObSEArray<sharding_result_t, COMMON_SHARDING_RESULT_COUNT> sharding_result_arr_;
      int64_t           seamless_result_count_;
      int64_t           cur_sharding_result_idx_;
      const sql::ObSqlScanParam    *scan_param_;
      common::ObNewRange              scan_range_;
      int64_t           total_mem_size_used_;
    };
  }
}

#endif /* MERGESERVER_OB_MS_SQL_SORTED_OPERATOR_H_ */
