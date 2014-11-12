/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_rpc_scan.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_TABLE_RPC_SCAN_H
#define _OB_TABLE_RPC_SCAN_H 1
#include "ob_table_scan.h"
#include "ob_rpc_scan.h"
#include "ob_sql_expression.h"
#include "ob_table_rename.h"
#include "ob_project.h"
#include "ob_filter.h"
#include "ob_scalar_aggregate.h"
#include "ob_merge_groupby.h"
#include "ob_sort.h"
#include "ob_limit.h"
#include "ob_empty_row_filter.h"
#include "ob_sql_context.h"
#include "common/ob_row.h"
#include "common/ob_hint.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTableRpcScan: public ObTableScan
    {
      public:
        ObTableRpcScan();
        virtual ~ObTableRpcScan();
        virtual void reset();
        virtual void reuse();
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual ObPhyOperatorType get_type() const;

        int init(ObSqlContext *context, const common::ObRpcScanHint *hint = NULL);

        /**
         * 添加一个需输出的column
         *
         * @note 只有通过复合列结算新生成的列才需要new_column_id
         * @param expr [in] 需输出的列（这个列可能是个复合列的结果）
         *
         * @return OB_SUCCESS或错误码
         */
        int add_output_column(const ObSqlExpression& expr);

        /**
         * 设置table_id
         * @note 只有基本表被重命名的情况才会使两个不相同id，其实两者相同时base_table_id可以给个默认值。
         * @param table_id [in] 输出的table_id
         * @param base_table_id [in] 被访问表的id
         *
         * @return OB_SUCCESS或错误码
         */
        int set_table(const uint64_t table_id, const uint64_t base_table_id);
        /**
         * 添加一个filter
         *
         * @param expr [in] 过滤表达式
         *
         * @return OB_SUCCESS或错误码
         */
        int add_filter(ObSqlExpression *expr);
        int add_group_column(const uint64_t tid, const uint64_t cid);
        int add_aggr_column(const ObSqlExpression& expr);

        /**
         * 指定limit/offset
         *
         * @param limit [in]
         * @param offset [in]
         *
         * @return OB_SUCCESS或错误码
         */
        int set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        void set_phy_plan(ObPhysicalPlan *the_plan);
        int32_t get_child_num() const;

        void set_rowkey_cell_count(const int64_t rowkey_cell_count)
        {
          rpc_scan_.set_rowkey_cell_count(rowkey_cell_count);
        }

        inline void set_need_cache_frozen_data(bool need_cache_frozen_data)
        {
          rpc_scan_.set_need_cache_frozen_data(need_cache_frozen_data);
        }
        inline void set_cache_bloom_filter(bool cache_bloom_filter)
        {
          rpc_scan_.set_cache_bloom_filter(cache_bloom_filter);
        }

        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // disallow copy
        ObTableRpcScan(const ObTableRpcScan &other);
        ObTableRpcScan& operator=(const ObTableRpcScan &other);
      private:
        // data members
        ObRpcScan rpc_scan_;
        ObFilter select_get_filter_;
        ObScalarAggregate *scalar_agg_; // very big
        ObMergeGroupBy *group_; // very big
        ObSort group_columns_sort_;
        ObLimit limit_;
        ObEmptyRowFilter empty_row_filter_;
        bool has_rpc_;
        bool has_scalar_agg_;
        bool has_group_;
        bool has_group_columns_sort_;
        bool has_limit_;
        bool is_skip_empty_row_;
        int32_t read_method_;
    };
    inline void ObTableRpcScan::set_phy_plan(ObPhysicalPlan *the_plan)
    {
      ObPhyOperator::set_phy_plan(the_plan);
      rpc_scan_.set_phy_plan(the_plan);
      select_get_filter_.set_phy_plan(the_plan);
      group_columns_sort_.set_phy_plan(the_plan);
      limit_.set_phy_plan(the_plan);
      limit_.set_phy_plan(the_plan);
    }
    inline int32_t ObTableRpcScan::get_child_num() const
    {
      return 0;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_SCAN_H */
