/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_aggregate_function.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_AGGREGATE_FUNCTION_H
#define _OB_AGGREGATE_FUNCTION_H 1
#include "ob_sql_expression.h"
#include "common/ob_array.h"
#include "common/hash/ob_hashset.h"
#include "common/ob_row_store.h"
#include "common/ob_se_array.h"
#include "common/ob_vector.h"
#include <stdint.h>
namespace oceanbase
{
  namespace sql
  {
    class ObAggregateFunction
    {
      public:
        ObAggregateFunction();
        ~ObAggregateFunction();
        void reset();
        void reuse();
        void set_int_div_as_double(bool did);
        bool get_int_div_as_double() const;

        int init(const ObRowDesc &input_row_desc, ObExpressionArray &aggr_columns);
        void destroy();
        const ObRow& get_curr_row() const;
        const ObRowDesc& get_row_desc() const;

        int prepare(const ObRow &row);
        int process(const ObRow &row);
        int get_result(const ObRow *&row);

        // used by ScalarAggregate operator when there's no input rows
        int get_result_for_empty_set(const ObRow *&row);

        int64_t get_used_mem_size() const;
      private:
        // types and constants
        typedef common::hash::ObHashSet<const common::ObObj*> DedupSet;
        static const int64_t DEDUP_HASH_SET_SIZE = (1024*1024);
      private:
        // disallow copy
        ObAggregateFunction(const ObAggregateFunction &other);
        ObAggregateFunction& operator=(const ObAggregateFunction &other);
        // function members
        int aggr_get_cell(const uint64_t table_id, const uint64_t column_id, common::ObExprObj *&cell);
        int aux_get_cell(const uint64_t table_id, const uint64_t column_id, common::ObExprObj *&cell);
        int init_aggr_cell(const ObItemType aggr_fun, const ObObj &oprand, ObExprObj &res1, ObExprObj &res2);
        int calc_aggr_cell(const ObItemType aggr_fun, const ObObj &oprand, ObExprObj &res1, ObExprObj &res2);
        int clone_expr_cell(const common::ObExprObj &cell, common::ObExprObj &cell_clone);
        int clone_cell(const common::ObObj &cell, common::ObObj &cell_clone);
        int init_dedup_sets();
        void destroy_dedup_sets();
      private:
        // data members
        ObExpressionArray *aggr_columns_;
        ObRowDesc row_desc_;
        ObRow curr_row_;        // current row for output
        ObExprObj empty_expr_obj_;
        common::ObSEArray<ObExprObj, OB_PREALLOCATED_NUM> aggr_cells_;
        common::ObSEArray<ObExprObj, OB_PREALLOCATED_NUM> aux_cells_;// to store count for avg()
        char* varchar_buffs_[common::OB_ROW_MAX_COLUMNS_COUNT];
        int64_t varchar_buffs_count_;
        common::ObRowStore row_store_;
        ObRowDesc dedup_row_desc_;
        //不能使用ObSEArray
        DedupSet dedup_sets_[common::OB_ROW_MAX_COLUMNS_COUNT];
        bool did_int_div_as_double_;
    };

    inline void ObAggregateFunction::set_int_div_as_double(bool did)
    {
      did_int_div_as_double_ = did;
    }

    inline bool ObAggregateFunction::get_int_div_as_double() const
    {
      return did_int_div_as_double_;
    }

    inline const ObRow& ObAggregateFunction::get_curr_row() const
    {
      return curr_row_;
    }

    inline const ObRowDesc& ObAggregateFunction::get_row_desc() const
    {
      return row_desc_;
    }

    inline int64_t ObAggregateFunction::get_used_mem_size() const
    {
      return row_store_.get_used_mem_size();
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_AGGREGATE_FUNCTION_H */
