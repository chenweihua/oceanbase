/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scalar_aggregate.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef _OB_SCALAR_AGGREGATE_H
#define _OB_SCALAR_AGGREGATE_H
#include "sql/ob_merge_groupby.h"
#include "sql/ob_single_child_phy_operator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObScalarAggregate: public ObSingleChildPhyOperator
    {
      public:
        ObScalarAggregate();
        virtual ~ObScalarAggregate();
        virtual void reset();
        virtual void reuse();
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_SCALAR_AGGREGATE; }
        virtual int get_next_row(const ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        /**
         * 添加一个聚集表达式
         *
         * @param expr [in] 聚集函数表达式
         *
         * @return OB_SUCCESS或错误码
         */
        virtual int add_aggr_column(const ObSqlExpression& expr);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        void set_phy_plan(ObPhysicalPlan *the_plan);

        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
     
      private:
        // disallow copy
        ObScalarAggregate(const ObScalarAggregate &other);
        ObScalarAggregate& operator=(const ObScalarAggregate &other);
      private:
        ObMergeGroupBy merge_groupby_; // use MergeGroupBy to implement this operator
        bool is_first_row_;
        bool is_input_empty_;
    };
    inline void ObScalarAggregate::set_phy_plan(ObPhysicalPlan *the_plan)
    {
      ObPhyOperator::set_phy_plan(the_plan);
      merge_groupby_.set_phy_plan(the_plan);
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SCALAR_AGGREGATE_H */
