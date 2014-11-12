/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_expression.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SQL_EXPRESSION_H
#define _OB_SQL_EXPRESSION_H 1
#include "common/ob_object.h"
#include "ob_postfix_expression.h"
#include "common/ob_row.h"
#include "common/dlist.h"
class ObAggregateFunctionTest;

namespace oceanbase
{
  namespace sql
  {

    class ObSqlExpression: public common::DLink
    {
      public:
        ObSqlExpression();
        virtual ~ObSqlExpression();

        ObSqlExpression(const ObSqlExpression &other);
        ObSqlExpression& operator=(const ObSqlExpression &other);

        void set_int_div_as_double(bool did);

        void set_tid_cid(const uint64_t tid, const uint64_t cid);
        const uint64_t get_column_id() const;
        const uint64_t get_table_id() const;

        void set_aggr_func(ObItemType aggr_fun, bool is_distinct);
        int get_aggr_column(ObItemType &aggr_fun, bool &is_distinct) const;
        /**
         * 设置表达式
         * @param expr [in] 表达式，表达方式与实现相关，目前定义为后缀表达式
         *
         * @return error code
         */
        int add_expr_obj(const ObObj &obj);
        int add_expr_item(const ExprItem &item);
        int add_expr_item_end();
        void reset();

        /**
         * 获取解码后的表达式
         */
        inline const ObPostfixExpression &get_decoded_expression() const;
        inline bool is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const;
        /**
         * 根据表达式语义对row的值进行计算
         *
         * @param row [in] 输入行
         * @param result [out] 计算结果
         *
         * @return error code
         */
        int calc(const common::ObRow &row, const common::ObObj *&result);
        /// 打印表达式
        int64_t to_string(char* buf, const int64_t buf_len) const;

        // check expression type
        inline int is_const_expr(bool &is_const_type) const;
        inline int is_column_index_expr(bool &is_idx_type) const;
        inline int is_simple_condition(bool &is_simple_cond_type) const;
        inline int get_column_index_expr(uint64_t &tid, uint64_t &cid, bool &is_idx_type) const;
        inline int merge_expr(const ObSqlExpression &expr1, const ObSqlExpression &expr2, const ExprItem &op);
        inline bool is_simple_condition(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &const_val, ObPostfixExpression::ObPostExprNodeType *val_type = NULL) const;
        inline bool is_simple_between(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &cond_start, ObObj &cond_end) const;
        inline bool is_simple_in_expr(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
            common::PageArena<ObObj,common::ModulePageAllocator> &allocator) const;
        inline bool is_aggr_func() const;
        inline bool is_empty() const;
        inline void set_owner_op(ObPhyOperator *owner_op);
        inline ObPhyOperator* get_owner_op();
        NEED_SERIALIZE_AND_DESERIALIZE;
      public:
        static ObSqlExpression* alloc();
        static void free(ObSqlExpression* ptr);
      private:
        friend class ::ObAggregateFunctionTest;
        // data members
        ObPostfixExpression post_expr_;
        uint64_t column_id_;
        uint64_t table_id_;
        bool is_aggr_func_;
        bool is_distinct_;
        ObItemType aggr_func_;
      private:
        // method
        int serialize_basic_param(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_basic_param_serialize_size(void) const;
    };
    typedef common::ObArray<ObSqlExpression, ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > ObExpressionArray;
    class ObSqlExpressionUtil
    {
      public:
        static int make_column_expr(const uint64_t tid, const uint64_t cid, ObSqlExpression &expr);
      private:
        DISALLOW_COPY_AND_ASSIGN(ObSqlExpressionUtil);
        ObSqlExpressionUtil();
        ~ObSqlExpressionUtil();
    };

    inline void ObSqlExpression::reset(void)
    {
      DLink::reset();
      post_expr_.reset();
      column_id_ = OB_INVALID_ID;
      table_id_ = OB_INVALID_ID;
      is_aggr_func_ = is_distinct_ = false;
    }

    inline void ObSqlExpression::set_int_div_as_double(bool did)
    {
      post_expr_.set_int_div_as_double(did);
    }

    inline void ObSqlExpression::set_tid_cid(const uint64_t tid, const uint64_t cid)
    {
      table_id_ = tid;
      column_id_ = cid;
    }

    inline const uint64_t ObSqlExpression::get_column_id() const
    {
      return column_id_;
    }

    inline const uint64_t ObSqlExpression::get_table_id() const
    {
      return table_id_;
    }

    inline int ObSqlExpression::get_aggr_column(ObItemType &aggr_fun, bool &is_distinct) const
    {
      int ret = OB_SUCCESS;
      if (!is_aggr_func_)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "this expression is not an aggr function");
      }
      else
      {
        aggr_fun = aggr_func_;
        is_distinct = is_distinct_;
      }
      return ret;
    }

    inline void ObSqlExpression::set_aggr_func(ObItemType aggr_func, bool is_distinct)
    {
      OB_ASSERT(aggr_func >= T_FUN_MAX && aggr_func <= T_FUN_AVG);
      is_aggr_func_ = true;
      aggr_func_ = aggr_func;
      is_distinct_ = is_distinct;
    }

    inline const ObPostfixExpression &ObSqlExpression::get_decoded_expression() const
    {
      return post_expr_;
    }

    inline bool ObSqlExpression::is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const
    {
      return post_expr_.is_equijoin_cond(c1, c2);
    }
    inline int ObSqlExpression::is_const_expr(bool &is_const_type) const
    {
      return post_expr_.is_const_expr(is_const_type);
    }

    inline int ObSqlExpression::get_column_index_expr(uint64_t &tid, uint64_t &cid, bool &is_idx_type) const
    {
      return post_expr_.get_column_index_expr(tid, cid, is_idx_type);
    }
    inline int ObSqlExpression::is_column_index_expr(bool &is_idx_type) const
    {
      return post_expr_.is_column_index_expr(is_idx_type);
    }
    inline bool ObSqlExpression::is_simple_condition(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &const_val, ObPostfixExpression::ObPostExprNodeType *val_type) const
    {
      return post_expr_.is_simple_condition(real_val, column_id, cond_op, const_val, val_type);
    }
    inline bool ObSqlExpression::is_simple_between(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &cond_start, ObObj &cond_end) const
    {
      return post_expr_.is_simple_between(real_val, column_id, cond_op, cond_start, cond_end);
    }
    inline bool ObSqlExpression::is_simple_in_expr(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
        common::PageArena<ObObj,common::ModulePageAllocator>  &allocator) const
    {
      return post_expr_.is_simple_in_expr(real_val, info, rowkey_array, allocator);
    }
    inline bool ObSqlExpression::is_aggr_func() const
    {
      return is_aggr_func_;
    }
    inline bool ObSqlExpression::is_empty() const
    {
      return post_expr_.is_empty();
    }
    inline void ObSqlExpression::set_owner_op(ObPhyOperator *owner_op)
    {
      post_expr_.set_owner_op(owner_op);
    }
    inline ObPhyOperator* ObSqlExpression::get_owner_op()
    {
      return post_expr_.get_owner_op();
    }
    inline int ObSqlExpression::merge_expr(const ObSqlExpression &expr1, const ObSqlExpression &expr2, const ExprItem &op)
    {
      reset();
      return post_expr_.merge_expr(expr1.post_expr_, expr2.post_expr_, op);
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SQL_EXPRESSION_H */
