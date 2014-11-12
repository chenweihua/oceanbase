/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_rpc_scan.cpp
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_table_rpc_scan.h"
#include "common/utility.h"
#include "ob_sql_read_strategy.h"
#include "mergeserver/ob_merge_server_main.h"

#define CREATE_PHY_OPERRATOR_NEW(op, type_name, physical_plan, err)    \
  ({err = OB_SUCCESS;                                                   \
    op = OB_NEW(type_name, ObModIds::OB_SQL_TABLE_RPC_SCAN);            \
   if (op == NULL) \
   { \
     err = OB_ERR_PARSER_MALLOC_FAILED; \
     TBSYS_LOG(WARN, "Can not malloc space for %s", #type_name);  \
   } \
   else\
   {\
     op->set_phy_plan(physical_plan);              \
     ob_inc_phy_operator_stat(op->get_type());\
   } \
   op;})


namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTableRpcScan, PHY_TABLE_RPC_SCAN);
  }
}

namespace oceanbase
{
  namespace sql
  {
    ObTableRpcScan::ObTableRpcScan() :
      rpc_scan_(), scalar_agg_(NULL), group_(NULL), group_columns_sort_(), limit_(),
      has_rpc_(false), has_scalar_agg_(false), has_group_(false),
      has_group_columns_sort_(false), has_limit_(false), is_skip_empty_row_(true),
      read_method_(ObSqlReadStrategy::USE_SCAN)
    {
    }

    ObTableRpcScan::~ObTableRpcScan()
    {
      if (NULL != group_)
      {
        ob_dec_phy_operator_stat(group_->get_type());
        OB_DELETE(ObMergeGroupBy, ObModIds::OB_SQL_TABLE_RPC_SCAN, group_);
        group_ = NULL;
      }

      if (NULL != scalar_agg_)
      {
        ob_dec_phy_operator_stat(scalar_agg_->get_type());
        OB_DELETE(ObScalarAggregate, ObModIds::OB_SQL_TABLE_RPC_SCAN, scalar_agg_);
        scalar_agg_ = NULL;
      }
    }

    void ObTableRpcScan::reset()
    {
      if (has_rpc_)
      {
        rpc_scan_.reset();
        has_rpc_ = false;
      }
      select_get_filter_.reset();
      if (has_scalar_agg_)
      {
        if (NULL != scalar_agg_)
        {
          scalar_agg_->reset();
        }
        has_scalar_agg_ = false;
      }
      if (has_group_)
      {
        if (NULL != group_)
        {
          group_->reset();
        }
        has_group_ = false;
      }
      if (has_group_columns_sort_)
      {
        group_columns_sort_.reset();
        has_group_columns_sort_ = false;
      }
      if (has_limit_)
      {
        limit_.reset();
        has_limit_ = false;
      }
      empty_row_filter_.reset();
      is_skip_empty_row_ = true;
      read_method_ = ObSqlReadStrategy::USE_SCAN;
      ObTableScan::reset();
    }

    void ObTableRpcScan::reuse()
    {
      if (has_rpc_)
      {
        rpc_scan_.reuse();
        has_rpc_ = false;
      }
      select_get_filter_.reuse();
      if (has_scalar_agg_)
      {
        if (NULL != scalar_agg_)
        {
          scalar_agg_->reuse();
        }
        has_scalar_agg_ = false;
      }
      if (has_group_)
      {
        if (NULL == group_)
        {
          group_->reuse();
        }
        has_group_ = false;
      }
      if (has_group_columns_sort_)
      {
        group_columns_sort_.reuse();
        has_group_columns_sort_ = false;
      }
      if (has_limit_)
      {
        limit_.reuse();
        has_limit_ = false;
      }
      empty_row_filter_.reuse();
      is_skip_empty_row_ = true;
      read_method_ = ObSqlReadStrategy::USE_SCAN;
    }

    int ObTableRpcScan::open()
    {
      int ret = OB_SUCCESS;
      if (child_op_ == NULL)
      {
        // rpc_scan_ is the leaf operator
        if (OB_SUCCESS == ret && has_rpc_)
        {
          child_op_ = &rpc_scan_;
          child_op_->set_phy_plan(my_phy_plan_);
          if (ObSqlReadStrategy::USE_GET == read_method_
            && is_skip_empty_row_)
          {
            empty_row_filter_.set_child(0, *child_op_);
            select_get_filter_.set_child(0, empty_row_filter_);
            child_op_ = &select_get_filter_;
          }
        }
        else
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(WARN, "must call init() before call open(). ret=%d", ret);
        }
        // more operation over the leaf
        // pushed-down group by or scalar aggregation
        if (OB_SUCCESS == ret && (has_group_ || has_scalar_agg_))
        {
          if (has_group_ && has_scalar_agg_)
          {
            ret = OB_ERR_GEN_PLAN;
            TBSYS_LOG(WARN, "Group operator and scalar aggregate operator can not appear in TableScan at the same time. ret=%d", ret);
          }
          else if (has_scalar_agg_)
          {
            // add scalar aggregation
            if (OB_SUCCESS != (ret = scalar_agg_->set_child(0, *child_op_)))
            {
              TBSYS_LOG(WARN, "Fail to set child of scalar aggregate operator. ret=%d", ret);
            }
            else
            {
              child_op_ = scalar_agg_;
              child_op_->set_phy_plan(my_phy_plan_);
            }
          }
          else if (has_group_)
          {
            // add group by
            if (!has_group_columns_sort_)
            {
              ret = OB_ERR_GEN_PLAN;
              TBSYS_LOG(WARN, "Physical plan error, group need a sort operator. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = group_columns_sort_.set_child(0, *child_op_)))
            {
              TBSYS_LOG(WARN, "Fail to set child of sort operator. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = group_->set_child(0, group_columns_sort_)))
            {
              TBSYS_LOG(WARN, "Fail to set child of group operator. ret=%d", ret);
            }
            else
            {
              child_op_ = group_;
              child_op_->set_phy_plan(my_phy_plan_);
            }
          }
        }
        // limit
        if (OB_SUCCESS == ret && has_limit_)
        {
          if (OB_SUCCESS != (ret = limit_.set_child(0, *child_op_)))
          {
            TBSYS_LOG(WARN, "fail to set limit child. ret=%d", ret);
          }
          else
          {
            child_op_ = &limit_;
            child_op_->set_phy_plan(my_phy_plan_);
          }
        }
      }

      // open the operation chain
      if (OB_SUCCESS == ret)
      {
        ret = child_op_->open();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to open table scan. ret=%d", ret);
        }
      }
      return ret;
    }

    int ObTableRpcScan::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->close();
      }

      return ret;
    }

    int ObTableRpcScan::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_next_row(row);
      }
      return ret;
    }

    int ObTableRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_row_desc(row_desc);
      }
      return ret;
    }

    int ObTableRpcScan::init(ObSqlContext *context, const common::ObRpcScanHint *hint)
    {
      int ret = OB_SUCCESS;
      if (NULL != context)
      {
        if (hint)
        {
          read_method_ = hint->read_method_;
          is_skip_empty_row_ = hint->is_get_skip_empty_row_;
        }
        ret = rpc_scan_.init(context, hint);
        if (OB_SUCCESS == ret)
        {
          has_rpc_ = true;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "fail to init table rpc scan. params(null)");
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }

    int ObTableRpcScan::add_output_column(const ObSqlExpression& expr)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        // add output column to scan param
        ret = rpc_scan_.add_output_column(expr);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to add column to rpc scan operator. ret=%d", ret);
        }
      }
      return ret;
    }

    int ObTableRpcScan::add_group_column(const uint64_t tid, const uint64_t cid)
    {
      int ret = OB_SUCCESS;
      if (has_scalar_agg_)
      {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Can not adding group column after adding aggregate function(s). ret=%d", ret);
      }
      if (OB_SUCCESS == ret)
      {
        if (group_ == NULL)
        {
          CREATE_PHY_OPERRATOR_NEW(group_, ObMergeGroupBy, my_phy_plan_, ret);
        }
        if (OB_SUCCESS == ret)
        {
          group_->set_phy_plan(my_phy_plan_);
          if ((ret = group_columns_sort_.add_sort_column(tid, cid, true)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Add sort column of TableRpcScan sort operator failed. ret=%d", ret);
          }
          else if ((ret = group_->add_group_column(tid, cid)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Add group column of TableRpcScan group operator failed. ret=%d", ret);
          }
          else
          {
            has_group_ = true;
            has_group_columns_sort_ = true;
            ret = rpc_scan_.add_group_column(tid, cid);
          }
        }
      }
      return ret;
    }

    int ObTableRpcScan::add_aggr_column(const ObSqlExpression& expr)
    {
      int ret = OB_SUCCESS;
      ObItemType aggr_type = T_INVALID;
      bool is_distinct;
      if ((ret = expr.get_aggr_column(aggr_type, is_distinct)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Get aggregate function type failed. ret=%d", ret);
      }
      else if (is_distinct)
      {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Distinct aggregate function can not be processed in TableRpcScan. ret=%d", ret);
      }
      else if (aggr_type == T_FUN_AVG)
      {
        // avg() = sum()/count()
        // avg() is no longer in TableRpcScan
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Avg() aggregate function can not appears in TableRpcScan. ret=%d", ret);
      }
      else
      {
        ObSqlExpression part_expr(expr);
        part_expr.set_tid_cid(expr.get_table_id(), expr.get_column_id() - 1);
        ret = rpc_scan_.add_aggr_column(part_expr);
      }

      // generate local aggregate function
      ObSqlExpression local_expr;
      if (ret == OB_SUCCESS)
      {
        ObBinaryRefRawExpr col_expr(expr.get_table_id(), expr.get_column_id() - 1, T_REF_COLUMN);
        ObAggFunRawExpr sub_agg_expr(&col_expr, is_distinct, aggr_type);
        ObSqlRawExpr col_raw_expr(common::OB_INVALID_ID, expr.get_table_id(), expr.get_column_id(), &sub_agg_expr);
        if ((ret = col_raw_expr.fill_sql_expression(local_expr)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Generate local aggregate function of TableRpcScan failed. ret=%d", ret);
        }
        else if (aggr_type == T_FUN_COUNT)
        {
          local_expr.set_aggr_func(T_FUN_SUM, is_distinct);
        }
      }

      // add local aggregate function
      if (ret == OB_SUCCESS)
      {
        if (has_group_)
        {
          if ((ret = group_->add_aggr_column(local_expr)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Add aggregate function to TableRpcScan group operator failed. ret=%d", ret);
          }
        }
        else
        {
          if (scalar_agg_ == NULL)
          {
            CREATE_PHY_OPERRATOR_NEW(scalar_agg_, ObScalarAggregate, my_phy_plan_, ret);
          }
          if (OB_SUCCESS == ret)
          {
            scalar_agg_->set_phy_plan(my_phy_plan_);
            has_scalar_agg_ = true;
            if ((ret = scalar_agg_->add_aggr_column(local_expr)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "Add aggregate function to TableRpcScan scalar aggregate operator failed. ret=%d", ret);
            }
          }
        }
      }
      return ret;
    }

    int ObTableRpcScan::set_table(const uint64_t table_id, const uint64_t base_table_id)
    {
      int ret = OB_SUCCESS;
      // add table id to scan param
      if (OB_SUCCESS != (ret = rpc_scan_.set_table(table_id, base_table_id)))
      {
        TBSYS_LOG(WARN, "fail to add table id to rpc scan operator. table_id=%lu, ret=%d", base_table_id, ret);
      }
      return ret;
    }

    int ObTableRpcScan::add_filter(ObSqlExpression *expr)
    {
      int ret = OB_SUCCESS;
      ObSqlExpression* expr_clone = ObSqlExpression::alloc(); // @todo temporary work around
      if (NULL == expr_clone)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "no memory");
      }
      else
      {
        *expr_clone = *expr;
        if (OB_SUCCESS != (ret = rpc_scan_.add_filter(expr)))
        {
          TBSYS_LOG(WARN, "fail to add filter to rpc scan operator. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = select_get_filter_.add_filter(expr_clone)))
        {
          TBSYS_LOG(WARN, "fail to add filter to filter for select get. ret=%d", ret);
        }
      }
      return ret;
    }

    int ObTableRpcScan::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
    {
      int ret = OB_SUCCESS;
      if ((ret = limit_.set_limit(limit, offset)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fail to set limit. ret=%d", ret);
      }
      else
      {
        has_limit_ = true;
        // add limit to scan param
        if (offset.is_empty())
        {
          ret = rpc_scan_.set_limit(limit, offset);
        }
        else if (limit.is_empty())
        {
          ObSqlExpression empty_offset;
          ret = rpc_scan_.set_limit(limit, empty_offset);
        }
        else
        {
          ObSqlExpression new_limit;
          ObSqlExpression empty_offset;
          ExprItem op;
          op.type_ = T_OP_ADD;
          op.data_type_ = ObIntType;
          op.value_.int_ = 2;
          if ((ret = new_limit.merge_expr(limit, offset, op)) != OB_SUCCESS
            || (ret = rpc_scan_.set_limit(new_limit, empty_offset)) != OB_SUCCESS)
          {
          }
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "Fail to add limit/offset to rpc scan operator. ret=%d", ret);
        }
      }
      return ret;
    }

    int64_t ObTableRpcScan::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "TableRpcScan(read_method=%s, ", read_method_ == ObSqlReadStrategy::USE_SCAN ? "SCAN":"GET");
      if (has_limit_)
      {
        databuff_printf(buf, buf_len, pos, "limit=<");
        pos += limit_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_scalar_agg_)
      {
        databuff_printf(buf, buf_len, pos, "ScalarAggregate=<");
        pos += scalar_agg_->to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_group_)
      {
        databuff_printf(buf, buf_len, pos, "GroupBy=<");
        pos += group_->to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_group_columns_sort_)
      {
        databuff_printf(buf, buf_len, pos, "Sort=<");
        pos += group_columns_sort_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      databuff_printf(buf, buf_len, pos, "\nrpc_scan=<");
      pos += rpc_scan_.to_string(buf+pos, buf_len-pos);
      databuff_printf(buf, buf_len, pos, ">)\n");
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }

    PHY_OPERATOR_ASSIGN(ObTableRpcScan)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObTableRpcScan);
      reset();
      rpc_scan_.set_phy_plan(my_phy_plan_);
      select_get_filter_.set_phy_plan(my_phy_plan_);
      group_columns_sort_.set_phy_plan(my_phy_plan_);
      limit_.set_phy_plan(my_phy_plan_);
      empty_row_filter_.set_phy_plan(my_phy_plan_);

      if ((ret = rpc_scan_.assign(&o_ptr->rpc_scan_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign rpc_scan_ failed. ret=%d", ret);
      }
      else if ((ret = select_get_filter_.assign(&o_ptr->select_get_filter_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign select_get_filter_ failed. ret=%d", ret);
      }
      else if ((ret = group_columns_sort_.assign(&o_ptr->group_columns_sort_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign group_columns_sort_ failed. ret=%d", ret);
      }
      else if ((ret = limit_.assign(&o_ptr->limit_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign limit_ failed. ret=%d", ret);
      }
      else if ((ret = empty_row_filter_.assign(&o_ptr->empty_row_filter_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign empty_row_filter_ failed. ret=%d", ret);
      }
      else
      {
        if (o_ptr->scalar_agg_)
        {
          if (!scalar_agg_)
          {
            CREATE_PHY_OPERRATOR_NEW(scalar_agg_, ObScalarAggregate, my_phy_plan_, ret);
            if (OB_SUCCESS == ret)
            {
              scalar_agg_->set_phy_plan(my_phy_plan_);
            }
          }
          if (ret == OB_SUCCESS
            && (ret = scalar_agg_->assign(o_ptr->scalar_agg_)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Assign scalar_agg_ failed. ret=%d", ret);
          }
        }
        if (o_ptr->group_)
        {
          if (!group_)
          {
            CREATE_PHY_OPERRATOR_NEW(group_, ObMergeGroupBy, my_phy_plan_, ret);
            if (OB_SUCCESS == ret)
            {
              group_->set_phy_plan(my_phy_plan_);
            }
          }
          if (ret == OB_SUCCESS
            && (ret = group_->assign(o_ptr->group_)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Assign group_ failed. ret=%d", ret);
          }
        }
        has_rpc_ = o_ptr->has_rpc_;
        has_scalar_agg_ = o_ptr->has_scalar_agg_;
        has_group_ = o_ptr->has_group_;
        has_group_columns_sort_ = o_ptr->has_group_columns_sort_;
        has_limit_ = o_ptr->has_limit_;
        is_skip_empty_row_ = o_ptr->is_skip_empty_row_;
        read_method_ = o_ptr->read_method_;
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObTableRpcScan)
    {
      UNUSED(buf);
      UNUSED(buf_len);
      UNUSED(pos);
      return OB_NOT_IMPLEMENT;
#if 0
      int ret = OB_SUCCESS;
#define ENCODE_OP(has_op, op) \
      if (OB_SUCCESS == ret) \
      { \
        if (OB_SUCCESS != (ret = common::serialization::encode_bool(buf, buf_len, pos, has_op))) \
        { \
          TBSYS_LOG(WARN, "fail to encode " #has_op ":ret[%d]", ret); \
        } \
        else if (has_op) \
        { \
          if (OB_SUCCESS != (ret = op.serialize(buf, buf_len, pos))) \
          { \
            TBSYS_LOG(WARN, "fail to serialize " #op ":ret[%d]", ret); \
          } \
        } \
      }

      ENCODE_OP(has_scalar_agg_, scalar_agg_);
      ENCODE_OP(has_group_columns_sort_, group_columns_sort_);
      ENCODE_OP(has_group_, group_);
      ENCODE_OP(has_limit_, limit_);

#undef ENCODE_OP
      return ret;
#endif
    }

    DEFINE_DESERIALIZE(ObTableRpcScan)
    {
      UNUSED(buf);
      UNUSED(data_len);
      UNUSED(pos);
      return OB_NOT_IMPLEMENT;
#if 0
      int ret = OB_SUCCESS;
#define DECODE_OP(has_op, op) \
      if (OB_SUCCESS == ret) \
      { \
        if (OB_SUCCESS != (ret = common::serialization::decode_bool(buf, data_len, pos, &has_op))) \
        { \
          TBSYS_LOG(WARN, "fail to decode " #has_op ":ret[%d]", ret); \
        } \
        else if (has_op) \
        { \
          if (OB_SUCCESS != (ret = op.deserialize(buf, data_len, pos))) \
          { \
            TBSYS_LOG(WARN, "fail to deserialize " #op ":ret[%d]", ret); \
          } \
        } \
      }

      scalar_agg_.reset();
      DECODE_OP(has_scalar_agg_, scalar_agg_);
      group_columns_sort_.reset();
      DECODE_OP(has_group_columns_sort_, group_columns_sort_);
      group_.reset();
      DECODE_OP(has_group_, group_);
      limit_.reset();
      DECODE_OP(has_limit_, limit_);
#undef DECODE_OP
      return ret;
#endif
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTableRpcScan)
    {
      return 0;
#if 0
      int64_t size = 0;
#define GET_OP_SERIALIZE_SIZE(size, has_op, op) \
      size += common::serialization::encoded_length_bool(has_op); \
      if (has_op) \
      { \
        size += op.get_serialize_size(); \
      }

      GET_OP_SERIALIZE_SIZE(size, has_scalar_agg_, scalar_agg_);
      GET_OP_SERIALIZE_SIZE(size, has_group_columns_sort_, group_columns_sort_);
      GET_OP_SERIALIZE_SIZE(size, has_group_, group_);
      GET_OP_SERIALIZE_SIZE(size, has_limit_, limit_);
#undef GET_OP_SERIALIZE_SIZE
      return size;
#endif
    }

    ObPhyOperatorType ObTableRpcScan::get_type() const
    {
      return PHY_TABLE_RPC_SCAN;
    }
  } // end namespace sql
} // end namespace oceanbase
