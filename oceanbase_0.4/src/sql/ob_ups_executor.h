/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_executor.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_EXECUTOR_H
#define _OB_UPS_EXECUTOR_H 1
#include "ob_no_children_phy_operator.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "ob_physical_plan.h"
#include "ob_result_set.h"
#include "ob_sql_session_info.h"
namespace oceanbase
{
  namespace sql
  {
    class ObUpsExecutor: public ObNoChildrenPhyOperator
    {
      public:
        ObUpsExecutor();
        virtual ~ObUpsExecutor();
        virtual void reset();
        virtual void reuse();
        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc){rpc_ = rpc;}
        void set_inner_plan(ObPhysicalPlan *plan) {inner_plan_ = plan;}
        ObPhysicalPlan *get_inner_plan() { return inner_plan_; }

        /// execute the insert statement
        virtual int open();
        virtual int close() {return common::OB_SUCCESS;};
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const {UNUSED(row_desc); return common::OB_NOT_SUPPORTED;}
        virtual enum ObPhyOperatorType get_type() const {return PHY_UPS_EXECUTOR;};
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // types and constants
      private:
        // disallow copy
        ObUpsExecutor(const ObUpsExecutor &other);
        ObUpsExecutor& operator=(const ObUpsExecutor &other);
        // function members
        int make_fake_desc(const int64_t column_num);
        int set_trans_params(ObSQLSessionInfo *session, common::ObTransReq &req);
      private:
        // data members
        mergeserver::ObMergerRpcProxy* rpc_;
        ObPhysicalPlan *inner_plan_;
        ObUpsResult local_result_;
        common::ObRow curr_row_;
        common::ObRowDesc row_desc_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_EXECUTOR_H */
