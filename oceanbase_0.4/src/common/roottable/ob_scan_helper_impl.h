/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scan_helper.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SCAN_HELPER_IMPL_H
#define _OB_SCAN_HELPER_IMPL_H 1
#include "common/ob_scan_param.h"
#include "common/ob_scanner.h"
#include "common/ob_common_rpc_stub.h"
#include "ob_scan_helper.h"
#include "ob_ms_provider.h"
#include "ob_ups_provider.h"

namespace oceanbase
{
  namespace common
  {
    // thread-safe if ms_provider and ups_provider is thread-safe
    class ObScanHelperImpl: public ObScanHelper
    {
      public:
        ObScanHelperImpl();
        virtual ~ObScanHelperImpl();
        void set_ms_provider(ObMsProvider * ms_provider);
        void set_ups_provider(ObUpsProvider * ups_provider);
        void set_rpc_stub(ObCommonRpcStub * rpc_stub);
        void set_scan_timeout(const int64_t timeout);
        void set_scan_retry_times(const int64_t retry_times);
        void set_mutate_timeout(const int64_t timeout);
      public:
        virtual int scan(const ObScanParam& scan_param, ObScanner &out) const;
        virtual int mutate(ObMutator& mutator);
      private:
        bool check_inner_stat(void) const;
      private:
        DISALLOW_COPY_AND_ASSIGN(ObScanHelperImpl);
        // data members
        int64_t scan_timeout_us_;
        int64_t mutate_timeout_us_;
        int64_t scan_retry_times_;
        ObMsProvider * ms_provider_;
        ObUpsProvider * ups_provider_;
        ObCommonRpcStub * rpc_stub_;
    };

    inline bool ObScanHelperImpl::check_inner_stat(void) const
    {
      return ((NULL != ms_provider_) && (NULL != rpc_stub_)
          && (scan_timeout_us_ > 0) && (scan_retry_times_ >= 0)
          && (mutate_timeout_us_ > 0));
    }

    inline void ObScanHelperImpl::set_ms_provider(ObMsProvider *ms_provider)
    {
      ms_provider_ = ms_provider;
    }

    inline void ObScanHelperImpl::set_rpc_stub(ObCommonRpcStub *rpc_stub)
    {
      rpc_stub_ = rpc_stub;
    }

    inline void ObScanHelperImpl::set_scan_timeout(const int64_t timeout)
    {
      scan_timeout_us_ = timeout;
    }

    inline void ObScanHelperImpl::set_mutate_timeout(const int64_t timeout)
    {
      mutate_timeout_us_ = timeout;
    }

    inline void ObScanHelperImpl::set_ups_provider(ObUpsProvider *ups_provider)
    {
      ups_provider_ = ups_provider;
    }

    inline void ObScanHelperImpl::set_scan_retry_times(const int64_t retry_times)
    {
      scan_retry_times_ = retry_times;
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_SCAN_HELPER_IMPL_H */

