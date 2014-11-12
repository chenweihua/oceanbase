/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scan_helper.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_scan_helper_impl.h"
#include "common/utility.h"
using namespace oceanbase::common;

ObScanHelperImpl::ObScanHelperImpl():scan_timeout_us_(0), mutate_timeout_us_(0),
   scan_retry_times_(OB_TABLET_MAX_REPLICA_COUNT), ms_provider_(NULL),
   ups_provider_(NULL), rpc_stub_(NULL)
{
}

ObScanHelperImpl::~ObScanHelperImpl()
{
}

int ObScanHelperImpl::scan(const ObScanParam& scan_param, ObScanner &out) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "scan help not init");
  }
  else
  {
    ObServer ms;
    for (int64_t i = 0; i < scan_retry_times_; ++i)
    {
      if (OB_SUCCESS != (ret = ms_provider_->get_ms(scan_param, i, ms)))
      {
        TBSYS_LOG(WARN, "failed to get one mergeserver, err=%d", ret);
      }
      else if (0 == ms.get_port() || 0 == ms.get_ipv4())
      {
        TBSYS_LOG(WARN, "invalid merge server address, i=%ld", i);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS == (ret = rpc_stub_->scan(ms, scan_param, out, scan_timeout_us_)))
      {
        TBSYS_LOG(DEBUG, "scan from ms=%s", to_cstring(ms));
        break;
      }
      else
      {
        TBSYS_LOG(WARN, "scan ms timeout, scan_timeout_us_=%ld, ms=%s, retry=%ld",
            scan_timeout_us_, to_cstring(ms), i);
      }
    } // end for
  }
  return ret;
}

int ObScanHelperImpl::mutate(ObMutator& mutator)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "scan help not init");
  }
  else
  {
    ObServer ups;
    if (OB_SUCCESS != (ret = ups_provider_->get_ups(ups)))
    {
      TBSYS_LOG(WARN, "failed to get ups, err=%d", ret);
    }
    else if (0 == ups.get_port() || 0 == ups.get_ipv4())
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "invalid update server address");
    }
    else if (OB_SUCCESS != (ret = rpc_stub_->mutate(ups, mutator, mutate_timeout_us_)))
    {
      TBSYS_LOG(WARN, "failed to mutate, mutate_timeout_us_=%ld, ups=%s, err=%d",
          mutate_timeout_us_, to_cstring(ups), ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "ups mutate succ, ups=%s", to_cstring(ups));
    }
  }
  return ret;
}

