/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_service_ms_provider.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_schema_service_ms_provider.h"
#include "common/utility.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObSchemaServiceMsProvider::ObSchemaServiceMsProvider(const ObChunkServerManager &server_manager)
  :server_manager_(server_manager), count_(0)
{
}

ObSchemaServiceMsProvider:: ~ObSchemaServiceMsProvider()
{
}

bool ObSchemaServiceMsProvider::did_need_reset()
{
  bool ret = true;
  tbsys::CRLockGuard guard(rwlock_);
  for (int64_t i = 0; i < count_; ++i)
  {
    if (MAX_MS_RETRY > ms_carray_[i].retry_count_)
    {
      ret = false;
      break;
    }
  } // end for
  return ret;
}

void ObSchemaServiceMsProvider::update_ms_retry(const ObServer &ms)
{
  tbsys::CWLockGuard guard(rwlock_);
  for (int64_t i = 0; i < count_; ++i)
  {
    if (ms == ms_carray_[i].ms_)
    {
      ++ms_carray_[i].retry_count_;
      break;
    }
  }
}

int ObSchemaServiceMsProvider::reset()
{
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(rwlock_);
  count_ = 0;
  ObChunkServerManager::const_iterator it;
  for (it = server_manager_.begin();
       server_manager_.end() != it && count_ < MAX_SERVER_COUNT;
       ++it)
  {
    if (ObServerStatus::STATUS_DEAD != it->ms_status_)
    {
      ObServer ms = it->server_;
      ms.set_port(it->port_ms_);
      ms_carray_[count_].ms_ = ms;
      ms_carray_[count_].retry_count_ = 0;
      ++count_;
      TBSYS_LOG(DEBUG, "schema service ms provider found count_=%ld, ms=%s", count_, to_cstring(ms));
    }
  } // end for
  // shuffle the server list after reset
  if (count_ > 0)
  {
    // std::random_shuffle(ms_carray_, ms_carray_ + count_);
  }
  return ret;
}

int ObSchemaServiceMsProvider::get_ms(const ObScanParam &scan_param, const int64_t retry_num, ObServer &ms)
{
  int ret = OB_SUCCESS;
  UNUSED(scan_param);
  if (0 < retry_num
      && 0 != ms.get_port()
      && 0 != ms.get_ipv4())
  {
    update_ms_retry(ms);
  }
  if (did_need_reset())
  {
    if (OB_SUCCESS != (ret = reset()))
    {
      TBSYS_LOG(WARN, "failed to init schema service ms provider, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (retry_num >= count_)
    {
      TBSYS_LOG(DEBUG, "no more ms for scan, retry=%ld count=%ld", retry_num, count_);
      ret = OB_MS_ITER_END;
    }
    else
    {
      tbsys::CRLockGuard guard(rwlock_);
      ms = ms_carray_[retry_num].ms_;
    }
  }
  return ret;
}

