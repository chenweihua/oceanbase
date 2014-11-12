/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_ups_rpc_proxy.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_SQL_UPS_RPC_PROXY_H
#define _OB_SQL_UPS_RPC_PROXY_H 1

#include "ob_server.h"
#include "ob_new_scanner.h"
#include "ob_get_param.h"
#include "ob_scan_param.h"

namespace oceanbase
{
  namespace common
  {
    class ObSqlUpsRpcProxy
    {
      public:
        virtual ~ObSqlUpsRpcProxy() {}

        virtual int sql_ups_get(const ObGetParam & get_param, ObNewScanner & new_scanner, const int64_t timeout) = 0;
        virtual int sql_ups_scan(const ObScanParam & scan_param, ObNewScanner & new_scanner, const int64_t timeout) = 0;
    };
  }
}

#endif /* _OB_SQL_UPS_RPC_PROXY_H */

