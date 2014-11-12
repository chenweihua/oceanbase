/**
  * (C) 2007-2010 Taobao Inc.
  *
  * This program is free software; you can redistribute it and/or modify
  * it under the terms of the GNU General Public License version 2 as
  * published by the Free Software Foundation.
  *
  * Version: $Id$
  *
  * Authors:
  *   zhidong <xielun.szd@taobao.com>
  *     - some work details if you want
  */

#ifndef OB_ROOT_SQL_PROXY_H_
#define OB_ROOT_SQL_PROXY_H_

#include "ob_root_ms_provider.h"

namespace oceanbase
{
  namespace common
  {
    class ObString;
  }
  namespace rootserver
  {
    class ObRootRpcStub;
    class ObChunkServerManager;
    // thread safe sql proxy
    class ObRootSQLProxy
    {
    public:
      ObRootSQLProxy(ObChunkServerManager & server_manager, ObRootServerConfig &config, ObRootRpcStub & rpc_stub);
      virtual ~ObRootSQLProxy();
    public:
      // exectue sql query
      int query(const int64_t retry_times, const int64_t timeout, const common::ObString & sql);
      int query(const bool query_master_cluster, const int64_t retry_times, const int64_t timeout, const common::ObString & sql);
    private:
      ObRootMsProvider ms_provider_;
      ObRootRpcStub & rpc_stub_;
    };
  }
}

#endif //OB_ROOT_SQL_PROXY_H_
