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
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_SLAVE_MGR_H_
#define OCEANBASE_UPDATESERVER_OB_SLAVE_MGR_H_

#include "common/ob_slave_mgr.h"
#include "common/ob_ack_queue.h"
#include "ob_ups_role_mgr.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    class ObUpsSlaveMgr : private common::ObSlaveMgr
    {
      public:
        static const int32_t RPC_VERSION = 1;
        static const int64_t MAX_SLAVE_NUM = 8;
        static const int64_t DEFAULT_ACK_QUEUE_LEN = 1024;
        ObUpsSlaveMgr();
        virtual ~ObUpsSlaveMgr();

        /// @brief 初始化
        int init(IObAsyncClientCallback* callback, ObUpsRoleMgr *role_mgr, ObCommonRpcStub *rpc_stub,
            int64_t log_sync_timeout);

        /// @brief 向各台Slave发送数据
        /// 目前依次向各台Slave发送数据, 并且等待Slave的成功返回
        /// Slave返回操作失败或者发送超时的情况下, 将Slave下线并等待租约(Lease)超时
        /// @param [in] data 发送数据缓冲区
        /// @param [in] length 缓冲区长度
        /// @retval OB_SUCCESS 成功
        /// @retval OB_PARTIAL_FAILED 同步Slave过程中有失败
        /// @retval otherwise 其他错误
        int send_data(const char* data, const int64_t length);
        int set_log_sync_timeout_us(const int64_t timeout);
        int post_log_to_slave(const common::ObLogCursor& start_cursor, const common::ObLogCursor& end_cursor, const char* data, const int64_t length);
        int wait_post_log_to_slave(const char* data, const int64_t length, int64_t& delay);
        int64_t get_acked_clog_id() const;
        int get_slaves(ObServer* slaves, int64_t limit, int64_t& slave_count);

        int grant_keep_alive();
        int add_server(const ObServer &server);
        int delete_server(const ObServer &server);
        int reset_slave_list();
        int set_send_log_point(const ObServer &server, const uint64_t send_log_point);
        int get_num() const;
        void print(char *buf, const int64_t buf_len, int64_t& pos);
      private:
        int64_t n_slave_last_post_;
        ObUpsRoleMgr *role_mgr_;
        ObAckQueue ack_queue_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_SLAVE_MGR_H_
