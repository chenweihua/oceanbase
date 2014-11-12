/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "ob_root_server2.h"
#include "ob_root_worker.h"
#include "ob_heartbeat_checker.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;


ObHeartbeatChecker::ObHeartbeatChecker(ObRootServer2 * root_server):root_server_(root_server)
{
  OB_ASSERT(root_server != NULL);
}

ObHeartbeatChecker::~ObHeartbeatChecker()
{
}

void ObHeartbeatChecker::run(tbsys::CThread * thread, void * arg)
{
  UNUSED(thread);
  UNUSED(arg);
  ObServer tmp_server;
  bool master = true;
  int64_t now = 0;
  int64_t preview_rotate_time = 0;
  TBSYS_LOG(INFO, "[NOTICE] heart beat checker thread start");
  while (!_stop)
  {
    now = tbsys::CTimeUtil::getTime();
    if ((now > preview_rotate_time + 10 *1000 * 1000) &&
        ((now/(1000 * 1000)  - timezone) % (24 * 3600)  == 0))
    {
      preview_rotate_time = now;
      TBSYS_LOG(INFO, "rotateLog");
      TBSYS_LOGGER.rotateLog(NULL, NULL);
    }
    if (root_server_->is_master())
    {
      now = tbsys::CTimeUtil::getTime();
      ObChunkServerManager::iterator it = root_server_->server_manager_.begin();
      for (; it != root_server_->server_manager_.end(); ++it)
      {
        if (it->status_ != ObServerStatus::STATUS_DEAD)
        {
          if (it->is_alive(now, root_server_->config_.cs_lease_duration_time))
          {
            if (now - it->last_hb_time_ >= (root_server_->config_.cs_lease_duration_time / 2
                  + it->hb_retry_times_ * HB_RETRY_FACTOR))
            {
              it->hb_retry_times_ ++;
              tmp_server = it->server_;
              tmp_server.set_port(it->port_cs_);
              if (root_server_->worker_->get_rpc_stub()
                  .heartbeat_to_cs(tmp_server,
                    root_server_->config_.cs_lease_duration_time,
                    root_server_->get_frozen_version_for_cs_heartbeat(),
                    root_server_->get_schema_version(),
                    root_server_->get_config_version()) != OB_SUCCESS)
              {
                TBSYS_LOG(WARN, "heartbeat to cs fail, cs:[%s]", to_cstring(tmp_server));
                //do nothing
              }
            }
          }
          else
          {
            ObServer cs = it->server_;
            cs.set_port(it->port_cs_);
            TBSYS_LOG(ERROR,"chunkserver[%s] is down, now:%ld, lease_duration:%s",
                to_cstring(cs), now, root_server_->config_.cs_lease_duration_time.str());
            root_server_->server_manager_.set_server_down(it);
            root_server_->log_worker_->server_is_down(it->server_, now);
            // scoped lock
            {
              //this for only one thread modify root_table
              tbsys::CThreadGuard mutex_guard(&(root_server_->root_table_build_mutex_));
              tbsys::CWLockGuard guard(root_server_->root_table_rwlock_);
              if (root_server_->root_table_ != NULL)
              {
                root_server_->root_table_->server_off_line(static_cast<int32_t>
                    (it - root_server_->server_manager_.begin()), now);
                // some cs is down, signal the balance worker
                root_server_->balancer_thread_->wakeup();
              }
              else
              {
                TBSYS_LOG(ERROR, "root_table_for_query_ = NULL, server_index=%ld",
                    it - root_server_->server_manager_.begin());
              }
            }
            if (master)
            {
              root_server_->commit_task(SERVER_OFFLINE, OB_CHUNKSERVER, it->server_, 0, "hb server version null");
            }
          }
        }

        if (it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
        {
          if (it->is_ms_alive(now, root_server_->config_.cs_lease_duration_time) )
          {
            if (now - it->last_hb_time_ms_ >
                (root_server_->config_.cs_lease_duration_time / 2))
            {
              //hb to ms
              tmp_server = it->server_;
              tmp_server.set_port(it->port_ms_);
              if (OB_SUCCESS != root_server_->worker_->get_rpc_stub()
                  .heartbeat_to_ms(tmp_server,
                    root_server_->config_.cs_lease_duration_time,
                    root_server_->last_frozen_mem_version_,
                    root_server_->get_schema_version(),
                    root_server_->get_obi_role(),
                    root_server_->get_privilege_version(),
                    root_server_->get_config_version()))
              {
                TBSYS_LOG(WARN, "heartbeat to ms fail, ms:[%s]", to_cstring(tmp_server));
              }
            }
          }
          else
          {
            tmp_server = it->server_;
            tmp_server.set_port(it->port_ms_);
            root_server_->server_manager_.set_server_down_ms(it);
            if (master)
            {
              // no sql port for chunkserver manager
              if (!it->lms_)
              {
                root_server_->commit_task(SERVER_OFFLINE, OB_MERGESERVER, tmp_server,
                    it->port_ms_sql_, "hb server version null");
              }
            }
          }
        }
      } //end for
    } //end if master
    //async heart beat
    usleep(CHECK_INTERVAL_US);
  }
  TBSYS_LOG(INFO, "[NOTICE] heart beat checker thread exit");
}
