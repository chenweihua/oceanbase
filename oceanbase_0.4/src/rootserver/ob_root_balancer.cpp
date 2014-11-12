/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_balancer.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_balancer.h"
#include "common/utility.h"
#include "ob_root_util.h"
#include "ob_root_ms_provider.h"
#include "ob_root_ups_provider.h"
#include "common/roottable/ob_scan_helper_impl.h"
#include "common/ob_table_id_name.h"
#include "common/ob_common_stat.h"
#include "common/file_utils.h"
#include "ob_root_server2.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;


ObLoadDataInfo::ObLoadDataInfo()
{
  reset();
}

int ObLoadDataInfo::clone(ObLoadDataInfo& info)
{
  reset();
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = buffer_.write_string(info.table_name_, &table_name_)))
  {
    TBSYS_LOG(WARN, "failed to write table name, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = buffer_.write_string(info.uri_, &uri_)))
  {
    TBSYS_LOG(WARN, "failed to write uri_, ret=%d", ret);
  }
  else
  {
    table_id_ = info.table_id_;
    old_table_id_ = info.old_table_id_;
    start_time_ = info.start_time_;
    end_time_ = info.end_time_;
    tablet_version_ = info.tablet_version_;
    status_ = info.status_;
    data_source_type_ = info.data_source_type_;
  }
  return ret;
}

const char* ObLoadDataInfo::get_status() const
{
  const char* ret = "ERROR";
  switch(status_)
  {
    case INIT:
      ret = "INIT";
      break;
    case PREPARE:
      ret = "PREPARE";
      break;
    case DOING:
      ret = "DOING";
      break;
    case DONE:
      ret = "DONE";
      break;
    case FAILED:
      ret = "FAILED";
      break;
    case KILLED:
      ret = "KILLED";
      break;
    default:
      break;
  }
  return ret;
}

void ObLoadDataInfo::reset()
{
  table_name_.reset();
  table_id_ = 0;
  old_table_id_ = 0;
  start_time_ = -1;
  end_time_ = 0;
  tablet_version_ = 0;
  uri_.reset();
  status_ = INIT;
  data_source_type_ = ObDataSourceDesc::UNKNOWN;
  buffer_.clear();
}

int ObLoadDataInfo::set_info(const ObString& table_name, const uint64_t table_id, const uint64_t old_table_id, ObString& uri,
    const int64_t tablet_version, const int64_t start_time, const ObLoadDataStatus status)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = buffer_.write_string(table_name, &table_name_)))
  {
    TBSYS_LOG(WARN, "failed to write table name, ret=%d", ret);
  }
  else
  {
    ObString tmp;
    int32_t proxy_uri_prefix_len = static_cast<int32_t>(strlen(proxy_uri_prefix));
    int32_t oceanbase_uri_prefix_len = static_cast<int32_t>(strlen(oceanbase_uri_prefix));


    if (uri.length() > proxy_uri_prefix_len &&
          0 == strncasecmp(proxy_uri_prefix, uri.ptr(), proxy_uri_prefix_len))
    {
      data_source_type_ = ObDataSourceDesc::DATA_SOURCE_PROXY;
      tmp.assign_ptr(uri.ptr() + proxy_uri_prefix_len, uri.length() - proxy_uri_prefix_len);
    }
    else if (uri.length() > oceanbase_uri_prefix_len &&
        0 == strncasecmp(oceanbase_uri_prefix, uri.ptr(), oceanbase_uri_prefix_len))
    {
      data_source_type_ = ObDataSourceDesc::OCEANBASE_OUT;
      tmp.assign_ptr(uri.ptr() + oceanbase_uri_prefix_len, uri.length() - oceanbase_uri_prefix_len);
    }
    else
    {
      ret = OB_DATA_SOURCE_WRONG_URI_FORMAT;
      TBSYS_LOG(ERROR, "wrong uri format[%.*s] for load table %.*s %lu",
          uri.length(), uri.ptr(), table_name.length(), table_name.ptr(), table_id);
    }

    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = buffer_.write_string(tmp, &uri_)))
    {
      TBSYS_LOG(WARN, "failed to write uri_, ret=%d", ret);
    }
  }

  if (tablet_version < 0)
  {
    TBSYS_LOG(ERROR, "load data tablet version must %ld >= 0", tablet_version);
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_SUCCESS == ret)
  {
    tablet_version_ = tablet_version;
    table_id_ = table_id;
    old_table_id_ = old_table_id;
    start_time_ = start_time;
    status_ = status;
  }
  else
  {
    reset();
  }
  return ret;
}

ObRootBalancer::ObRootBalancer()
  :config_(NULL),
   root_table_(NULL),
   server_manager_(NULL),
   root_table_rwlock_(NULL),
   server_manager_rwlock_(NULL),
   load_data_lock_(),
   log_worker_(NULL),
   role_mgr_(NULL),
   ups_manager_(NULL),
   balance_start_time_us_(0),
   balance_timeout_us_(0),
   balance_last_migrate_succ_time_(0),
   balance_next_table_seq_(0),
   balance_batch_migrate_count_(0),
   balance_batch_migrate_done_num_(0),
   balance_select_dest_start_pos_(0),
   balance_batch_copy_count_(0),
   balance_batch_delete_count_(0),
   data_source_mgr_(),
   is_loading_data_(false),
   balancer_thread_(NULL)
{
}

ObRootBalancer::~ObRootBalancer()
{
}

void ObRootBalancer::check_components() const
{
  OB_ASSERT(config_);
  OB_ASSERT(rpc_stub_);
  OB_ASSERT(root_table_);
  OB_ASSERT(server_manager_);
  OB_ASSERT(root_table_rwlock_);
  OB_ASSERT(server_manager_rwlock_);
  OB_ASSERT(log_worker_);
  OB_ASSERT(role_mgr_);
  OB_ASSERT(ups_manager_);
  OB_ASSERT(restart_server_);
  OB_ASSERT(balancer_thread_);
}

void ObRootBalancer::do_balance_or_load()
{
  check_components();
  // check timeout migrate task
  {
    ObServer src_server;
    ObServer dest_server;
    ObDataSourceDesc::ObDataSourceType type;
    while(server_manager_->check_migrate_info_timeout(
          config_->load_data_max_timeout_per_range, src_server, dest_server, type))
    {
      tbsys::CRLockGuard guard(*server_manager_rwlock_);
      ObServerStatus *src_cs = server_manager_->find_by_ip(src_server);
      ObServerStatus *dest_cs = server_manager_->find_by_ip(dest_server);

      if (NULL != dest_cs)
      {
        dest_cs->balance_info_.dec_in();
        if (dest_cs->balance_info_.cur_in_ < 0)
        {
          TBSYS_LOG(ERROR, "dest cs %s cur_in_=%d must not less than 0", to_cstring(dest_server), dest_cs->balance_info_.cur_in_);
        }
      }

      if (ObDataSourceDesc::OCEANBASE_INTERNAL == type)
      {
        if (src_server.is_same_ip(dest_server))
        {
          TBSYS_LOG(ERROR, "OCEANBASE_INTERNAL must not happen in same cs src:%s dest=%s",
              to_cstring(src_server), to_cstring(dest_server));
        }

        if (NULL != src_cs)
        {
          src_cs->balance_info_.dec_out();
          if (src_cs->balance_info_.cur_out_ < 0)
          {
            TBSYS_LOG(ERROR, "src cs %s cur out =%d must not less than 0", to_cstring(src_server), src_cs->balance_info_.cur_out_);
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "src_cs %s not found in server_manager", to_cstring(src_server));
        }
      }
      src_server.reset();
      dest_server.reset();
    }
  }

  if (root_server_->is_daily_merge_tablet_error())
  {
    TBSYS_LOG(ERROR, "daily merge process is error. blance or loading_data are delayed. error msg is %s",
        root_server_->get_daily_merge_error_msg());
  }
  else if (is_loading_data())
  {
    do_load_data();
  }
  else if (config_->enable_balance || config_->enable_rereplication)
  {
    do_balance();
  }
}

// @return 0 do not copy, 1 copy immediately, -1 delayed copy
inline int ObRootBalancer::need_copy(int32_t available_num, int32_t lost_num)
{
  OB_ASSERT(0 <= available_num);
  OB_ASSERT(0 <= lost_num);
  int ret = 0;
  if ((0 == available_num && !is_loading_data_)|| 0 == lost_num)
  {
    ret = 0;
  }
  else if (1 == lost_num)
  {
    ret = -1;
  }
  else                          // lost_num >= 2
  {
    ret = 1;
  }
  return ret;
}

int32_t ObRootBalancer::nb_get_table_count()
{
  int32_t ret = OB_SUCCESS;
  common::ObSchemaManagerV2 *schema_manager = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == schema_manager)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    ret = root_server_->get_schema(false, false, *schema_manager);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get schema manager. ret=%d", ret);
    }
  }
  int32_t count = 0;
  if (OB_SUCCESS == ret)
  {
    const ObTableSchema* it = NULL;
    for (it = schema_manager->table_begin(); schema_manager->table_end() != it; it++)
    {
      count ++;
    }
  }
  if (NULL != schema_manager)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_manager);
    schema_manager = NULL;
  }
  return count;
}

uint64_t ObRootBalancer::nb_get_next_table_id(int32_t table_count, int32_t seq/*=-1*/)
{
  uint64_t ret = OB_INVALID_ID;
  int err = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "get next table id. table_count=%d, seq=%d", table_count, seq);
  if (0 >= table_count)
  {
    // no table
  }
  else
  {
    if (0 > seq)
    {
      seq = balance_next_table_seq_;
      balance_next_table_seq_++;
    }
    common::ObSchemaManagerV2* schema_manager = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
    if (NULL == schema_manager)
    {
      TBSYS_LOG(WARN, "fail to new schema_manager.");
      err = OB_ALLOCATE_MEMORY_FAILED;
    }

    if (OB_SUCCESS == err)
    {
      err = root_server_->get_schema(false, false, *schema_manager);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to get schema manager. ret=%d", err);
      }
    }
    if (OB_SUCCESS == err)
    {
      int32_t idx = 0;
      const ObTableSchema* it = NULL;
      for (it = schema_manager->table_begin(); schema_manager->table_end() != it; ++it)
      {
        if (seq % table_count == idx)
        {
          ret = it->get_table_id();
          break;
        }
        idx++;
      }
    }
    if (NULL != schema_manager)
    {
      OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_manager);
      schema_manager = NULL;
    }
  }
  return ret;
}

int ObRootBalancer::nb_find_dest_cs(ObRootTable2::const_iterator meta, int64_t low_bound, int32_t cs_num,
                                   int32_t &dest_cs_idx, ObChunkServerManager::iterator &dest_it)
{
  int ret = OB_ENTRY_NOT_EXIST;
  dest_cs_idx = OB_INVALID_INDEX;
  if (0 < cs_num)
  {
    int64_t mnow = tbsys::CTimeUtil::getTime();
    ObChunkServerManager::iterator it = server_manager_->begin();
    it += balance_select_dest_start_pos_ % cs_num;
    if (it >= server_manager_->end())
    {
      it = server_manager_->begin();
    }
    ObChunkServerManager::iterator it_start_pos = it;
    int32_t cs_idx = OB_INVALID_INDEX;
    // scan from start_pos to end(), then from start() to start_pos
    while(true)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD
          && it->status_ != ObServerStatus::STATUS_SHUTDOWN
          && it->balance_info_.table_sstable_count_ < low_bound
          && it->balance_info_.cur_in_ < config_->balance_max_migrate_in_per_cs
          && mnow > (it->register_time_ + config_->cs_probation_period))
      {
        cs_idx = static_cast<int32_t>(it - server_manager_->begin());
        // this cs does't have this tablet
        if (!meta->did_cs_have(cs_idx))
        {
          dest_it = it;
          dest_cs_idx = cs_idx;
          TBSYS_LOG(DEBUG, "find dest cs, start_pos=%ld cs_idx=%d",
                    it_start_pos-server_manager_->begin(), dest_cs_idx);
          balance_select_dest_start_pos_ = dest_cs_idx + 1;
          ret = OB_SUCCESS;
          break;
        }
      }
      ++it;
      if (it == server_manager_->end())
      {
        it = server_manager_->begin();
      }
      if (it == it_start_pos)
      {
        break;
      }
    } // end while
  }
  return ret;
}

int ObRootBalancer::nb_check_rereplication(ObRootTable2::const_iterator it, RereplicationAction &act)
{
  int ret = OB_SUCCESS;
  act = RA_NOP;
  int64_t last_version = 0;
  int32_t valid_replicas_num = 0;
  int32_t lost_copy = 0;
  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
  {
    if (OB_INVALID_INDEX != it->server_info_indexes_[i])
    {
      valid_replicas_num ++;
      if (it->tablet_version_[i] > last_version)
      {
        last_version = it->tablet_version_[i];
      }
    }
  }
  if (!is_loading_data_ && 0 == valid_replicas_num)
  {
    const ObTabletInfo* tablet = root_table_->get_tablet_info(it);
    if (NULL != tablet)
    {
      TBSYS_LOG(ERROR, "no valid replica found for %s", to_cstring(tablet->range_));
    }
    else
    {
      TBSYS_LOG(ERROR, "no valid replica found for %p", it);
    }
  }

  int64_t tablet_replicas_num = config_->tablet_replicas_num;
  if (valid_replicas_num < tablet_replicas_num)
  {
    const ObTabletInfo* tablet = root_table_->get_tablet_info(it);
    if (NULL == tablet)
    {
      TBSYS_LOG(ERROR, "no tablet info in roottable");
    }
    else
    {
      uint64_t table_id = tablet->range_.table_id_;
      lost_copy = static_cast<int32_t>(tablet_replicas_num - valid_replicas_num);
      int did_need_copy = need_copy(valid_replicas_num, lost_copy);
      int64_t now = tbsys::CTimeUtil::getTime();
      if (1 == did_need_copy)
      {
        act = RA_COPY;
      }
      else if (-1 == did_need_copy)
      {
        if (now - it->last_dead_server_time_ > config_->safe_lost_one_time || is_table_loading(table_id))
        {
          act = RA_COPY;
        }
        else
        {
          TBSYS_LOG(DEBUG, "copy delayed, now=%ld lost_replica_time=%ld safe_log_time=%s",
              now, it->last_dead_server_time_, config_->safe_lost_one_time.str());
        }
      }
    }
  }
  else if (valid_replicas_num > tablet_replicas_num)
  {
    act = RA_DELETE;
  }
  return ret;
}

int ObRootBalancer::nb_select_copy_src(ObRootTable2::const_iterator it,
                                      int32_t &src_cs_idx, ObChunkServerManager::iterator &src_it,
                                      int64_t& tablet_version)
{
  int ret = OB_ENTRY_NOT_EXIST;
  src_cs_idx = OB_INVALID_INDEX;
  int32_t min_count = INT32_MAX;
  tablet_version = 0;
  // find cs with min migrate out count
  for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
  {
    if (OB_INVALID_INDEX != it->server_info_indexes_[i])
    {
      ObServerStatus *src_cs = server_manager_->get_server_status(it->server_info_indexes_[i]);
      int32_t migrate_count = src_cs->balance_info_.cur_out_;
      int64_t cs_tablet_version = it->tablet_version_[i];
      if (migrate_count < config_->balance_max_migrate_out_per_cs &&
           (cs_tablet_version > tablet_version ||
            (cs_tablet_version == tablet_version && min_count > migrate_count )))
      {
        min_count = migrate_count;
        tablet_version = cs_tablet_version;
        src_cs_idx = static_cast<int32_t>(src_cs - server_manager_->begin());
        src_it = src_cs;
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObRootBalancer::nb_add_copy(ObRootTable2::const_iterator it, const ObTabletInfo* tablet, int64_t low_bound, int32_t cs_num)
{
  int ret = OB_SUCCESS;
  int32_t dest_cs_idx = OB_INVALID_INDEX;
  ObServerStatus *dest_it = NULL;
  ObDataSourceDesc::ObDataSourceType data_source_type;
  ObServer src_server;
  ObServer dest_server;
  int64_t tablet_version = 0;
  ObString* uri = NULL;
  bool found_src = false;

  if (OB_SUCCESS != nb_find_dest_cs(it, low_bound, cs_num, dest_cs_idx, dest_it)
      || OB_INVALID_INDEX == dest_cs_idx)
  {
    if (OB_SUCCESS != nb_find_dest_cs(it, INT64_MAX, cs_num, dest_cs_idx, dest_it)
        || OB_INVALID_INDEX == dest_cs_idx)
    {
      TBSYS_LOG(DEBUG, "cannot find dest cs");
    }
  }
  if (OB_INVALID_INDEX != dest_cs_idx)
  {
    int32_t src_cs_idx = OB_INVALID_INDEX;
    ObServerStatus *src_it = NULL;

    if (it->get_copy_count() > 0)
    { // fetch data from oceanbase cluster internal
      if (OB_SUCCESS != nb_select_copy_src(it, src_cs_idx, src_it, tablet_version)
          || OB_INVALID_INDEX == src_cs_idx || NULL == src_it)
      {
        TBSYS_LOG(DEBUG, "cannot find src cs");
      }
      else
      {
        data_source_type = ObDataSourceDesc::OCEANBASE_INTERNAL;
        src_server = src_it->server_;
        src_server.set_port(src_it->port_cs_);
        found_src = true;
      }
    }
    else
    { // fetch data from data source
      if(OB_SUCCESS != (ret = get_data_load_info(tablet->range_.table_id_, data_source_type, uri, tablet_version)))
      { // this range has copy count=0, but not a data load task...
        TBSYS_LOG(ERROR, "no tablet exist in roottable for range %s, but it doesn't belong to a data load task. ret=%d",
            to_cstring(tablet->range_), ret);
      }
      else if (NULL == uri)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "uri must not null");
      }
      else if (OB_SUCCESS != (ret = data_source_mgr_.select_data_source(data_source_type, *uri, dest_it->server_, src_server)))
      {
        TBSYS_LOG(ERROR, "no data source found for range=%s, ret=%d", to_cstring(tablet->range_), ret);
      }
      else
      {
        found_src = true;
      }
    }

    if (OB_SUCCESS == ret && found_src)
    {
      ObDataSourceDesc data_source_desc;
      data_source_desc.type_ = data_source_type;
      data_source_desc.range_ = tablet->range_;
      data_source_desc.src_server_ = src_server;
      data_source_desc.sstable_version_ = 0; // cs will fill this version
      data_source_desc.tablet_version_ = tablet_version;
      data_source_desc.keep_source_ = true;
      if (NULL != uri)
      {
        data_source_desc.uri_ = *uri;
      }

      ObServer dest_server = dest_it->server_;
      dest_server.set_port(dest_it->port_cs_);
      data_source_desc.dst_server_ = dest_server;

      ret = server_manager_->add_migrate_info(src_server, dest_server, data_source_desc);
      if (OB_ROOT_MIGRATE_INFO_EXIST == ret)
      {
        // is in migrating
        TBSYS_LOG(DEBUG, "OB_ROOT_MIGRATE_INFO_EXIST %s %s", to_cstring(dest_server), to_cstring(data_source_desc));
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to add migrate %s from %s to cs %s, ret=%d",
            to_cstring(data_source_desc.range_), to_cstring(src_server), to_cstring(dest_server), ret);
      }
      else
      {
        dest_it->balance_info_.table_sstable_count_++;
        balance_batch_migrate_count_++;
        balance_batch_copy_count_++;
        dest_it->balance_info_.inc_in();
        if (ObDataSourceDesc::OCEANBASE_INTERNAL == data_source_type)
        {
          src_it->balance_info_.inc_out();
          if (dest_server.is_same_ip(src_server))
          {
            TBSYS_LOG(ERROR, "OCEANBASE_INTERNAL must not happen in same cs src:%s dest=%s",
                to_cstring(src_server), to_cstring(dest_server));
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = send_msg_migrate(dest_server, data_source_desc);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to send msg migrate %s from %s to cs %s, ret=%d",
            to_cstring(data_source_desc.range_), to_cstring(src_server), to_cstring(dest_server), ret);
        }
      }
    }
  }
  return ret;
}

int ObRootBalancer::nb_check_add_migrate(ObRootTable2::const_iterator it, const ObTabletInfo* tablet, int64_t avg_count,
                                        int32_t cs_num, int64_t migrate_out_per_cs)
{
  int ret = OB_SUCCESS;
  int64_t delta_count = config_->balance_tolerance_count;
  for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
  {
    int32_t cs_idx = it->server_info_indexes_[i];
    int64_t cs_tablet_version = it->tablet_version_[i];
    if (OB_INVALID_INDEX != cs_idx)
    {
      ObServerStatus *src_cs = server_manager_->get_server_status(cs_idx);
      if (NULL != src_cs && ObServerStatus::STATUS_DEAD != src_cs->status_)
      {
        if ((src_cs->balance_info_.table_sstable_count_ > avg_count + delta_count
             ||src_cs->status_ == ObServerStatus::STATUS_SHUTDOWN)
            && src_cs->balance_info_.cur_out_ < migrate_out_per_cs)
        {
          // move out this sstable
          // find dest cs, no locking
          int32_t dest_cs_idx = OB_INVALID_INDEX;
          ObServerStatus *dest_it = NULL;
          if (OB_SUCCESS != nb_find_dest_cs(it, avg_count - delta_count, cs_num, dest_cs_idx, dest_it)
              || OB_INVALID_INDEX == dest_cs_idx)
          {
            if (src_cs->status_ == ObServerStatus::STATUS_SHUTDOWN)
            {
              if (OB_SUCCESS != nb_find_dest_cs(it, INT64_MAX, cs_num, dest_cs_idx, dest_it)
                  || OB_INVALID_INDEX == dest_cs_idx)
              {
                TBSYS_LOG(WARN, "cannot find dest cs");
              }
            }
          }
          if (OB_INVALID_INDEX != dest_cs_idx)
          {
            ObDataSourceDesc data_source_desc;
            data_source_desc.type_ = ObDataSourceDesc::OCEANBASE_INTERNAL;
            data_source_desc.range_ = tablet->range_;
            data_source_desc.src_server_ = src_cs->server_;
            data_source_desc.sstable_version_ = 0; // cs will fill this version
            data_source_desc.tablet_version_ = cs_tablet_version;
            data_source_desc.keep_source_ = false;
            data_source_desc.uri_.assign_ptr(NULL, 0);
            data_source_desc.dst_server_ = dest_it->server_;

            ret = server_manager_->add_migrate_info(src_cs->server_, dest_it->server_,data_source_desc);
            if (OB_SUCCESS == ret)
            {
              if (dest_it->server_.is_same_ip(src_cs->server_))
              {
                TBSYS_LOG(ERROR, "OCEANBASE_INTERNAL must not happen in same cs src:%s dest=%s",
                    to_cstring(src_cs->server_), to_cstring(dest_it->server_));
              }
              src_cs->balance_info_.table_sstable_count_--;
              dest_it->balance_info_.table_sstable_count_++;
              balance_batch_migrate_count_++;
              dest_it->balance_info_.inc_in();
              src_cs->balance_info_.inc_out();
            }
            if (OB_SUCCESS == ret)
            {
              ret = send_msg_migrate(dest_it->server_, data_source_desc);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "failed to send msg migrate %s from %s to cs %s, ret=%d",
                    to_cstring(data_source_desc.range_), to_cstring(src_cs->server_),
                    to_cstring(dest_it->server_), ret);
              }
            }
          }
          break;
        }
      }
    }
  } // end for
  return ret;
}

int ObRootBalancer::nb_del_copy(ObRootTable2::const_iterator it, const ObTabletInfo* tablet, int32_t &last_delete_cs_index)
{
  int ret = OB_ENTRY_NOT_EXIST;
  int64_t min_version = INT64_MAX;
  int32_t delete_idx = -1;
  int32_t valid_replicas_num = 0;
  bool all_copy_have_same_version = true;
  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
  {
    if (OB_INVALID_INDEX != it->server_info_indexes_[i])
    {
      valid_replicas_num ++;
      if (INT64_MAX != min_version && it->tablet_version_[i] != min_version)
      {
        all_copy_have_same_version = false;
      }
      if (it->tablet_version_[i] < min_version)
      {
        min_version = it->tablet_version_[i];
        delete_idx = i;
      }
    }
  } // end for
  int64_t tablet_replicas_num = config_->tablet_replicas_num;
  if (valid_replicas_num > tablet_replicas_num
      && all_copy_have_same_version)
  {
    delete_idx = -1;
    for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
    {
      if (OB_INVALID_INDEX != it->server_info_indexes_[i]
          && last_delete_cs_index != it->server_info_indexes_[i])
      {
        delete_idx = i;
        last_delete_cs_index = it->server_info_indexes_[i];
        break;
      }
    }
  }

  // remove one replica if necessary
  if (valid_replicas_num > tablet_replicas_num
      && -1 != delete_idx)
  {
    delete_list_.reset();
    ObTabletReportInfo to_delete;
    to_delete.tablet_info_ = *tablet;
    to_delete.tablet_location_.tablet_version_ = it->tablet_version_[delete_idx];
    // set port to server index
    to_delete.tablet_location_.chunkserver_.set_port(it->server_info_indexes_[delete_idx]);
    ObServer cs = server_manager_->get_cs(it->server_info_indexes_[delete_idx]);
    if (OB_SUCCESS != (ret = delete_list_.add_tablet(to_delete)))
    {
      TBSYS_LOG(WARN, "failed to add into delete list");
    }
    else
    {
      it->server_info_indexes_[delete_idx] = OB_INVALID_INDEX;
      if (role_mgr_->is_master())
      {
        if (OB_SUCCESS != log_worker_->remove_replica(to_delete))
        {
          TBSYS_LOG(ERROR, "write log error");
        }
        else
        {
          ++balance_batch_delete_count_;
          int tmp_ret = ObRootUtil::delete_tablets(*rpc_stub_, *server_manager_, delete_list_, config_->network_timeout);
          if (OB_SUCCESS != tmp_ret)
          {
            TBSYS_LOG(WARN, "failed to delete tablet %s on cs %s, ret=%d",
                to_cstring(to_delete.tablet_info_.range_), to_cstring(cs), tmp_ret);
          }
        }
      }
      TBSYS_LOG(DEBUG, "delete replica, version=%ld cs_idx=%d",
          it->tablet_version_[delete_idx], delete_idx);
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

void ObRootBalancer::check_table_rereplication(const uint64_t table_id,
    const int64_t avg_count, const int64_t cs_num, bool& scan_next_table)
{
  OB_ASSERT(config_);
  RereplicationAction ract;
  int64_t delta_count = config_->balance_tolerance_count;
  // scan the root table
  int32_t last_delete_cs_index = OB_INVALID_INDEX;
  ObRootTable2::const_iterator it;
  const ObTabletInfo* tablet = NULL;
  bool table_found = false;
  tbsys::CRLockGuard guard(*root_table_rwlock_);
  for (it = root_table_->begin(); it != root_table_->end(); ++it)
  {
    tablet = root_table_->get_tablet_info(it);
    if (NULL != tablet)
    {
      if (tablet->range_.table_id_ == table_id)
      {
        if (!table_found)
        {
          table_found = true;
        }
        // check re-replication
        int add_ret = OB_ERROR;
        if (OB_SUCCESS == nb_check_rereplication(it, ract))
        {
          if (RA_COPY == ract)
          {
            TBSYS_LOG(DEBUG, "RA_COPY: %s", to_cstring(tablet->range_));
            add_ret = nb_add_copy(it, tablet, avg_count - delta_count, static_cast<int32_t>(cs_num));
          }
          else if (RA_DELETE == ract)
          {
            add_ret = nb_del_copy(it, tablet, last_delete_cs_index);
          }
        }
        // terminate condition
        if (server_manager_->is_migrate_infos_full())
        {
          scan_next_table = false;
          break;
        }
      }
      else
      {
        if (table_found)
        {
          break;
        }
      }
    }
  }
  if (false == table_found)
  {
    TBSYS_LOG(ERROR, "not find the table in root table:table_id[%lu], root_table[%p]", table_id, root_table_);
  }
}

bool ObRootBalancer::check_not_ini_table(const uint64_t table_id) const
{
  bool exist = false;
  ObSchemaManagerV2* schema_manager = root_server_->get_ini_schema();
  if (NULL == schema_manager)
  {
    TBSYS_LOG(ERROR, "schema_manager is NULL");
  }
  else
  {
    for(const ObTableSchema* table = schema_manager->table_begin();
        table != schema_manager->table_end(); ++table)
    {
      if (table->get_table_id() == table_id)
      {
        exist = true;
        break;
      }
    }
  }
  return !exist;
}

int ObRootBalancer::do_rereplication_by_table(const uint64_t table_id, bool &scan_next_table)
{
  bool need_replicate = true;
  bool found_table = false;
  int64_t total_count = 0;
  int64_t safe_count = 0;
  return do_rereplication_by_table(table_id, scan_next_table, need_replicate, found_table, total_count, safe_count);
}

int ObRootBalancer::do_rereplication_by_table(const uint64_t table_id, bool &scan_next_table,
    bool& need_replicate, bool& table_found, int64_t& total_tablet_count, int64_t& safe_tablet_count)
{
  check_components();
  int ret = OB_SUCCESS;
  int64_t avg_size = 0;
  int64_t avg_count = 0;
  int32_t cs_num = 0;
  int64_t migrate_out_per_cs = 0;
  int32_t shutdown_num = 0;
  scan_next_table = true;
  need_replicate = true;
  table_found = false;
  if (OB_SUCCESS != (ret = nb_calculate_sstable_count(table_id, avg_size, avg_count,
          cs_num, migrate_out_per_cs, shutdown_num, need_replicate, table_found,
          total_tablet_count, safe_tablet_count)))
  {
    TBSYS_LOG(WARN, "calculate table size error, err=%d", ret);
  }
  else if (0 < cs_num)
  {
    if (!table_found)
    {
      if (table_id < OB_APP_MIN_TABLE_ID)
      {
        TBSYS_LOG(ERROR, "system table not find:table_id[%lu]", table_id);
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        TBSYS_LOG(ERROR, "find table not in root table:role[%d], master[%d], table_id[%lu]",
            root_server_->get_obi_role().get_role(), root_server_->is_master(), table_id);
      }
    }
    else
    {
      check_table_rereplication(table_id, avg_count, cs_num, scan_next_table);
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "no cs found , no need to do replicate");
  }
  return ret;
}

void ObRootBalancer::check_shutdown_process()
{
  ObChunkServerManager::const_iterator it;
  for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
  {
    if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
    {
      int32_t cs_index = static_cast<int32_t>(it - server_manager_->begin());
      int64_t count = 0;
      ObRootTable2::const_iterator root_it;
      tbsys::CRLockGuard guard(*root_table_rwlock_);
      for (root_it = root_table_->begin(); root_it != root_table_->sorted_end() && 0 == count; ++root_it)
      {
        for (int i = 0; i < OB_SAFE_COPY_COUNT && 0 == count; ++i)
        {
          if (cs_index == root_it->server_info_indexes_[i])
          {
            count ++;
          }
        }
      }
      if (0 == count)
      {
        TBSYS_LOG(INFO, "shutdown chunkserver[%s] is finished", to_cstring(it->server_));
      }
    }
  }
}
int ObRootBalancer::nb_balance_by_table(const uint64_t table_id, bool &scan_next_table)
{
  int ret = OB_SUCCESS;
  int64_t avg_size = 0;
  int64_t avg_count = 0;
  int32_t cs_num = 0;
  int64_t migrate_out_per_cs = 0;
  int32_t shutdown_num = 0;
  scan_next_table = true;
  bool need_replicate = false;
  bool table_found = false;
  int64_t total_count = 0;
  int64_t safe_count = 0;

  if (OB_SUCCESS != (ret = nb_calculate_sstable_count(table_id, avg_size, avg_count,
          cs_num, migrate_out_per_cs, shutdown_num, need_replicate, table_found, total_count, safe_count)))
  {
    TBSYS_LOG(WARN, "calculate table size error, err=%d", ret);
  }
  else if (!table_found)
  {
    TBSYS_LOG(WARN, "can't find table_id %lu in root table, no balance need", table_id);
  }
  else if (0 < cs_num)
  {
    if (shutdown_num > 0) //exist shutdown task.check the process
    {
      check_shutdown_process();
    }
    bool is_curr_table_balanced = nb_is_curr_table_balanced(avg_count);
    if (!is_curr_table_balanced)
    {
      TBSYS_LOG(DEBUG, "balance table, table_id=%lu avg_count=%ld", table_id, avg_count);
      nb_print_balance_info();
    }

    ObRootTable2::const_iterator it;
    const ObTabletInfo* tablet = NULL;
    table_found = false;
    // scan the root table
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    for (it = root_table_->begin(); it != root_table_->end(); ++it)
    {
      tablet = root_table_->get_tablet_info(it);
      if (NULL != tablet)
      {
        if (tablet->range_.table_id_ == table_id)
        {
          if (!table_found)
          {
            table_found = true;
          }
          // do balnace if needed
          if ((!is_curr_table_balanced || 0 < shutdown_num)
              && config_->enable_balance
              && it->can_be_migrated_now(config_->tablet_migrate_disabling_period))
          {
            nb_check_add_migrate(it, tablet, avg_count, cs_num, migrate_out_per_cs);
          }
          // terminate condition
          if (server_manager_->is_migrate_infos_full())
          {
            scan_next_table = false;
            break;
          }
        }
        else
        {
          if (table_found)
          {
            // another table
            break;
          }
        }
      } // end if tablet not NULL
    } // end for
  }
  return ret;
}

bool ObRootBalancer::nb_is_curr_table_balanced(int64_t avg_sstable_count, const ObServer& except_cs) const
{
  bool ret = true;
  int64_t delta_count = config_->balance_tolerance_count;
  int32_t cs_out = 0;
  int32_t cs_in = 0;
  ObChunkServerManager::const_iterator it;
  for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
  {
    if (it->status_ != ObServerStatus::STATUS_DEAD)
    {
      if (except_cs == it->server_)
      {
        // do not consider except_cs
        continue;
      }
      if ((avg_sstable_count + delta_count) < it->balance_info_.table_sstable_count_)
      {
        cs_out++;
      }
      else if ((avg_sstable_count - delta_count) > it->balance_info_.table_sstable_count_)
      {
        cs_in++;
      }
      if (0 < cs_out && 0 < cs_in)
      {
        ret = false;
        break;
      }
    }
  } // end for
  return ret;
}

bool ObRootBalancer::nb_is_curr_table_balanced(int64_t avg_sstable_count) const
{
  ObServer not_exist_cs;
  return nb_is_curr_table_balanced(avg_sstable_count, not_exist_cs);
}


int ObRootBalancer::nb_calculate_sstable_count(const uint64_t table_id, int64_t &avg_size, int64_t &avg_count,
    int32_t &cs_count, int64_t &migrate_out_per_cs, int32_t &shutdown_count)
{
  bool need_replicate = false;
  bool table_found = false;
  int64_t total_tablet_count = 0;
  int64_t saft_tablet_count = 0;
  return nb_calculate_sstable_count(table_id, avg_size, avg_count, cs_count, migrate_out_per_cs, shutdown_count,
      need_replicate, table_found, total_tablet_count, saft_tablet_count);
}

int ObRootBalancer::nb_calculate_sstable_count(const uint64_t table_id, int64_t &avg_size, int64_t &avg_count,
    int32_t &cs_count, int64_t &migrate_out_per_cs, int32_t &shutdown_count, bool& need_replicate, bool& table_found,
    int64_t& total_tablet_count, int64_t& safe_tablet_count)
{
  int ret = OB_SUCCESS;
  avg_size = 0;
  avg_count = 0;
  cs_count = 0;
  shutdown_count = 0;
  int64_t total_size = 0;
  int64_t total_count = 0;
  total_tablet_count = 0;
  safe_tablet_count = 0;
  need_replicate = false;
  table_found = false;
  {
    // prepare
    tbsys::CWLockGuard guard(*server_manager_rwlock_);
    server_manager_->reset_balance_info_for_table(cs_count, shutdown_count);
    TBSYS_LOG(DEBUG, "reset balance info. cs_count=%d, shutdown_count=%d", cs_count, shutdown_count);
  } // end lock
  {
    // calculate sum
    ObRootTable2::const_iterator it;
    const ObTabletInfo* tablet = NULL;
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    int64_t count = 0;
    for (it = root_table_->begin(); it != root_table_->end(); ++it)
    {
      tablet = root_table_->get_tablet_info(it);
      if (NULL != tablet)
      {
        if (tablet->range_.table_id_ == table_id)
        {
          if (!table_found)
          {
            table_found = true;
          }
          count = 0;
          for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
          {
            if (OB_INVALID_INDEX != it->server_info_indexes_[i])
            {
              ObServerStatus *cs = server_manager_->get_server_status(it->server_info_indexes_[i]);
              if (NULL != cs && ObServerStatus::STATUS_DEAD != cs->status_)
              {
                cs->balance_info_.table_sstable_total_size_ += tablet->occupy_size_;
                total_size += tablet->occupy_size_;
                cs->balance_info_.table_sstable_count_++;
                total_count++;
                ++count;
              }
            }
          } // end for

          ++total_tablet_count;
          if (count < config_->tablet_replicas_num)
          {
            need_replicate = true;
          }
          else
          {
            ++safe_tablet_count;
          }
        }
        else
        {
          if (table_found)
          {
            break;
          }
        }
      }
    } // end for
  }   // end lock
  if (0 < cs_count)
  {
    avg_size = total_size / cs_count;
    avg_count = total_count / cs_count;
    if (0 < shutdown_count && shutdown_count < cs_count)
    {
      avg_size = total_size / (cs_count - shutdown_count);
      // make sure the shutting-down servers can find dest cs
      avg_count = total_count / (cs_count - shutdown_count) + 1 + config_->balance_tolerance_count;
    }
    int64_t sstable_avg_size = -1;
    if (0 != total_count)
    {
      sstable_avg_size = total_size/total_count;
    }
    int32_t out_cs = 0;
    ObServerStatus *it = NULL;
    for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        if (it->balance_info_.table_sstable_count_ > avg_count + config_->balance_tolerance_count
            || ObServerStatus::STATUS_SHUTDOWN == it->status_)
        {
          out_cs++;
        }
      }
    }
    migrate_out_per_cs = config_->balance_max_migrate_out_per_cs;
    if (0 < out_cs && out_cs < cs_count && migrate_out_per_cs > server_manager_->get_max_migrate_num() / out_cs)
    {
      migrate_out_per_cs = server_manager_->get_max_migrate_num() / out_cs;
      if (migrate_out_per_cs <= 0)
      { // avoid an old bug: when migrate_out_per_cs < 0, balance and migrate will not work
        TBSYS_LOG(WARN, "max_migrate_num(%ld)/out_cs_count(%d) is 0, use migrate_out_per_cs =1",
            server_manager_->get_max_migrate_num(), out_cs);
        migrate_out_per_cs = 1;
      }
    }
    TBSYS_LOG(DEBUG, "sstable distribution, table_id=%lu total_size=%ld total_count=%ld "
        "cs_num=%d shutdown_num=%d avg_size=%ld avg_count=%ld sstable_avg_size=%ld migrate_out_per_cs=%ld",
        table_id, total_size, total_count,
        cs_count, shutdown_count, avg_size, avg_count, sstable_avg_size, migrate_out_per_cs);
  }
  return ret;
}

void ObRootBalancer::nb_print_balance_info() const
{
  char addr_buf[OB_IP_STR_BUFF];
  const ObServerStatus *it = server_manager_->begin();
  for (; it != server_manager_->end(); ++it)
  {
    if (NULL != it && ObServerStatus::STATUS_DEAD != it->status_)
    {
      it->server_.to_string(addr_buf, OB_IP_STR_BUFF);
      TBSYS_LOG(DEBUG, "cs=%s sstables_count=%ld sstables_size=%ld cur_in_=%d cur_out_=%d",
                addr_buf, it->balance_info_.table_sstable_count_,
                it->balance_info_.table_sstable_total_size_,
                it->balance_info_.cur_in_,
                it->balance_info_.cur_out_);
    }
  }
}

void ObRootBalancer::nb_print_balance_info(char *buf, const int64_t buf_len, int64_t& pos) const
{
  char addr_buf[OB_IP_STR_BUFF];
  const ObServerStatus *it = server_manager_->begin();
  for (; it != server_manager_->end(); ++it)
  {
    if (NULL != it && ObServerStatus::STATUS_DEAD != it->status_)
    {
      it->server_.to_string(addr_buf, OB_IP_STR_BUFF);
      databuff_printf(buf, buf_len, pos, "%s %ld %ld\n",
                      addr_buf, it->balance_info_.table_sstable_count_,
                      it->balance_info_.table_sstable_total_size_);
    }
  }
}

int ObRootBalancer::send_msg_migrate(const ObServer &dest, const ObDataSourceDesc& data_source_desc)
{
  int ret = OB_SUCCESS;
  ret = rpc_stub_->migrate_tablet(dest, data_source_desc, config_->network_timeout);
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "migrate tablet: type=%s, tablet=%s tablet_version=%ld src=%s dest=%s keep_src=%c",
              data_source_desc.get_type(), to_cstring(data_source_desc.range_), data_source_desc.tablet_version_,
              to_cstring(data_source_desc.src_server_), to_cstring(dest), data_source_desc.keep_source_?'Y':'N');
  }
  else
  {
    TBSYS_LOG(WARN, "failed to send migrate tablet: err=%d: type=%s, tablet=%s tablet_version=%ld src=%s dest=%s keep_src=%c",
        ret, data_source_desc.get_type(), to_cstring(data_source_desc.range_), data_source_desc.tablet_version_,
        to_cstring(data_source_desc.src_server_), to_cstring(dest), data_source_desc.keep_source_?'Y':'N');
  }
  return ret;
}

void ObRootBalancer::nb_print_migrate_infos() const
{
  TBSYS_LOG(INFO, "print migrate infos:");
  if (NULL == server_manager_)
  {
    TBSYS_LOG(ERROR, "server_manager_ must no null");
  }
  else
  {
    int32_t total_in = 0;
    int32_t total_out = 0;

    // print load data task
    for (int64_t i = 0; i < MAX_LOAD_INFO_CONCURRENCY; ++i)
    {
      const ObLoadDataInfo& info = load_data_infos_[i];
      if (ObLoadDataInfo::INIT != info.status_)
      {
        TBSYS_LOG(INFO, "load table info[%ld]: table_name=%.*s table_id=%lu start_time=%s uri=%.*s status=%s",
            i, info.table_name_.length(), info.table_name_.ptr(),
            info.table_id_, time2str(info.start_time_),
            info.uri_.length(), info.uri_.ptr(), info.get_status());
      }
    }

    // print cs in and out
    ObChunkServerManager::const_iterator it;
    for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        total_in += it->balance_info_.cur_in_;
        total_out += it->balance_info_.cur_out_;
        TBSYS_LOG(INFO, "migrate info: cs=%s in=%d out=%d",
            to_cstring(it->server_), it->balance_info_.cur_in_,
            it->balance_info_.cur_out_);

      }
    }
    TBSYS_LOG(INFO, "migrate info: total_in=%d total_out=%d", total_in, total_out);

    // print migrating task
    server_manager_->print_migrate_info();

    // print data souce infos
    data_source_mgr_.print_data_source_info();
  }
}

void ObRootBalancer::dump_migrate_info() const
{
  TBSYS_LOG(INFO, "balance batch migrate infos, is_loading_data='%c' start_us=%ld timeout_us=%ld done=%d total=%d",
            is_loading_data_?'Y':'N', balance_start_time_us_, balance_timeout_us_,
            balance_batch_migrate_done_num_, balance_batch_migrate_count_);
  nb_print_migrate_infos();
}

int ObRootBalancer::nb_trigger_next_migrate(const ObDataSourceDesc& desc, int32_t result)
{
  check_components();
  int ret = OB_SUCCESS;
  const ObServer & dest_server = desc.dst_server_;
  int free_migrate_info_ret = server_manager_->free_migrate_info(desc.range_, desc.src_server_, dest_server);

  {
    tbsys::CRLockGuard guard(*server_manager_rwlock_);
    ObServerStatus *src_cs = server_manager_->find_by_ip(desc.src_server_);
    ObServerStatus *dest_cs = server_manager_->find_by_ip(dest_server);

    if (OB_SUCCESS == free_migrate_info_ret)
    {
      if (NULL != dest_cs)
      {
        dest_cs->balance_info_.dec_in();
        if (dest_cs->balance_info_.cur_in_ < 0)
        {
          TBSYS_LOG(ERROR, "dest cs %s cur_in_=%d must not less than 0", to_cstring(dest_server), dest_cs->balance_info_.cur_in_);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "desc_cs %s not found in server_manager", to_cstring(dest_server));
      }

      if (ObDataSourceDesc::OCEANBASE_INTERNAL == desc.type_)
      {
        if (desc.src_server_.is_same_ip(dest_server))
        {
          TBSYS_LOG(ERROR, "OCEANBASE_INTERNAL must not happen in same cs src:%s dest=%s",
              to_cstring(desc.src_server_), to_cstring(dest_server));
        }

        if (NULL != src_cs)
        {
          src_cs->balance_info_.dec_out();
          if (src_cs->balance_info_.cur_out_ < 0)
          {
            TBSYS_LOG(ERROR, "src cs %s cur out =%d must not less than 0", to_cstring(desc.src_server_), src_cs->balance_info_.cur_out_);
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "src_cs %s not found in server_manager", to_cstring(desc.src_server_));
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN, "not a record migrate task: result=%d, dest_cs=%s, desc=%s",
          result, to_cstring(dest_server), to_cstring(desc));
      ret = OB_ENTRY_NOT_EXIST;
    }

    if (NULL == dest_cs)
    {
      TBSYS_LOG(ERROR, "invalid arg, dest_cs=%p ip=%s, result = %d",
          dest_cs, to_cstring(dest_server), result);
      ret = OB_INVALID_ARGUMENT;
    }

    if (OB_SUCCESS == ret && ObServerStatus::STATUS_DEAD == dest_cs->status_)
    {
      TBSYS_LOG(ERROR, "dest cs %s is offline", to_cstring(dest_server));
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  if (OB_SUCCESS == free_migrate_info_ret)
  {
    if (is_loading_data())
    {
      switch(result)
      {
        case OB_SUCCESS:
          balancer_thread_->wakeup();
          break;
        case OB_DATA_SOURCE_NOT_EXIST:
        case OB_DATA_SOURCE_TABLE_NOT_EXIST:
        case OB_DATA_SOURCE_RANGE_NOT_EXIST:
        case OB_DATA_SOURCE_DATA_NOT_EXIST:
        case OB_SSTABLE_VERSION_UNEQUAL:
          handle_load_table_failed(desc.range_.table_id_);
          break;
        case OB_DATA_SOURCE_TIMEOUT:
        case OB_DATA_SOURCE_SYS_ERROR:
          if (ObDataSourceDesc::DATA_SOURCE_PROXY == desc.type_)
          {
            data_source_mgr_.inc_failed_count(desc.src_server_);
          }
          break;
        case OB_CS_EAGAIN:
          nb_print_migrate_infos();
          break;
        default:
          break;
      }
    }
    else
    {
      if (OB_SUCCESS == result)
      {
        ++balance_batch_migrate_done_num_;
        balance_last_migrate_succ_time_ = tbsys::CTimeUtil::getTime();
        if (desc.keep_source_)
        {
          OB_STAT_INC(ROOTSERVER, INDEX_COPY_COUNT);
        }
        else
        {
          OB_STAT_INC(ROOTSERVER, INDEX_MIGRATE_COUNT);
        }
        balancer_thread_->wakeup();
      }
      else if (OB_CS_EAGAIN == result)
      {
        nb_print_migrate_infos();
      }
    }
  }
  return ret;
}

// 负载均衡入口函数
void ObRootBalancer::do_balance()
{
  check_components();
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = restart_server_->restart_servers()))
  {
    TBSYS_LOG(WARN, "failed to restart servers, ret=%d", tmp_ret);
  }

  {
    tbsys::CWLockGuard guard(load_data_lock_);
    if (!is_loading_data_ && 0 == server_manager_->get_migrate_num() && 0 == balance_batch_migrate_count_)
    {
      balance_batch_migrate_done_num_ = 0;
      balance_last_migrate_succ_time_ = 0;
      balance_start_time_us_ = tbsys::CTimeUtil::getTime();
      server_manager_->reset_balance_info();
      TBSYS_LOG(INFO, "rereplication begin");
    }
  }

  if (server_manager_->get_max_migrate_num() != config_->balance_max_concurrent_migrate_num)
  {
    server_manager_->set_max_migrate_num(config_->balance_max_concurrent_migrate_num);
  }

  int32_t table_count = nb_get_table_count();
  bool scan_next_table = true;
  balance_batch_migrate_count_ = 0;
  balance_batch_delete_count_ = 0;
  balance_batch_copy_count_ = 0;
  TBSYS_LOG(DEBUG, "table_count = %d", table_count);

  for (int32_t i = 0; i < table_count && scan_next_table && config_->enable_rereplication; ++i) // for each table
  {
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count)))
    {
      ret = do_rereplication_by_table(table_id, scan_next_table);
      TBSYS_LOG(DEBUG, "rereplication table, table_id=%lu table_count=%d copy_count=%d delete_count=%d",
                table_id, table_count, balance_batch_migrate_count_, balance_batch_delete_count_);
    }
  }
  for (int32_t i = 0; i < table_count && scan_next_table && config_->enable_balance; ++i) // for each table
  {
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count)))
    {
      ret = nb_balance_by_table(table_id, scan_next_table);
      TBSYS_LOG(DEBUG, "balance table, table_id=%lu table_count=%d migrate_count=%d",
                table_id, table_count, balance_batch_migrate_count_);
    }
  }

  if (0 < balance_batch_migrate_count_ || 0 < balance_batch_delete_count_)
  {
    TBSYS_LOG(INFO, "batch migrate begin, count=%d(copy=%d) delete=%d",
              balance_batch_migrate_count_, balance_batch_copy_count_, balance_batch_delete_count_);
  }
  else
  {
    tbsys::CWLockGuard guard(load_data_lock_);
    if (!is_loading_data_ && 0 == server_manager_->get_migrate_num() && 0 == balance_batch_migrate_count_)
    {
      int64_t mnow = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(INFO, "balance batch migrate done, elapsed_us=%ld done=%d",
          mnow - balance_start_time_us_, balance_batch_migrate_done_num_);

      server_manager_->reset_balance_info();
      balance_start_time_us_ = 0;
      balance_batch_migrate_count_ = 0;
      balance_batch_copy_count_ = 0;
      balance_batch_migrate_done_num_ = 0;
    }
  }
}

int64_t ObRootBalancer::get_loading_data_infos_count()
{
  int count = 0;
  for(int64_t i=0; i <MAX_LOAD_INFO_CONCURRENCY; ++i)
  {
    if (load_data_infos_[i].status_ == ObLoadDataInfo::DOING
        || load_data_infos_[i].status_ == ObLoadDataInfo::PREPARE)
    {
      ++count;
    }
  }
  return count;
}

void ObRootBalancer::do_load_data()
{
  check_components();
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = restart_server_->restart_servers()))
  {
    TBSYS_LOG(WARN, "failed to restart servers, ret=%d", tmp_ret);
  }

  if (!root_server_->is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "can not do load table on not master rs");
  }
  else if (0 == get_loading_data_infos_count())
  {
    bool need_make_checkpoint = false;
    { // lock
      tbsys::CWLockGuard guard(load_data_lock_);

      if (0 == get_loading_data_infos_count())
      {
        if (OB_SUCCESS != root_server_->unlock_frozen_version())
        {
          TBSYS_LOG(ERROR, "failed to unlock frozen version");
        }
        root_server_->set_bypass_flag(false);
        need_make_checkpoint = true;
        is_loading_data_ = false;
        server_manager_->reset_balance_info();
        TBSYS_LOG(INFO, "all load table task is done, exit loading data status. current frozen verion=%ld",
            root_server_->get_frozen_version_for_cs_heartbeat());
      }
    }

    if (need_make_checkpoint)
    {
      root_server_->make_checkpointing();
    }
  }
  else
  {
    data_source_mgr_.update_data_source_proxy();// update every 60s

    if (server_manager_->get_max_migrate_num() != config_->balance_max_concurrent_migrate_num)
    {
      server_manager_->set_max_migrate_num(config_->balance_max_concurrent_migrate_num);
    }


    // load table by start_time
    bool scan_next_table = true;
    int64_t last_load_table_start_time = 0;
    int64_t cur_load_table_start_time = 0;
    int64_t idx = -1;
    uint64_t table_id = 0;

    while (scan_next_table)
    {
      { // find next load table id
        tbsys::CWLockGuard guard(load_data_lock_);

        last_load_table_start_time = cur_load_table_start_time;
        cur_load_table_start_time = 0;
        idx = -1;
        for (int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
        {
          if (load_data_infos_[i].status_ == ObLoadDataInfo::DOING &&
              load_data_infos_[i].start_time_ > last_load_table_start_time)
          {
            if (0 == cur_load_table_start_time || load_data_infos_[i].start_time_ < cur_load_table_start_time)
            {
              table_id = load_data_infos_[i].table_id_;
              idx = i;
              cur_load_table_start_time = load_data_infos_[i].start_time_;
            }
          }
        }
        if (idx < 0)
        {
          TBSYS_LOG(DEBUG, "no loading table found");
          break;
        }
      }

      int64_t start_time = tbsys::CTimeUtil::getTime();
      bool need_replicate = true;
      bool found_table = false;
      int64_t total_tablet_count = 0;
      int64_t safe_tablet_count = 0;
      TBSYS_LOG(INFO, "do_rereplication_by_table table_id %lu: start", table_id);
      if (OB_SUCCESS != (ret = do_rereplication_by_table(table_id, scan_next_table,
              need_replicate, found_table, total_tablet_count, safe_tablet_count )))
      {
        TBSYS_LOG(WARN, "failed to do_rereplication_by_table table_id %lu", table_id);
      }
      else if (!need_replicate && found_table && root_server_->get_obi_role().get_role() == ObiRole::MASTER)
      {
        if (OB_SUCCESS != (ret = handle_load_table_done(table_id)))
        {
          TBSYS_LOG(ERROR, "failed to finish loading table %lu", table_id);
        }
      }
      else
      {
        TBSYS_LOG(INFO, "do_rereplication_by_table table_id=%lu safe_tablet_count=%ld, "
            "total_tablet_count=%ld: cost time %ldus",
            table_id, safe_tablet_count, total_tablet_count, tbsys::CTimeUtil::getTime() - start_time);
      }
    }
  }
}

// only for test
bool ObRootBalancer::nb_is_all_tables_balanced(const common::ObServer &except_cs)
{
  bool ret = true;
  int32_t table_count = nb_get_table_count();
  for (int32_t i = 0; i < table_count; ++i) // for each table
  {
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count, i)))
    {
      int64_t avg_size = 0;
      int64_t avg_count = 0;
      int32_t cs_num = 0;
      int64_t out_per_cs = 0;
      int32_t shutdown_num = 0;
      if(OB_SUCCESS == nb_calculate_sstable_count(table_id, avg_size, avg_count, cs_num, out_per_cs, shutdown_num))
      {
        nb_print_balance_info();
        ret = nb_is_curr_table_balanced(avg_count, except_cs);
        if (!ret)
        {
          TBSYS_LOG(DEBUG, "table not balanced, id=%lu", table_id);
          break;
        }
      }
      else
      {
        ret = false;
        break;
      }
    }
  }
  return ret;
}

// only for test
bool ObRootBalancer::nb_is_all_tables_balanced()
{
  ObServer not_exist_cs;
  return nb_is_all_tables_balanced(not_exist_cs);
}

void ObRootBalancer::nb_print_balance_infos(char* buf, const int64_t buf_len, int64_t &pos)
{
  int32_t table_count = nb_get_table_count();
  for (int32_t i = 0; i < table_count; ++i) // for each table
  {
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count, i)))
    {
      int64_t avg_size = 0;
      int64_t avg_count = 0;
      int32_t cs_num = 0;
      int64_t out_per_cs = 0;
      int32_t shutdown_num = 0;
      bool need_replicate = false;
      bool table_found = false;
      int64_t total_tablet_count = 0;
      int64_t safe_tablet_count = 0;
      if(OB_SUCCESS == nb_calculate_sstable_count(table_id, avg_size, avg_count, cs_num,
            out_per_cs, shutdown_num, need_replicate, table_found, total_tablet_count, safe_tablet_count))
      {
        databuff_printf(buf, buf_len, pos, "table_id=%lu avg_count=%ld avg_size=%ld cs_num=%d out_per_cs=%ld shutdown_num=%d"
            " total_tablet=%ld safe_tablet=%ld unsafe_tablet=%ld\n",
            table_id, avg_count, avg_size, cs_num, out_per_cs, shutdown_num,
            total_tablet_count, safe_tablet_count, total_tablet_count - safe_tablet_count);
        databuff_printf(buf, buf_len, pos, "cs sstables_count sstables_size\n");
        nb_print_balance_info(buf, buf_len, pos);
        databuff_printf(buf, buf_len, pos, "--------\n");
      }
    }
  }
}

bool ObRootBalancer::nb_is_all_tablets_replicated(int32_t expected_replicas_num)
{
  bool ret = true;
  ObRootTable2::const_iterator it;
  int32_t replicas_num = 0;
  tbsys::CRLockGuard guard(*root_table_rwlock_);
  for (it = root_table_->begin(); it != root_table_->end(); ++it)
  {
    replicas_num = 0;
    for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
    {
      if (OB_INVALID_INDEX != it->server_info_indexes_[i])
      {
        replicas_num++;
      }
    }
    if (replicas_num < expected_replicas_num)
    {
      TBSYS_LOG(DEBUG, "tablet not replicated, num=%d expected=%d",
          replicas_num, expected_replicas_num);
      ret = false;
      break;
    }
  }
  return ret;
}

bool ObRootBalancer::nb_did_cs_have_no_tablets(const common::ObServer &cs) const
{
  bool ret = false;
  int32_t cs_index = -1;
  {
    tbsys::CRLockGuard guard(*server_manager_rwlock_);
    if (OB_SUCCESS != server_manager_->get_server_index(cs, cs_index))
    {
      TBSYS_LOG(WARN, "cs not exist");
      ret = true;
    }
  }
  if (!ret)
  {
    ret = true;
    ObRootTable2::const_iterator it;
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    for (it = root_table_->begin(); it != root_table_->end(); ++it)
    {
      for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
      {
        if (OB_INVALID_INDEX != it->server_info_indexes_[i]
            && cs_index == it->server_info_indexes_[i])
        {
          ret = false;
          break;
        }
      }
    } // end for
  }
  return ret;
}

namespace oceanbase
{
  namespace rootserver
  {
    namespace balancer
    {
      struct ObShutDownProgress
      {
        ObServer server_;
        int32_t server_idx_;
        int32_t tablet_count_;
        ObShutDownProgress()
          :server_idx_(-1), tablet_count_(0)
        {
        }
      };
    } // end namespace balancer
  } // end namespace rootserver
} // end namespace oceanbase

void ObRootBalancer::nb_print_shutting_down_progress(char *buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObArray<balancer::ObShutDownProgress> shutdown_servers;
  {
    // find shutting-down servers
    ObChunkServerManager::const_iterator it;
    balancer::ObShutDownProgress sdp;
    tbsys::CRLockGuard guard(*server_manager_rwlock_);
    for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
    {
      if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
      {
        sdp.server_ = it->server_;
        sdp.server_.set_port(it->port_cs_);
        sdp.server_idx_ = static_cast<int32_t>(it - server_manager_->begin());
        sdp.tablet_count_ = 0;
        if (OB_SUCCESS != (ret = shutdown_servers.push_back(sdp)))
        {
          TBSYS_LOG(ERROR, "array push error, err=%d", ret);
          break;
        }
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObRootTable2::const_iterator it;
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    for (it = root_table_->begin(); it != root_table_->end(); ++it)
    {
      // for each tablet
      for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
      {
        // for each replica
        if (OB_INVALID_INDEX != it->server_info_indexes_[i])
        {
          for (int j = 0; j < shutdown_servers.count(); ++j)
          {
            balancer::ObShutDownProgress &sdp = shutdown_servers.at(j);
            if (it->server_info_indexes_[i] == sdp.server_idx_)
            {
              sdp.tablet_count_++;
              break;
            }
          }
        }
      } // end for
    }   // end for
  }
  databuff_printf(buf, buf_len, pos, "shutting-down chunkservers: %ld\n", shutdown_servers.count());
  databuff_printf(buf, buf_len, pos, "chunkserver tablet_count\n");
  for (int j = 0; j < shutdown_servers.count(); ++j)
  {
    balancer::ObShutDownProgress &sdp = shutdown_servers.at(j);
    databuff_printf(buf, buf_len, pos, "%s %d\n", sdp.server_.to_cstring(), sdp.tablet_count_);
  }
}

int ObRootBalancer::get_data_load_info(const uint64_t table_id, ObDataSourceDesc::ObDataSourceType& data_source_type,
    ObString*& uri, int64_t& tablet_version)
{
  int ret = OB_NOT_DATA_LOAD_TABLE;
  uri = NULL;
  for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
  {
    if (load_data_infos_[i].table_id_ == table_id &&
        load_data_infos_[i].status_ == ObLoadDataInfo::DOING)
    {
      uri = &load_data_infos_[i].uri_;
      data_source_type = load_data_infos_[i].data_source_type_;
      tablet_version = load_data_infos_[i].tablet_version_;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRootBalancer::add_load_table(const ObString& table_name, const uint64_t table_id, ObString& uri, const int64_t start_time)
{
  check_components();
  int ret = OB_SUCCESS;
  ObLoadDataInfo* info = NULL;
  ModulePageAllocator mod(ObModIds::OB_RS_BALANCE);
  ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
  ObList<ObNewRange*> range_table;
  bool need_handle_failed = false;
  bool need_write_handle_failed_log = false;
  char uri_buf[OB_MAX_URI_LENGTH];
  ObString simple_uri(OB_MAX_URI_LENGTH, 0, uri_buf);
  ObDataSourceDesc::ObDataSourceType data_source_type = ObDataSourceDesc::UNKNOWN;

  if (!root_server_->is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "can not load table on not master rs");
  }
  else if (OB_MAX_TABLE_NAME_LENGTH < table_name.length())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "table name max length is %lu, but input table_name=%.*s, length=%d",
        OB_MAX_TABLE_NAME_LENGTH, table_name.length(), table_name.ptr(), table_name.length());
  }
  else if (OB_MAX_URI_LENGTH < uri.length())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "uri length=%d, but OB_MAX_URI_LENGTH=%ld", uri.length(), OB_MAX_URI_LENGTH);
  }

  if (OB_SUCCESS == ret)
  {
    tbsys::CWLockGuard guard(load_data_lock_);
    // init load data env
    if (!is_loading_data_)
    {
      ret = init_load_data_env();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to init load data env, ret=%d", ret);
      }
    }

    // add load table task info
    if (OB_SUCCESS == ret)
    {
      ret = add_load_table_task_info(table_name, table_id, uri, start_time, simple_uri, data_source_type);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to add_load_table_task_info, table_name=%.*s table_id=%lu uri=%.*s, ret=%d",
            table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), ret);
      }
      else
      {
        need_handle_failed = true;
      }
    }
  }

  // fetch range table for data source
  if (OB_SUCCESS == ret)
  {
    data_source_mgr_.update_data_source_proxy();// update every 60s

    if (OB_SUCCESS != (ret = fetch_range_list(
            data_source_type, simple_uri, table_name, table_id, range_table, allocator)))
    {
      TBSYS_LOG(WARN, "failed to fetch range list, ret=%d", ret);
    }
  }

  // insert into root table
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = root_server_->add_range_for_load_data(range_table)))
    {
      TBSYS_LOG(ERROR, "failed to insert range table to root table, ret=%d", ret);
    }
  }

  // update load info task status
  if (OB_SUCCESS == ret)
  {
    tbsys::CWLockGuard guard(load_data_lock_);
    info = NULL;
    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
    {
      if (load_data_infos_[i].table_id_ == table_id &&
          load_data_infos_[i].table_name_ == table_name)
      {
        info = &load_data_infos_[i];
        if (load_data_infos_[i].status_ != ObLoadDataInfo::PREPARE)
        {
          info = NULL;
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "status must be ObLoadDataInfo::PREPARE(%d), "
              "but current status is %d: table_name=%.*s table_id=%lu uri=%.*s",
              ObLoadDataInfo::PREPARE, load_data_infos_[i].status_,
              load_data_infos_[i].table_name_.length(), load_data_infos_[i].table_name_.ptr(),
              load_data_infos_[i].table_id_, load_data_infos_[i].uri_.length(), load_data_infos_[i].uri_.ptr());
        }
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (NULL == info)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "info of table %.*s with table_id=%lu must not null",
            table_name.length(), table_name.ptr(), table_id);
        nb_print_migrate_infos();
      }
      else
      {
        // write add_load_table_log
        if (OB_SUCCESS == ret && root_server_->is_master())
        {
          need_write_handle_failed_log = true;
          log_worker_->add_load_table(table_name, table_id, info->old_table_id_, uri, start_time, info->tablet_version_);
        }

        info->status_ = ObLoadDataInfo::DOING;
        if (root_server_->is_master() && root_server_->get_obi_role().get_role() == ObiRole::MASTER)
        {
          update_load_table_history(*info);
        }
      }
    }
  }

  // if failed
  if (OB_SUCCESS != ret && need_handle_failed)
  {
    handle_load_table_failed(table_id, need_write_handle_failed_log);
  }

  balancer_thread_->wakeup();
  return ret;
}

int ObRootBalancer::handle_load_table_done(const uint64_t table_id)
{
  check_components();
  int ret = OB_SUCCESS;
  ObLoadDataInfo info;
  bool is_finished = true;
  if (!root_server_->is_master() || root_server_->get_obi_role().get_role() != ObiRole::MASTER)
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is the master rs of the master cluster, should not do handle_load_table_done");
  }
  else
  { // lock load_data_lock_ and get table_info
    tbsys::CWLockGuard guard(load_data_lock_);

    ObLoadDataInfo* info_ptr = NULL;
    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
    {
      if (load_data_infos_[i].table_id_ == table_id &&
          load_data_infos_[i].status_ == ObLoadDataInfo::DOING)
      {
        info_ptr = &load_data_infos_[i];
        break;
      }
    }

    if (NULL == info_ptr)
    {
      ret = OB_NOT_DATA_LOAD_TABLE;
      TBSYS_LOG(ERROR, "table %lu is not loading", table_id);
    }
    else if (OB_SUCCESS != (ret = info.clone(*info_ptr)))
    {
      TBSYS_LOG(WARN, "failed to dup import table info with table name=%.*s, ret=%d",
          info_ptr->table_name_.length(), info_ptr->table_name_.ptr(), ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = check_import_status_of_all_clusters(info.table_name_, info.table_id_, is_finished);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to check_import_status_of_all_clusters, with table_name=%.*s table_id=%lu, ret=%d",
          info.table_name_.length(), info.table_name_.ptr(), info.table_id_, ret);
    }
    else if (is_finished)
    {
      TBSYS_LOG(INFO, "[import] all clusters have copied table: table_name=%s table_id=%lu",
          to_cstring(info.table_name_), info.table_id_);
    }
  }

  // change table id
  if (OB_SUCCESS == ret && is_finished)
  {
    if (OB_SUCCESS != (ret = root_server_->change_table_id(info.table_name_, info.table_id_)))
    {
      TBSYS_LOG(ERROR, "[import] failed to do load table done, table_name=%.*s table_id=%lu, ret=%d",
          info.table_name_.length(), info.table_name_.ptr(), table_id, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "[import] load table success, table_name=%.*s table_id=%lu",
          info.table_name_.length(), info.table_name_.ptr(), info.table_id_);
    }
  }

  // notify_switch_schema
  if (OB_SUCCESS == ret && is_finished)
  {
    int tmp_ret = root_server_->start_notify_switch_schema();
    if (OB_SUCCESS != tmp_ret)
    {
      TBSYS_LOG(ERROR, "[import] failed to notify_switch_schema, table_name=%.*s table_id=%lu old_table_id=%lu ret=%d",
          info.table_name_.length(), info.table_name_.ptr(), info.table_id_, info.old_table_id_, tmp_ret);
    }
    else
    {
      TBSYS_LOG(INFO, "[import] succeed to notify_switch_schema, table_name=%.*s table_id=%lu old_table_id=%lu",
          info.table_name_.length(), info.table_name_.ptr(), info.table_id_, info.old_table_id_);
    }
  }

  if (OB_SUCCESS == ret && is_finished)
  {
    info.status_ = ObLoadDataInfo::DONE;
    ret = start_set_import_status(info.table_name_, info.table_id_, info.status_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "[import] failed to set import status, table_name=%.*s table_id=%lu old_table_id=%lu, ret=%d",
          info.table_name_.length(), info.table_name_.ptr(), info.table_id_, info.old_table_id_, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "[import] succeed to set import status, table_name=%.*s table_id=%lu old_table_id=%lu",
          info.table_name_.length(), info.table_name_.ptr(), info.table_id_, info.old_table_id_);
    }
  }

  if (OB_SUCCESS != ret)
  {
    info.status_ = ObLoadDataInfo::FAILED;
    info.end_time_ = tbsys::CTimeUtil::getTime();
    TBSYS_LOG(WARN, "[import] failed to handle_load_table_done, table_name=%.*s table_id=%lu old_table_id=%lu, ret=%d",
        info.table_name_.length(), info.table_name_.ptr(), info.table_id_, info.old_table_id_, ret);
    if (root_server_->is_master() && root_server_->get_obi_role().get_role() == ObiRole::MASTER)
    {
      update_load_table_history(info);
    }
  }
  else if (is_finished)
  {
    TBSYS_LOG(INFO, "[import] succeed to handle_load_table_done, table_name=%.*s table_id=%lu old_table_id=%lu",
        info.table_name_.length(), info.table_name_.ptr(), info.table_id_, info.old_table_id_);
    info.end_time_ = tbsys::CTimeUtil::getTime();
    if (root_server_->is_master() && root_server_->get_obi_role().get_role() == ObiRole::MASTER)
    {
      update_load_table_history(info);
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "[import] not all clusters copied, table_name=%.*s table_id=%lu old_table_id=%lu",
        info.table_name_.length(), info.table_name_.ptr(), info.table_id_, info.old_table_id_);
  }

  return ret;
}

void ObRootBalancer::handle_load_table_failed(const uint64_t table_id, const bool need_write_handle_failed_log /*=true*/)
{
  int ret = OB_SUCCESS;
  char table_name_buf[OB_MAX_TABLE_NAME_LENGTH];
  ObString table_name(OB_MAX_TABLE_NAME_LENGTH, 0, table_name_buf);
  int64_t end_time = 0;
  ObLoadDataInfo::ObLoadDataStatus status = ObLoadDataInfo::FAILED;

  {
    tbsys::CWLockGuard guard(load_data_lock_);

    ObLoadDataInfo* info = NULL;
    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
    {
      if (load_data_infos_[i].table_id_ == table_id &&
          (load_data_infos_[i].status_ == ObLoadDataInfo::DOING || load_data_infos_[i].status_ == ObLoadDataInfo::PREPARE))
      {
        info = &load_data_infos_[i];
        break;
      }
    }

    if (NULL == info)
    {
      ret = OB_NOT_DATA_LOAD_TABLE;
      TBSYS_LOG(WARN, "table %lu is not loading, nothing to clean", table_id);
    }
    else if (OB_SUCCESS != table_name.clone(info->table_name_))//if failed, no table_name is printed later
    {
      TBSYS_LOG(ERROR, "failed to dup table_name=%.*s",
          info->table_name_.length(), info->table_name_.ptr());
    }
    else
    {
      status = ObLoadDataInfo::FAILED;
      info->status_ = status;
      info->end_time_ = tbsys::CTimeUtil::getTime();
      end_time = info->end_time_;
      if (root_server_->is_master() && root_server_->get_obi_role().get_role() == ObiRole::MASTER)
      {
        update_load_table_history(*info);
      }
      info->reset();
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (root_server_->is_master() && need_write_handle_failed_log)
    {
      log_worker_->delete_load_table(table_id, static_cast<int32_t>(status), end_time);
    }
    TBSYS_LOG(WARN, "[import] load table failed, table_name=%.*s table_id=%lu",
        table_name.length(), table_name.ptr(), table_id);
    if (OB_SUCCESS != (ret = root_server_->load_data_fail(table_id)))
    {
      TBSYS_LOG(WARN, "[import] failed to clean table, table_name=%.*s table_id=%lu, ret=%d",
          table_name.length(), table_name.ptr(), table_id, ret);
    }

  }
}

int ObRootBalancer::kill_load_table(const ObString& table_name, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  int64_t end_time = 0;
  ObLoadDataInfo::ObLoadDataStatus status = ObLoadDataInfo::KILLED;
  {
    tbsys::CWLockGuard guard(load_data_lock_);

    ObLoadDataInfo* info = NULL;
    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
    {
      if (load_data_infos_[i].table_id_ == table_id &&
          load_data_infos_[i].table_name_ == table_name &&
          (load_data_infos_[i].status_ == ObLoadDataInfo::DOING
           || load_data_infos_[i].status_ == ObLoadDataInfo::PREPARE))
      {
        info = &load_data_infos_[i];
        break;
      }
    }

    if (NULL == info)
    {
      ret = OB_NOT_DATA_LOAD_TABLE;
      TBSYS_LOG(ERROR, "table %.*s %lu is not loading, nothing to clean",
          table_name.length(), table_name.ptr(), table_id);
    }
    else
    {
      info->status_ = ObLoadDataInfo::KILLED;
      info->end_time_ = tbsys::CTimeUtil::getTime();

      if (root_server_->is_master() && root_server_->get_obi_role().get_role() == ObiRole::MASTER)
      {
        update_load_table_history(*info);
      }

      status = info->status_;
      end_time = info->end_time_;
      info->reset();
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (root_server_->is_master())
    {
      log_worker_->delete_load_table(table_id, static_cast<int32_t>(status), end_time);
    }
    TBSYS_LOG(INFO, "kill load table, table_name=%.*s table_id=%lu",
        table_name.length(), table_name.ptr(), table_id);
    if (OB_SUCCESS != (ret = root_server_->load_data_fail(table_id)))
    {
      TBSYS_LOG(WARN, " failed to clean table, table_name=%.*s table_id=%lu, ret=%d",
          table_name.length(), table_name.ptr(), table_id, ret);
    }
  }

  return ret;
}

void ObRootBalancer::update_load_table_history(const ObLoadDataInfo& info)
{ //TODO: use config table name
  check_components();
  int ret = OB_SUCCESS;
  const int64_t timeout = config_->inner_table_network_timeout;
  char sql_buf[OB_MAX_SQL_LENGTH];
  int n = 0;
  ObString sql;

  if (info.status_ != ObLoadDataInfo::DOING && info.status_ != ObLoadDataInfo::PREPARE)
  {
    n = snprintf(sql_buf, sizeof(sql_buf),
      "replace into load_data_history(start_time, end_time, table_name, table_id, status, uri)"
      " values(%ld, %ld, '%.*s', %lu, '%s', '%.*s')",
      info.start_time_, info.end_time_, info.table_name_.length(), info.table_name_.ptr(),
      info.table_id_, info.get_status(), info.uri_.length(), info.uri_.ptr());
  }
  else
  {
    n = snprintf(sql_buf, sizeof(sql_buf),
      "replace into load_data_history(start_time, end_time, table_name, table_id, status, uri)"
      " values(%ld, null, '%.*s', %lu, '%s', '%.*s')",
      info.start_time_, info.table_name_.length(), info.table_name_.ptr(),
      info.table_id_, info.get_status(), info.uri_.length(), info.uri_.ptr());
  }

  if (n<0 || n >= OB_MAX_SQL_LENGTH)
  {
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(ERROR, "can't generate sql: table_name=%.*s, table_id=%ld, uri=%.*s, status=%s",
        info.table_name_.length(), info.table_name_.ptr(), info.table_id_,
        info.uri_.length(), info.uri_.ptr(), info.get_status());
  }
  else
  {
    sql.assign_ptr(sql_buf, n);
    TBSYS_LOG(INFO, "[import] update load table history:%.*s", sql.length(), sql.ptr());
  }

  ObChunkServerManager::const_iterator it = server_manager_->get_serving_ms();
  if (server_manager_-> end() == it)
  {
    TBSYS_LOG(ERROR, "no serving ms found, failed to do sql: %.*s", sql.length(), sql.ptr());
    ret = OB_MS_NOT_EXIST;
  }
  else
  {
    ObServer ms_server(it->server_);
    ms_server.set_port(it->port_ms_);
    if (OB_SUCCESS != (ret = rpc_stub_->execute_sql(ms_server, sql, timeout)))
    {
      TBSYS_LOG(ERROR, "update load table result failed: sql=%.*s, ret=%d",
          sql.length(), sql.ptr(), ret);
    }
  }
}

int ObRootBalancer::add_load_table_from_log(const ObString& table_name, const uint64_t table_id,
    const uint64_t old_table_id, ObString& uri, const int64_t start_time, const int64_t tablet_version)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    if (root_table_->table_is_exist(table_id))
    {
      ret = OB_ROOT_TABLE_ID_EXIST;
      TBSYS_LOG(ERROR, "table id %lu is exist in root table, can't load it again", table_id);
    }
  }

  tbsys::CWLockGuard guard(load_data_lock_);
  if (!is_loading_data_)
  {
    is_loading_data_ = true;
    balance_batch_migrate_count_ = 0;
    balance_batch_copy_count_ = 0;
    balance_batch_migrate_done_num_ = 0;
    balance_last_migrate_succ_time_ = 0;
    server_manager_->reset_balance_info();
    TBSYS_LOG(INFO, "entry load table stat");
  }

  if (OB_SUCCESS == ret)
  {
    // check if the table is loading
    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY && OB_SUCCESS == ret; ++i)
    {
      if ((load_data_infos_[i].status_ == ObLoadDataInfo::DOING || load_data_infos_[i].status_ == ObLoadDataInfo::PREPARE)
          && (load_data_infos_[i].table_id_ == table_id || load_data_infos_[i].table_name_ == table_name))
      {
        ret = OB_DATA_LOAD_TABLE_DUPLICATED;
        TBSYS_LOG(ERROR, "table %.*s %lu is loading, can't load table %.*s %lu again",
            load_data_infos_[i].table_name_.length(), load_data_infos_[i].table_name_.ptr(),
            load_data_infos_[i].table_id_, table_name.length(), table_name.ptr(), table_id);
      }
    }

    // find free pos and add info
    int64_t oldest_time = 0;
    ObLoadDataInfo *info = NULL;
    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY && OB_SUCCESS == ret; ++i)
    {
      if (load_data_infos_[i].status_ != ObLoadDataInfo::DOING
          && load_data_infos_[i].status_ != ObLoadDataInfo::PREPARE
          && load_data_infos_[i].start_time_ < oldest_time)
      {
        oldest_time = load_data_infos_[i].start_time_;
        info = &load_data_infos_[i];
      }
    }
    if (NULL == info)
    {
      ret = OB_DATA_SOURCE_CONCURRENCY_FULL;
      TBSYS_LOG(ERROR, "max load info conrurrency is %d, no new load table task is allowed", MAX_LOAD_INFO_CONCURRENCY);
    }
    else if (OB_SUCCESS != (ret = info->set_info(table_name, table_id, old_table_id, uri, tablet_version, start_time, ObLoadDataInfo::DOING)))
    {
      TBSYS_LOG(WARN, "failed to add load table table_name=%.*s table_id=%lu uri=%.*s, ret=%d",
          table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), ret);
    }
  }
  return ret;
}

int ObRootBalancer::delete_load_table_from_log(const uint64_t table_id, const int32_t status, const int64_t end_time)
{
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(load_data_lock_);

  ObLoadDataInfo* info = NULL;
  for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
  {
    if (load_data_infos_[i].table_id_ == table_id &&
        (load_data_infos_[i].status_ == ObLoadDataInfo::DOING || load_data_infos_[i].status_ == ObLoadDataInfo::PREPARE))
    {
      info = &load_data_infos_[i];
      break;
    }
  }

  if (NULL == info)
  {
    ret = OB_NOT_DATA_LOAD_TABLE;
    TBSYS_LOG(ERROR, "table %lu is not loading", table_id);
  }
  else
  {
    info->status_ = static_cast<ObLoadDataInfo::ObLoadDataStatus>(status);
    info->end_time_ = end_time;
    info->reset();
  }

  return ret;
}

bool ObRootBalancer::is_table_loading(uint64_t table_id) const
{
  bool is_loading = false;
  for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
  {
    if (load_data_infos_[i].table_id_ == table_id &&
        (load_data_infos_[i].status_ == ObLoadDataInfo::DOING || load_data_infos_[i].status_ == ObLoadDataInfo::PREPARE))
    {
      is_loading = true;
    }
  }
  return is_loading;
}

int ObRootBalancer::write_to_file(const char* filename)
{ // only write doing status
  int ret = OB_SUCCESS;
  common::FileUtils fu;
  char* data_buffer = NULL;
  if (filename == NULL)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(INFO, "file name can not be NULL");
  }
  else if (0 > fu.open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0644))
  {
    ret = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "create file [%s] failed", filename);
  }
  else
  {
    tbsys::CWLockGuard guard(load_data_lock_);
    int64_t count = 0;
    int64_t encode_length = 0;

    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
    {
      if (load_data_infos_[i].status_ == ObLoadDataInfo::DOING)
      {
        ++count;
        encode_length += load_data_infos_[i].table_name_.get_serialize_size();
        encode_length += serialization::encoded_length_i64(load_data_infos_[i].table_id_);
        encode_length += serialization::encoded_length_i64(load_data_infos_[i].old_table_id_);
        encode_length += serialization::encoded_length_i64(load_data_infos_[i].start_time_);
        encode_length += serialization::encoded_length_i64(load_data_infos_[i].tablet_version_);
        encode_length += load_data_infos_[i].uri_.get_serialize_size();
      }
    }
    encode_length += serialization::encoded_length_i64(count);

    if (NULL == (data_buffer = static_cast<char*>(ob_malloc(encode_length, ObModIds::OB_LOAD_DATA_INFO))))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "failed to malloc data buffer");
    }
    common::ObDataBuffer buffer(data_buffer, encode_length);

    if (OB_SUCCESS != (ret = serialization::encode_i64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), count)))
    {
      TBSYS_LOG(WARN, "failed to  encode count of load info list, count=%ld", count);
    }
    else
    {
      for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY && OB_SUCCESS == ret; ++i)
      {
        if (load_data_infos_[i].status_ == ObLoadDataInfo::DOING)
        {
          if (OB_SUCCESS != (ret = load_data_infos_[i].table_name_.serialize(
                  buffer.get_data(), buffer.get_capacity(), buffer.get_position())))
          {
            TBSYS_LOG(WARN, "failed to encode table_name=%.*s, capacity=%ld, pos=%ld, ret=%d",
                load_data_infos_[i].table_name_.length(), load_data_infos_[i].table_name_.ptr(),
                buffer.get_capacity(), buffer.get_position(), ret);
          }
          else if (OB_SUCCESS != (ret = serialization::encode_i64(
                  buffer.get_data(), buffer.get_capacity(), buffer.get_position(), load_data_infos_[i].table_id_)))
          {
            TBSYS_LOG(WARN, "failed to encode table_id=%ld, capacity=%ld, pos=%ld, ret=%d",
                load_data_infos_[i].table_id_, buffer.get_capacity(), buffer.get_position(), ret);
          }
          else if (OB_SUCCESS != (ret = serialization::encode_i64(
                  buffer.get_data(), buffer.get_capacity(), buffer.get_position(), load_data_infos_[i].start_time_)))
          {
            TBSYS_LOG(WARN, "failed to encode start_time=%ld, capacity=%ld, pos=%ld, ret=%d",
                load_data_infos_[i].start_time_, buffer.get_capacity(), buffer.get_position(), ret);
          }
          else if (OB_SUCCESS != (ret = serialization::encode_i64(
                  buffer.get_data(), buffer.get_capacity(), buffer.get_position(), load_data_infos_[i].tablet_version_)))
          {
            TBSYS_LOG(WARN, "failed to encode tablet_version=%ld, capacity=%ld, pos=%ld, ret=%d",
                load_data_infos_[i].tablet_version_, buffer.get_capacity(), buffer.get_position(), ret);
          }
          else if (OB_SUCCESS != (ret = load_data_infos_[i].uri_.serialize(
                  buffer.get_data(), buffer.get_capacity(), buffer.get_position())))
          {
            TBSYS_LOG(WARN, "failed to encode uri=%.*s, capacity=%ld, pos=%ld, ret=%d",
                load_data_infos_[i].uri_.length(), load_data_infos_[i].uri_.ptr(),
                buffer.get_capacity(), buffer.get_position(), ret);
          }

        }
      }
    }

    if (OB_SUCCESS == ret)
    {
       if (encode_length != buffer.get_position())
       {
         ret = OB_ERR_SYS;
         TBSYS_LOG(WARN, "encode length should be %ld, but indeed is %ld", encode_length, buffer.get_position());
       }
    }

    if (OB_SUCCESS == ret)
    {
      int64_t write_length = fu.write(data_buffer, encode_length);
      if (encode_length != write_length)
      {
        ret = OB_IO_ERROR;
        TBSYS_LOG(ERROR, "write data info [%s] failed", filename);
      }
    }
  }

  fu.close();

  if (NULL != data_buffer)
  {
    ob_free(data_buffer);
    data_buffer = NULL;
  }
  return ret;
}

int ObRootBalancer::read_from_file(const char* filename)
{
  int ret = OB_SUCCESS;
  common::FileUtils fu;
  char* data_buffer = NULL;
  int64_t size = 0;
  if (filename == NULL)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(INFO, "filename can not be NULL");
  }

  if (ret == OB_SUCCESS)
  {
    int32_t rc = fu.open(filename, O_RDONLY);
    if (rc < 0)
    {
      ret = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "open file [%s] failed", filename);
    }
    else
    {
      size = fu.get_size();
    }
  }

  if (OB_SUCCESS == ret)
  {
    int64_t read_size = 0;
    int64_t pos = 0;
    int64_t count = 0;
    if (NULL == (data_buffer = static_cast<char*>(ob_malloc(size, ObModIds::OB_LOAD_DATA_INFO))))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "failed to malloc data buffer");
    }
    else if (size != (read_size = fu.read(data_buffer, size)))
    {
      ret = OB_IO_ERROR;
      TBSYS_LOG(ERROR, "read data info from [%s] failed, expect size=%ld, indeed read size=%ld",
          filename, size, read_size);
    }
    else if (OB_SUCCESS != (ret = serialization::decode_i64(data_buffer, size, pos, &count)))
    {
      TBSYS_LOG(WARN, "failed to decode count, capacity=%ld, pos=%ld, ret=%d", size, pos, ret);
    }
    else
    {
      ObString table_name;
      uint64_t table_id;
      uint64_t old_table_id;
      int64_t start_time;
      int64_t tablet_version;
      ObString uri;
      for(int64_t i=0; i<count && OB_SUCCESS == ret; ++i)
      {
        if (OB_SUCCESS != (ret = table_name.deserialize(data_buffer, size, pos)))
        {
          TBSYS_LOG(WARN, "failed to decode table_name, capacity=%ld, pos=%ld, ret=%d", size, pos, ret);
        }
        else if (OB_SUCCESS != (ret = serialization::decode_i64(data_buffer, size, pos, reinterpret_cast<int64_t*>(&table_id))))
        {
          TBSYS_LOG(WARN, "failed to decode table_id, capacity=%ld, pos=%ld, ret=%d", size, pos, ret);
        }
        else if (OB_SUCCESS != (ret = serialization::decode_i64(data_buffer, size, pos, reinterpret_cast<int64_t*>(&old_table_id))))
        {
          TBSYS_LOG(WARN, "failed to decode old_table_id, capacity=%ld, pos=%ld, ret=%d", size, pos, ret);
        }
        else if (OB_SUCCESS != (ret = serialization::decode_i64(data_buffer, size, pos, &start_time)))
        {
          TBSYS_LOG(WARN, "failed to decode start_time, capacity=%ld, pos=%ld, ret=%d", size, pos, ret);
        }
        else if (OB_SUCCESS != (ret = serialization::decode_i64(data_buffer, size, pos, &tablet_version)))
        {
          TBSYS_LOG(WARN, "failed to decode tablet_version, capacity=%ld, pos=%ld, ret=%d", size, pos, ret);
        }
        else if (OB_SUCCESS != (ret = uri.deserialize(data_buffer, size, pos)))
        {
          TBSYS_LOG(WARN, "failed to decode uri, capacity=%ld, pos=%ld, ret=%d", size, pos, ret);
        }
        else if (OB_SUCCESS != (ret = add_load_table_from_log(table_name, table_id, old_table_id, uri, start_time, tablet_version)))
        {
          TBSYS_LOG(WARN, "failed to add load table info, table_name=%.*s table_id=%lu old_table_id=%lu"
              " uri=%.*s start_time=%ld tablet_version=%ld",
              table_name.length(), table_name.ptr(), table_id, old_table_id, uri.length(), uri.ptr(),
              start_time, tablet_version);
        }
      }
    }
  }

  fu.close();
  if (NULL != data_buffer)
  {
    ob_free(data_buffer);
    data_buffer = NULL;
  }

  return ret;
}

int ObRootBalancer::fetch_range_list(const ObDataSourceDesc::ObDataSourceType data_source_type,
    ObString& uri, const ObString& table_name, const uint64_t table_id,
    ObList<ObNewRange*>& range_table, ModuleArena& allocator)
{
  check_components();
  int ret = OB_SUCCESS;
  int64_t retry = 3;
  ObServer data_source_server;
  int64_t timeout = config_->import_rpc_timeout;

  do {
    range_table.clear();
    allocator.reuse();

    if (OB_SUCCESS != (ret = data_source_mgr_.get_next_data_source(data_source_type, uri, data_source_server)))
    {
      TBSYS_LOG(WARN, "failed to get data source proxy, table_name=%.*s table_id=%lu"
          " data_source_type=%d uri=%.*s, ret=%d",
          table_name.length(), table_name.ptr(), table_id, data_source_type, uri.length(), uri.ptr(), ret);
    }
    else if (OB_SUCCESS != (ret = rpc_stub_->fetch_range_table(data_source_server, table_name,
            uri, range_table, allocator, timeout)))
    {
      TBSYS_LOG(WARN, "failed to fetch range table from data source server=%s, "
          "table_name=%.*s, table_id=%lu uri=%.*s, timeout=%ld, ret=%d",
          to_cstring(data_source_server), table_name.length(), table_name.ptr(),
          table_id, uri.length(), uri.ptr(), timeout, ret);
    }
    else if (range_table.size() == 0)
    {
      TBSYS_LOG(WARN, "range list must not empty, data source server=%s, "
          "table_name=%.*s, table_id=%lu uri=%.*s",
          to_cstring(data_source_server), table_name.length(), table_name.ptr(),
          table_id, uri.length(), uri.ptr());
      ret = OB_ERR_SYS;
    }
    else
    {
      bool flag = true;
      bool first = true;
      common::ObRowkey last_row_key;
      // check range list
      for (common::ObList<ObNewRange*>::const_iterator it = range_table.begin();
          it != range_table.end() && OB_SUCCESS == ret; ++it)
      {
        if (NULL == *it)
        {
          ret = OB_ERR_SYS;
          TBSYS_LOG(ERROR, "range pointer must not null");
        }
        else if (table_id != (*it)->table_id_)
        {
          ret = OB_DATA_SOURCE_TABLE_NOT_EXIST;
          TBSYS_LOG(ERROR, "the tablet on uri=%.*s's table id is %lu, but request table_id is %lu",
              uri.length(), uri.ptr(), (*it)->table_id_, table_id);
        }
        else if (first)
        {
          last_row_key = (*it)->end_key_;
          if (false == (*it)->start_key_.is_min_row())
          {
            TBSYS_LOG(WARN, "first range is not start with min, range: %s",  to_cstring(*(*it)));
            ret = OB_DATA_SOURCE_SYS_ERROR;
          }
          first = false;
        }
        else if (last_row_key != (*it)->start_key_)
        {
          TBSYS_LOG(WARN, "last row key %s not same with the start key of range %s", to_cstring(last_row_key), to_cstring(*(*it)));
          ret = OB_DATA_SOURCE_SYS_ERROR;
          flag = false;
        }
        else
        {
          last_row_key = (*it)->end_key_;
        }
      }
      if (OB_SUCCESS == ret && false == last_row_key.is_max_row())
      {
        flag = false;
        TBSYS_LOG(WARN, "last range is not end with max, range: %s",  to_cstring(last_row_key));
        ret = OB_DATA_SOURCE_SYS_ERROR;
      }
    }
  } while (--retry > 0 && OB_SUCCESS != ret);

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "failed to fetch range table from data source server=%s, "
        "table_name=%.*s, table_id=%lu uri=%.*s, ret=%d",
        to_cstring(data_source_server), table_name.length(), table_name.ptr(),
        table_id, uri.length(), uri.ptr(), ret);
  }
  else
  {
    TBSYS_LOG(INFO, "succeed to fetch range table from data source server=%s, "
        "table_name=%.*s, table_id=%lu uri=%.*s, count=%ld, ret=%d",
        to_cstring(data_source_server), table_name.length(), table_name.ptr(),
        table_id, uri.length(), uri.ptr(), range_table.size(), ret);
  }
  return ret;
}

int ObRootBalancer::check_import_status_of_all_clusters(const ObString& table_name,
    const uint64_t table_id, bool& is_finished)
{
  check_components();
  int ret = OB_SUCCESS;
  is_finished = false;
  ObLoadDataInfo::ObLoadDataStatus status = ObLoadDataInfo::INIT;

  ObServer master_rs;
  ObServer slave_rs[OB_MAX_CLUSTER_COUNT];
  int64_t slave_count = OB_MAX_CLUSTER_COUNT;
  ObServer ms_server;
  master_rs.set_ipv4_addr(config_->master_root_server_ip, static_cast<int32_t>(config_->master_root_server_port));
  if (OB_SUCCESS != (ret = get_ms(ms_server)))
  {
    TBSYS_LOG(WARN, "failed to get serving ms, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_stub_->fetch_slave_cluster_list(
          ms_server, master_rs, slave_rs, slave_count, config_->inner_table_network_timeout)))
  {
    TBSYS_LOG(ERROR, "failed to get slave cluster rs list, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  { // check import status on all clusters
    int32_t status_i32 = 0;
    int tmp_ret = rpc_stub_->get_import_status(master_rs, table_name, table_id, status_i32, config_->import_rpc_timeout);
    if (OB_SUCCESS != tmp_ret)
    {
      ret = tmp_ret;
      TBSYS_LOG(WARN, "failed to get import status, master_rs=%s, table_name=%.*s, table_id=%lu, ret=%d",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id, tmp_ret);
    }
    else
    {
      status = static_cast<ObLoadDataInfo::ObLoadDataStatus>(status_i32);
      if (ObLoadDataInfo::COPIED == status)
      {
        is_finished = true;
        TBSYS_LOG(INFO, "got import status, master_rs=%s, table_name=%.*s, table_id=%lu status=COPIED",
            to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id);
      }
      else if (ObLoadDataInfo::DOING == status)
      {
        TBSYS_LOG(INFO, "got import status, master_rs=%s, table_name=%.*s, table_id=%lu status=DOING",
            to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id);
      }
      else
      {
        is_finished = false;
        ret = OB_DATA_LOAD_TABLE_STATUS_ERROR;
        TBSYS_LOG(ERROR, "got import status, master_rs=%s table_name=%.*s table_id=%lu"
            " status=%d, but the status of master rs should be %d(COPIED) or %d(DOING)",
            to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id,
            status_i32, ObLoadDataInfo::COPIED, ObLoadDataInfo::DOING);
      }
    }

    for (int64_t i = 0; i < slave_count && OB_SUCCESS == ret; ++i)
    {
      tmp_ret = rpc_stub_->get_import_status(slave_rs[i], table_name, table_id, status_i32, config_->import_rpc_timeout);
      if (OB_SUCCESS != tmp_ret)
      {
        ret = tmp_ret;
        is_finished = false;
        TBSYS_LOG(WARN, "failed to get import status, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu, ret=%d",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id, ret);
      }
      else
      {
        status = static_cast<ObLoadDataInfo::ObLoadDataStatus>(status_i32);
        if (ObLoadDataInfo::COPIED == status)
        {
          TBSYS_LOG(INFO, "got import status, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu status=COPIED",
              i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id);
        }
        else if (ObLoadDataInfo::DOING == status)
        {
          is_finished = false;
          TBSYS_LOG(INFO, "got import status, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu status=DOING",
              i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id);
        }
        else if (ObLoadDataInfo::PREPARE== status)
        {
          is_finished = false;
          TBSYS_LOG(INFO, "got import status, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu status=PREPARE",
              i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id);
        }
        else
        {
          is_finished = false;
          ret = OB_DATA_LOAD_TABLE_STATUS_ERROR;
          TBSYS_LOG(ERROR, "got import status, slave_rs[%ld]=%s table_name=%.*s table_id=%lu"
            " status=%d, but the status should be %d(COPIED) or %d(DOING)",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id,
            status_i32,ObLoadDataInfo::COPIED, ObLoadDataInfo::DOING);
        }
      }
    }
  }

  if (OB_SUCCESS != ret && is_finished)
  {
    is_finished = false;
    TBSYS_LOG(WARN, "failed to check import status, set is_finished = false");
  }

  return ret;
}

int ObRootBalancer::start_set_import_status(const ObString& table_name, const uint64_t table_id,
    const ObLoadDataInfo::ObLoadDataStatus& status)
{
  check_components();
  int ret = OB_SUCCESS;
  ObServer master_rs;
  ObServer slave_rs[OB_MAX_CLUSTER_COUNT];
  int64_t slave_count = OB_MAX_CLUSTER_COUNT;
  ObServer ms_server;
  master_rs.set_ipv4_addr(config_->master_root_server_ip, static_cast<int32_t>(config_->master_root_server_port));

  TBSYS_LOG(INFO, "start to set import table status: table_name=%.*s table_id=%lu",
      table_name.length(), table_name.ptr(), table_id);

  if (!root_server_->is_master() || root_server_->get_obi_role().get_role() != ObiRole::MASTER)
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(WARN, "this rs is not master of marster cluster, cannot set import task status");
  }
  else if (OB_SUCCESS != (ret = get_ms(ms_server)))
  {
    TBSYS_LOG(WARN, "failed to get serving ms, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_stub_->fetch_slave_cluster_list(
          ms_server, master_rs, slave_rs, slave_count, config_->inner_table_network_timeout)))
  {
    TBSYS_LOG(ERROR, "failed to get slave cluster rs list, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  { // set import status on all clusters
    const int32_t status_32 = static_cast<int32_t>(status);
    int tmp_ret = rpc_stub_->set_import_status(master_rs, table_name, table_id, status_32, config_->import_rpc_timeout);
    if (OB_SUCCESS == tmp_ret)
    {
      TBSYS_LOG(INFO, "succeed to set import, master_rs=%s, table_name=%.*s, table_id=%lu, status=%d",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id, status_32);
    }
    else
    {
      ret = tmp_ret;
      TBSYS_LOG(WARN, "failed to set import, master_rs=%s, table_name=%.*s, table_id=%lu, status=%d, ret=%d",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id, status_32, tmp_ret);
    }

    for (int64_t i = 0; i < slave_count; ++i)
    {
      tmp_ret = rpc_stub_->set_import_status(slave_rs[i], table_name, table_id, status_32, config_->import_rpc_timeout);
      if (OB_SUCCESS == tmp_ret)
      {
        TBSYS_LOG(INFO, "succeed to set import, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu, status=%d",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id, status_32);
      }
      else
      {
        ret = tmp_ret;
        TBSYS_LOG(WARN, "failed to set import, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu, status=%d, ret=%d",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id, status_32, ret);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "succeed to set import table: table_name=%.*s table_id=%lu status=%d",
        table_name.length(), table_name.ptr(), table_id, status);
  }
  else
  {
    TBSYS_LOG(WARN, "failed to set import table: table_name=%.*s table_id=%lu status=%d",
        table_name.length(), table_name.ptr(), table_id, status);
  }

  return ret;
}

int ObRootBalancer::get_import_status(const ObString& table_name,
    const uint64_t table_id, ObLoadDataInfo::ObLoadDataStatus& status)
{
  int ret = OB_SUCCESS;
  bool found = false;
  status = ObLoadDataInfo::FAILED;
  if (!root_server_->is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "can not load table on not master rs");
  }
  else
  {
    tbsys::CRLockGuard guard(load_data_lock_);
    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
    {
      if (load_data_infos_[i].table_id_ == table_id &&
          load_data_infos_[i].table_name_ == table_name)
      {
        status = load_data_infos_[i].status_;
        found = true;
        if (status == ObLoadDataInfo::DOING)
        {
          bool is_replicated = false;
          ret = is_table_replicated(table_id, load_data_infos_[i].tablet_version_, is_replicated);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to check if table_id=%lu with tablet_version=%ld is replicated, ret=%d",
                table_id, load_data_infos_[i].tablet_version_, ret);
          }
          else if (is_replicated)
          {
            status = ObLoadDataInfo::COPIED;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "the status of table %.*s with table_id=%lu and uri=%.*s is not DOING",
              table_name.length(), table_name.ptr(), table_id,
              load_data_infos_[i].uri_.length(), load_data_infos_[i].uri_.ptr());
        }
        break;
      }
    }
  }
  if (OB_SUCCESS == ret && !found)
  {
    ret = OB_NOT_DATA_LOAD_TABLE;
    TBSYS_LOG(WARN, "table %.*s with table_id=%lu is not loading", table_name.length(), table_name.ptr(), table_id);
  }
  return ret;
}

int ObRootBalancer::set_import_status(const ObString& table_name,
    const uint64_t table_id, const ObLoadDataInfo::ObLoadDataStatus& status)
{
  check_components();
  int ret = OB_SUCCESS;
  bool found = false;
  int64_t end_time = tbsys::CTimeUtil::getTime();
  if (!root_server_->is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "can not load table on not master rs");
  }
  else
  {
    tbsys::CWLockGuard guard(load_data_lock_);
    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
    {
      if (load_data_infos_[i].table_id_ == table_id &&
          load_data_infos_[i].table_name_ == table_name)
      {
        found = true;
        if (load_data_infos_[i].status_ == ObLoadDataInfo::DOING
            && status == ObLoadDataInfo::DONE)
        {
          load_data_infos_[i].status_ = status;
          if (root_server_->is_master())
          {
            log_worker_->delete_load_table(table_id, static_cast<int32_t>(status), end_time);
            if (OB_SUCCESS != root_server_->load_data_done(table_name, load_data_infos_[i].old_table_id_))
            {
              TBSYS_LOG(WARN, "failed to do load data done, table_name=%s, table_id=%lu",
                  to_cstring(table_name), table_id);
            }
          }
        }
        else if ((load_data_infos_[i].status_ == ObLoadDataInfo::INIT
                  || load_data_infos_[i].status_ == ObLoadDataInfo::PREPARE
                  || load_data_infos_[i].status_ == ObLoadDataInfo::DOING)
                && (status == ObLoadDataInfo::FAILED|| status == ObLoadDataInfo::KILLED))
        {
          load_data_infos_[i].status_ = status;
          if (root_server_->is_master())
          {
            log_worker_->delete_load_table(table_id, static_cast<int32_t>(status), end_time);
            if (OB_SUCCESS != root_server_->load_data_fail(table_id))
            {
              TBSYS_LOG(WARN, "failed to do load data fail, table_name=%s, table_id=%lu",
                  to_cstring(table_name), table_id);
            }
          }
        }
        else
        {
          ret = OB_DATA_LOAD_TABLE_STATUS_ERROR;
          TBSYS_LOG(ERROR, "import status of table %.*s with table id=%lu is %s(%d), can't set to be %d",
              table_name.length(), table_name.ptr(), table_id, load_data_infos_[i].get_status(),
              load_data_infos_[i].status_, status);
        }
        load_data_infos_[i].reset();
        break;
      }
    }
  }
  if (OB_SUCCESS == ret && !found)
  {
    ret = OB_NOT_DATA_LOAD_TABLE;
    TBSYS_LOG(WARN, "table %.*s with table_id=%lu is not loading", table_name.length(), table_name.ptr(), table_id);
  }
  return ret;
}

int ObRootBalancer::is_table_replicated(const uint64_t table_id, const int64_t tablet_version, bool& is_replicated)
{
  OB_ASSERT(config_);
  int ret = OB_SUCCESS;
  int64_t tablet_replicas_num = config_->tablet_replicas_num;
  ObRootTable2::const_iterator it;
  const ObTabletInfo* tablet = NULL;
  is_replicated = true;
  bool table_found = false;
  int64_t valid_replicas_num = 0;
  int64_t safe_count = 0;
  int64_t total_count = 0;
  tbsys::CRLockGuard guard(*root_table_rwlock_);
  for (it = root_table_->begin(); it != root_table_->end() && is_replicated; ++it)
  {
    tablet = root_table_->get_tablet_info(it);
    if (NULL != tablet)
    {
      if (tablet->range_.table_id_ == table_id)
      {
        if (!table_found)
        {
          table_found = true;
        }
        ++total_count;
        valid_replicas_num = 0;
        for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; ++i)
        {
          if (OB_INVALID_INDEX != it->server_info_indexes_[i])
          {
            if (it->tablet_version_[i] == tablet_version)
            {
              ++valid_replicas_num;
            }
            else
            {
              is_replicated = false;
              TBSYS_LOG(WARN, "%s has tablet_verion[%ld] not equals with need tablet version[%ld]",
                  to_cstring(tablet->range_), it->tablet_version_[i], tablet_version);
              break;
            }
          }
        }
        if (valid_replicas_num < tablet_replicas_num)
        {
          is_replicated = false;
          break;
        }
        else
        {
          ++safe_count;
        }
      }
      else if (table_found)
      {
        break;
      }
    }
  }
  if (false == table_found)
  {
    is_replicated = false;
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "not find the table in root table:table_id[%lu], root_table[%p]", table_id, root_table_);
  }
  else if (is_replicated)
  {
    TBSYS_LOG(INFO, "table with table_id = %lu is replicated, tablet_version=%ld, tablet_replicas_num=%ld total_count=%ld",
        table_id, tablet_version, tablet_replicas_num, total_count);
  }
  else
  {
    TBSYS_LOG(INFO, "table with table_id = %lu is not replicated, tablet_version=%ld,"
        " tablet_replicas_num=%ld, safe_count=%ld, total_count=%ld",
        table_id, tablet_version, tablet_replicas_num, safe_count, total_count);
  }
  return ret;
}

int ObRootBalancer::get_ms(ObServer& ms_server)
{
  check_components();
  int ret = root_server_->get_ms_provider().get_ms(ms_server);

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "no serving ms found, ret=%d", ret);
    ret = OB_MS_NOT_EXIST;
  }
  return ret;
}

int ObRootBalancer::init_load_data_env()
{ // load_data_lock_ must be locked by caller
  int ret = OB_SUCCESS;
  bool has_set_bypass_flag = false;
  if (OB_SUCCESS != root_server_->set_bypass_flag(true))
  { // this flag will also be set by "build root table", to change name of this method in 0.5
    ret = OB_EAGAIN;
    TBSYS_LOG(WARN, "root server is building root table, can't start load table");
  }
  else
  {
    has_set_bypass_flag = true;
  }
  
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = root_server_->lock_frozen_version()))
    {
      TBSYS_LOG(ERROR, "failed to lock frozen version");
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = check_replica_count_for_import(root_server_->get_frozen_version_for_cs_heartbeat());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "load table could not start:  failed to check_replica_count_for_import, ret=%d", ret);
      if (OB_SUCCESS != root_server_->unlock_frozen_version())
      {
        TBSYS_LOG(ERROR, "failed to unlock frozen version");
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "entry load table stat, set tablet version as %ld",
        root_server_->get_frozen_version_for_cs_heartbeat());
    balance_batch_migrate_count_ = 0;
    balance_batch_copy_count_ = 0;
    balance_batch_migrate_done_num_ = 0;
    balance_last_migrate_succ_time_ = 0;
    server_manager_->reset_balance_info();
    is_loading_data_ = true;
  }

  if (has_set_bypass_flag && !is_loading_data_)
  {
    root_server_->set_bypass_flag(false);
  }
  return ret;
}

int ObRootBalancer::add_load_table_task_info(const ObString& table_name, const uint64_t table_id,
    ObString& uri, const int64_t start_time, ObString& simple_uri,
    ObDataSourceDesc::ObDataSourceType& data_source_type)
{ // load_data_lock_ must be locked by caller
  int ret = OB_SUCCESS;
  ObLoadDataInfo* info = NULL;
  uint64_t old_table_id = 0;

  { // lock root_table_rwlock_
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    if (root_table_->table_is_exist(table_id))
    {
      ret = OB_ROOT_TABLE_ID_EXIST;
      TBSYS_LOG(ERROR, "table id %lu is exist in root table, can't load it again", table_id);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = root_server_->get_table_id(table_name, old_table_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "fail to get table schema. table_name=%s, ret=%d", to_cstring(table_name), ret);
    }
    else if (old_table_id == table_id)
    {
      ret = OB_ERR_SYS;
      TBSYS_LOG(ERROR, "old table id must not same as current table id, cant do add load table. table_name=%s, table_id=%lu",
          to_cstring(table_name), table_id);
    }
  }

  // check if the table is loading
  for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY && OB_SUCCESS == ret; ++i)
  {
    if ((load_data_infos_[i].status_ == ObLoadDataInfo::DOING || load_data_infos_[i].status_ == ObLoadDataInfo::PREPARE)
        && (load_data_infos_[i].table_id_ == table_id || load_data_infos_[i].table_name_ == table_name))
    {
      ret = OB_DATA_LOAD_TABLE_DUPLICATED;
      TBSYS_LOG(ERROR, "table %.*s %lu is loading, can't load table %.*s %lu again",
          load_data_infos_[i].table_name_.length(), load_data_infos_[i].table_name_.ptr(),
          load_data_infos_[i].table_id_, table_name.length(), table_name.ptr(), table_id);
    }
  }

  // find free pos and add info
  if (OB_SUCCESS == ret)
  {
    int64_t tablet_version = root_server_->get_frozen_version_for_cs_heartbeat();
    for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY && OB_SUCCESS == ret; ++i)
    {
      if (load_data_infos_[i].status_ == ObLoadDataInfo::INIT)
      {
        info = &load_data_infos_[i];
        break;
      }
    }
    if (NULL == info)
    {
      ret = OB_DATA_SOURCE_CONCURRENCY_FULL;
      TBSYS_LOG(ERROR, "max load info conrurrency is %d, no new load table task is allowed", MAX_LOAD_INFO_CONCURRENCY);
      for(int64_t i=0; i<MAX_LOAD_INFO_CONCURRENCY; ++i)
      {
        TBSYS_LOG(INFO, "%ld: status=%s table_name=%.*s table_id=%lu start_time=%ld", i, load_data_infos_[i].get_status(),
            load_data_infos_[i].table_name_.length(), load_data_infos_[i].table_name_.ptr(), load_data_infos_[i].table_id_,
            load_data_infos_[i].start_time_);
      }
    }
    else if (OB_SUCCESS != (ret = info->set_info(
            table_name, table_id, old_table_id, uri, tablet_version, start_time, ObLoadDataInfo::PREPARE)))
    {
      TBSYS_LOG(WARN, "failed to add load table table_name=%.*s table_id=%lu old_table_id=%lu uri=%.*s, ret=%d",
          table_name.length(), table_name.ptr(), table_id, old_table_id, uri.length(), uri.ptr(), ret);
    }
    else
    {
      if (root_server_->is_master() && root_server_->get_obi_role().get_role() == ObiRole::MASTER)
      {
        update_load_table_history(*info);
      }
      if (OB_SUCCESS != (ret = simple_uri.clone(info->uri_)))
      {
        TBSYS_LOG(WARN, "failed to copy info->uri, info->uri=%.*s, ret=%d",
            info->uri_.length(), info->uri_.ptr(), ret);
      }
      else
      {
        data_source_type = info->data_source_type_;
      }
    }
  }
  return ret;
}

int ObRootBalancer::check_replica_count_for_import(int64_t tablet_version)
{
  check_components();
  int ret = OB_SUCCESS;
  int32_t chunk_server_count = server_manager_->get_alive_server_count(true);
  int64_t min_replica_count = config_->tablet_replicas_num;
  bool is_merged = false;
  if (chunk_server_count > 0 && chunk_server_count < min_replica_count)
  {
    ret = OB_ERR_SYS;
    TBSYS_LOG(ERROR, "check chunkserver count less than replica num:server[%d], replica[%ld]",
        chunk_server_count, min_replica_count);
  }

  if (min_replica_count > 2)
  {
    min_replica_count = 2;
  }

  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    if (root_table_->is_empty())
    {
      TBSYS_LOG(WARN, "root table is empty, try it later");
      is_merged = false;
    }
    else
    {
      ret = root_table_->check_tablet_version_merged(tablet_version, min_replica_count, is_merged);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to check_tablet_version_merged, tablet_version=%ld min_replica_count=%ld, ret=%d",
            tablet_version, min_replica_count, ret);
        ret = OB_RS_STATE_NOT_ALLOW;
      }
      else if (!is_merged)
      {
        ret = OB_RS_STATE_NOT_ALLOW;
        TBSYS_LOG(INFO, "root table does not satisfy tablet_version=%ld min_replica_count=%ld",
            tablet_version, min_replica_count);
      }
    }
  }

  return ret;
}
