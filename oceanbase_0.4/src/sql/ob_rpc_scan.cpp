/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rpc_scan.cpp
 *
 * ObRpcScan operator
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_rpc_scan.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_obj_cast.h"
#include "mergeserver/ob_merge_server_service.h"
#include "mergeserver/ob_merge_server_main.h"
#include "mergeserver/ob_ms_sql_get_request.h"
#include "ob_sql_read_strategy.h"
#include "ob_duplicate_indicator.h"
#include "common/ob_profile_type.h"
#include "common/ob_profile_log.h"
#include "mergeserver/ob_insert_cache.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::mergeserver;

ObRpcScan::ObRpcScan() :
  timeout_us_(0),
  sql_scan_request_(NULL),
  sql_get_request_(NULL),
  scan_param_(NULL),
  get_param_(NULL),
  read_param_(NULL),
  is_scan_(false),
  is_get_(false),
  cache_proxy_(NULL),
  async_rpc_(NULL),
  session_info_(NULL),
  merge_service_(NULL),
  cur_row_(),
  cur_row_desc_(),
  table_id_(OB_INVALID_ID),
  base_table_id_(OB_INVALID_ID),
  start_key_buf_(NULL),
  end_key_buf_(NULL),
  is_rpc_failed_(false),
  need_cache_frozen_data_(false),
  cache_bloom_filter_(false),
  rowkey_not_exists_(false),
  insert_cache_iter_counter_(0),
  insert_cache_need_revert_(false)
{
  sql_read_strategy_.set_rowkey_info(rowkey_info_);
}


ObRpcScan::~ObRpcScan()
{
  this->destroy();
}

void ObRpcScan::reset()
{
  this->destroy();
  timeout_us_ = 0;
  read_param_ = NULL;
  get_param_ = NULL;
  scan_param_ = NULL;
  is_scan_ = false;
  is_get_ = false;
  cache_proxy_ = NULL;
  async_rpc_ = NULL;
  session_info_ = NULL;
  merge_service_ = NULL;
  table_id_ = OB_INVALID_ID;
  base_table_id_ = OB_INVALID_ID;
  start_key_buf_ = NULL;
  end_key_buf_ = NULL;
  is_rpc_failed_ = false;
  need_cache_frozen_data_ = false;
  cache_bloom_filter_ = false;
  rowkey_not_exists_ = false;
  frozen_data_.clear();
  cached_frozen_data_.reset();
  insert_cache_iter_counter_ = 0;
  cleanup_request();
  //cur_row_.reset(false, ObRow::DEFAULT_NULL);
  cur_row_desc_.reset();
  sql_read_strategy_.destroy();
  insert_cache_need_revert_ = false;
}

void ObRpcScan::reuse()
{
  reset();
}

int ObRpcScan::init(ObSqlContext *context, const common::ObRpcScanHint *hint)
{
  int ret = OB_SUCCESS;
  if (NULL == context)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == context->cache_proxy_
           || NULL == context->async_rpc_
           || NULL == context->schema_manager_
           || NULL == context->merge_service_)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (base_table_id_ == OB_INVALID_ID)
  {
    TBSYS_LOG(WARN, "must set table_id_ first. table_id_=%ld", table_id_);
    ret = OB_NOT_INIT;
  }
  else
  {
    // init rowkey_info
    const ObTableSchema * schema = NULL;
    if (NULL == (schema = context->schema_manager_->get_table_schema(base_table_id_)))
    {
      TBSYS_LOG(WARN, "fail to get table schema. table_id[%ld]", base_table_id_);
      ret = OB_ERROR;
    }
    else
    {
      cache_proxy_ = context->cache_proxy_;
      async_rpc_ = context->async_rpc_;
      session_info_ = context->session_info_;
      merge_service_ = context->merge_service_;
      // copy
      rowkey_info_ = schema->get_rowkey_info();
    }
  }
  obj_row_not_exists_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  if (hint)
  {
    this->set_hint(*hint);
    if (hint_.read_method_ == ObSqlReadStrategy::USE_SCAN)
    {
      OB_ASSERT(NULL == scan_param_);
      scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
      if (NULL == scan_param_)
      {
        TBSYS_LOG(WARN, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        scan_param_->set_phy_plan(get_phy_plan());
        read_param_ = scan_param_;
      }
    }
    else if (hint_.read_method_ == ObSqlReadStrategy::USE_GET)
    {
      OB_ASSERT(NULL == get_param_);
      get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
      if (NULL == get_param_)
      {
        TBSYS_LOG(WARN, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        get_param_->set_phy_plan(get_phy_plan());
        read_param_ = get_param_;
      }
      if (OB_SUCCESS == ret && hint_.is_get_skip_empty_row_)
      {
        ObSqlExpression special_column;
        special_column.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
        if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, special_column)))
        {
          TBSYS_LOG(WARN, "fail to create column expression. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = get_param_->add_output_column(special_column)))
        {
          TBSYS_LOG(WARN, "fail to add special is-row-empty-column to project. ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "add special column to read param");
        }
      }
    }
    else
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "read method must be either scan or get. method=%d", hint_.read_method_);
    }
  }
  return ret;
}

int ObRpcScan::cast_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  bool need_buf = false;
  int64_t used_buf_len = 0;
  if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(rowkey_info_, range.start_key_, need_buf)))
  {
    TBSYS_LOG(WARN, "err=%d", ret);
  }
  else if (need_buf)
  {
    if (NULL == start_key_buf_)
    {
      start_key_buf_ = (char*)ob_malloc(OB_MAX_ROW_LENGTH, ObModIds::OB_SQL_RPC_SCAN);
    }
    if (NULL == start_key_buf_)
    {
      TBSYS_LOG(ERROR, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (OB_SUCCESS != (ret = ob_cast_rowkey(rowkey_info_, range.start_key_,
                                            start_key_buf_, OB_MAX_ROW_LENGTH, used_buf_len)))
    {
      TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(rowkey_info_, range.end_key_, need_buf)))
    {
      TBSYS_LOG(WARN, "err=%d", ret);
    }
    else if (need_buf)
    {
      if (NULL == end_key_buf_)
      {
        end_key_buf_ = (char*)ob_malloc(OB_MAX_ROW_LENGTH, ObModIds::OB_SQL_RPC_SCAN);
      }
      if (NULL == end_key_buf_)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      if (OB_SUCCESS != (ret = ob_cast_rowkey(rowkey_info_, range.end_key_,
                                              end_key_buf_, OB_MAX_ROW_LENGTH, used_buf_len)))
      {
        TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
      }
    }
  }
  return ret;
}

int ObRpcScan::create_scan_param(ObSqlScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  // until all columns and filters are set, we could know the exact range
  if (OB_SUCCESS != (ret = fill_read_param(scan_param)))
  {
    TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_scan_range(range)))
  {
    TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
  }
  // TODO: lide.wd 将range 深拷贝到ObSqlScanParam内部的buffer_pool_中
  else if (OB_SUCCESS != (ret = scan_param.set_range(range)))
  {
    TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
  }
  TBSYS_LOG(DEBUG, "scan_param=%s", to_cstring(scan_param));
  TBSYS_LOG(TRACE, "dump scan range: %s", to_cstring(range));
  return ret;
}


int ObRpcScan::create_get_param(ObSqlGetParam &get_param)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = fill_read_param(get_param)))
  {
    TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cons_get_rows(get_param)))
    {
      TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
    }
  }
  TBSYS_LOG(DEBUG, "get_param=%s", to_cstring(get_param));
  return ret;
}

int ObRpcScan::fill_read_param(ObSqlReadParam &dest_param)
{
  int ret = OB_SUCCESS;
  ObObj val;
  OB_ASSERT(NULL != session_info_);
  if (OB_SUCCESS == ret)
  {
    dest_param.set_is_result_cached(true);
    if (OB_SUCCESS != (ret = dest_param.set_table_id(table_id_, base_table_id_)))
    {
      TBSYS_LOG(WARN, "fail to set table id and scan range. ret=%d", ret);
    }
  }

  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObRpcScan, PHY_RPC_SCAN);
  }
}

int64_t ObRpcScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "RpcScan(");
  pos += hint_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ", ");
  pos += read_param_->to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")");
  //databuff_printf(buf, buf_len, pos, "RpcScan(row_desc=");
  //pos += cur_row_desc_.to_string(buf+pos, buf_len-pos);
  //databuff_printf(buf, buf_len, pos, ", ");
  //pos += sql_read_strategy_.to_string(buf+pos, buf_len-pos);
  //databuff_printf(buf, buf_len, pos, ", read_param=");
  //pos += hint_.to_string(buf+pos, buf_len-pos);
  //databuff_printf(buf, buf_len, pos, ", ");
  //pos += read_param_->to_string(buf+pos, buf_len-pos);
  //databuff_printf(buf, buf_len, pos, ")");
  return pos;
}

void ObRpcScan::set_hint(const common::ObRpcScanHint &hint)
{
  hint_ = hint;
  // max_parallel_count
  if (hint_.max_parallel_count <= 0)
  {
    hint_.max_parallel_count = 20;
  }
  // max_memory_limit
  if (hint_.max_memory_limit < 1024 * 1024 * 2)
  {
    hint_.max_memory_limit = 1024 * 1024 * 2;
  }
}

int ObRpcScan::open()
{
  int ret = OB_SUCCESS;
  if (NULL == (sql_scan_request_ = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().alloc()))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "alloc scan request from scan request pool failed, ret=%d", ret);
  }
  else if (NULL == (sql_get_request_ = ObMergeServerMain::get_instance()->get_merge_server().get_get_req_pool().alloc()))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "alloc get request from get request pool failed, ret=%d", ret);
  }
  else
  {
    sql_scan_request_->set_tablet_location_cache_proxy(cache_proxy_);
    sql_get_request_->set_tablet_location_cache_proxy(cache_proxy_);
    sql_scan_request_->set_merger_async_rpc_stub(async_rpc_);
    sql_get_request_->set_merger_async_rpc_stub(async_rpc_);
    if (NULL != session_info_)
    {
      ///  query timeout for sql level
      ObObj val;
      int64_t query_timeout = 0;
      if (OB_SUCCESS == session_info_->get_sys_variable_value(ObString::make_string(OB_QUERY_TIMEOUT_PARAM), val))
      {
        if (OB_SUCCESS != val.get_int(query_timeout))
        {
          TBSYS_LOG(WARN, "fail to get query timeout from session, ret=%d", ret);
          query_timeout = 0; // use default
        }
      }

      if (OB_APP_MIN_TABLE_ID > base_table_id_)
      {
        // internal table
        // BUGFIX: this timeout value should not be larger than the plan timeout 
        // (see ob_transformer.cpp/int ObResultSet::open())
        // bug ref: http://bugfree.corp.taobao.com/bug/252871
        hint_.timeout_us = std::max(query_timeout, OB_DEFAULT_STMT_TIMEOUT); // prevent bad config, min timeout=3sec
      }
      else
      {
        // app table
        if (query_timeout > 0)
        {
          hint_.timeout_us = query_timeout;
        }
        else
        {
          // use default hint_.timeout_us, usually 10 sec
        }
      }
      TBSYS_LOG(DEBUG, "query timeout is %ld", hint_.timeout_us);
    }

    timeout_us_ = hint_.timeout_us;
    OB_ASSERT(my_phy_plan_);
    FILL_TRACE_LOG(" hint.read_consistency=%s ", get_consistency_level_str(hint_.read_consistency_));
    if (hint_.read_consistency_ == common::FROZEN)
    {
      ObVersion frozen_version = my_phy_plan_->get_curr_frozen_version();
      read_param_->set_data_version(frozen_version);
      FILL_TRACE_LOG("static_data_version = %s", to_cstring(frozen_version));
    }
    read_param_->set_is_only_static_data(hint_.read_consistency_ == STATIC);
    read_param_->set_is_read_consistency(hint_.read_consistency_ == STRONG);
    FILL_TRACE_LOG("only_static = %c", read_param_->get_is_only_static_data() ? 'Y' : 'N');

    if (NULL == cache_proxy_ || NULL == async_rpc_)
    {
      ret = OB_NOT_INIT;
    }

    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(DEBUG, "read_method_ [%s]", hint_.read_method_ == ObSqlReadStrategy::USE_SCAN ? "SCAN" : "GET");
      // common initialization
      cur_row_.set_row_desc(cur_row_desc_);
      FILL_TRACE_LOG("open %s", to_cstring(cur_row_desc_));
    }
    // update语句的need_cache_frozen_data_ 才会被设置为true
    ObFrozenDataCache & frozen_data_cache = ObMergeServerMain::
        get_instance()->get_merge_server().get_frozen_data_cache();
    if (frozen_data_cache.get_in_use() && need_cache_frozen_data_ && (hint_.read_consistency_ == FROZEN))
    {
      int64_t size = 0;
      if (OB_SUCCESS != (ret = read_param_->set_table_id(table_id_, base_table_id_)))
      {
        TBSYS_LOG(ERROR, "set_table_id error, ret: %d", ret);
      }
      else
      {
        size = read_param_->get_serialize_size();
      }
      if (OB_SUCCESS != ret)
      {}
      else if (OB_SUCCESS != (ret = frozen_data_key_buf_.alloc_buf(size)))
      {
        TBSYS_LOG(ERROR, "ObFrozenDataKeyBuf alloc_buf error, ret: %d", ret);
      }
      else if (OB_SUCCESS != (ret = read_param_->serialize(frozen_data_key_buf_.buf,
              frozen_data_key_buf_.buf_len, frozen_data_key_buf_.pos)))
      {
        TBSYS_LOG(ERROR, "ObSqlReadParam serialize error, ret: %d", ret);
      }
      else
      {
        frozen_data_key_.frozen_version = my_phy_plan_->get_curr_frozen_version();
        frozen_data_key_.param_buf = frozen_data_key_buf_.buf;
        frozen_data_key_.len = frozen_data_key_buf_.pos;
        ret = frozen_data_cache.get(frozen_data_key_, cached_frozen_data_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObFrozenDataCache get_frozen_data error, ret: %d", ret);
        }
        else if (cached_frozen_data_.has_data())
        {
          OB_STAT_INC(SQL, SQL_UPDATE_CACHE_HIT);
          cached_frozen_data_.set_row_desc(cur_row_desc_);
        }
        else
        {
          OB_STAT_INC(SQL, SQL_UPDATE_CACHE_MISS);
        }
      }
    }

    if (!cached_frozen_data_.has_data())
    {
      // Scan
      if (OB_SUCCESS == ret && hint_.read_method_ == ObSqlReadStrategy::USE_SCAN)
      {
        is_scan_ = true;
        if (OB_SUCCESS != (ret = sql_scan_request_->initialize()))
        {
          TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
        }
        else
        {
          sql_scan_request_->alloc_request_id();
          if (OB_SUCCESS != (ret = sql_scan_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN)))
          {
            TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = create_scan_param(*scan_param_)))
          {
            TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          sql_scan_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
          if(OB_SUCCESS != (ret = sql_scan_request_->set_request_param(*scan_param_, hint_)))
          {
            TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                hint_.max_parallel_count, ret);
          }
        }
      }
      // Get
      if (OB_SUCCESS == ret && hint_.read_method_ == ObSqlReadStrategy::USE_GET)
      {
        // insert and select
        is_get_ = true;
        get_row_desc_.reset();
        sql_get_request_->alloc_request_id();
        if (OB_SUCCESS != (ret = sql_get_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_GET)))
        {
          TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = create_get_param(*get_param_)))
        {
          TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
        }
        else
        {
          // 暂时只处理单行
          // 只有insert语句的cache_bloom_filter_才会设置为true
          ObInsertCache & insert_cache = ObMergeServerMain::get_instance()->get_merge_server().get_insert_cache();
          if (insert_cache.get_in_use() && cache_bloom_filter_ && get_param_->get_row_size() == 1)
          {
            int err = OB_SUCCESS;
            ObTabletLocationList loc_list;
            loc_list.set_buffer(tablet_location_list_buf_);
            const ObRowkey *rowkey = (*get_param_)[0];
            if (OB_SUCCESS == (err = cache_proxy_->get_tablet_location(get_param_->get_table_id(), *rowkey, loc_list)))
            {
              const ObNewRange &tablet_range = loc_list.get_tablet_range();
              InsertCacheKey key;
              key.range_ = tablet_range;
              int64_t version = my_phy_plan_->get_curr_frozen_version();
              key.tablet_version_ = (reinterpret_cast<ObVersion*>(&version))->major_;
              if (OB_SUCCESS == (err = insert_cache.get(key, value_)))
              {
                if (!value_.bf_->may_contain(*rowkey))
                {
                  OB_STAT_INC(SQL,SQL_INSERT_CACHE_HIT);
                  // rowkey not exists
                  rowkey_not_exists_ = true;
                }
                else
                {
                  OB_STAT_INC(SQL,SQL_INSERT_CACHE_MISS);
                }
                insert_cache_need_revert_ = true;
              }
              else if (OB_ENTRY_NOT_EXIST == err)
              {
                OB_STAT_INC(SQL,SQL_INSERT_CACHE_MISS);
                // go on
                char *buff = reinterpret_cast<char*>(ob_tc_malloc(
                      sizeof(ObBloomFilterTask) + rowkey->get_deep_copy_size(),
                      ObModIds::OB_MS_UPDATE_BLOOM_FILTER));
                if (buff == NULL)
                {
                  err = OB_ALLOCATE_MEMORY_FAILED;
                  TBSYS_LOG(ERROR, "malloc failed, ret=%d", err);
                }
                else
                {
                  ObBloomFilterTask *task = new (buff) ObBloomFilterTask();
                  task->table_id = get_param_->get_table_id();
                  char *obj_buf = buff + sizeof(ObBloomFilterTask);
                  // deep copy task->key
                  common::AdapterAllocator alloc;
                  alloc.init(obj_buf);
                  if (OB_SUCCESS != (err = rowkey->deep_copy(task->rowkey, alloc)))
                  {
                    TBSYS_LOG(ERROR, "deep copy rowkey failed, err=%d", err);
                  }
                  else
                  {
                    err = ObMergeServerMain::get_instance()->get_merge_server().get_bloom_filter_task_queue_thread().push(task);
                    if (OB_SUCCESS != err && OB_TOO_MANY_BLOOM_FILTER_TASK != err)
                    {
                      TBSYS_LOG(ERROR, "push task to bloom_filter_task_queue failed,ret=%d", err);
                    }
                    else if (OB_TOO_MANY_BLOOM_FILTER_TASK == err)
                    {
                      TBSYS_LOG(DEBUG, "push task to bloom_filter_task_queue failed, ret=%d", err);
                    }
                    else
                    {
                      TBSYS_LOG(DEBUG, "PUSH TASK SUCCESS");
                    }
                  }
                  if (OB_SUCCESS != err)
                  {
                    task->~ObBloomFilterTask();
                    ob_tc_free(reinterpret_cast<void*>(task));
                  }
                }
              }// OB_ENTRY_NOT_EXIST == err
              else
              {
                //go on
                TBSYS_LOG(ERROR, "get from insert cache failed, err=%d", err);
              }
            }
            else
            {
              //go on
              TBSYS_LOG(ERROR, "get tablet location failed, table_id=%lu,rowkey=%s, err=%d", get_param_->get_table_id(), to_cstring(*rowkey), err);
            }
          }
        }
        if (!rowkey_not_exists_)
        {
          if (OB_SUCCESS != (ret = cons_row_desc(*get_param_, get_row_desc_)))
          {
            TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
          }
          else if (OB_SUCCESS != (ret = sql_get_request_->set_row_desc(get_row_desc_)))
          {
            TBSYS_LOG(WARN, "fail to set row desc:ret[%d]", ret);
          }
          else if(OB_SUCCESS != (ret = sql_get_request_->set_request_param(*get_param_, timeout_us_)))
          {
            TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                hint_.max_parallel_count, ret);
          }
          if (OB_SUCCESS == ret)
          {
            sql_get_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
            if (OB_SUCCESS != (ret = sql_get_request_->open()))
            {
              TBSYS_LOG(WARN, "fail to open get request. ret=%d", ret);
            }
          }
        }
      }//Get
    }
    if (OB_SUCCESS != ret)
    {
      is_rpc_failed_ = true;
    }
  }
  return ret;
}

void ObRpcScan::destroy()
{
  sql_read_strategy_.destroy();
  if (NULL != start_key_buf_)
  {
    ob_free(start_key_buf_, ObModIds::OB_SQL_RPC_SCAN);
    start_key_buf_ = NULL;
  }
  if (NULL != end_key_buf_)
  {
    ob_free(end_key_buf_, ObModIds::OB_SQL_RPC_SCAN);
    end_key_buf_ = NULL;
  }
  if (NULL != get_param_)
  {
    get_param_->~ObSqlGetParam();
    ob_free(get_param_);
    get_param_ = NULL;
  }
  if (NULL != scan_param_)
  {
    scan_param_->~ObSqlScanParam();
    ob_free(scan_param_);
    scan_param_ = NULL;
  }
  cleanup_request();
}

int ObRpcScan::close()
{
  int ret = OB_SUCCESS;
  ObFrozenDataCache & frozen_data_cache = ObMergeServerMain::
      get_instance()->get_merge_server().get_frozen_data_cache();
  if (cached_frozen_data_.has_data())
  {
    ret = frozen_data_cache.revert(cached_frozen_data_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObFrozenDataCache revert error, ret: %d", ret);
    }
  }
  else if (frozen_data_cache.get_in_use() && need_cache_frozen_data_ && (hint_.read_consistency_ == FROZEN))
  {
    if (!is_rpc_failed_)
    {
      int err = frozen_data_cache.put(frozen_data_key_, frozen_data_);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "ObFrozenDataCache put error, err: %d", err);
      }
    }
    frozen_data_.reuse();
  }
  if (is_scan_)
  {
    sql_scan_request_->close();
    sql_scan_request_->reset();
    if (NULL != scan_param_)
    {
      scan_param_->reset_local();
    }
    is_scan_ = false;
  }
  if (is_get_)
  {
    sql_get_request_->close();
    sql_get_request_->reset();
    if (NULL != get_param_)
    {
      get_param_->reset_local();
    }
    is_get_ = false;
    tablet_location_list_buf_.reuse();
    rowkey_not_exists_ = false;
    if (insert_cache_need_revert_)
    {
      if (OB_SUCCESS == (ret = ObMergeServerMain::get_instance()->get_merge_server().get_insert_cache().revert(value_)))
      {
        value_.reset();
      }
      else
      {
        TBSYS_LOG(WARN, "revert bloom filter failed, ret=%d", ret);
      }
      insert_cache_need_revert_ = false;
    }
    //rowkeys_exists_.clear();
  }
  insert_cache_iter_counter_ = 0;
  cleanup_request();
  return ret;
}
void ObRpcScan::cleanup_request()
{
  int tmp_ret = OB_SUCCESS;
  if (sql_scan_request_ != NULL)
  {
    if (OB_SUCCESS != (tmp_ret = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().free(sql_scan_request_)))
    {
      TBSYS_LOG(WARN, "free scan request back to scan req pool failed, ret=%d", tmp_ret);
    }
    sql_scan_request_ = NULL;
  }
  if (sql_get_request_ != NULL)
  {
    if (OB_SUCCESS != (tmp_ret = ObMergeServerMain::get_instance()->get_merge_server().get_get_req_pool().free(sql_get_request_)))
    {
      TBSYS_LOG(WARN, "free get request back to get req pool failed, ret=%d", tmp_ret);
    }
    sql_get_request_ = NULL;
  }
}

int ObRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(base_table_id_ <= 0 || 0 >= cur_row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init, tid=%lu, column_num=%ld", base_table_id_, cur_row_desc_.get_column_num());
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &cur_row_desc_;
  }
  return ret;
}

int ObRpcScan::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (rowkey_not_exists_)
  {
    if (insert_cache_iter_counter_ == 0)
    {
      row_not_exists_.set_row_desc(cur_row_desc_);
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = row_not_exists_.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, obj_row_not_exists_)))
      {
        TBSYS_LOG(ERROR, "set cell failed, err=%d", err);
      }
      else
      {
        row = &row_not_exists_;
        insert_cache_iter_counter_ ++;
        ret = OB_SUCCESS;
      }
    }
    else if (insert_cache_iter_counter_ == 1)
    {
      ret = OB_ITER_END;
    }
  }
  else
  {
    if (cached_frozen_data_.has_data())
    {
      ret = cached_frozen_data_.get_next_row(row);
      if (OB_SUCCESS != ret && OB_ITER_END != ret)
      {
        TBSYS_LOG(ERROR, "ObCachedFrozenData get_next_row error, ret: %d", ret);
      }
    }
    else
    {
      if (ObSqlReadStrategy::USE_SCAN == hint_.read_method_)
      {
        ret = get_next_compact_row(row); // 可能需要等待CS返回
      }
      else if (ObSqlReadStrategy::USE_GET == hint_.read_method_)
      {
        ret = sql_get_request_->get_next_row(cur_row_);
      }
      else
      {
        TBSYS_LOG(WARN, "not init. read_method_=%d", hint_.read_method_);
        ret = OB_NOT_INIT;
      }
      row = &cur_row_;
      if (ObMergeServerMain::get_instance()->get_merge_server().get_frozen_data_cache().get_in_use()
          && need_cache_frozen_data_ && (hint_.read_consistency_ == FROZEN) && OB_SUCCESS == ret)
      {
        int64_t cur_size_counter;
        ret = frozen_data_.add_row(*row, cur_size_counter);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObRowStore add_row error, ret: %d", ret);
        }
      }
    }
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    is_rpc_failed_ = true;
  }
  return ret;
}


/**
 * 函数功能： 从scan_event中获取一行数据
 * 说明：
 * wait的功能：从finish_queue中阻塞地pop出一个事件（如果没有事件则阻塞）， 然后调用process_result()处理事件
 */
int ObRpcScan::get_next_compact_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool can_break = false;
  int64_t remain_us = 0;
  row = NULL;
  do
  {
    if (OB_UNLIKELY(my_phy_plan_->is_timeout(&remain_us)))
    {
      can_break = true;
      ret = OB_PROCESS_TIMEOUT;
    }
    else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
    {
      can_break = true;
      TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
    }
    else if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request_->get_next_row(cur_row_))))
    {
      // got a row without block,
      // no need to check timeout, leave this work to upper layer
      can_break = true;
    }
    else if (OB_ITER_END == ret && sql_scan_request_->is_finish())
    {
      // finish all data
      // can break;
      can_break = true;
    }
    else if (OB_ITER_END == ret)
    {
      // need to wait for incomming data
      can_break = false;
      timeout_us_ = std::min(timeout_us_, remain_us);
      if( OB_SUCCESS != (ret = sql_scan_request_->wait_single_event(timeout_us_)))
      {
        if (timeout_us_ <= 0)
        {
          TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", timeout_us_);
        }
        can_break = true;
      }
      else
      {
        TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", timeout_us_);
      }
    }
    else
    {
      // encounter an unexpected error or
      TBSYS_LOG(WARN, "Unexprected error. ret=%d, cur_row_desc[%s], read_method_[%d]", ret, to_cstring(cur_row_desc_), hint_.read_method_);
      can_break = true;
    }
  }while(false == can_break);
  if (OB_SUCCESS == ret)
  {
    row = &cur_row_;
  }
  return ret;
}


int ObRpcScan::add_output_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  bool is_cid = false;
  //if (table_id_ <= 0 || table_id_ != expr.get_table_id())
  if (base_table_id_ <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "must call set_table() first. base_table_id_=%lu",
        base_table_id_);
  }
  else if ((OB_SUCCESS == (ret = expr.is_column_index_expr(is_cid)))  && (true == is_cid))
  {
    // 添加基本列
    if (OB_SUCCESS != (ret = read_param_->add_output_column(expr)))
    {
      TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
    }
  }
  else
  {
    // 添加复合列
    if (OB_SUCCESS != (ret = read_param_->add_output_column(expr)))
    {
      TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
    }
  }
  // cons row desc
  if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(expr.get_table_id(), expr.get_column_id()))))
  {
    TBSYS_LOG(WARN, "fail to add column to scan param. ret=%d, tid_=%lu, cid=%lu", ret, expr.get_table_id(), expr.get_column_id());
  }
  return ret;
}



int ObRpcScan::set_table(const uint64_t table_id, const uint64_t base_table_id)
{
  int ret = OB_SUCCESS;
  if (0 >= base_table_id)
  {
    TBSYS_LOG(WARN, "invalid table id: %lu", base_table_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    table_id_ = table_id;
    base_table_id_ = base_table_id;
  }
  return ret;
}

int ObRpcScan::cons_get_rows(ObSqlGetParam &get_param)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  get_rowkey_array_.clear();
  // TODO lide.wd: rowkey obj storage needed. varchar use orginal buffer, will be copied later
  PageArena<ObObj,ModulePageAllocator> rowkey_objs_allocator(
      PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_RPC_SCAN2));
  // try  'where (k1,k2,kn) in ((a,b,c), (e,f,g))'
  if (OB_SUCCESS != (ret = sql_read_strategy_.find_rowkeys_from_in_expr(true, get_rowkey_array_, rowkey_objs_allocator)))
  {
    TBSYS_LOG(WARN, "fail to find rowkeys in IN operator. ret=%d", ret);
  }
  else if (get_rowkey_array_.count() > 0)
  {
    ObDuplicateIndicator indicator;
    bool is_dup = false;
    if (get_rowkey_array_.count() > 1)
    {
      if ((ret = indicator.init(get_rowkey_array_.count())) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Init ObDuplicateIndicator failed:ret[%d]", ret);
      }
    }
    for (idx = 0; idx < get_rowkey_array_.count(); idx++)
    {
      if (OB_UNLIKELY(get_rowkey_array_.count() > 1))
      {
        if (OB_SUCCESS != (ret = indicator.have_seen(get_rowkey_array_.at(idx), is_dup)))
        {
          TBSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
          break;
        }
        else if (is_dup)
        {
          continue;
        }
      }
      //深拷贝，从rowkey_objs_allocator 拷贝到了allocator_中
      if (OB_SUCCESS != (ret = get_param.add_rowkey(get_rowkey_array_.at(idx), true)))
      {
        TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
        break;
      }
    }
  }
  // try  'where k1=a and k2=b and kn=n', only one rowkey
  else if (OB_SUCCESS != (ret = sql_read_strategy_.find_rowkeys_from_equal_expr(true, get_rowkey_array_, rowkey_objs_allocator)))
  {
    TBSYS_LOG(WARN, "fail to find rowkeys from where equal condition, ret=%d", ret);
  }
  else if (get_rowkey_array_.count() > 0)
  {
    for (idx = 0; idx < get_rowkey_array_.count(); idx++)
    {
      if (OB_SUCCESS != (ret = get_param.add_rowkey(get_rowkey_array_.at(idx), true)))
      {
        TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
        break;
      }
    }
    OB_ASSERT(idx == 1);
  }
  rowkey_objs_allocator.free();
  return ret;
}

int ObRpcScan::cons_scan_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  bool found = false;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.table_id_ = base_table_id_;
  OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
  // range 指向sql_read_strategy_的空间
  if (OB_SUCCESS != (ret = sql_read_strategy_.find_scan_range(range, found, false)))
  {
    TBSYS_LOG(WARN, "fail to find range %lu", base_table_id_);
  }
  return ret;
}

int ObRpcScan::get_min_max_rowkey(const ObArray<ObRowkey> &rowkey_array, ObObj *start_key_objs_, ObObj *end_key_objs_, int64_t rowkey_size)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  if (1 == rowkey_array.count())
  {
    const ObRowkey &rowkey = rowkey_array.at(0);
    for (i = 0; i < rowkey_size && i < rowkey.get_obj_cnt(); i++)
    {
      start_key_objs_[i] = rowkey.ptr()[i];
      end_key_objs_[i] = rowkey.ptr()[i];
    }
    for ( ; i < rowkey_size; i++)
    {
      start_key_objs_[i] = ObRowkey::MIN_OBJECT;
      end_key_objs_[i] = ObRowkey::MAX_OBJECT;
    }
  }
  else
  {
    TBSYS_LOG(WARN, "only support single insert row for scan optimization. rowkey_array.count=%ld", rowkey_array.count());
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObRpcScan::add_filter(ObSqlExpression* expr)
{
  int ret = OB_SUCCESS;
  expr->set_owner_op(this);
  if (OB_SUCCESS != (ret = sql_read_strategy_.add_filter(*expr)))
  {
    TBSYS_LOG(WARN, "fail to add filter to sql read strategy:ret[%d]", ret);
  }
  if (OB_SUCCESS == ret && OB_SUCCESS != (ret = read_param_->add_filter(expr)))
  {
    TBSYS_LOG(WARN, "fail to add composite column to scan param. ret=%d", ret);
  }
  return ret;
}

int ObRpcScan::add_group_column(const uint64_t tid, const uint64_t cid)
{
  return read_param_->add_group_column(tid, cid);
}

int ObRpcScan::add_aggr_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (cur_row_desc_.get_column_num() <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "Output column(s) of ObRpcScan must be set first, ret=%d", ret);
  }
  else if ((ret = cur_row_desc_.add_column_desc(
                      expr.get_table_id(),
                      expr.get_column_id())) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Failed to add column desc, err=%d", ret);
  }
  else if ((ret = read_param_->add_aggr_column(expr)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Failed to add aggregate column desc, err=%d", ret);
  }
  return ret;
}

int ObRpcScan::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
{
  return read_param_->set_limit(limit, offset);
}

int ObRpcScan::cons_row_desc(const ObSqlGetParam &sql_get_param, ObRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if ( !sql_get_param.has_project() )
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "should has project");
  }

  if (OB_SUCCESS == ret)
  {
    const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &columns = sql_get_param.get_project().get_output_columns();
    for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
    {
      const ObSqlExpression &expr = columns.at(i);
      if (OB_SUCCESS != (ret = row_desc.add_column_desc(expr.get_table_id(), expr.get_column_id())))
      {
        TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if ( sql_get_param.get_row_size() <= 0 )
    {
      ret = OB_ERR_LACK_OF_ROWKEY_COL;
      TBSYS_LOG(WARN, "should has a least one row");
    }
    else
    {
      row_desc.set_rowkey_cell_count(sql_get_param[0]->length());
    }
  }

  return ret;
}

PHY_OPERATOR_ASSIGN(ObRpcScan)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObRpcScan);
  reset();
  rowkey_info_ = o_ptr->rowkey_info_;
  table_id_ = o_ptr->table_id_;
  base_table_id_ = o_ptr->base_table_id_;
  hint_ = o_ptr->hint_;
  need_cache_frozen_data_ = o_ptr->need_cache_frozen_data_;
  cache_bloom_filter_ = o_ptr->cache_bloom_filter_;
  if ((ret = cur_row_desc_.assign(o_ptr->cur_row_desc_)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Assign ObRowDesc failed, ret=%d", ret);
  }
  else if ((ret = sql_read_strategy_.assign(&o_ptr->sql_read_strategy_, this)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Assign ObSqlReadStrategy failed, ret=%d", ret);
  }
  else if (o_ptr->scan_param_)
  {
    scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
    if (NULL == scan_param_)
    {
      TBSYS_LOG(WARN, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      scan_param_->set_phy_plan(get_phy_plan());
      read_param_ = scan_param_;
      if ((ret = scan_param_->assign(o_ptr->scan_param_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign Scan param failed, ret=%d", ret);
      }
    }
  }
  else if (o_ptr->get_param_)
  {
    get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
    if (NULL == get_param_)
    {
      TBSYS_LOG(WARN, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      get_param_->set_phy_plan(get_phy_plan());
      read_param_ = get_param_;
      if ((ret = get_param_->assign(o_ptr->get_param_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign Get param failed, ret=%d", ret);
      }
    }
  }
  sql_read_strategy_.set_rowkey_info(rowkey_info_);
  return ret;
}
