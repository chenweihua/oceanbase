/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-09-26
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *          maoqi(maoqi@taobao.com)
 *
 *
 ================================================================*/
#include <new>
#include <string.h>
#include <cmath>
#include <tbsys.h>

#include "common/ob_schema.h"
#include "common/ob_range.h"
#include "common/ob_scanner.h"
#include "common/ob_define.h"
#include "common/ob_action_flag.h"
#include "common/ob_atomic.h"
#include "common/ob_version.h"
#include "common/utility.h"
#include "common/ob_rowkey_helper.h"
#include "common/ob_array.h"
#include "common/ob_table_id_name.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_inner_table_operator.h"
#include "common/roottable/ob_scan_helper_impl.h"
#include "common/ob_schema_service_impl.h"
#include "common/ob_extra_tables_schema.h"
#include "common/utility.h"
#include "common/ob_schema_helper.h"
#include "common/ob_common_stat.h"
#include "common/ob_bypass_task_info.h"
#include "common/ob_rowkey.h"
#include "common/file_directory_utils.h"
#include "ob_root_server2.h"
#include "ob_root_worker.h"
#include "ob_root_stat_key.h"
#include "ob_root_util.h"
#include "ob_root_bootstrap.h"
#include "ob_root_ms_provider.h"
#include "ob_root_monitor_table.h"
#include "ob_rs_trigger_event_util.h"
#include "ob_root_ups_provider.h"
#include "ob_root_ddl_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

using oceanbase::common::databuff_printf;

namespace oceanbase
{
  namespace rootserver
  {
    const int WAIT_SECONDS = 1;
    const int RETURN_BACH_COUNT = 8;
    const int MAX_RETURN_BACH_ROW_COUNT = 1000;
    const int MIN_BALANCE_TOLERANCE = 1;

    const char* ROOT_1_PORT =   "1_port";
    const char* ROOT_1_MS_PORT = "1_ms_port";
    const char* ROOT_1_IPV6_1 = "1_ipv6_1";
    const char* ROOT_1_IPV6_2 = "1_ipv6_2";
    const char* ROOT_1_IPV6_3 = "1_ipv6_3";
    const char* ROOT_1_IPV6_4 = "1_ipv6_4";
    const char* ROOT_1_IPV4   = "1_ipv4";
    const char* ROOT_1_TABLET_VERSION= "1_tablet_version";

    const char* ROOT_2_PORT =   "2_port";
    const char* ROOT_2_MS_PORT = "2_ms_port";
    const char* ROOT_2_IPV6_1 = "2_ipv6_1";
    const char* ROOT_2_IPV6_2 = "2_ipv6_2";
    const char* ROOT_2_IPV6_3 = "2_ipv6_3";
    const char* ROOT_2_IPV6_4 = "2_ipv6_4";
    const char* ROOT_2_IPV4   = "2_ipv4";
    const char* ROOT_2_TABLET_VERSION= "2_tablet_version";

    const char* ROOT_3_PORT =   "3_port";
    const char* ROOT_3_MS_PORT = "3_ms_port";
    const char* ROOT_3_IPV6_1 = "3_ipv6_1";
    const char* ROOT_3_IPV6_2 = "3_ipv6_2";
    const char* ROOT_3_IPV6_3 = "3_ipv6_3";
    const char* ROOT_3_IPV6_4 = "3_ipv6_4";
    const char* ROOT_3_IPV4   = "3_ipv4";
    const char* ROOT_3_TABLET_VERSION= "3_tablet_version";

    const char* ROOT_OCCUPY_SIZE = "occupy_size";
    const char* ROOT_RECORD_COUNT = "record_count";

    char max_row_key[oceanbase::common::OB_MAX_ROW_KEY_LENGTH];

    const int NO_REPORTING = 0;
    const int START_REPORTING = 1;
    const int WAIT_REPORT = 3;
  }
}

ObBootState::ObBootState():state_(OB_BOOT_NO_META)
{
}

bool ObBootState::is_boot_ok() const
{
  return (state_ == OB_BOOT_OK);
}

void ObBootState::set_boot_ok()
{
  ObSpinLockGuard guard(lock_);
  state_ = OB_BOOT_OK;
}

ObBootState::State ObBootState::get_boot_state() const
{
  return state_;
}

void ObBootState::set_boot_strap()
{
  ObSpinLockGuard guard(lock_);
  state_ = OB_BOOT_STRAP;
}

void ObBootState::set_boot_recover()
{
  ObSpinLockGuard guard(lock_);
  state_ = OB_BOOT_RECOVER;
}

bool ObBootState::can_boot_strap() const
{
  ObSpinLockGuard guard(lock_);
  return OB_BOOT_NO_META == state_;
}

bool ObBootState::can_boot_recover() const
{
  ObSpinLockGuard guard(lock_);
  return OB_BOOT_NO_META == state_;
}

const char* ObBootState::to_cstring() const
{
  ObSpinLockGuard guard(lock_);
  const char* ret = "UNKNOWN";
  switch(state_)
  {
    case OB_BOOT_NO_META:
      ret = "BOOT_NO_META";
      break;
    case OB_BOOT_OK:
      ret = "BOOT_OK";
      break;
    case OB_BOOT_STRAP:
      ret = "BOOT_STRAP";
      break;
    case OB_BOOT_RECOVER:
      ret = "BOOT_RECOVER";
      break;
    default:
      break;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
const char* ObRootServer2::ROOT_TABLE_EXT = "rtable";
const char* ObRootServer2::CHUNKSERVER_LIST_EXT = "clist";
const char* ObRootServer2::LOAD_DATA_EXT = "load_data";
const char* ObRootServer2::SCHEMA_FILE_NAME = "./schema_bypass.ini";
const char* ObRootServer2::TMP_SCHEMA_LOCATION = "./tmp_schema.ini";

ObRootServer2::ObRootServer2(ObRootServerConfig &config)
  : config_(config),
    worker_(NULL),
    log_worker_(NULL),
    root_table_(NULL), tablet_manager_(NULL),
    have_inited_(false),
    first_cs_had_registed_(false), receive_stop_(false),
    last_frozen_mem_version_(OB_INVALID_VERSION), last_frozen_time_(1),
    time_stamp_changing_(-1),
    ups_manager_(NULL), ups_heartbeat_thread_(NULL),
    ups_check_thread_(NULL),
    balancer_(NULL), balancer_thread_(NULL),
    restart_server_(NULL),
    schema_timestamp_(0),
    privilege_timestamp_(0),
    schema_service_(NULL),
    schema_service_scan_helper_(NULL),
    schema_service_ms_provider_(NULL),
    schema_service_ups_provider_(NULL),
    first_meta_(NULL),
    rt_service_(NULL),
    merge_checker_(this),
    heart_beat_checker_(this),
    ms_provider_(server_manager_),
    local_schema_manager_(NULL),
    schema_manager_for_cache_(NULL)
{
}

ObRootServer2::~ObRootServer2()
{
  if (local_schema_manager_)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, local_schema_manager_);
    local_schema_manager_ = NULL;
  }
  if (schema_manager_for_cache_)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_manager_for_cache_);
    schema_manager_for_cache_ = NULL;
  }
  if (root_table_)
  {
    OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_);
    root_table_ = NULL;
  }
  if (tablet_manager_)
  {
    OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager_);
    tablet_manager_ = NULL;
  }
  if (NULL != ups_manager_)
  {
    delete ups_manager_;
    ups_manager_ = NULL;
  }
  if (NULL != ups_heartbeat_thread_)
  {
    delete ups_heartbeat_thread_;
    ups_heartbeat_thread_ = NULL;
  }
  if (NULL != ups_check_thread_)
  {
    delete ups_check_thread_;
    ups_check_thread_ = NULL;
  }
  if (NULL != balancer_thread_)
  {
    delete balancer_thread_;
    balancer_thread_ = NULL;
  }
  if (NULL != balancer_)
  {
    delete balancer_;
    balancer_ = NULL;
  }
  if (NULL != first_meta_)
  {
    delete first_meta_;
    first_meta_ = NULL;
  }
  if (NULL != schema_service_)
  {
    delete schema_service_;
    schema_service_ = NULL;
  }
  if (NULL != rt_service_)
  {
    delete rt_service_;
    rt_service_ = NULL;
  }
  if (NULL != restart_server_)
  {
    delete restart_server_;
    restart_server_ = NULL;
  }
  have_inited_ = false;
}

const ObChunkServerManager& ObRootServer2::get_server_manager() const
{
  return server_manager_;
}

ObRootAsyncTaskQueue * ObRootServer2::get_task_queue()
{
  return &seq_task_queue_;
}

ObFirstTabletEntryMeta* ObRootServer2::get_first_meta()
{
  return first_meta_;
}

ObConfigManager* ObRootServer2::get_config_mgr()
{
  ObConfigManager *config_mgr_ = NULL;
  if (NULL == worker_)
  {
    TBSYS_LOG(ERROR, "worker is NULL!");
  }
  else
  {
    config_mgr_ = &worker_->get_config_mgr();
  }
  return config_mgr_;
}

bool ObRootServer2::init(const int64_t now, ObRootWorker* worker)
{
  bool res = false;
  UNUSED(now);
  if (NULL == worker)
  {
    TBSYS_LOG(ERROR, "worker=NULL");
  }
  else if (have_inited_)
  {
    TBSYS_LOG(ERROR, "already inited");
  }
  else
  {
    time(&start_time_);
    worker_ = worker;
    log_worker_ = worker_->get_log_manager()->get_log_worker();
    obi_role_.set_role(ObiRole::INIT); // init as init instance
    worker_->get_config_mgr().got_version(0);
    timer_.init();
    operation_helper_.init(this, &config_, &worker_->get_rpc_stub(), &server_manager_);
    schema_timestamp_ = 0;
    TBSYS_LOG(INFO, "init schema_version=%ld", schema_timestamp_);
    res = false;
    tbsys::CConfig config;
    int err = OB_SUCCESS;
    if (NULL == (schema_manager_for_cache_ = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, 0)))
    {
      TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
    }
    if (NULL == (local_schema_manager_ = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, now)))
    {
      TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
    }
    else if (!local_schema_manager_->parse_from_file(config_.schema_filename, config))
    {
      TBSYS_LOG(ERROR, "parse schema error chema file is %s ", config_.schema_filename.str());
      res = false;
    }
    else if (NULL == (tablet_manager_ = OB_NEW(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER)))
    {
      TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
    }
    else if (NULL == (root_table_ = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, tablet_manager_)))
    {
      TBSYS_LOG(ERROR, "new ObRootTable2 error");
    }
    else if (NULL == (ups_manager_
                      = new(std::nothrow) ObUpsManager(worker_->get_rpc_stub(),
                                                       worker_, config_.network_timeout,
                                                       config_.ups_lease_time,
                                                       config_.ups_lease_reserved_time,
                                                       config_.ups_waiting_register_time,
                                                       obi_role_, schema_timestamp_,
                                                       worker_->get_config_mgr().get_version())))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (ups_heartbeat_thread_ = new(std::nothrow) ObUpsHeartbeatRunnable(*ups_manager_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (ups_check_thread_ = new(std::nothrow) ObUpsCheckRunnable(*ups_manager_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (first_meta_
                      = new(std::nothrow) ObFirstTabletEntryMeta((int32_t)config_.read_queue_size,
                                                                 get_file_path(config_.rs_data_dir,
                                                                               config_.first_meta_filename))))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (schema_service_ms_provider_ = new(std::nothrow) ObSchemaServiceMsProvider(server_manager_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (schema_service_ups_provider_ = new(std::nothrow) ObSchemaServiceUpsProvider(*ups_manager_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (schema_service_scan_helper_ = new(std::nothrow) ObScanHelperImpl()))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (schema_service_ = new(std::nothrow) ObSchemaServiceImpl()))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (OB_SUCCESS != (err = schema_service_->init(schema_service_scan_helper_, false)))
    {
      TBSYS_LOG(WARN, "failed to init schema service, err=%d", err);
    }
    else if (NULL == (rt_service_ = new(std::nothrow) ObRootTableService(*first_meta_, *schema_service_)))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (balancer_ = new(std::nothrow) ObRootBalancer()))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (balancer_thread_ = new(std::nothrow) ObRootBalancerRunnable(
        config_, *balancer_, *worker_->get_role_manager())))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else if (NULL == (restart_server_ = new(std::nothrow) ObRestartServer()))
    {
      TBSYS_LOG(ERROR, "no memory");
    }
    else
    {
      // task queue init max task count
      seq_task_queue_.init(10 * 1024);
      ups_manager_->set_async_queue(&seq_task_queue_);
      ddl_tool_.init(this, schema_service_);

      TBSYS_LOG(INFO, "new root table created, root_table_=%p", root_table_);
      TBSYS_LOG(INFO, "tablet_manager=%p", tablet_manager_);
      TBSYS_LOG(INFO, "first_meta=%p", first_meta_);
      TBSYS_LOG(INFO, "schema_service=%p", schema_service_);
      TBSYS_LOG(INFO, "rt_service=%p", rt_service_);
      TBSYS_LOG(INFO, "balancer=%p", balancer_);
      TBSYS_LOG(INFO, "balancer_thread=%p", balancer_thread_);
      TBSYS_LOG(INFO, "ups_manager=%p", ups_manager_);
      TBSYS_LOG(INFO, "ups_heartbeat_thread=%p", ups_heartbeat_thread_);
      TBSYS_LOG(INFO, "ups_check_thread=%p", ups_check_thread_);
      ups_manager_->set_ups_config((int32_t)config_.read_master_master_ups_percent,
          (int32_t)config_.read_slave_master_ups_percent);
      config_.get_root_server(my_addr_);
      // init client helper
      client_helper_.init(worker_->get_client_manager(),
          worker_->get_thread_buffer(), worker_->get_rs_master(),
          worker_->get_network_timeout());
      // init components for balancer
      balancer_->set_config(&config_);
      balancer_->set_root_server(this);
      balancer_->set_root_table(root_table_);
      balancer_->set_server_manager(&server_manager_);
      balancer_->set_root_table_lock(&root_table_rwlock_);
      balancer_->set_server_manager_lock(&server_manager_rwlock_);
      balancer_->set_rpc_stub(&worker_->get_rpc_stub());
      balancer_->set_log_worker(log_worker_);
      balancer_->set_role_mgr(worker_->get_role_manager());
      balancer_->set_ups_manager(ups_manager_);
      balancer_->set_restart_server(restart_server_);
      balancer_->set_boot_state(&boot_state_);
      balancer_->set_ddl_operation_mutex(&mutex_lock_);
      // init schema_service_scan_helper
      schema_service_scan_helper_->set_ms_provider(schema_service_ms_provider_);
      schema_service_scan_helper_->set_ups_provider(schema_service_ups_provider_);
      schema_service_scan_helper_->set_rpc_stub(&worker_->get_rpc_stub());
      schema_service_scan_helper_->set_scan_timeout(config_.inner_table_network_timeout);
      schema_service_scan_helper_->set_mutate_timeout(config_.inner_table_network_timeout);
      schema_service_scan_helper_->set_scan_retry_times(3); // @todo config
      // init restart_server_
      restart_server_->set_root_config(&config_);
      restart_server_->set_server_manager(&server_manager_);
      TBSYS_LOG(INFO, "root_table_ address 0x%p", root_table_);
      restart_server_->set_root_table2(root_table_);
      restart_server_->set_root_rpc_stub(&worker_->get_rpc_stub());
      restart_server_->set_root_log_worker(log_worker_);
      restart_server_->set_root_table_build_mutex(&root_table_build_mutex_);
      restart_server_->set_server_manager_rwlock(&server_manager_rwlock_);
      restart_server_->set_root_table_rwlock(&root_table_rwlock_);
      ms_provider_.init(config_, worker_->get_rpc_stub());
      // init root trigger
      root_trigger_.set_ms_provider(&ms_provider_);
      root_trigger_.set_rpc_stub(&worker_->get_general_rpc_stub());
      res = true;
      have_inited_ = res;
    }
  }
  return res;
}

void ObRootServer2::after_switch_to_master()
{
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  worker_->get_config_mgr().got_version(timestamp);
  privilege_timestamp_ = timestamp;
  //report is needed, maybe slave was doing clean roottable before switching
  request_cs_report_tablet();
  TBSYS_LOG(INFO, "[NOTICE] start service now");
}

int ObRootServer2::init_boot_state()
{
  int ret = OB_SUCCESS;
  ret = first_meta_->init();
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "failed to init the first meta, err=%d", ret);
  }
  if (OB_INIT_TWICE == ret || OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "rootserver alreay bootstrap, set boot ok.");
    boot_state_.set_boot_ok();
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObRootServer2::start_master_rootserver()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "start master rootserver");

  if (OB_SUCCESS == ret)
  {
    request_cs_report_tablet();
    // after restart master root server refresh the schema
    static const int64_t RETRY_TIMES = 3;
    for (int64_t i = 0; i < RETRY_TIMES; ++i)
    {
      ret = worker_->schedule_after_restart_task(1000000, false);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "fail to schedule after_restart task, ret=%d", ret);
      }
      else
      {
        break;
      }
    }
  }
  privilege_timestamp_ = tbsys::CTimeUtil::getTime();
  TBSYS_LOG(INFO, "set privilege timestamp to %ld", privilege_timestamp_);
  return ret;
}

void ObRootServer2::start_threads()
{
  balancer_thread_->start();
  heart_beat_checker_.start();
  ups_heartbeat_thread_->start();
  ups_check_thread_->start();
}

void ObRootServer2::start_merge_check()
{
  merge_checker_.start();
  TBSYS_LOG(INFO, "start merge check");
}

const ObRootServerConfig& ObRootServer2::get_config() const
{
  return config_;
}

void ObRootServer2::stop_threads()
{
  receive_stop_ = true;
  heart_beat_checker_.stop();
  heart_beat_checker_.wait();
  TBSYS_LOG(INFO, "cs heartbeat thread stopped");
  if (NULL != balancer_thread_)
  {
    balancer_thread_->stop();
    balancer_thread_->wakeup();
    balancer_thread_->wait();
    TBSYS_LOG(INFO, "balance worker thread stopped");
  }
  merge_checker_.stop();
  merge_checker_.wait();
  TBSYS_LOG(INFO, "merge checker thread stopped");
  if (NULL != ups_heartbeat_thread_)
  {
    ups_heartbeat_thread_->stop();
    ups_heartbeat_thread_->wait();
    TBSYS_LOG(INFO, "ups heartbeat thread stopped");
  }
  if (NULL != ups_check_thread_)
  {
    ups_check_thread_->stop();
    ups_check_thread_->wait();
    TBSYS_LOG(INFO, "ups check thread stopped");
  }
}

int ObRootServer2::boot_recover()
{
  int ret = OB_SUCCESS;
  if (!boot_state_.can_boot_recover())
  {
    TBSYS_LOG(WARN, "boot strap state cann't be recover. boot_state=%s", boot_state_.to_cstring());
    ret = OB_ERROR;
  }
  else
  {
    boot_state_.set_boot_recover();
    ObBootstrap bootstrap(*this);
    while (!receive_stop_)
    {
      if (OB_SUCCESS != (ret = request_cs_report_tablet()))
      {
        TBSYS_LOG(WARN, "fail to request cs to report tablet. err=%d", ret);
      }
      else
      {
        usleep(1 * 1000 * 1000);
        //get tablet info from root_table
        ObNewRange key_range;
        key_range.table_id_ = OB_FIRST_TABLET_ENTRY_TID;
        key_range.set_whole_range();
        ObRootTable2::const_iterator first;
        ObRootTable2::const_iterator last;
        ret = root_table_->find_range(key_range, first, last);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "cann't find first_tablet_entry's tablet. ret=%d", ret);
        }
        else
        {
          common::ObTabletInfo *tablet_info = root_table_->get_tablet_info(first);
          if (first == last
              && tablet_info != NULL
              && key_range.equal(tablet_info->range_))
          {
            //write meta_file
            ObArray<ObServer> create_cs;
            for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
            {
              int64_t server_index = first->server_info_indexes_[i];
              if (OB_INVALID_INDEX != server_index)
              {
                ObServer cs = server_manager_.get_cs(static_cast<int32_t>(server_index));
                create_cs.push_back(cs);
              }
              else
              {
                break;
              }
            }
            if (OB_SUCCESS != (bootstrap.init_meta_file(create_cs)))
            {
              TBSYS_LOG(WARN, "fail to recover first_meta.bin. ret=%d", ret);
            }
            else
            {
              TBSYS_LOG(INFO, "recover first_meta.bin success.");
              break;
            }
          }
          else if (config_.is_ups_flow_control)
          {
            ups_manager_->set_ups_config((int32_t)config_.read_master_master_ups_percent,
                                         (int32_t)config_.read_slave_master_ups_percent);
          }
          else
          {
            TBSYS_LOG(WARN, "cann't find first_tablet_entry's tablet. ret=%d", ret);
          }
        } //end else
      }//end
    }//end while
  }//end else
  return ret;
}

int ObRootServer2::renew_core_schema(void)
{
  int ret = OB_ERROR;
  const static int64_t retry_times = 3;
  for (int64_t i = 0; (ret != OB_SUCCESS) && (i < retry_times); ++i)
  {
    if (OB_SUCCESS != (ret = notify_switch_schema(true)))
    {
      TBSYS_LOG(WARN, "fail to notify_switch_schema, retry_time=%ld, ret=%d", i, ret);
      sleep(1);
    }
  }
  return ret;
}

int ObRootServer2::renew_user_schema(int64_t & count)
{
  int ret = OB_ERROR;
  const static int64_t retry_times = 3;
  for (int64_t i = 0; (ret != OB_SUCCESS) && (i < retry_times); ++i)
  {
    if (OB_SUCCESS != (ret = refresh_new_schema(count)))
    {
      TBSYS_LOG(WARN, "fail to refresh schema, retry_time=%ld, ret=%d", i, ret);
      sleep(1);
    }
    else
    {
      TBSYS_LOG(INFO, "refresh schema success. count=%ld", count);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = OB_ERROR;
    for (int64_t i = 0; (ret != OB_SUCCESS) && (i < retry_times); ++i)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      {
        TBSYS_LOG(WARN, "fail to notify_switch_schema, retry_time=%ld, ret=%d", i, ret);
        sleep(1);
      }
      else
      {
        TBSYS_LOG(INFO, "notify switch schema success.");
      }
    }
  }
  return ret;
}

int ObRootServer2::boot_strap(void)
{
  int ret = OB_ERROR;
  TBSYS_LOG(INFO, "ObRootServer2::bootstrap() start");
  ObBootstrap bootstrap(*this);
  if (!boot_state_.can_boot_strap())
  {
    TBSYS_LOG(WARN, "cannot bootstrap twice, boot_state=%s", boot_state_.to_cstring());
    ret = OB_INIT_TWICE;
  }
  else
  {
    boot_state_.set_boot_strap();
    bootstrap.set_log_worker(log_worker_);
    int64_t mem_version = -1;
    // step 1. wait for master ups election ok
    while (!receive_stop_)
    {
      ret = fetch_mem_version(mem_version);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get mem frozen version failed");
        sleep(2);
      }
      else
      {
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = do_bootstrap(bootstrap)))
      {
        TBSYS_LOG(ERROR, "bootstrap failed! ret: [%d]", ret);
      }
    }
  }

  TBSYS_LOG(INFO, "ObRootServer2::bootstrap() end");
  return ret;
}

int ObRootServer2::do_bootstrap(ObBootstrap & bootstrap)
{
  int ret = OB_SUCCESS;
  // step 2. create core tables
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = bootstrap.bootstrap_core_tables()))
    {
      TBSYS_LOG(ERROR, "bootstrap core tables error, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "bootstrap create core tables succ");
      ret = renew_core_schema();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "renew schema error after core sys tables, err = %d", ret);
      }
    }
  }
  // step 3. create sys tables
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = bootstrap.bootstrap_sys_tables()))
    {
      TBSYS_LOG(ERROR, "bootstrap create other sys tables error, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "bootstrap create other sys tables succ");
    }
  }
  // step 4. create schema.ini tables
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = bootstrap.bootstrap_ini_tables()))
    {
      TBSYS_LOG(ERROR, "bootstrap create schmea.ini tables error, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "create schema.ini tables ok, all tables created");
    }
  }
  // step 5. renew schema manager and notify the servers
  if (OB_SUCCESS == ret)
  {
    int64_t table_count = 0;
    ret = renew_user_schema(table_count);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "renew schema error after create sys tables, err = %d", ret);
    }
    else if (table_count != bootstrap.get_table_count())
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(ERROR, "check renew schema table count failed:right[%ld], cur[%ld]",
                bootstrap.get_table_count(), table_count);
    }
    else
    {
      boot_state_.set_boot_ok();
      TBSYS_LOG(INFO, "bootstrap set boot ok, all initial tables created");
    }
  }
  // step 6. for schema ini tables report
  if (OB_SUCCESS == ret)
  {
    static const int retry_times = 3;
    ret = OB_ERROR;
    int i = 0;
    while (OB_SUCCESS != ret && !receive_stop_)
    {
      if (OB_SUCCESS != (ret = request_cs_report_tablet()))
      {
        TBSYS_LOG(ERROR, "fail to request cs report tablet. retry_time=%d, ret=%d", i, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "let chunkservers report tablets");
      }
      i ++;
      if (i >= retry_times) break;
    }
  }
  // step 7. init sys table content
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = after_boot_strap(bootstrap)))
    {
      TBSYS_LOG(ERROR, "execute after boot strap fail, ret: [%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "bootstrap successfully!");
    }
  }
  /* check if async task queue empty */
  if (OB_SUCCESS == ret)
  {
    ret = OB_NOT_INIT;
    for (int i = 0; i < 100; i++) /* 10s */
    {
      if (async_task_queue_empty())
      {
        ret = OB_SUCCESS;
        break;
      }
      usleep(100000);
      TBSYS_LOG(DEBUG, "waiting async task queue empty");
    }
  }
  return ret;
}

int ObRootServer2::after_boot_strap(ObBootstrap & bootstrap)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = bootstrap.init_system_table()))
  {
    TBSYS_LOG(ERROR, "fail to init all_sys_param and all_sys_stat table. err=%d", ret);
  }
  else
  {
    // wait fetch ups list
    usleep(1000 * 1000);
    // force push the mem version to mergeserves
    int err = force_heartbeat_all_servers();
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "force heartbeat all servers failed:err[%d]", err);
    }
  }
  const ObiRole &role = get_obi_role();
  if ((OB_SUCCESS == ret) && (role.get_role() == ObiRole::MASTER))
  {
    ret = ObRootTriggerUtil::slave_boot_strap(root_trigger_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "write trigger table for slave cluster bootstrap failed, ret:%d", ret);
    }
    else
    {
      //TBSYS_LOG(INFO, "[TRIGGER] notify slave to do boot strap succ");
      TBSYS_LOG(INFO, "write to all_sys/all_param succ");
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "[NOTICE] start service now:role[%s]", role.get_role_str());
  }
  return ret;
}

int ObRootServer2::slave_boot_strap()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "ObRootServer2::bootstrap() start");
  ObBootstrap bootstrap(*this);
  if (!boot_state_.can_boot_strap())
  {
    TBSYS_LOG(WARN, "cannot bootstrap twice, boot_state=%s", boot_state_.to_cstring());
    ret = OB_INIT_TWICE;
  }
  else
  {
    boot_state_.set_boot_strap();
    bootstrap.set_log_worker(log_worker_);
    if (OB_SUCCESS != (ret = do_bootstrap(bootstrap)))
    {
      TBSYS_LOG(ERROR, "Slave bootstrap failed! ret: [%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "slave cluster bootstrap succ");
    }
  }
  return ret;
}

int ObRootServer2::after_restart()
{
  TBSYS_LOG(INFO, "fresh schema after restart.");
  int64_t table_count = 0;
  int ret = renew_user_schema(table_count);
  int err = ret;
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to renew user schema try again. err=%d", ret);
    static const int64_t RETRY_TIMES = 3;
    for (int64_t i = 0; i < RETRY_TIMES; ++i)
    {
      ret = worker_->schedule_after_restart_task(1000000, false);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "fail to schedule after restart task.ret=%d", ret);
        sleep(1);
      }
      else
      {
        ret = err;
        break;
      }
    }
  }
  else
  {
    // renew schema version and config version succ
    // start service right now
    worker_->get_config_mgr().got_version(tbsys::CTimeUtil::getTime());
    TBSYS_LOG(INFO, "after restart renew schema succ:count[%ld]", table_count);
    TBSYS_LOG(INFO, "[NOTICE] start service now");

    //fix bug: update rootserver info to __all_server
    ObServer rs;
    config_.get_root_server(rs);
    char server_version[OB_SERVER_VERSION_LENGTH] = "";
    get_package_and_svn(server_version, sizeof(server_version));
    commit_task(SERVER_ONLINE, OB_ROOTSERVER, rs, 0, server_version);
  }
  return ret;
}

int ObRootServer2::start_notify_switch_schema()
{
  int ret = OB_SUCCESS;
  ObServer master_rs;
  ObServer slave_rs[OB_MAX_CLUSTER_COUNT];
  int64_t slave_count = OB_MAX_CLUSTER_COUNT;
  ObRootRpcStub& rpc_stub = get_rpc_stub();
  ObServer ms_server;
  master_rs.set_ipv4_addr(config_.master_root_server_ip, (int32_t)config_.master_root_server_port);

  TBSYS_LOG(INFO, "start_notify_switch_schema");

  if (!is_master() || obi_role_.get_role() != ObiRole::MASTER)
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(WARN, "this rs is not master of marster cluster, cannot start_notify_switch_schema");
  }
  else if (OB_SUCCESS != (ret = get_ms(ms_server)))
  {
    TBSYS_LOG(WARN, "failed to get serving ms, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_stub.fetch_slave_cluster_list(
          ms_server, master_rs, slave_rs, slave_count, config_.inner_table_network_timeout)))
  {
    TBSYS_LOG(ERROR, "failed to get slave cluster rs list, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    const int64_t timeout = 30 * 1000L * 1000L; // 30s
    int tmp_ret = rpc_stub.notify_switch_schema(master_rs, timeout);
    if (OB_SUCCESS == tmp_ret)
    {
      TBSYS_LOG(INFO, "succeed  to notify_switch_schema, master_rs=%s", to_cstring(master_rs));
    }
    else
    {
      ret = tmp_ret;
      TBSYS_LOG(WARN, "failed to notify_switch_schema, master_rs=%s,ret=%d",
          to_cstring(master_rs), tmp_ret);
    }

    for (int64_t i = 0; i < slave_count; ++i)
    {
      tmp_ret = rpc_stub.notify_switch_schema(slave_rs[i], timeout);
      if (OB_SUCCESS == tmp_ret)
      {
        TBSYS_LOG(INFO, "succeed to notify_switch_schema, slave_rs[%ld]=%s", i, to_cstring(slave_rs[i]));
      }
      else
      {
        ret = tmp_ret;
        TBSYS_LOG(WARN, "failed to notify_switch_schema, slave_rs[%ld]=%s, ret=%d", i, to_cstring(slave_rs[i]), ret);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "succeed to notify_switch_schema");
  }
  else
  {
    TBSYS_LOG(ERROR, "failed to notify_switch_schema, ret=%d",ret);
  }

  return ret;
}

int ObRootServer2::notify_switch_schema(bool only_core_tables, bool force_update)
{
  int ret = OB_SUCCESS;
  ObSchemaManagerV2 *out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (OB_SUCCESS != (ret = get_schema(force_update, only_core_tables, *out_schema)))
  {
    TBSYS_LOG(WARN, "fail to get schema. err = %d", ret);
  }
  else
  {
    if (obi_role_.get_role() == ObiRole::MASTER)
    {
      ObUps ups_master;
      if (OB_SUCCESS != (ret = ups_manager_->get_ups_master(ups_master)))
      {
        TBSYS_LOG(WARN, "fail to get ups master. err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = worker_->get_rpc_stub().switch_schema(ups_master.addr_,
              *out_schema, config_.network_timeout)))
      {
        TBSYS_LOG(ERROR, "fail to switch schema to ups. ups: [%s] err=%d",
                  to_cstring(ups_master.addr_), ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify ups to switch schema succ! version = %ld only_core_tables = %s",
            out_schema->get_version(), (only_core_tables == true) ? "YES" : "NO");
      }
    }
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = force_sync_schema_all_servers(*out_schema)))
      {
        TBSYS_LOG(WARN, "fail to sync schema to ms and cs:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify cs/ms to switch schema succ! version = %ld only_core_tables = %s",
            out_schema->get_version(), (only_core_tables == true) ? "YES" : "NO");
      }
    }
  }
  if (NULL != out_schema)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
  }
  return ret;
}

ObBootState* ObRootServer2::get_boot()
{
  return &boot_state_;
}
ObBootState::State ObRootServer2::get_boot_state()const
{
  return boot_state_.get_boot_state();
}

ObRootRpcStub& ObRootServer2::get_rpc_stub()
{
  return worker_->get_rpc_stub();
}

ObSchemaManagerV2* ObRootServer2::get_ini_schema() const
{
  return local_schema_manager_;
}

int ObRootServer2::get_schema(const bool force_update, bool only_core_tables, ObSchemaManagerV2& out_schema)
{
  int ret = OB_SUCCESS;
  bool cache_found = false;
  if (config_.enable_cache_schema && !only_core_tables)
  {
    if (!force_update)
    {
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      out_schema = *schema_manager_for_cache_;
      cache_found = true;
    }
  }
  if (false == cache_found)
  {
    TBSYS_LOG(INFO, "get schema begin, force_update=%c only_core_tables=%c "
            "curr_version=%ld cache_found=%c",
            force_update?'Y':'N', only_core_tables?'Y':'N',
            schema_timestamp_, cache_found?'Y':'N');

    ObTableIdNameIterator tables_id_name;
    ObTableIdName* tid_name = NULL;
    int32_t table_count = 0;
    ObArray<TableSchema> table_schema_array;
    if (NULL == schema_service_)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "schema_service_ not init");
    }
    else
    {
      schema_service_->init(schema_service_scan_helper_, only_core_tables);
      if (OB_SUCCESS != (ret = tables_id_name.init(schema_service_scan_helper_, only_core_tables)))
      {
        TBSYS_LOG(WARN, "failed to init iterator, err=%d", ret);
      }
      else
      {
        while (OB_SUCCESS == ret
               && OB_SUCCESS == (ret = tables_id_name.next()))
        {
          TableSchema table_schema;
          if (OB_SUCCESS != (ret = tables_id_name.get(&tid_name)))
          {
            TBSYS_LOG(WARN, "failed to get next name, err=%d", ret);
          }
          else
          {
            if (OB_SUCCESS != (ret = schema_service_->get_table_schema(tid_name->table_name_, table_schema)))
            {
              TBSYS_LOG(WARN, "failed to get table schema, err=%d, table_name=%.*s", ret,
                  tid_name->table_name_.length(), tid_name->table_name_.ptr());
              ret = OB_INNER_STAT_ERROR;
            }
            else
            {
              // @todo DEBUG
              table_schema_array.push_back(table_schema);
              TBSYS_LOG(DEBUG, "get table schema add into shemaManager, tname=%.*s",
                  tid_name->table_name_.length(), tid_name->table_name_.ptr());
              ++table_count;
            }
          }
        } // end while
        if (OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN,  "failed to get all table schema, only_core_tables=%s, ret=%d",
              only_core_tables ? "true" : "false", ret);
        }

        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = out_schema.add_new_table_schema(table_schema_array)))
          {
            TBSYS_LOG(WARN, "failed to add table schema into the schema manager, err=%d", ret);
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (!only_core_tables)
          {
            tbsys::CWLockGuard guard(schema_manager_rwlock_);
            schema_timestamp_ = tbsys::CTimeUtil::getTime();
            out_schema.set_version(schema_timestamp_);
            if (OB_SUCCESS != (ret = out_schema.sort_column()))
            {
              TBSYS_LOG(WARN, "failed to sort columns in schema manager, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = switch_schema_manager(out_schema)))
            {
              TBSYS_LOG(WARN, "fail to switch schema. ret=%d", ret);
            }
            else
            {
              TBSYS_LOG(INFO, "get schema succ. table count=%d, version[%ld]", table_count, schema_timestamp_);
            }
          }
          else
          {
            out_schema.set_version(CORE_SCHEMA_VERSION);
          }
        }
      }
    }
  }
  //
  if (ret == OB_SUCCESS)
  {
    if (out_schema.get_table_count() <= 0)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(WARN, "check schema table count less than 3:core[%d], version[%ld], count[%ld]",
        only_core_tables, out_schema.get_version(), out_schema.get_table_count());
    }
  }
  return ret;
}

int64_t ObRootServer2::get_last_frozen_version() const
{
  return last_frozen_mem_version_;
}

int ObRootServer2::switch_schema_manager(const ObSchemaManagerV2 & schema_manager)
{
  int ret = OB_SUCCESS;
  if (NULL == schema_manager_for_cache_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid argument. schema_manager_for_cache_=%p", schema_manager_for_cache_);
  }
  else if (boot_state_.is_boot_ok() && schema_manager.get_table_count() <= 3)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(INFO, "schema verison:%ld", schema_manager.get_version());
    schema_manager.print_info();
    TBSYS_LOG(ERROR, "check new schema table count failed:count[%ld]", schema_manager.get_table_count());
  }
  else
  {
    *schema_manager_for_cache_ = schema_manager;
  }
  return ret;
}

int ObRootServer2::get_table_id_name(ObTableIdNameIterator *table_id_name, bool& only_core_tables)
{
  int ret = OB_SUCCESS;
  ObRootMsProvider ms_provider(server_manager_);
  ms_provider.init(config_, worker_->get_rpc_stub());
  ObUps ups_master;
  ups_manager_->get_ups_master(ups_master);
  ObRootUpsProvider ups_provider(ups_master.addr_);
  ObScanHelperImpl scan_helper;
  scan_helper.set_ms_provider(&ms_provider);
  scan_helper.set_rpc_stub(&worker_->get_rpc_stub());
  scan_helper.set_ups_provider(&ups_provider);
  scan_helper.set_scan_timeout(config_.inner_table_network_timeout);
  scan_helper.set_mutate_timeout(config_.inner_table_network_timeout);
  only_core_tables = false;
  if (ObBootState::OB_BOOT_OK != boot_state_.get_boot_state())
  {
    TBSYS_LOG(INFO, "rs is not bootstarp. fetch core_schema only");
    only_core_tables = true;
  }
  if (OB_SUCCESS != (ret = table_id_name->init(&scan_helper, only_core_tables)))
  {
    TBSYS_LOG(WARN, "failed to init iterator, err=%d", ret);
  }
  return ret;
}

int ObRootServer2::get_table_schema(const uint64_t table_id, TableSchema &table_schema)
{
  ObString table_name;
  int ret = schema_service_->get_table_name(table_id, table_name);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get table_name. err=%d, table_id=%lu", ret, table_id);
  }
  else
  {
    ret = schema_service_->get_table_schema(table_name, table_schema);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get table schema. table_name=%s, ret=%d", table_name.ptr(), ret);
    }
  }
  return ret;
}

int64_t ObRootServer2::get_schema_version() const
{
  return schema_timestamp_;
}

int64_t ObRootServer2::get_privilege_version() const
{
  return privilege_timestamp_;
}

void ObRootServer2::set_privilege_version(const int64_t privilege_version)
{
  privilege_timestamp_ = privilege_version;
}

void ObRootServer2::set_daily_merge_tablet_error(const char* err_msg, const int64_t length)
{
 state_.set_daily_merge_error(err_msg, length);
}

void ObRootServer2::clean_daily_merge_tablet_error()
{
  state_.clean_daily_merge_error();
}

bool ObRootServer2::is_daily_merge_tablet_error()const
{
  return state_.is_daily_merge_tablet_error();
}
char* ObRootServer2::get_daily_merge_error_msg()
{
  return state_.get_error_msg();
}
int64_t ObRootServer2::get_config_version() const
{
  return worker_->get_config_mgr().get_version();
}

int ObRootServer2::get_max_tablet_version(int64_t &version) const
{
  int err = OB_SUCCESS;
  version = -1;
  if (NULL != root_table_)
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    version = root_table_->get_max_tablet_version();
    TBSYS_LOG(INFO, "root table max tablet version =%ld", version);
  }
  return err;
}

void ObRootServer2::print_alive_server() const
{
  TBSYS_LOG(INFO, "start dump server info");
  //tbsys::CRLockGuard guard(server_manager_rwlock_); // do not need this lock
  ObChunkServerManager::const_iterator it = server_manager_.begin();
  int32_t index = 0;
  for (; it != server_manager_.end(); ++it, ++index)
  {
    if (!it->is_lms())
    {
      it->dump(index);
    }
  }
  TBSYS_LOG(INFO, "dump server info complete");
  return;
}

int ObRootServer2::get_cs_info(ObChunkServerManager* out_server_manager) const
{
  int ret = OB_SUCCESS;
  tbsys::CRLockGuard guard(server_manager_rwlock_);
  if (out_server_manager != NULL)
  {
    *out_server_manager = server_manager_;
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}

void ObRootServer2::reset_hb_time()
{
  TBSYS_LOG(INFO, "reset heartbeat time");
  tbsys::CRLockGuard guard(server_manager_rwlock_);
  ObChunkServerManager::iterator it = server_manager_.begin();
  ObServer server;
  for(; it != server_manager_.end(); ++it)
  {
    server = it->server_;
    if (it->status_ != ObServerStatus::STATUS_DEAD
        && it->port_cs_ > 0)
    {
      server.set_port(it->port_cs_);
      receive_hb(server, it->port_cs_, false, OB_CHUNKSERVER);
    }
    if (it->ms_status_ != ObServerStatus::STATUS_DEAD
        && it->port_ms_ > 0)
    {
      server.set_port(it->port_ms_);
      receive_hb(server, it->port_ms_sql_, it->lms_, OB_MERGESERVER);
    }
  }
  TBSYS_LOG(INFO, "reset heartbeat time end");
}

void ObRootServer2::dump_root_table() const
{
  tbsys::CRLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "dump root table");
  if (root_table_ != NULL)
  {
    root_table_->dump();
  }
}

bool ObRootServer2::check_root_table(const common::ObServer &expect_cs) const
{
  bool err = false;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  int server_index = get_server_index(expect_cs);
  if (server_index == OB_INVALID_INDEX)
  {
    TBSYS_LOG(INFO, "can not find server's info, just check roottable");
  }
  else
  {
    TBSYS_LOG(INFO, "check roottable without cs=%s in considered.", to_cstring(expect_cs));
  }
  if (root_table_ != NULL)
  {
    err = root_table_->check_lost_data(server_index);
  }
  return err;
}

int ObRootServer2::dump_cs_tablet_info(const common::ObServer & cs, int64_t &tablet_num) const
{
  int err = OB_SUCCESS;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "dump cs[%s]'s tablet info", cs.to_cstring());
  int server_index = get_server_index(cs);
  if (server_index == OB_INVALID_INDEX)
  {
    TBSYS_LOG(WARN, "can not find server's info, server=%s", cs.to_cstring());
    err = OB_ENTRY_NOT_EXIST;
  }
  else if (root_table_ != NULL)
  {
    root_table_->dump_cs_tablet_info(server_index, tablet_num);
  }
  return err;
}

void ObRootServer2::dump_unusual_tablets() const
{
  int32_t num = 0;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "dump unusual tablets");
  if (root_table_ != NULL)
  {
    root_table_->dump_unusual_tablets(last_frozen_mem_version_,
                                      (int32_t)config_.tablet_replicas_num, num);
  }
}

int ObRootServer2::try_create_new_table(int64_t frozen_version, const uint64_t table_id)
{
  int err = OB_SUCCESS;
  bool table_not_exist_in_rt = false;
  TBSYS_LOG(INFO, "create new table start: table_id=%lu", table_id);
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  if (NULL == root_table_)
  {
    TBSYS_LOG(WARN, "root table is not init; wait another second.");
  }
  else
  {
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      if(!root_table_->table_is_exist(table_id))
      {
        TBSYS_LOG(INFO, "table not exist, table_id=%lu", table_id);
        table_not_exist_in_rt = true;
      }
    }
    bool is_exist_in_cs = true;
    if (table_not_exist_in_rt)
    {
      //check cs number
      if (DEFAULT_SAFE_CS_NUMBER >= get_alive_cs_number())
      {
        //check table not exist in chunkserver
        if (OB_SUCCESS != (err = table_exist_in_cs(table_id, is_exist_in_cs)))
        {
          TBSYS_LOG(WARN, "fail to check table. table_id=%lu, err=%d", table_id, err);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "cs number is too litter. alive_cs_number=%ld", get_alive_cs_number());
      }
    }
    if (!is_exist_in_cs)
    {
      ObTabletInfoList tablets;
      if (OB_SUCCESS != (err = split_table_range(frozen_version, table_id, tablets)))
      {
        TBSYS_LOG(WARN, "fail to get tablet info for table[%lu], err=%d", table_id, err);
      }
      else
      {
        err = create_table_tablets(table_id, tablets);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fail to create table[%lu] tablets, err=%d", table_id, err);
        }
      }
    }
  }
  return err;
}

int ObRootServer2::create_table_tablets(const uint64_t table_id, const ObTabletInfoList & list)
{
  int err = OB_SUCCESS;
  int64_t version = -1;
  if (OB_SUCCESS != (err = check_tablets_legality(list)))
  {
    TBSYS_LOG(WARN, "get wrong tablets. err=%d", err);
  }
  else if (OB_SUCCESS != (err = get_max_tablet_version(version)))
  {
    TBSYS_LOG(WARN, "fail get max tablet version. err=%d", err);
  }
  else if (OB_SUCCESS != (err = create_tablet_with_range(version, list)))
  {
    TBSYS_LOG(ERROR, "fail to create tablet. rt_version=%ld, err=%d", version, err);
  }
  else
  {
    TBSYS_LOG(INFO, "create tablet for table[%lu] success.", table_id);
  }
  return err;
}

int64_t ObRootServer2::get_alive_cs_number()
{
  int64_t number = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      number++;
    }
  }
  return number;
}

int ObRootServer2::check_tablets_legality(const common::ObTabletInfoList &tablets)
{
  int err = OB_SUCCESS;
  common::ObTabletInfo *p_tablet_info = NULL;
  if (0 == tablets.get_tablet_size())
  {
    TBSYS_LOG(WARN, "tablets has zero tablet_info. tablets size = %ld", tablets.get_tablet_size());
    err = OB_INVALID_ARGUMENT;
  }
  static char buff[OB_MAX_ROW_KEY_LENGTH * 2];
  if (OB_SUCCESS == err)
  {
    p_tablet_info = tablets.tablet_list.at(0);
    if (NULL == p_tablet_info)
    {
      TBSYS_LOG(WARN, "p_tablet_info should not be NULL");
      err = OB_INVALID_ARGUMENT;
    }
    else if (!p_tablet_info->range_.start_key_.is_min_row())
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "range not start correctly. first tablet start_key=%s",
                to_cstring(p_tablet_info->range_));
    }
  }
  if (OB_SUCCESS == err)
  {
    p_tablet_info = tablets.tablet_list.at(tablets.get_tablet_size() - 1);
    if (NULL == p_tablet_info)
    {
      TBSYS_LOG(WARN, "p_tablet_info should not be NULL");
      err = OB_INVALID_ARGUMENT;
    }
    else if (!p_tablet_info->range_.end_key_.is_max_row())
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "range not end correctly. last tablet range=%s",
                to_cstring(p_tablet_info->range_));
    }
  }
  if (OB_SUCCESS == err)
  {
    for (int64_t i = 1; OB_SUCCESS == err && i < tablets.get_tablet_size(); i++)
    {
      p_tablet_info = tablets.tablet_list.at(i);
      if (NULL == p_tablet_info)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "p_tablet_info should not be NULL");
      }
      else if (!p_tablet_info->range_.is_left_open_right_closed())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "range not legal, should be left open right closed. range=%s", buff);
      }
      else if (p_tablet_info->range_.start_key_ == p_tablet_info->range_.end_key_)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "range not legal, start_key = end_key; range=%s", buff);
      }
      else if (0 != p_tablet_info->range_.start_key_.compare(tablets.tablet_list.at(i - 1)->range_.end_key_))
      {
        TBSYS_LOG(WARN, "range not continuous. %ldth tablet range=%s, %ldth tablet range=%s",
            i-1, to_cstring(tablets.tablet_list.at(i - 1)->range_), i, to_cstring(p_tablet_info->range_));
        err = OB_INVALID_ARGUMENT;
      }
    }
  }
  return err;
}

int ObRootServer2::try_create_new_tables(int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  if (NULL == root_table_)
  {
    TBSYS_LOG(WARN, "root table is not init; wait another second.");
  }
  else
  {
    for (const ObTableSchema* it=schema_manager_for_cache_->table_begin();
         it != schema_manager_for_cache_->table_end(); ++it)
    {
      if (it->get_table_id() != OB_INVALID_ID)
      {
        {
          tbsys::CRLockGuard guard(root_table_rwlock_);
          if(!root_table_->table_is_exist(it->get_table_id()))
          {
            TBSYS_LOG(INFO, "table not exist, table_id=%lu", it->get_table_id());
            table_id = it->get_table_id();
          }
        }
        if (OB_INVALID_ID != table_id)
        {
          err = try_create_new_table(frozen_version, table_id);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "fail to create table.table_id=%ld", table_id);
            ret = err;
          }
        }
      }
    }
  }
  return ret;
}

int ObRootServer2::split_table_range(const int64_t frozen_version, const uint64_t table_id,
                                     common::ObTabletInfoList &tablets)
{
  int err = OB_SUCCESS;
  //step1: get master ups
  ObServer master_ups;
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = get_master_ups(master_ups, false)))
    {
      TBSYS_LOG(WARN, "fail to get master ups addr. err=%d", err);
    }
  }
  //setp2: get tablet_info form ups ,retry
  int64_t rt_version = 0;
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = get_max_tablet_version(rt_version)))
    {
      TBSYS_LOG(WARN, "fail to get max tablet version.err=%d", err);
    }
  }
  int64_t fetch_version = rt_version;
  if (1 == fetch_version)
  {
    fetch_version = 2;
  }
  if (OB_SUCCESS == err)
  {
    while (fetch_version <= frozen_version)
    {
      common::ObTabletInfoList tmp_tablets;
      if (OB_SUCCESS != (err = worker_->get_rpc_stub().get_split_range(master_ups,
                                                                       config_.network_timeout, table_id,
                                                                       fetch_version, tmp_tablets)))
      {
        TBSYS_LOG(WARN, "fail to get split range from ups. ups=%s, table_id=%ld, fetch_version=%ld, forzen_version=%ld",
                  master_ups.to_cstring(), table_id, fetch_version, frozen_version);
        if (OB_UPS_INVALID_MAJOR_VERSION == err)
        {
          err = OB_SUCCESS;
          TBSYS_LOG(INFO, "fetch tablets retruen invalid_major_version, version=%ld, try next version.", fetch_version);
        }
        else
        {
          TBSYS_LOG(WARN, "get split_range has some trouble, abort it. err=%d", err);
          break;
        }
      }
      else
      {
        if ((1 == tmp_tablets.get_tablet_size())
            && (0 == tmp_tablets.tablet_list.at(0)->row_count_))
        {
          tablets = tmp_tablets;
          if (fetch_version == frozen_version)
          {
            TBSYS_LOG(INFO, "reach the bigger fetch_version, equal to frozen_version[%ld]", frozen_version);
            break;
          }
          else
          {
            TBSYS_LOG(INFO, "fetch only one empty tablet. fetch_version=%ld, frozen_version=%ld, row_count=%ld, try the next version",
                      fetch_version, frozen_version, tmp_tablets.tablet_list.at(0)->row_count_);
          }
        }
        else
        {
          tablets = tmp_tablets;
          break;
        }
      }
      sleep(WAIT_SECONDS);
      fetch_version ++;
    }
    if (OB_SUCCESS != err && fetch_version > frozen_version)
    {
      err = OB_ERROR;
      TBSYS_LOG(ERROR, "retry all version and failed. check it.");
    }
    else if (OB_SUCCESS == err)
    {
      TBSYS_LOG(INFO, "fetch tablet_list succ. fetch_version=%ld, tablet_list size=%ld",
                fetch_version, tablets.get_tablet_size());
      ObTabletInfo *tablet_info = NULL;
      for (int64_t i = 0; i < tablets.get_tablet_size(); i++)
      {
        tablet_info = tablets.tablet_list.at(i);
        TBSYS_LOG(INFO, "%ldth tablet:range=%s", i, to_cstring(tablet_info->range_));
      }
    }
  }
  return err;
}

int ObRootServer2::create_tablet_with_range(const int64_t frozen_version, const ObTabletInfoList& tablets)
{
  int ret = OB_SUCCESS;
  int64_t index = tablets.tablet_list.get_array_index();
  ObTabletInfo* p_table_info = NULL;
  ObRootTable2* root_table_for_split = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
  if (NULL == root_table_for_split)
  {
    TBSYS_LOG(WARN, "new ObRootTable2 fail.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    *root_table_for_split = *root_table_;
  }

  int32_t **server_index = NULL;
  int32_t *create_count = NULL;
  if (OB_SUCCESS == ret)
  {
    create_count = new (std::nothrow)int32_t[index];
    server_index = new (std::nothrow)int32_t*[index];
    if (NULL == server_index || NULL == create_count)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      for (int32_t i = 0; i < index; i++)
      {
        server_index[i] = new(std::nothrow) int32_t[OB_SAFE_COPY_COUNT];
        if (NULL == server_index[i])
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < index; i ++)
  {
    p_table_info = tablets.tablet_list.at(i);
    if (p_table_info != NULL)
    {
      TBSYS_LOG(INFO, "tablet_index=%ld, range=%s", i, to_cstring(p_table_info->range_));
      if (!p_table_info->range_.is_left_open_right_closed())
      {
        TBSYS_LOG(WARN, "illegal tablet, range=%s", to_cstring(p_table_info->range_));
      }
      else
      {
        //通知cs建立tablet
        if (OB_SUCCESS != (ret =
                           create_empty_tablet_with_range(frozen_version, root_table_for_split,
                                                          *p_table_info, create_count[i], server_index[i])))
        {
          TBSYS_LOG(WARN, "fail to create tablet, ret =%d, range=%s", ret, to_cstring(p_table_info->range_));
        }
        else
        {
          TBSYS_LOG(INFO, "create tablet succ. range=%s", to_cstring(p_table_info->range_));
        }
      }
    }//end p_table_info != NULL
  }//end for
  if (OB_SUCCESS == ret && root_table_for_split != NULL)
  {
    TBSYS_LOG(INFO, "create tablet success.");
    if (is_master())
    {
      int ret2 = log_worker_->batch_add_new_tablet(tablets, server_index, create_count, frozen_version);
      if (OB_SUCCESS != ret2)
      {
        TBSYS_LOG(WARN, "failed to log add_new_tablet, err=%d", ret2);
      }
    }
    switch_root_table(root_table_for_split, NULL);
    root_table_for_split = NULL;
  }
  else
  {
    if (NULL != root_table_for_split)
    {
      OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_for_split);
    }
  }
  for (int64_t i = 0; i < index; i++)
  {
    if (NULL != server_index[i])
    {
      delete [] server_index[i];
    }
  }
  if (NULL != server_index)
  {
    delete [] server_index;
  }
  if (NULL != create_count)
  {
    delete [] create_count;
  }
  return ret;
}

int ObRootServer2::create_empty_tablet_with_range(const int64_t frozen_version,
    ObRootTable2 *root_table, const common::ObTabletInfo &tablet,
    int32_t& created_count, int* t_server_index)
{
  int ret = OB_SUCCESS;
  int32_t server_index[OB_SAFE_COPY_COUNT];
  if (NULL == root_table)
  {
    TBSYS_LOG(WARN, "invalid argument. root_table_for_split is NULL;");
    ret = OB_INVALID_ARGUMENT;
  }

  for (int i = 0; i < OB_SAFE_COPY_COUNT; i++)
  {
    server_index[i] = OB_INVALID_INDEX;
    t_server_index[i] = OB_INVALID_INDEX;
  }

  int32_t result_num = -1;
  if (OB_SUCCESS == ret)
  {
    get_available_servers_for_new_table(server_index, (int32_t)config_.tablet_replicas_num, result_num);
    if (0 >= result_num)
    {
      TBSYS_LOG(WARN, "no cs seleted");
      ret = OB_NO_CS_SELECTED;
    }
  }

  created_count = 0;
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "cs selected for create new table, num=%d", result_num);
    for (int32_t i = 0; i < result_num; i++)
    {
      if (server_index[i] != OB_INVALID_INDEX)
      {
        ObServerStatus* server_status = server_manager_.get_server_status(server_index[i]);
        if (server_status != NULL)
        {
          common::ObServer server = server_status->server_;
          server.set_port(server_status->port_cs_);
          int err = worker_->get_rpc_stub().create_tablet(server, tablet.range_, frozen_version,
                                                          config_.network_timeout);
          if (OB_SUCCESS != err && OB_ENTRY_EXIST != err)
          {
            TBSYS_LOG(WARN, "fail to create tablet replica, server=%d", server_index[i]);
          }
          else
          {
            err = OB_SUCCESS;
            t_server_index[created_count] = server_index[i];
            created_count++;
            TBSYS_LOG(INFO, "create tablet replica, table_id=%lu server=%d version=%ld",
                      tablet.range_.table_id_, server_index[i], frozen_version);
          }
        }
        else
        {
          server_index[i] = OB_INVALID_INDEX;
        }
      }
    }
    if (created_count > 0)
    {
      ObArray<int> sia;
      for (int64_t i = 0 ; i < created_count; ++i)
      {
        sia.push_back(t_server_index[i]);
      }
      ret = root_table->create_table(tablet, sia, frozen_version);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to create table.err=%d", ret);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "no tablet created");
      ret = OB_NO_TABLETS_CREATED;
    }
  }
  return ret;
}

// WARN: if safe_count is zero, then use system min(safe replica count , alive chunk server count) as safe_count
int ObRootServer2::check_tablet_version(const int64_t tablet_version, const int64_t safe_count, bool &is_merged) const
{
  int err = OB_SUCCESS;
  int32_t chunk_server_count = server_manager_.get_alive_server_count(true);
  TBSYS_LOG(TRACE, "check tablet version[required_version=%ld]", tablet_version);
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != root_table_)
  {
    int64_t min_replica_count = safe_count;
    if (0 == safe_count)
    {
      min_replica_count = config_.tablet_replicas_num;
      if ((chunk_server_count > 0) && (chunk_server_count < min_replica_count))
      {
        TBSYS_LOG(TRACE, "check chunkserver count less than replica num:server[%d], replica[%ld]",
            chunk_server_count, safe_count);
        min_replica_count = chunk_server_count;
      }
    }
    if (root_table_->is_empty())
    {
      TBSYS_LOG(WARN, "root table is empty, try it later");
      is_merged = false;
    }
    else
    {
      err = root_table_->check_tablet_version_merged(tablet_version, min_replica_count, is_merged);
    }
  }
  else
  {
    err = OB_ERROR;
    TBSYS_LOG(WARN, "fail to check tablet_version_merged. root_table_ = null");
  }
  return err;
}

void ObRootServer2::dump_migrate_info() const
{
  balancer_->dump_migrate_info();
}

int ObRootServer2::get_deleted_tables(const common::ObSchemaManagerV2 &old_schema,
                                      const common::ObSchemaManagerV2 &new_schema,
                                      common::ObArray<uint64_t> &deleted_tables)
{
  int ret = OB_SUCCESS;
  deleted_tables.clear();
  const ObTableSchema* it = old_schema.table_begin();
  const ObTableSchema* it2 = NULL;
  for (; it != old_schema.table_end(); ++it)
  {
    if (NULL == (it2 = new_schema.get_table_schema(it->get_table_id())))
    {
      if (OB_SUCCESS != (ret = deleted_tables.push_back(it->get_table_id())))
      {
        TBSYS_LOG(WARN, "failed to push array, err=%d", ret);
        break;
      }
      else
      {
        //TBSYS_LOG(INFO, "table deleted, table_id=%lu", it->get_table_id());
      }
    }
  }
  return ret;
}

void ObRootServer2::switch_root_table(ObRootTable2 *rt, ObTabletInfoManager *ti)
{
  OB_ASSERT(rt);
  OB_ASSERT(rt != root_table_);
  tbsys::CWLockGuard guard(root_table_rwlock_);
  TBSYS_LOG(INFO, "switch to new root table, old=%p, new=%p", root_table_, rt);
  if (NULL != root_table_)
  {
    OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_);
    root_table_ = NULL;
  }
  root_table_ = rt;
  balancer_->set_root_table(root_table_);
  restart_server_->set_root_table2(root_table_);

  if (NULL != ti && ti != tablet_manager_)
  {
    if (NULL != tablet_manager_)
    {
      OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager_);
      tablet_manager_ = NULL;
    }
    tablet_manager_ = ti;
  }
}

/*
 * chunk server register
 * @param out status 0 do not start report 1 start report
 */
int ObRootServer2::regist_chunk_server(const ObServer& server, const char* server_version, int32_t& status, int64_t time_stamp)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "regist chunk server:addr[%s], version[%s]", to_cstring(server), server_version);
  bool master = is_master();
  if (master)
  {
    time_stamp = tbsys::CTimeUtil::getTime();
    ret = log_worker_->regist_cs(server, server_version, time_stamp);
  }
  if (ret == OB_SUCCESS)
  {
    // regist
    {
      time_stamp = tbsys::CTimeUtil::getTime();
      tbsys::CWLockGuard guard(server_manager_rwlock_);
      server_manager_.receive_hb(server, time_stamp, false, false, 0, true);
      if (master)
      {
        commit_task(SERVER_ONLINE, OB_CHUNKSERVER, server, 0, server_version);
      }
    }
    first_cs_had_registed_ = true;
    //now we always want cs report its tablet
    status = START_REPORTING;
    if (START_REPORTING == status)
    {
      tbsys::CThreadGuard mutex_guard(&root_table_build_mutex_); //this for only one thread modify root_table
      tbsys::CWLockGuard root_table_guard(root_table_rwlock_);
      tbsys::CWLockGuard server_info_guard(server_manager_rwlock_);
      ObChunkServerManager::iterator it;
      it = server_manager_.find_by_ip(server);
      if (it != server_manager_.end())
      {
        {
          //remove this server's tablet
          if (root_table_ != NULL)
          {
            root_table_->server_off_line(static_cast<int32_t>(it - server_manager_.begin()), 0);
          }
        }
        if (it->status_ == ObServerStatus::STATUS_SERVING || it->status_ == ObServerStatus::STATUS_WAITING_REPORT)
        {
          // chunk server will start report
          it->status_ = ObServerStatus::STATUS_REPORTING;
        }
      }
    }

  }
  TBSYS_LOG(INFO, "process regist server:%s ret:%d", server.to_cstring(), ret);

  if(OB_SUCCESS == ret)
  {
    if(NULL != balancer_thread_)
    {
      balancer_thread_->wakeup();
    }
    else
    {
      TBSYS_LOG(WARN, "balancer_thread_ is null");
    }
  }
  return ret;
}

void ObRootServer2::commit_task(const ObTaskType type, const ObRole role, const ObServer & server,
    int32_t inner_port, const char* server_version, const int32_t cluster_role)
{
  ObRootAsyncTaskQueue::ObSeqTask task;
  task.type_ = type;
  task.role_ = role;
  task.server_ = server;
  task.inner_port_ = inner_port;
  task.cluster_role_ = cluster_role;
  int64_t server_version_length = strlen(server_version);
  if (server_version_length < OB_SERVER_VERSION_LENGTH)
  {
    strncpy(task.server_version_, server_version, server_version_length + 1);
  }
  else
  {
    strncpy(task.server_version_, server_version, OB_SERVER_VERSION_LENGTH - 1);
    task.server_version_[OB_SERVER_VERSION_LENGTH - 1] = '\0';
  }

  // change server inner port
  if (OB_MERGESERVER == role)
  {
    task.server_.set_port(inner_port);
    task.inner_port_ = server.get_port();
  }
  int ret = seq_task_queue_.push(task);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "commit inner task failed:server[%s], task_type[%d], server_role[%s], inner_port[%d], ret[%d]",
        task.server_.to_cstring(), type, print_role(task.role_), task.inner_port_, ret);
  }
  else
  {
    TBSYS_LOG(INFO, "commit inner task succ:server[%s], task_type[%d], server_role[%s], inner_port[%d]",
        task.server_.to_cstring(), type, print_role(task.role_), task.inner_port_);
  }
}

int ObRootServer2::regist_merge_server(const common::ObServer& server, const int32_t sql_port,
    const bool is_listen_ms, const char * server_version, int64_t time_stamp)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "regist merge server:addr[%s], sql_port[%d], server_version[%s], is_listen_ms=%s",
            to_cstring(server), sql_port, server_version, is_listen_ms ? "true" : "false");
  bool master = is_master();
  if (master)
  {
    if (!is_listen_ms)
    {
      time_stamp = tbsys::CTimeUtil::getTime();
      ret = log_worker_->regist_ms(server, sql_port, server_version, time_stamp);
    }
    else
    {
      ret = log_worker_->regist_lms(server, sql_port, server_version, time_stamp);
    }
  }

  if (ret == OB_SUCCESS)
  {
    time_stamp = tbsys::CTimeUtil::getTime();
    tbsys::CWLockGuard guard(server_manager_rwlock_);
    // regist merge server
    server_manager_.receive_hb(server, time_stamp, true, is_listen_ms, sql_port, true);
    if (master)
    {
      if (!is_listen_ms)
      {
        commit_task(SERVER_ONLINE, OB_MERGESERVER, server, sql_port, server_version);
      }
      else
      {
        ObServer cluster_vip;
        config_.get_root_server(cluster_vip);
        commit_task(LMS_ONLINE, OB_MERGESERVER, cluster_vip, sql_port, server_version);
      }
    }
  }
  return ret;
}

/*
 * chunk server更新自己的磁盘情况信息
 */
int ObRootServer2::update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used)
{
  int ret = OB_SUCCESS;
  if (is_master())
  {
    ret = log_worker_->report_cs_load(server, capacity, used);
  }

  if (ret == OB_SUCCESS)
  {
    ObServerDiskInfo disk_info;
    disk_info.set_capacity(capacity);
    disk_info.set_used(used);
    tbsys::CWLockGuard guard(server_manager_rwlock_);
    ret = server_manager_.update_disk_info(server, disk_info);
  }

  TBSYS_LOG(INFO, "server %s update capacity info capacity is %ld,"
            " used is %ld ret is %d", to_cstring(server), capacity, used, ret);

  return ret;
}

/*
 * 迁移完成操作
 */
int ObRootServer2::migrate_over(const int32_t result, const ObDataSourceDesc& desc,
    const int64_t occupy_size, const uint64_t crc_sum, const uint64_t row_checksum, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  const ObNewRange& range = desc.range_;
  const ObServer& src_server = desc.src_server_;
  const common::ObServer& dest_server = desc.dst_server_;
  const int64_t tablet_version = desc.tablet_version_;
  const bool keep_src = desc.keep_source_;
  if (OB_SUCCESS == result)
  {
    TBSYS_LOG(INFO, "migrate_over received, result=%d dest_cs=%s desc=%s occupy_size=%ld crc_sum=%lu row_count=%ld",
       result, to_cstring(dest_server), to_cstring(desc), occupy_size, crc_sum, row_count);
  }
  else
  {
    TBSYS_LOG(WARN, "migrate_over received, result=%d dest_cs=%s desc=%s occupy_size=%ld crc_sum=%lu row_count=%ld",
       result, to_cstring(dest_server), to_cstring(desc), occupy_size, crc_sum, row_count);
  }

  if (!is_loading_data() && state_.get_bypass_flag())
  {
    TBSYS_LOG(WARN, "rootserver in clean root table process, refuse to migrate over report");
    ret = OB_RS_STATE_NOT_ALLOW;
  }
  else
  {
    int src_server_index = get_server_index(src_server);
    int dest_server_index = get_server_index(dest_server);

    ObRootTable2::const_iterator start_it;
    ObRootTable2::const_iterator end_it;
    common::ObTabletInfo *tablet_info = NULL;
    if (OB_SUCCESS == result)
    {
      tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
      tbsys::CWLockGuard guard(root_table_rwlock_);
      int find_ret = root_table_->find_range(range, start_it, end_it);
      if (OB_SUCCESS == find_ret && start_it == end_it)
      {
        tablet_info = root_table_->get_tablet_info(start_it);
        if (NULL == tablet_info)
        {
          TBSYS_LOG(ERROR, "tablet_info must not null, start_it=%p, range=%s", start_it, to_cstring(desc.range_));
          ret = OB_ENTRY_NOT_EXIST;
        }
        else if (range.equal(tablet_info->range_))
        {
          if (keep_src)
          {
            if (OB_INVALID_INDEX == dest_server_index)
            {
              // dest cs is down
              TBSYS_LOG(WARN, "can not find cs, src=%d dest=%d", src_server_index, dest_server_index);
              ret = OB_ENTRY_NOT_EXIST;
            }
            else
            {
              // add replica
              ret = root_table_->modify(start_it, dest_server_index, tablet_version);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "failed to add new replica, range=%s dest_server_index=%d tablet_version=%ld, ret=%d",
                    to_cstring(tablet_info->range_), dest_server_index, tablet_version, ret);
              }
              else
              {
                ObTabletCrcHistoryHelper *crc_helper = root_table_->get_crc_helper(start_it);
                if (NULL == crc_helper)
                {
                  TBSYS_LOG(ERROR, "crc_helper must not null, range=%s start_it=%p",
                      to_cstring(tablet_info->range_), start_it);
                  ret = OB_ERR_SYS;
                }
                else
                {
                  int64_t min_version = 0;
                  int64_t max_version = 0;
                  crc_helper->get_min_max_version(min_version, max_version);
                  if (0 == min_version && 0 == max_version)
                  { // load tablet from outside, update tablet info
                    if (OB_SUCCESS != (ret = crc_helper->check_and_update(tablet_version, crc_sum, row_checksum)))
                    {
                      TBSYS_LOG(ERROR, "failed to check and update crc_sum, range=%s, ret=%d",
                          to_cstring(tablet_info->range_), ret);
                    }
                    else
                    {
                      tablet_info->row_count_ = row_count;
                      tablet_info->occupy_size_ = occupy_size;
                      tablet_info->crc_sum_ = crc_sum;
                      tablet_info->row_checksum_ = row_checksum;
                    }
                  }
                }
              }
            }
          }
          else
          {
            // dest_server_index and src_server_index may be INVALID
            ret = root_table_->replace(start_it, src_server_index, dest_server_index, tablet_version);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to mofidy replica, range=%s src_server_index=%d, "
                  "dest_server_index=%d tablet_version=%ld, ret=%d",
                  to_cstring(tablet_info->range_), src_server_index, dest_server_index, tablet_version, ret);
            }
          }
          ObServerStatus* src_status = server_manager_.get_server_status(src_server_index);
          ObServerStatus* dest_status = server_manager_.get_server_status(dest_server_index);
          if (src_status != NULL && dest_status != NULL && tablet_info != NULL)
          {
            if (!keep_src)
            {
              if (OB_INVALID_INDEX != src_server_index)
              {
                src_status->disk_info_.set_used(src_status->disk_info_.get_used() - tablet_info->occupy_size_);
              }
            }
            if (OB_INVALID_INDEX != dest_server_index)
            {
              dest_status->disk_info_.set_used(dest_status->disk_info_.get_used() + tablet_info->occupy_size_);
            }
          }

          if (OB_SUCCESS == ret && server_manager_.get_migrate_num() != 0
              && balancer_->is_loading_data() == false)
          {
            const_cast<ObRootTable2::iterator>(start_it)->has_been_migrated();
          }

          if (is_master())
          {
            ret = log_worker_->cs_migrate_done(result, desc, occupy_size, crc_sum, row_checksum, row_count);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to write migrate over log, range=%s, src_server=%s, "
                  "dest_server=%s, keep_src=%s, tablet_version=%ld, ret=%d",
                  to_cstring(range), to_cstring(src_server), to_cstring(dest_server),
                  keep_src ? "true" : "false", tablet_version, ret);
            }
          }
        }
        else
        {
          ret = OB_ROOT_TABLE_RANGE_NOT_EXIST;
          TBSYS_LOG(INFO, "can not find the range %s in root table, ignore this", to_cstring(range));
        }
      }
      else
      {
        TBSYS_LOG(WARN, "fail to find range %s in roottable. find_ret=%d, start_it=%p, end_it=%p",
            to_cstring(range), find_ret, start_it, end_it);
        ret = OB_ROOT_TABLE_RANGE_NOT_EXIST;
      }
    }

    if (is_master() || worker_->get_role_manager()->get_role() == ObRoleMgr::STANDALONE)
    {
      if (ret == OB_SUCCESS)
      {
        balancer_->nb_trigger_next_migrate(desc, result);
      }
    }
  }
  return ret;
}

int ObRootServer2::make_out_cell(ObCellInfo& out_cell, ObRootTable2::const_iterator first,
                                 ObRootTable2::const_iterator last, ObScanner& scanner, const int32_t max_row_count, const int32_t max_key_len) const
{
  static ObString s_root_1_port(static_cast<int32_t>(strlen(ROOT_1_PORT)), static_cast<int32_t>(strlen(ROOT_1_PORT)), (char*)ROOT_1_PORT);
  static ObString s_root_1_ms_port(static_cast<int32_t>(strlen(ROOT_1_MS_PORT)), static_cast<int32_t>(strlen(ROOT_1_MS_PORT)), (char*)ROOT_1_MS_PORT);
  static ObString s_root_1_ipv6_1(static_cast<int32_t>(strlen(ROOT_1_IPV6_1)), static_cast<int32_t>(strlen(ROOT_1_IPV6_1)), (char*)ROOT_1_IPV6_1);
  static ObString s_root_1_ipv6_2(static_cast<int32_t>(strlen(ROOT_1_IPV6_2)), static_cast<int32_t>(strlen(ROOT_1_IPV6_2)), (char*)ROOT_1_IPV6_2);
  static ObString s_root_1_ipv6_3(static_cast<int32_t>(strlen(ROOT_1_IPV6_3)), static_cast<int32_t>(strlen(ROOT_1_IPV6_3)), (char*)ROOT_1_IPV6_3);
  static ObString s_root_1_ipv6_4(static_cast<int32_t>(strlen(ROOT_1_IPV6_4)), static_cast<int32_t>(strlen(ROOT_1_IPV6_4)), (char*)ROOT_1_IPV6_4);
  static ObString s_root_1_ipv4(  static_cast<int32_t>(strlen(ROOT_1_IPV4)), static_cast<int32_t>(strlen(ROOT_1_IPV4)), (char*)ROOT_1_IPV4);
  static ObString s_root_1_tablet_version(static_cast<int32_t>(strlen(ROOT_1_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_1_TABLET_VERSION)), (char*)ROOT_1_TABLET_VERSION);

  static ObString s_root_2_port(   static_cast<int32_t>(strlen(ROOT_2_PORT)),    static_cast<int32_t>(strlen(ROOT_2_PORT)),    (char*)ROOT_2_PORT);
  static ObString s_root_2_ms_port(static_cast<int32_t>(strlen(ROOT_2_MS_PORT)), static_cast<int32_t>(strlen(ROOT_2_MS_PORT)), (char*)ROOT_2_MS_PORT);
  static ObString s_root_2_ipv6_1( static_cast<int32_t>(strlen(ROOT_2_IPV6_1)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_1)),  (char*)ROOT_2_IPV6_1);
  static ObString s_root_2_ipv6_2( static_cast<int32_t>(strlen(ROOT_2_IPV6_2)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_2)),  (char*)ROOT_2_IPV6_2);
  static ObString s_root_2_ipv6_3( static_cast<int32_t>(strlen(ROOT_2_IPV6_3)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_3)),  (char*)ROOT_2_IPV6_3);
  static ObString s_root_2_ipv6_4( static_cast<int32_t>(strlen(ROOT_2_IPV6_4)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_4)),  (char*)ROOT_2_IPV6_4);
  static ObString s_root_2_ipv4(   static_cast<int32_t>(strlen(ROOT_2_IPV4)),    static_cast<int32_t>(strlen(ROOT_2_IPV4)),    (char*)ROOT_2_IPV4);
  static ObString s_root_2_tablet_version(static_cast<int32_t>(strlen(ROOT_2_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_2_TABLET_VERSION)), (char*)ROOT_2_TABLET_VERSION);

  static ObString s_root_3_port(   static_cast<int32_t>(strlen(ROOT_3_PORT)),    static_cast<int32_t>(strlen(ROOT_3_PORT)),    (char*)ROOT_3_PORT);
  static ObString s_root_3_ms_port(static_cast<int32_t>(strlen(ROOT_3_MS_PORT)), static_cast<int32_t>(strlen(ROOT_3_MS_PORT)), (char*)ROOT_3_MS_PORT);
  static ObString s_root_3_ipv6_1( static_cast<int32_t>(strlen(ROOT_3_IPV6_1)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_1)),  (char*)ROOT_3_IPV6_1);
  static ObString s_root_3_ipv6_2( static_cast<int32_t>(strlen(ROOT_3_IPV6_2)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_2)),  (char*)ROOT_3_IPV6_2);
  static ObString s_root_3_ipv6_3( static_cast<int32_t>(strlen(ROOT_3_IPV6_3)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_3)),  (char*)ROOT_3_IPV6_3);
  static ObString s_root_3_ipv6_4( static_cast<int32_t>(strlen(ROOT_3_IPV6_4)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_4)),  (char*)ROOT_3_IPV6_4);
  static ObString s_root_3_ipv4(   static_cast<int32_t>(strlen(ROOT_3_IPV4)),    static_cast<int32_t>(strlen(ROOT_3_IPV4)),    (char*)ROOT_3_IPV4);
  static ObString s_root_3_tablet_version(static_cast<int32_t>(strlen(ROOT_3_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_3_TABLET_VERSION)), (char*)ROOT_3_TABLET_VERSION);

  static ObString s_root_occupy_size (static_cast<int32_t>(strlen(ROOT_OCCUPY_SIZE)), static_cast<int32_t>(strlen(ROOT_OCCUPY_SIZE)), (char*)ROOT_OCCUPY_SIZE);
  static ObString s_root_record_count(static_cast<int32_t>(strlen(ROOT_RECORD_COUNT)), static_cast<int32_t>(strlen(ROOT_RECORD_COUNT)), (char*)ROOT_RECORD_COUNT);

  int ret = OB_SUCCESS;
  UNUSED(max_row_key);
  UNUSED(max_key_len);
  const common::ObTabletInfo* tablet_info = NULL;
  int count = 0;
  for (ObRootTable2::const_iterator it = first; it <= last; it++)
  {
    if (count > max_row_count) break;
    tablet_info = ((const ObRootTable2*)root_table_)->get_tablet_info(it);
    if (tablet_info == NULL)
    {
      TBSYS_LOG(ERROR, "you should not reach this bugs");
      break;
    }
    out_cell.row_key_ = tablet_info->range_.end_key_;
    TBSYS_LOG(DEBUG,"add range %s",to_cstring(tablet_info->range_));
    TBSYS_LOG(DEBUG, "add a row key to out cell, rowkey = %s", to_cstring(out_cell.row_key_));
    count++;
    //start one row
    out_cell.column_name_ = s_root_occupy_size;
    out_cell.value_.set_int(tablet_info->occupy_size_);
    if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
    {
      break;
    }

    out_cell.column_name_ = s_root_record_count;
    out_cell.value_.set_int(tablet_info->row_count_);
    if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
    {
      break;
    }

    const ObServerStatus* server_status = NULL;
    if (it->server_info_indexes_[0] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[0])) != NULL)
    {
      out_cell.column_name_ = s_root_1_port;
      out_cell.value_.set_int(server_status->port_cs_);

      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4)
      {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_1_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_1_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_1_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[0]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
    }
    if (it->server_info_indexes_[1] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[1])) != NULL)
    {
      out_cell.column_name_ = s_root_2_port;
      out_cell.value_.set_int(server_status->port_cs_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4)
      {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_2_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_2_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_2_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[1]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
    }
    if (it->server_info_indexes_[2] != OB_INVALID_INDEX &&
        (server_status = server_manager_.get_server_status(it->server_info_indexes_[2])) != NULL)
    {
      out_cell.column_name_ = s_root_3_port;
      out_cell.value_.set_int(server_status->port_cs_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
      if (server_status->server_.get_version() != ObServer::IPV4)
      {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      out_cell.column_name_ = s_root_3_ms_port;
      out_cell.value_.set_int(server_status->port_ms_);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_3_ipv4;
      out_cell.value_.set_int(server_status->server_.get_ipv4());
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }

      out_cell.column_name_ = s_root_3_tablet_version;
      out_cell.value_.set_int(it->tablet_version_[2]);
      if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
      {
        break;
      }
    }

  } // end for each tablet
  return ret;
}

int ObRootServer2::find_monitor_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const ObCellInfo* cell = NULL;

  TBSYS_LOG(DEBUG, "find root table key");
  if (NULL == (cell = get_param[0]))
  {
    TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObRootMonitorTable monitor_table;
    monitor_table.init(my_addr_, server_manager_, *ups_manager_);

    ret = monitor_table.get(cell->row_key_, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get scanner from monitor root table failed:table_id[%lu], ret[%d]",
          cell->table_id_, ret);
    }
  }
  return ret;
}

int ObRootServer2::find_session_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const ObCellInfo* cell = NULL;

  TBSYS_LOG(DEBUG, "find root table key");
  if (NULL == (cell = get_param[0]))
  {
    TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObRootMonitorTable monitor_table;
    monitor_table.init(my_addr_, server_manager_, *ups_manager_);
    ret = monitor_table.get_ms_only(cell->row_key_, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get scanner from monitor root table failed:table_id[%lu], ret[%d]",
          cell->table_id_, ret);
    }
  }
  return ret;
}

int ObRootServer2::find_statement_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const ObCellInfo* cell = NULL;

  TBSYS_LOG(DEBUG, "find statement table key");
  if (NULL == (cell = get_param[0]))
  {
    TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObRootMonitorTable monitor_table;
    monitor_table.init(my_addr_, server_manager_, *ups_manager_);
    ret = monitor_table.get_ms_only(cell->row_key_, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get scanner from monitor root table failed:table_id[%lu], ret[%d]",
          cell->table_id_, ret);
    }
  }
  return ret;
}

int ObRootServer2::find_root_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const ObCellInfo* cell = NULL;

  TBSYS_LOG(DEBUG, "find root table key");
  if (NULL == (cell = get_param[0]))
  {
    TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
    ret = OB_INVALID_ARGUMENT;
  }
  else if (get_param.get_is_read_consistency() && obi_role_.get_role() != ObiRole::MASTER)
  {
    TBSYS_LOG(WARN, "we are not a master instance");
    ret = OB_NOT_MASTER;
  }
  else
  {
    int8_t rt_type = 0;
    UNUSED(rt_type); // for now we ignore this; OP_RT_TABLE_TYPE or OP_RT_TABLE_INDEX_TYPE

    if (cell->table_id_ != 0 && cell->table_id_ != OB_INVALID_ID)
    {
      TBSYS_LOG(DEBUG, "get table info, table_id=%ld", cell->table_id_);
      ObString table_name;
      int32_t max_key_len = 0;
      if (OB_SUCCESS != (ret = find_root_table_key(cell->table_id_, table_name, max_key_len,
              cell->row_key_, scanner)))
      {
        TBSYS_LOG(WARN, "failed to get tablet, err=%d table_id=%lu rowkey=%s",
            ret, cell->table_id_, to_cstring(cell->row_key_));
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "get table info ");
      int32_t max_key_len = 0;
      uint64_t table_id = 0;
      ret = get_table_info(cell->table_name_, table_id, max_key_len);
      if (OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "failed to get table id, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = find_root_table_key(table_id, cell->table_name_, max_key_len,
            cell->row_key_, scanner)))
      {
        TBSYS_LOG(WARN, "failed to get tablet, err=%d table_id=%lu", ret, cell->table_id_);
      }
    }
  }
  return ret;
}

int ObRootServer2::get_rowkey_info(const uint64_t table_id, ObRowkeyInfo &info) const
{
  int ret = OB_SUCCESS;
  // @todo
  UNUSED(table_id);
  UNUSED(info);
  return ret;
}

int ObRootServer2::find_root_table_key(const uint64_t table_id, const ObString& table_name,
    const int32_t max_key_len, const common::ObRowkey& key, ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  if (table_id == OB_INVALID_ID || 0 == table_id)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObCellInfo out_cell;
    out_cell.table_name_ = table_name;
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_ == NULL)
    {
      ret = OB_NOT_INIT;
    }
    else
    {
      ObRootTable2::const_iterator first;
      ObRootTable2::const_iterator last;
      ObRootTable2::const_iterator ptr;
      ret = root_table_->find_key(table_id, key, RETURN_BACH_COUNT, first, last, ptr);
      TBSYS_LOG(DEBUG, "first %p last %p ptr %p", first, last, ptr);
      if (ret == OB_SUCCESS)
      {
        if (first == ptr)
        {
          // make a fake startkey
          out_cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
          out_cell.row_key_ = ObRowkey::MIN_ROWKEY;
          ret = scanner.add_cell(out_cell);
          out_cell.value_.reset();
        }
        if (OB_SUCCESS == ret)
        {
          ret = make_out_cell(out_cell, first, last, scanner, RETURN_BACH_COUNT, max_key_len);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to make cell out. err=%d", ret);
          }
        }
      }
      else if (config_.is_import && OB_ERROR_OUT_OF_RANGE == ret)
      {
        TBSYS_LOG(WARN, "import application cann't privide service while importing");
        ret = OB_IMPORT_NOT_IN_SERVER;
      }
      else
      {
        TBSYS_LOG(WARN, "fail to find key. table_id=%lu, key=%s, ret=%d",
                  table_id, to_cstring(key), ret);
      }
    }
  }
  return ret;
}

int ObRootServer2::find_root_table_range(const common::ObScanParam& scan_param, ObScanner& scanner)
{
  const ObString &table_name = scan_param.get_table_name();
  const ObNewRange &key_range = *scan_param.get_range();
  int32_t max_key_len = 0;
  uint64_t table_id = 0;
  int ret = get_table_info(table_name, table_id, max_key_len);
  if (0 == table_id || OB_INVALID_ID == table_id)
  {
    TBSYS_LOG(WARN,"table name are invaild:%.*s", table_name.length(), table_name.ptr());
    ret = OB_INVALID_ARGUMENT;
  }
  else if (scan_param.get_is_read_consistency() && obi_role_.get_role() != ObiRole::MASTER)
  {
    TBSYS_LOG(INFO, "we are not a master instance");
    ret = OB_NOT_MASTER;
  }
  else
  {
    ObCellInfo out_cell;
    out_cell.table_name_ = table_name;
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_ == NULL)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN,"scan request in initialize phase");
    }
    else
    {
      ObRootTable2::const_iterator first;
      ObRootTable2::const_iterator last;
      ObNewRange search_range = key_range;
      search_range.table_id_ = table_id;
      ret = root_table_->find_range(search_range, first, last);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"cann't find this range %s, ret[%d]", to_cstring(search_range), ret);
      }
      else
      {
        if ((ret = make_out_cell(out_cell, first, last, scanner,
                                 MAX_RETURN_BACH_ROW_COUNT, max_key_len)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"make out cell failed,ret[%d]",ret);
        }
      }
    }
  }
  return ret;
}

/*
 * CS汇报tablet结束
 *
 */
int ObRootServer2::waiting_job_done(const common::ObServer& server, const int64_t frozen_mem_version)
{
  int ret = OB_ENTRY_NOT_EXIST;
  UNUSED(frozen_mem_version);
  ObChunkServerManager::iterator it;
  int64_t cs_index = get_server_index(server);
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  it = server_manager_.find_by_ip(server);
  if (it != server_manager_.end())
  {
    TBSYS_LOG(INFO, "cs waiting_job_done, status=%d", it->status_);
    if (is_master() && !is_bypass_process())
    {
      log_worker_->cs_merge_over(server, frozen_mem_version);
    }
    if (is_bypass_process())
    {
      operation_helper_.waiting_job_done(static_cast<int32_t>(cs_index));
    }
    else if (it->status_ == ObServerStatus::STATUS_REPORTING)
    {
      it->status_ = ObServerStatus::STATUS_SERVING;
    }
    ret = OB_SUCCESS;
  }
  return ret;
}
int ObRootServer2::get_server_index(const common::ObServer& server) const
{
  int ret = OB_INVALID_INDEX;
  ObChunkServerManager::const_iterator it;
  tbsys::CRLockGuard guard(server_manager_rwlock_);
  it = server_manager_.find_by_ip(server);
  if (it != server_manager_.end())
  {
    if (ObServerStatus::STATUS_DEAD != it->status_)
    {
      ret = static_cast<int32_t>(it - server_manager_.begin());
    }
  }
  return ret;
}
int ObRootServer2::report_tablets(const ObServer& server, const ObTabletReportInfoList& tablets,
    const int64_t frozen_mem_version)
{
  int return_code = OB_SUCCESS;
  int server_index = get_server_index(server);
  if (server_index == OB_INVALID_INDEX)
  {
    TBSYS_LOG(WARN, "can not find server's info, server=%s", to_cstring(server));
    return_code = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    TBSYS_LOG_US(INFO, "[NOTICE] report tablets, server=%d ip=%s count=%ld version=%ld",
                 server_index, to_cstring(server),
                 tablets.tablet_list_.get_array_index(), frozen_mem_version);
    if (is_master())
    {
      log_worker_->report_tablets(server, tablets, frozen_mem_version);
    }
    return_code = got_reported(tablets, server_index, frozen_mem_version);
    TBSYS_LOG_US(INFO, "got_reported over");
  }
  return return_code;
}

/*
 * 收到汇报消息后调用
 */
int ObRootServer2::got_reported(const ObTabletReportInfoList& tablets, const int server_index,
    const int64_t frozen_mem_version, const bool for_bypass/*=false*/, const bool is_replay_log /*=false*/)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "will add tablet info to root_table_for_query");
  delete_list_.reset();
  ObTabletReportInfoList add_tablet;
  if (ObBootState::OB_BOOT_OK != boot_state_.get_boot_state() && !is_replay_log)
  {
    TBSYS_LOG(INFO, "rs has not boot strap and is_replay_log=%c, refuse report.", is_replay_log?'Y':'N');
    //ret = OB_NOT_INIT; cs cant handle this... modify this in cs later
  }
  else
  {
    for (int64_t i = 0; i < tablets.get_tablet_size() && OB_SUCCESS == ret; i ++)
    {
      ret = add_tablet.add_tablet(tablets.tablets_[i]);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to add tablet to report list, ret=%d", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = got_reported_for_query_table(add_tablet, server_index, frozen_mem_version, for_bypass);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to do got_reported_for_query_table, ret=%d", ret);
      }
    }
    if (0 < delete_list_.get_tablet_size())
    {
      if (is_master() || worker_->get_role_manager()->get_role() == ObRoleMgr::STANDALONE)
      {
        //ObRootUtil::delete_tablets(worker_->get_rpc_stub(), server_manager_, delete_list_, config_.network_timeout);
        TBSYS_LOG(INFO, "need to delete tablet. tablet_num=%ld", delete_list_.get_tablet_size());
        worker_->submit_delete_tablets_task(delete_list_);
      }
    }
  }
  return ret;
}
/*
 * 处理汇报消息, 直接写到当前的root table中
 * 如果发现汇报消息中有对当前root table的tablet的分裂或者合并
 * 要调用采用写拷贝机制的处理函数
 */
int ObRootServer2::got_reported_for_query_table(const ObTabletReportInfoList& tablets,
    const int32_t server_index, const int64_t frozen_mem_version, const bool for_bypass)
{
  UNUSED(frozen_mem_version);
  int ret = OB_SUCCESS;
  int64_t have_done_index = 0;
  bool need_split = false;
  bool need_add = false;

  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);

  ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
  if (state_.get_bypass_flag() && !is_loading_data())
  {
    ret = operation_helper_.report_tablets(tablets, server_index, frozen_mem_version);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to do report tablet. ret=%d", ret);
    }
  }
  else
  {
    if (new_server_status == NULL && !for_bypass)
    {
      TBSYS_LOG(ERROR, "can not find server");
    }
    else
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      ObTabletReportInfo* p_table_info = NULL;
      ObRootTable2::const_iterator first;
      ObRootTable2::const_iterator last;
      int64_t index = tablets.tablet_list_.get_array_index();
      int find_ret = OB_SUCCESS;
      common::ObTabletInfo* tablet_info = NULL;
      int range_pos_type = ObRootTable2::POS_TYPE_ERROR;

      for(have_done_index = 0; have_done_index < index; ++have_done_index)
      {
        p_table_info = tablets.tablet_list_.at(have_done_index);
        if (p_table_info != NULL)
        {
          //TODO(maoqi) check the table of this tablet is exist in schema
          //if (NULL == schema_manager_->get_table_schema(p_table_info->tablet_info_.range_.table_id_))
          //  continue;
          if (!p_table_info->tablet_info_.range_.is_left_open_right_closed())
          {
            TBSYS_LOG(WARN, "cs reported illegal tablet, server=%d range=%s", server_index, to_cstring(p_table_info->tablet_info_.range_));
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs reported  tablet, server=%d range=%s", server_index, to_cstring(p_table_info->tablet_info_.range_));
          }
          tablet_info = NULL;
          find_ret = root_table_->find_range(p_table_info->tablet_info_.range_, first, last);
          TBSYS_LOG(DEBUG, "root_table_for_query_->find_range ret = %d", find_ret);
          range_pos_type = ObRootTable2::POS_TYPE_ERROR;
          if (OB_SUCCESS == find_ret)
          {
            tablet_info = root_table_->get_tablet_info(first);
            if (NULL != tablet_info)
            {
              range_pos_type = root_table_->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
              TBSYS_LOG(DEBUG, " range_pos_type = %d", range_pos_type);
            }
            else
            {
              TBSYS_LOG(ERROR, "no tablet_info found");
            }
          }
          else if (OB_FIND_OUT_OF_RANGE == find_ret)
          {
            range_pos_type = ObRootTable2::POS_TYPE_ADD_RANGE;
            need_add = true;
            break;
          }

          if (range_pos_type == ObRootTable2::POS_TYPE_SPLIT_RANGE)
          {
            need_split = true;  //will create a new table to deal with the left
            break;
          }
          else if (range_pos_type == ObRootTable2::POS_TYPE_ADD_RANGE)
          {
            need_add = true;
            break;
          }

          if (NULL != tablet_info &&
              (range_pos_type == ObRootTable2::POS_TYPE_SAME_RANGE || range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE)
             )
          {
            if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                  p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_))
            {
              TBSYS_LOG(ERROR, "write new tablet error");
              char buff[OB_MAX_ERROR_MSG_LEN];
              snprintf(buff, OB_MAX_ERROR_MSG_LEN, "write new info to root_table fail. range=%s", to_cstring(p_table_info->tablet_info_.range_));
              set_daily_merge_tablet_error(buff, strlen(buff));
              TBSYS_LOG(INFO, "%s", to_cstring(p_table_info->tablet_info_.range_));
              //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
            }
          }
          else
          {
            TBSYS_LOG(WARN, "can not found range ignore this: tablet_info[%p], range_pos_type[%d] range:%s",
                tablet_info, range_pos_type, to_cstring(p_table_info->tablet_info_.range_));
            //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "null tablet report info in tablet report list, have_done_index[%ld]", have_done_index);
        }
      }
    } //end else, release lock
    if (need_split || need_add)
    {
      TBSYS_LOG(INFO, "update ranges: server=%d", server_index);
      ret = got_reported_with_copy(tablets, server_index, have_done_index, for_bypass);
    }
  }
  return ret;
}
/*
 * 写拷贝机制的,处理汇报消息
 */
int ObRootServer2::got_reported_with_copy(const ObTabletReportInfoList& tablets,
                                          const int32_t server_index, const int64_t have_done_index,
                                          const bool for_bypass)
{
  int ret = OB_SUCCESS;
  ObTabletReportInfo* p_table_info = NULL;
  ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;
  TBSYS_LOG(DEBUG, "root table write on copy");
  if (new_server_status == NULL && !for_bypass)
  {
    TBSYS_LOG(ERROR, "can not find server");
    ret = OB_ERROR;
  }
  else
  {
    ObRootTable2* root_table_for_split = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
    if (root_table_for_split == NULL)
    {
      TBSYS_LOG(ERROR, "new ObRootTable2 error");
      ret = OB_ERROR;
    }
    else
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      *root_table_for_split = *root_table_;
      int range_pos_type = ObRootTable2::POS_TYPE_UNINIT;
      for (int64_t index = have_done_index; OB_SUCCESS == ret && index < tablets.tablet_list_.get_array_index(); index++)
      {
        p_table_info = tablets.tablet_list_.at(index);
        if (p_table_info == NULL)
        {
          TBSYS_LOG(ERROR, "tablets.tablet_list_.at(%ld) should not be NULL", index);
          range_pos_type = ObRootTable2::POS_TYPE_ERROR;
        }
        else
        {
          range_pos_type = ObRootTable2::POS_TYPE_ERROR;
          int find_ret = root_table_for_split->find_range(p_table_info->tablet_info_.range_, first, last);
          if (OB_FIND_OUT_OF_RANGE == find_ret || OB_SUCCESS == find_ret)
          {
            range_pos_type = root_table_for_split->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
            TBSYS_LOG(DEBUG, "range_pos_type2 = %d", range_pos_type);
            if (range_pos_type == ObRootTable2::POS_TYPE_SAME_RANGE || range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE)
            { // TODO: no merge is implemented yet! so range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE is not proper
              if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                                                             p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_for_split))
              {
                TBSYS_LOG(ERROR, "write new tablet error");
                char buff[OB_MAX_ERROR_MSG_LEN];
                snprintf(buff, OB_MAX_ERROR_MSG_LEN, "write new info to root_table fail. range=%s", to_cstring(p_table_info->tablet_info_.range_));
                set_daily_merge_tablet_error(buff, strlen(buff));
                TBSYS_LOG(INFO, "p_table_info->tablet_info_.range_=%s", to_cstring(p_table_info->tablet_info_.range_));
                //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              }
            }
            else if (range_pos_type == ObRootTable2::POS_TYPE_SPLIT_RANGE)
            {
              if (first->get_max_tablet_version() >= p_table_info->tablet_location_.tablet_version_)
              {
                TBSYS_LOG(ERROR, "same version different range error !! version %ld",
                          p_table_info->tablet_location_.tablet_version_);
                char buff[OB_MAX_ERROR_MSG_LEN];
                snprintf(buff, OB_MAX_ERROR_MSG_LEN, "report tablet has split range, but version not satisfy split process, roottable version=%ld, report_version=%ld",
                    first->get_max_tablet_version(), p_table_info->tablet_location_.tablet_version_);
                set_daily_merge_tablet_error(buff, strlen(buff));
                TBSYS_LOG(INFO, "p_table_info->tablet_info_.range_=%s", to_cstring(p_table_info->tablet_info_.range_));
                //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              }
              else
              {
                ret = root_table_for_split->split_range(p_table_info->tablet_info_, first,
                                                        p_table_info->tablet_location_.tablet_version_, server_index);
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "split range error, ret = %d", ret);
                  char buff[OB_MAX_ERROR_MSG_LEN];
                  snprintf(buff, OB_MAX_ERROR_MSG_LEN, "split range error, ret=%d, range=%s", ret, to_cstring(p_table_info->tablet_info_.range_));
                  set_daily_merge_tablet_error(buff, strlen(buff));
                  TBSYS_LOG(INFO, "p_table_info->tablet_info_.range_=%s", to_cstring(p_table_info->tablet_info_.range_));
                  //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                }
              }
            }
            else if (range_pos_type == ObRootTable2::POS_TYPE_ADD_RANGE)
            {
              ret = root_table_for_split->add_range(p_table_info->tablet_info_, first,
                                                    p_table_info->tablet_location_.tablet_version_, server_index);
            }
            else
            {
              TBSYS_LOG(WARN, "error range be ignored range_pos_type =%d",range_pos_type );
              TBSYS_LOG(INFO, "p_table_info->tablet_info_.range_=%s", to_cstring(p_table_info->tablet_info_.range_));
              //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "find_ret[%d] != OB_FIND_OUT_OF_RANGE or OB_SUCCESS", find_ret);
            ret = OB_ERROR;
          }
        }
      }
    }

    if (OB_SUCCESS == ret && root_table_for_split != NULL)
    {
      switch_root_table(root_table_for_split, NULL);
      root_table_for_split = NULL;
    }
    else
    {
      TBSYS_LOG(ERROR, "update root table failed: ret[%d] root_table_for_split[%p]",
                ret, root_table_for_split);
      if (root_table_for_split != NULL)
      {
        OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_for_split);
      }
    }

  }
  return ret;
}

/*
 * 在一个tabelt的各份拷贝中, 寻找合适的备份替换掉
 */
int ObRootServer2::write_new_info_to_root_table(
  const ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index,
  ObRootTable2::const_iterator& first, ObRootTable2::const_iterator& last, ObRootTable2 *p_root_table)
{
  int ret = OB_SUCCESS;
  int32_t found_it_index = OB_INVALID_INDEX;
  int64_t max_tablet_version = 0;
  ObServerStatus* server_status = NULL;
  ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
  if (new_server_status == NULL)
  {
    TBSYS_LOG(ERROR, "can not find server");
    ret = OB_ERROR;
  }
  else
  {
    for (ObRootTable2::const_iterator it = first; it <= last; it++)
    {
      ObTabletInfo *p_tablet_write = p_root_table->get_tablet_info(it);
      ObTabletCrcHistoryHelper *crc_helper = p_root_table->get_crc_helper(it);
      if (crc_helper == NULL)
      {
        TBSYS_LOG(ERROR, "%s", "get src helper error should not reach this bugs!!");
        ret = OB_ERROR;
        break;
      }
      max_tablet_version = it->get_max_tablet_version();
      //TODO: no merge is supported yet. this code should be refacted to support merge.
      if (tablet_version >= max_tablet_version)
      {
        if (first != last)
        {
          TBSYS_LOG(ERROR, "we should not have merge tablet max tabelt is %ld this one is %ld",
                    max_tablet_version, tablet_version);
          ret = OB_ERROR;
          break;
        }
      }
      if (first == last)
      {
        //check crc sum
        ret = crc_helper->check_and_update(tablet_version, tablet_info.crc_sum_, tablet_info.row_checksum_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "check crc sum error crc is %lu tablet is", tablet_info.crc_sum_);
          TBSYS_LOG(INFO, "tablet_info.range_=%s", to_cstring(tablet_info.range_));
          it->dump();
          char buff[OB_MAX_ERROR_MSG_LEN];
          snprintf(buff, OB_MAX_ERROR_MSG_LEN, "crc error. range=%s, error crc=%ld", to_cstring(tablet_info.range_), tablet_info.crc_sum_);
          set_daily_merge_tablet_error(buff, strlen(buff));
          break;
        }
      }
      //try to over write dead server or old server;
      ObTabletReportInfo to_delete;
      to_delete.tablet_location_.chunkserver_.set_port(-1);
      found_it_index = ObRootTable2::find_suitable_pos(it, server_index, tablet_version, &to_delete);
      if (found_it_index == OB_INVALID_INDEX)
      {
        //find the server whose disk have max usage percent
        for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
        {
          server_status = server_manager_.get_server_status(it->server_info_indexes_[i]);
          if (server_status != NULL &&
              new_server_status->disk_info_.get_percent() > 0 &&
              server_status->disk_info_.get_percent() > new_server_status->disk_info_.get_percent())
          {
            found_it_index = i;
          }
        }
        if (OB_INVALID_INDEX != found_it_index)
        {
          to_delete.tablet_info_ = tablet_info;
          to_delete.tablet_location_.tablet_version_ = it->tablet_version_[found_it_index];
          to_delete.tablet_location_.chunkserver_.set_port(it->server_info_indexes_[found_it_index]);
          if (OB_SUCCESS != delete_list_.add_tablet(to_delete))
          {
            TBSYS_LOG(WARN, "failed to add into delete list");
          }
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "find it idx=%d", found_it_index);
        if (-1 != to_delete.tablet_location_.chunkserver_.get_port())
        {
          to_delete.tablet_info_ = tablet_info;
          if (OB_SUCCESS != delete_list_.add_tablet(to_delete))
          {
            TBSYS_LOG(WARN, "failed to add into delete list");
          }
        }
      }
      if (found_it_index != OB_INVALID_INDEX)
      {
        TBSYS_LOG(DEBUG, "write a tablet to root_table found_it_index = %d server_index =%d tablet_version = %ld",
                  found_it_index, server_index, tablet_version);

        //tablet_info.range_.hex_dump(TBSYS_LOG_LEVEL_DEBUG);
        TBSYS_LOG(DEBUG, "tablet_info.range_=%s", to_cstring(tablet_info.range_));
        //over write
        atomic_exchange((uint32_t*) &(it->server_info_indexes_[found_it_index]), server_index);
        atomic_exchange((uint64_t*) &(it->tablet_version_[found_it_index]), tablet_version);
        if (p_tablet_write != NULL)
        {
          atomic_exchange((uint64_t*) &(p_tablet_write->row_count_), tablet_info.row_count_);
          atomic_exchange((uint64_t*) &(p_tablet_write->occupy_size_), tablet_info.occupy_size_);
        }
      }
    }
  }
  return ret;
}

int64_t ObRootServer2::get_table_count(void) const
{
  int64_t ret = 0;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL == schema_manager_for_cache_)
  {
    TBSYS_LOG(WARN, "check schema manager failed");
  }
  else
  {
    ret = schema_manager_for_cache_->get_table_count();
  }
  return ret;
}

void ObRootServer2::get_tablet_info(int64_t & tablet_count, int64_t & row_count, int64_t & data_size) const
{
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != root_table_)
  {
    root_table_->get_tablet_info(tablet_count, row_count, data_size);
  }
}

int ObRootServer2::get_row_checksum(const int64_t tablet_version, const uint64_t table_id, ObRowChecksum &row_checksum)
{
  int ret = OB_SUCCESS;
  bool is_all_merged = false;
  common::ObSchemaManagerV2* schema_manager = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == schema_manager)
  {
    TBSYS_LOG(WARN, "fail to new schema_manager.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }

  if (OB_SUCCESS == ret)
  {
    ret = get_schema(false, false, *schema_manager);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get schema manager. ret=%d", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    //check tablet_version is merged
    if(OB_SUCCESS != (ret = check_tablet_version(tablet_version, 1, is_all_merged)))
    {
      TBSYS_LOG(WARN, "check tablet_version:%ld merged failed", tablet_version);
    }
    else if(!is_all_merged)
    {
      TBSYS_LOG(INFO, "tablet_version:%ld is not all merged", tablet_version);
      ret = OB_ENTRY_NOT_EXIST;
    }
    else
    {
      const ObTableSchema* it = NULL;
      uint64_t tmp_table_id = 0;
      row_checksum.data_version_ = tablet_version;
      ObRowChecksumInfo rc_info;
      uint64_t tmp_row_checksum = 0;
      for (it = schema_manager->table_begin(); schema_manager->table_end() != it; ++it)
      {
        tmp_row_checksum = 0;
        tmp_table_id = it->get_table_id();

        if(OB_INVALID_ID != table_id && tmp_table_id != table_id)
          continue;

        tbsys::CRLockGuard guard(root_table_rwlock_);
        if(OB_SUCCESS != (ret = root_table_->get_table_row_checksum(tablet_version, tmp_table_id, tmp_row_checksum)))
        {
          TBSYS_LOG(WARN, "get_row_checksum failed, table_id:%lu ret:%d", tmp_table_id, ret);
        }
        else
        {
          rc_info.reset();
          rc_info.table_id_ = tmp_table_id;
          rc_info.row_checksum_ = tmp_row_checksum;
          if(OB_SUCCESS != (ret = row_checksum.rc_array_.push_back(rc_info)))
          {
            TBSYS_LOG(WARN, "put row_checksum to rc_array failed, ret:%d", ret);
            break;
          }
        }
      }
    }
  }

  if (NULL != schema_manager)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_manager);
  }
  return ret;
}

// the array size of server_index should be larger than expected_num
void ObRootServer2::get_available_servers_for_new_table(int* server_index,
    int32_t expected_num, int32_t &results_num)
{
  //tbsys::CThreadGuard guard(&server_manager_mutex_);
  results_num = 0;
  int64_t mnow = tbsys::CTimeUtil::getTime();
  if (next_select_cs_index_ >= server_manager_.get_array_length())
  {
    next_select_cs_index_ = 0;
  }
  ObChunkServerManager::iterator it = server_manager_.begin() + next_select_cs_index_;
  for (; it != server_manager_.end() && results_num < expected_num; ++it)
  {
    if (it->status_ != ObServerStatus::STATUS_DEAD
        && mnow > it->register_time_ + config_.cs_probation_period)
    {
      int32_t cs_index = static_cast<int32_t>(it - server_manager_.begin());
      server_index[results_num] = cs_index;
      results_num++;
    }
  }
  if (results_num < expected_num)
  {
    it = server_manager_.begin();
    for(; it != server_manager_.begin() + next_select_cs_index_ && results_num < expected_num; ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD
          && mnow > it->register_time_ + config_.cs_probation_period)
      {
        int32_t cs_index = static_cast<int32_t>(it - server_manager_.begin());
        server_index[results_num] = cs_index;
        results_num++;
      }
    }
  }
  next_select_cs_index_ = it - server_manager_.begin() + 1;
}

int ObRootServer2::slave_batch_create_new_table(const common::ObTabletInfoList& tablets,
    int32_t** t_server_index, int32_t* replicas_num, const int64_t mem_version)
{
  int ret = OB_SUCCESS;
  int64_t index = tablets.get_tablet_size();
  ObArray<int32_t> server_array;
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  common::ObTabletInfo *p_table_info = NULL;
  {
    ObRootTable2* root_table_for_create = NULL;
    root_table_for_create = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
    if (root_table_for_create == NULL)
    {
      TBSYS_LOG(ERROR, "new ObRootTable2 error");
      ret = OB_ERROR;
    }
    if (NULL != root_table_for_create)
    {
      *root_table_for_create = *root_table_;
    }
    for (int64_t i = 0 ; i < index && OB_SUCCESS == ret; i++)
    {
      p_table_info = tablets.tablet_list.at(i);
      server_array.clear();
      for (int32_t j = 0; i < replicas_num[j]; ++j)
      {
        server_array.push_back(t_server_index[i][j]);
      }
      ret = root_table_for_create->create_table(*p_table_info, server_array, mem_version);
    }
    if (OB_SUCCESS == ret)
    {
      switch_root_table(root_table_for_create, NULL);
      root_table_for_create = NULL;
    }
    if (root_table_for_create != NULL)
    {
      OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_for_create);
    }
  }
  return ret;
}

// just for replay root server remove rplica commit log
int ObRootServer2::remove_replica(const bool did_replay, const common::ObTabletReportInfo &replica)
{
  int ret = OB_SUCCESS;
  UNUSED(did_replay);
  ObRootTable2::const_iterator start_it;
  ObRootTable2::const_iterator end_it;
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  tbsys::CWLockGuard guard(root_table_rwlock_);
  int find_ret = root_table_->find_range(replica.tablet_info_.range_, start_it, end_it);
  if (OB_SUCCESS == find_ret && start_it == end_it)
  {
    for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
    {
      if (OB_INVALID_INDEX != start_it->server_info_indexes_[i]
          && start_it->server_info_indexes_[i] == replica.tablet_location_.chunkserver_.get_port())
      {
        start_it->server_info_indexes_[i] = OB_INVALID_INDEX;
        break;
      }
    }
  }
  return ret;
}

// delete the replicas that chunk server remove automaticly when daily merge failed
int ObRootServer2::delete_replicas(const bool did_replay, const common::ObServer & cs, const common::ObTabletReportInfoList & replicas)
{
  int ret = OB_SUCCESS;
  ObRootTable2::const_iterator start_it;
  ObRootTable2::const_iterator end_it;
  // step 1. find server index for delete replicas
  int32_t server_index = get_server_index(cs);
  if (OB_INVALID_INDEX == server_index)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "get server index failed:server[%s], ret[%d]", cs.to_cstring(), server_index);
  }
  else
  {
    common::ObTabletReportInfo * replica = NULL;
    tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
    tbsys::CWLockGuard guard(root_table_rwlock_);
    // step 1. modify root table delete replicas
    for (int64_t i = 0; OB_SUCCESS == ret && i < replicas.tablet_list_.get_array_index(); ++i)
    {
      replica = replicas.tablet_list_.at(i);
      if (NULL == replica)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "check replica is null");
        break;
      }
      ret = root_table_->find_range(replica->tablet_info_.range_, start_it, end_it);
      if (OB_SUCCESS == ret)
      {
        ObRootTable2::const_iterator iter;
        for (iter = start_it; OB_SUCCESS == ret && iter <= end_it; ++iter)
        {
          int64_t replica_count = 0;
          int64_t find_index = OB_INVALID_INDEX;
          for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
          {
            if (iter->server_info_indexes_[i] != OB_INVALID_INDEX)
            {
              ++replica_count;
              if (server_index == iter->server_info_indexes_[i])
              {
                find_index = i;
              }
            }
          }
          // find the server
          if (find_index != OB_INVALID_INDEX)
          {
            if (replica_count > 1)
            {
              iter->server_info_indexes_[find_index] = OB_INVALID_INDEX;
            }
            else
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "can not remove this replica not safe:index[%d], server[%s]",
                  server_index, cs.to_cstring());
            }
          }
          else
          {
            TBSYS_LOG(WARN, "not find this server in service this tablet:index[%d], server[%s]",
                server_index, cs.to_cstring());
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "not find the tablet replica:range[%s], ret[%d]", to_cstring(replica->tablet_info_.range_), ret);
      }
    }
  }
  // step 2. write commit log if not replay
  if ((OB_SUCCESS == ret) && (false == did_replay))
  {
    if (OB_SUCCESS != (ret = log_worker_->delete_replicas(cs, replicas)))
    {
      TBSYS_LOG(WARN, "log_worker remove tablet replicas failed. err=%d", ret);
    }
  }
  return ret;
}

// @pre root_table_build_mutex_ locked
// @note Do not remove tablet entries in tablet_manager_ for now.
int ObRootServer2::delete_tables(const bool did_replay, const common::ObArray<uint64_t> &deleted_tables)
{
  OB_ASSERT(0 < deleted_tables.count());
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  ObRootTable2* rt1 = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
  if (NULL == rt1)
  {
    TBSYS_LOG(ERROR, "no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    *rt1 = *root_table_;
  }
  if (OB_SUCCESS == ret)
  {
    // step 1. modify new root table
    if (OB_SUCCESS != (ret = rt1->delete_tables(deleted_tables)))
    {
      TBSYS_LOG(WARN, "failed to delete tablets, err=%d", ret);
    }
    // step 2. write commit log if not replay
    if ((OB_SUCCESS == ret) && (false == did_replay))
    {
      TBSYS_LOG(TRACE, "write commit log for delete table");
      if (OB_SUCCESS != (ret = log_worker_->remove_table(deleted_tables)))
      {
        TBSYS_LOG(ERROR, "log_worker delete tables failed. err=%d", ret);
      }
    }
    // step 3. switch the new root table
    if (OB_SUCCESS == ret)
    {
      switch_root_table(rt1, NULL);
      rt1 = NULL;
    }
  }
  if (rt1 != NULL)
  {
    OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, rt1);
  }
  return ret;
}

int ObRootServer2::select_cs(const int64_t min_count, ObArray<std::pair<ObServer, int32_t> > &chunkservers)
{
  int ret = OB_SUCCESS;
  int64_t results_num = 0;
  if (server_manager_.size() <= 0)
  {
    ret = OB_NO_CS_SELECTED;
    TBSYS_LOG(WARN, "check chunk server manager size failed");
  }
  else
  {
    ObServer cs;
    // no need lock select_pos
    static int64_t select_pos = 0;
    if (select_pos >= server_manager_.size())
    {
      select_pos = 0;
    }
    int64_t visit_count = 0;
    do
    {
      ObChunkServerManager::const_iterator it = server_manager_.begin() + select_pos;
      for (; it != server_manager_.end() && results_num < min_count; ++it)
      {
        ++visit_count;
        ++select_pos;
        if (it->status_ != ObServerStatus::STATUS_DEAD)
        {
          cs = it->server_;
          cs.set_port(it->port_cs_);
          if (OB_SUCCESS != (ret = chunkservers.push_back(std::make_pair(cs, it - server_manager_.begin()))))
          {
            TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
            break;
          }
          else
          {
            ++results_num;
          }
        }
      } // end while
      if (results_num < min_count)
      {
        select_pos = 0;
      }
      else
      {
        break;
      }
    } while ((ret == OB_SUCCESS) && (visit_count < server_manager_.size()));
  }
  //
  if (0 == results_num)
  {
    ret = OB_NO_CS_SELECTED;
    TBSYS_LOG(ERROR, "not find valid chunkserver for create table");
  }
  else if (results_num < min_count)
  {
    TBSYS_LOG(WARN, "find valid chunkserver less than required replica count:select[%ld], required[%ld]",
        results_num, min_count);
  }
  return ret;
}

int ObRootServer2::fetch_mem_version(int64_t &mem_version)
{
  int ret = OB_SUCCESS;
  ObUps ups_master;
  if (OB_SUCCESS != (ret = ups_manager_->get_ups_master(ups_master)))
  {
    TBSYS_LOG(WARN, "failed to get ups master, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = get_rpc_stub().get_last_frozen_version(ups_master.addr_,
          config_.network_timeout, mem_version)))
  {
    TBSYS_LOG(WARN, "failed to get mem version from ups, err=%d", ret);
  }
  else if (0 >= mem_version)
  {
    ret = OB_INVALID_START_VERSION;
    TBSYS_LOG(WARN, "invalid mem version from ups, version=%ld ups=%s",
        mem_version, to_cstring(ups_master.addr_));
  }
  else
  {
    TBSYS_LOG(INFO, "fetch ups mem version=%ld", mem_version);
  }
  return ret;
}

int ObRootServer2::create_new_table(const bool did_replay, const common::ObTabletInfo& tablet,
    const common::ObArray<int32_t> &chunkservers, const int64_t mem_version)
{
  int ret = OB_SUCCESS;
  ObRootTable2* root_table_for_create = NULL;
  tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
  root_table_for_create = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, NULL);
  if (root_table_for_create == NULL)
  {
    TBSYS_LOG(ERROR, "new ObRootTable2 error");
    ret = OB_ERROR;
  }
  else
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    *root_table_for_create = *root_table_;
  }
  if (OB_SUCCESS == ret)
  {
    //fixbug:check roottable exist
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      if(root_table_->table_is_exist(tablet.range_.table_id_))
      {
        TBSYS_LOG(WARN, "table already exist, table_id=%lu", tablet.range_.table_id_);
        ret = OB_CREATE_TABLE_TWICE;
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    // step 1. modify new root table
    if (OB_SUCCESS != (ret = root_table_for_create->create_table(tablet, chunkservers, mem_version)))
    {
      TBSYS_LOG(WARN, "fail to create table. err=%d", ret);
    }
    // step 2. write commit log if not replay
    if ((OB_SUCCESS == ret) && (false == did_replay))
    {
      if (OB_SUCCESS != (ret = log_worker_->add_new_tablet(tablet, chunkservers, mem_version)))
      {
        TBSYS_LOG(WARN, "log_worker add new tablet failed. err=%d", ret);
      }
    }
    // step 3. switch the new root table
    if (OB_SUCCESS == ret)
    {
      switch_root_table(root_table_for_create, NULL);
      root_table_for_create = NULL;
    }
  }
  if (root_table_for_create != NULL)
  {
    OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table_for_create);
    root_table_for_create = NULL;
  }
  return ret;
}

int ObRootServer2::create_empty_tablet(TableSchema &tschema, ObArray<ObServer> &created_cs)
{
  int ret = OB_SUCCESS;
  if (!is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(WARN, "I'm not the master and can not create table");
  }
  else
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_->table_is_exist(tschema.table_id_))
    {
      ret = OB_ENTRY_EXIST;
      TBSYS_LOG(ERROR, "table is already created in root table. name=%s, table_id=%ld",
          tschema.table_name_, tschema.table_id_);
    }
  }
  int64_t mem_version = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = fetch_mem_version(mem_version)))
    {
      TBSYS_LOG(WARN, "fail to get mem_version. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObArray<std::pair<ObServer, int32_t> > chunkservers;
    if (OB_SUCCESS != (ret = select_cs(tschema.replica_num_, chunkservers)))
    {
      TBSYS_LOG(WARN, "failed to select chunkservers, err=%d", ret);
      ret = OB_NO_CS_SELECTED;
    }
    else
    {
      ObTabletInfo tablet;
      tablet.range_.table_id_ = tschema.table_id_;
      tablet.range_.border_flag_.unset_inclusive_start();
      tablet.range_.border_flag_.set_inclusive_end();
      tablet.range_.set_whole_range();
      tschema.create_mem_version_ = mem_version;
      int err = OB_SUCCESS;
      ObArray<int32_t> created_cs_id;
      int32_t created_count = 0;
      for (int64_t i = 0; i < chunkservers.count(); ++i)
      {
        err = worker_->get_rpc_stub().create_tablet(
          chunkservers.at(i).first, tablet.range_, tschema.create_mem_version_,
          config_.network_timeout);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to create tablet, err=%d tid=%lu cs=%s", err,
                    tablet.range_.table_id_, to_cstring(chunkservers.at(i).first));
        }
        else
        {
          ++created_count;
          created_cs.push_back(chunkservers.at(i).first);
          created_cs_id.push_back(chunkservers.at(i).second);
          TBSYS_LOG(INFO, "create tablet replica, table_id=%lu cs=%s version=%ld replica_num=%d",
                    tablet.range_.table_id_, to_cstring(chunkservers.at(i).first),
                    tschema.create_mem_version_, created_count);
        }
      } // end for
      if (0 >= created_count)
      {
        ret = OB_NO_TABLETS_CREATED;
        TBSYS_LOG(WARN, "no tablet created for create:table_id=%lu", tablet.range_.table_id_);
      }
      else
      {
        ret = create_new_table(false, tablet, created_cs_id, mem_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "create new table failed:table[%lu], ret[%d]", tablet.range_.table_id_, ret);
        }
      }
    }
    if (OB_ENTRY_EXIST == ret)
    {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRootServer2::delete_dropped_tables(int64_t & table_count)
{
  OB_ASSERT(balancer_);
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  common::ObArray<uint64_t> deleted_tables;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL == schema_manager_for_cache_)
  {
    TBSYS_LOG(WARN, "check schema manager failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if (schema_manager_for_cache_->get_status() != ObSchemaManagerV2::ALL_TABLES)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "schema_manager_for_cache_ is not ready, version is %ld, table count is %ld",
        schema_manager_for_cache_->get_version(),
        schema_manager_for_cache_->get_table_count());
  }
  else
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    ret = root_table_->get_deleted_table(*schema_manager_for_cache_, *balancer_, deleted_tables);
    if (OB_SUCCESS == ret)
    {
      table_count = deleted_tables.count();
    }
    else
    {
      TBSYS_LOG(WARN, "failed to get deleted table, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret && table_count > 0)
  {
    // no need write log
    ret = delete_tables(true, deleted_tables);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "delete the droped table from root table failed:table_id[%lu], ret[%d]",
          table_id, ret);
    }
  }
  return ret;
}

int ObRootServer2::check_table_exist(const common::ObString & table_name, bool & exist)
{
  int ret = OB_SUCCESS;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL == schema_manager_for_cache_)
  {
    TBSYS_LOG(WARN, "check schema manager failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    const ObTableSchema * table = schema_manager_for_cache_->get_table_schema(table_name);
    if (NULL == table)
    {
      exist = false;
    }
    else
    {
      exist = true;
    }
  }
  return ret;
}

int ObRootServer2::switch_ini_schema()
{
  int ret = OB_SUCCESS;
  tbsys::CConfig config;
  if (!local_schema_manager_->parse_from_file(config_.schema_filename, config))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "parse schema error chema file is %s ", config_.schema_filename.str());
  }
  else
  {
    ObBootstrap bootstrap(*this);
    ret = bootstrap.bootstrap_ini_tables();
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "create all the ini tables failed:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "create all the ini tables succ");
      // fire an event to tell all clusters
      ret = ObRootTriggerUtil::create_table(root_trigger_);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "trigger event for create table failed:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "trigger event for create table succ for load ini schema");
      }
    }
  }
  return ret;
}

int ObRootServer2::refresh_new_schema(int64_t & table_count)
{
  int ret = OB_SUCCESS;
  // if refresh failed output error log because maybe do succ if reboot
  ObSchemaManagerV2 * out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == out_schema)
  {
    ret = OB_MEM_OVERFLOW;
    TBSYS_LOG(ERROR, "allocate new schema failed");
  }
  else
  {
    // get new schema and update the schema version
    ret = get_schema(true, false, *out_schema);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "force refresh schema manager failed:ret[%d]", ret);
    }
    else
    {
      table_count = out_schema->get_table_count();
      TBSYS_LOG(INFO, "force refresh schema manager succ:version[%ld], table_count=%ld",
          out_schema->get_version(), table_count);
    }
  }
  if (out_schema != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
  }
  return ret;
}

/// for sql api
int ObRootServer2::alter_table(common::AlterTableSchema &tschema)
{
  size_t len = strlen(tschema.table_name_);
  ObString table_name((int32_t)len, (int32_t)len, tschema.table_name_);
  bool is_all_merged = false;
  int ret = check_tablet_version(last_frozen_mem_version_, 1, is_all_merged);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "tablet not merged to the last frozen version:ret[%d], version[%ld]",
        ret, last_frozen_mem_version_);
  }
  else if (true == is_all_merged)
  {
    bool exist = false;
    int err = OB_SUCCESS;
    // LOCK BLOCK
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      ret = check_table_exist(table_name, exist);
      if (OB_SUCCESS == ret)
      {
        if (!exist)
        {
          ret = OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(WARN, "check table not exist:tname[%s]", tschema.table_name_);
        }
      }
      // inner schema table operation
      if (OB_SUCCESS == ret)
      {
        err = ddl_tool_.alter_table(tschema);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "alter table throuth ddl tool failed:tname[%s], err[%d]",
              tschema.table_name_, err);
          ret = err;
        }
      }
      /// refresh the new schema and schema version no mater success or failed
      if (exist)
      {
        int64_t count = 0;
        err = refresh_new_schema(count);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "refresh new schema manager after alter table failed:"
              "tname[%s], err[%d], ret[%d]", tschema.table_name_, err, ret);
          ret = err;
        }
      }
    }
    // notify schema update to all servers
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      {
        TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify switch schema alter table succ:table[%s]",
            tschema.table_name_);
      }
    }
    // only refresh the new schema manager
    // fire an event to tell all clusters
    {
      err = ObRootTriggerUtil::alter_table(root_trigger_);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "trigger event for alter table failed:err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }
  }
  else
  {
    ret = OB_OP_NOT_ALLOW;
    TBSYS_LOG(WARN, "check tablet not merged to the last frozen version:ret[%d], version[%ld]",
        ret, last_frozen_mem_version_);
  }
  return ret;
}

/// for sql api
int ObRootServer2::create_table(bool if_not_exists, const common::TableSchema &tschema)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "create table, if_not_exists=%c table_name=%s",
      if_not_exists?'Y':'N', tschema.table_name_);
  // just for pass the schema checking
  uint64_t old_table_id = tschema.table_id_;
  if (OB_INVALID_ID == old_table_id)
  {
    const_cast<TableSchema &> (tschema).table_id_ = OB_APP_MIN_TABLE_ID - 1;
  }
  else if (tschema.table_id_ < OB_APP_MIN_TABLE_ID && !config_.ddl_system_table_switch)
  {
    TBSYS_LOG(USER_ERROR, "create table failed, while table_id[%ld] less than %ld, and drop system table switch is %s",
        tschema.table_id_, OB_APP_MIN_TABLE_ID, config_.ddl_system_table_switch?"true":"false");
    ret = OB_OP_NOT_ALLOW;
  }
  if(OB_SUCCESS == ret)
  {
    if (!tschema.is_valid())
    {
      TBSYS_LOG(WARN, "table schmea is invalid:table_name[%s]", tschema.table_name_);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      bool is_all_merged = false;
      ret = check_tablet_version(last_frozen_mem_version_, 1, is_all_merged);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "tablet not merged to the last frozen version:ret[%d], version[%ld]",
            ret, last_frozen_mem_version_);
      }
      else if (!is_all_merged)
      {
        ret = OB_OP_NOT_ALLOW;
        TBSYS_LOG(WARN, "check tablet not merged to the last frozen version:ret[%d], version[%ld]",
            ret, last_frozen_mem_version_);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    size_t len = strlen(tschema.table_name_);
    ObString table_name((int32_t)len, (int32_t)len, tschema.table_name_);
    bool exist = false;
    int err = OB_SUCCESS;
    // LOCK BLOCK
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      ret = check_table_exist(table_name, exist);
      if (OB_SUCCESS == ret)
      {
        if (exist && !if_not_exists)
        {
          ret = OB_ENTRY_EXIST;
          TBSYS_LOG(WARN, "check table already exist:tname[%s]", tschema.table_name_);
        }
        else if (exist)
        {
          TBSYS_LOG(INFO, "check table already exist:tname[%s]", tschema.table_name_);
        }
      }
      // inner schema table operation
      if ((OB_SUCCESS == ret) && !exist)
      {
        if (OB_INVALID_ID == old_table_id)
        {
          const_cast<TableSchema &> (tschema).table_id_ = old_table_id;
        }
        err = ddl_tool_.create_table(tschema);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "create table throuth ddl tool failed:tname[%s], err[%d]",
              tschema.table_name_, err);
          ret = err;
        }
      }
      /// refresh the new schema and schema version no mater success or failed
      if (!exist)
      {
        int64_t count = 0;
        err = refresh_new_schema(count);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "refresh new schema manager after create table failed:"
              "tname[%s], err[%d], ret[%d]", tschema.table_name_, err, ret);
          ret = err;
        }
        else
        {
          TBSYS_LOG(INFO, "refresh new schema manager after create table succ:"
              "err[%d], count[%ld], ret[%d]", err, count, ret);
        }
      }
    }
    // notify schema update to all servers
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      {
        TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify switch schema after create table succ:table[%s]",
            tschema.table_name_);
      }
    }
    // only refresh the new schema manager
    // fire an event to tell all clusters
    {
      err = ObRootTriggerUtil::create_table(root_trigger_, tschema.table_id_);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "trigger event for create table failed:err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }
  }
  const_cast<TableSchema &> (tschema).table_id_ = old_table_id;
  return ret;
}

/// for sql api
int ObRootServer2::drop_tables(bool if_exists, const common::ObStrings &tables)
{
  TBSYS_LOG(INFO, "drop table, if_exists=%c tables=[%s]",
            if_exists?'Y':'N', to_cstring(tables));
  ObString table_name;
  bool force_update_schema = false;
  bool is_all_merged = false;
  // at least one replica safe
  int ret = check_tablet_version(last_frozen_mem_version_, 1, is_all_merged);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "tablet not merged to the last frozen version:ret[%d], version[%ld]",
        ret, last_frozen_mem_version_);
  }
  else if (true == is_all_merged)
  {
    // BLOCK
    {
      tbsys::CThreadGuard guard(&mutex_lock_);
      for (int64_t i = 0; i < tables.count(); ++i)
      {
        ret = tables.get_string(i, table_name);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get table failed:index[%ld], ret[%d]", i, ret);
          break;
        }
        else
        {
          bool refresh = false;
          ret = drop_one_table(if_exists, table_name, refresh);
          if (true == refresh)
          {
            force_update_schema = true;
          }
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "drop this table failed:index[%ld], tname[%.*s], ret[%d]",
                i, table_name.length(), table_name.ptr(), ret);
            break;
          }
          else
          {
            TBSYS_LOG(INFO, "drop this table succ:index[%ld], tname[%.*s]",
                i, table_name.length(), table_name.ptr());
          }
        }
      }
    }
    /// refresh the new schema and schema version no mater success or failed
    // if failed maybe restart for sync the local schema according the inner table
    if (force_update_schema)
    {
      int64_t count = 0;
      // if refresh failed output error log because maybe do succ if reboot
      int err = refresh_new_schema(count);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "refresh new schema manager after drop tables failed:"
            "err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }
    // notify schema update to all servers
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      {
        TBSYS_LOG(WARN, "fail to notify switch schema:ret[%d]", ret);
      }
    }
    // only refresh the new schema manager
    // fire an event to tell all clusters
    {
      int err = ObRootTriggerUtil::notify_slave_refresh_schema(root_trigger_);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "trigger event for drop table failed:err[%d], ret[%d]", err, ret);
        ret = err;
      }
    }
  }
  else
  {
    ret = OB_OP_NOT_ALLOW;
    TBSYS_LOG(WARN, "check tablet not merged to the last frozen version:ret[%d], version[%ld]",
        ret, last_frozen_mem_version_);
  }
  return ret;
}

int ObRootServer2::drop_one_table(const bool if_exists, const ObString & table_name, bool & refresh)
{
  // TODO lock tool long time
  bool exist = false;
  refresh = false;
  int ret = check_table_exist(table_name, exist);
  if (OB_SUCCESS == ret)
  {
    if (!exist && !if_exists)
    {
      ret = OB_ENTRY_NOT_EXIST;
      TBSYS_LOG(WARN, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
    }
    else if (!exist)
    {
      TBSYS_LOG(INFO, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
    }
  }
  // inner schema table operation
  const ObTableSchema * table = NULL;
  uint64_t table_id = 0;
  if ((OB_SUCCESS == ret) && exist)
  {
    tbsys::CRLockGuard guard(schema_manager_rwlock_);
    if (NULL == schema_manager_for_cache_)
    {
      TBSYS_LOG(WARN, "check schema manager failed");
      ret = OB_INNER_STAT_ERROR;
    }
    else
    {
      table = schema_manager_for_cache_->get_table_schema(table_name);
      if (NULL == table)
      {
        ret = OB_ENTRY_NOT_EXIST;
        TBSYS_LOG(WARN, "check table not exist:tname[%.*s]", table_name.length(), table_name.ptr());
      }
      else
      {
        table_id = table->get_table_id();
        if (table_id < OB_APP_MIN_TABLE_ID && !config_.ddl_system_table_switch)
        {
          TBSYS_LOG(USER_ERROR, "drop table failed, while table_id[%ld] less than %ld, and drop system table switch is %s",
              table_id, OB_APP_MIN_TABLE_ID, config_.ddl_system_table_switch?"true":"false");
          ret = OB_OP_NOT_ALLOW;

        }
      }
    }
  }
  if ((OB_SUCCESS == ret) && exist)
  {
    refresh = true;
    ret = ddl_tool_.drop_table(table_name);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "drop table throuth ddl tool failed:tname[%.*s], ret[%d]",
          table_name.length(), table_name.ptr(), ret);
    }
    else
    {
      TBSYS_LOG(INFO, "drop table succ:tname[%.*s]", table_name.length(), table_name.ptr());
    }
  }
  if ((OB_SUCCESS == ret) && exist)
  {
    ret = ObRootTriggerUtil::drop_tables(root_trigger_, table_id);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "trigger event for drop table failed:table_id[%ld], ret[%d]", table_id, ret);
    }
  }
  return ret;
}

int ObRootServer2::get_master_ups(ObServer &ups_addr, bool use_inner_port)
{
  int ret = OB_ENTRY_NOT_EXIST;
  ObUps ups_master;
  if (NULL != ups_manager_)
  {
    if (OB_SUCCESS != (ret = ups_manager_->get_ups_master(ups_master)))
    {
      TBSYS_LOG(WARN, "not master ups exist");
    }
    else
    {
      ups_addr = ups_master.addr_;
      if (use_inner_port)
      {
        ups_addr.set_port(ups_master.inner_port_);
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "ups_manager is NULL, check it.");
  }
  return ret;
}

ObServer ObRootServer2::get_update_server_info(bool use_inner_port) const
{
  ObServer server;
  ObUps ups_master;
  if (NULL != ups_manager_)
  {
    ups_manager_->get_ups_master(ups_master);
    server = ups_master.addr_;
    if (use_inner_port)
    {
      server.set_port(ups_master.inner_port_);
    }
  }
  return server;
}

int ObRootServer2::get_table_info(const common::ObString& table_name, uint64_t& table_id, int32_t& max_row_key_length)
{
  int err = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  // not used
  max_row_key_length = 0;
  common::ObSchemaManagerV2 out_schema;
  if (config_.enable_cache_schema)
  {
    TBSYS_LOG(DEBUG, "__enable_cache_schema is on");
    {
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      out_schema = *schema_manager_for_cache_;
    }
    const ObTableSchema* table_schema = out_schema.get_table_schema(table_name);
    if (NULL == table_schema)
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(WARN, "table %.*s schema not exist in cached schema", table_name.length(), table_name.ptr());
    }
    else
    {
      table_id = table_schema->get_table_id();
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "__enable_cache_schema is off");
    // not used
    TableSchema table_schema;
    // will handle if table_name is one of the three core tables, then construct directly, or scan ms
    err = schema_service_->get_table_schema(table_name, table_schema);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "faile to get table %.*s schema, err=%d", table_name.length(), table_name.ptr(), err);
    }
    else
    {
      table_id = table_schema.table_id_;
    }
  }
  return err;
}

int ObRootServer2::do_check_point(const uint64_t ckpt_id)
{
  int ret = OB_SUCCESS;

  const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
  char filename[OB_MAX_FILE_NAME_LENGTH];

  int err = 0;
  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate root table file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS)
  {
    ret = root_table_->write_to_file(filename);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "write root table to file [%s] failed, err=%d", filename, ret);
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate chunk server list file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS)
  {
    ret = server_manager_.write_to_file(filename);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "write chunkserver list to file [%s] failed, err=%d", filename, ret);
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, LOAD_DATA_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate load data list file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS && NULL != balancer_)
  {
    ret = balancer_->write_to_file(filename);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "write load data list to file [%s] failed, err=%d", filename, ret);
    }
  }
  return ret;
}

int ObRootServer2::recover_from_check_point(const int server_status, const uint64_t ckpt_id)
{
  int ret = OB_SUCCESS;

  TBSYS_LOG(INFO, "server status recover from check point is %d", server_status);

  const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
  char filename[OB_MAX_FILE_NAME_LENGTH];

  int err = 0;
  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate root table file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS)
  {
    ret = root_table_->read_from_file(filename);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "recover root table from file [%s] failed, err=%d", filename, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "recover root table, file_name=%s, size=%ld", filename, root_table_->end() - root_table_->begin());
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate chunk server list file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS)
  {
    int32_t cs_num = 0;
    int32_t ms_num = 0;

    ret = server_manager_.read_from_file(filename, cs_num, ms_num);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "recover chunkserver list from file [%s] failed, err=%d", filename, ret);
    }
    else
    {
      TBSYS_LOG(INFO, "recover server list, cs_num=%d ms_num=%d", cs_num, ms_num);
    }
    if (0 < cs_num)
    {
      first_cs_had_registed_ = true;
    }
  }

  err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, LOAD_DATA_EXT);
  if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "generate load data file name failed, err=%d", err);
    ret = OB_BUF_NOT_ENOUGH;
  }

  if (ret == OB_SUCCESS && NULL != balancer_)
  {
    if (!FileDirectoryUtils::exists(filename))
    {
      TBSYS_LOG(ERROR, "load data check point file %s not exist, skip it. some loading table task may be lost.", filename);
    }
    else
    {
      ret = balancer_->read_from_file(filename);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "recover load data from file [%s] failed, err=%d", filename, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "recover load data, file_name=%s, size=%ld", filename, root_table_->end() - root_table_->begin());
      }
    }
  }

  return ret;
}

int ObRootServer2::receive_new_frozen_version(const int64_t rt_version, const int64_t frozen_version,
    const int64_t last_frozen_time, bool did_replay)
{
  int ret = OB_SUCCESS;
  UNUSED(rt_version);
  if (config_.is_import)
  {
    if (OB_SUCCESS != (ret = try_create_new_tables(frozen_version)))
    {
      TBSYS_LOG(WARN, "fail to create new table. ");
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = report_frozen_memtable(frozen_version, last_frozen_time, did_replay)))
    {
      TBSYS_LOG(WARN, "fail to deal with forzen_memtable report. err=%d", ret);
      ret = OB_SUCCESS;
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "fail to create empty tablet. err =%d", ret);
  }
  return ret;
}

bool ObRootServer2::check_all_tablet_safe_merged(void) const
{
  bool ret = false;
  int64_t last_frozen_mem_version = last_frozen_mem_version_;
  int err = check_tablet_version(last_frozen_mem_version, 0, ret);
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "check tablet merged failed:version[%ld], ret[%d]", last_frozen_mem_version, err);
  }
  else if (true == ret)
  {
    TBSYS_LOG(TRACE, "check tablet merged succ:version[%ld]", last_frozen_mem_version);
    tbsys::CRLockGuard guard(root_table_rwlock_);
    if (root_table_ != NULL)
    {
      ret = root_table_->check_lost_range();
      if (ret == false)
      {
        TBSYS_LOG(WARN, "check having lost tablet range");
      }
      else
      {
        TBSYS_LOG(INFO, "check tablet merged succ:version[%ld], result[%d]", last_frozen_mem_version, ret);
      }
    }
  }
  return ret;
}

int ObRootServer2::report_frozen_memtable(const int64_t frozen_version, const int64_t last_frozen_time, bool did_replay)
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  if ( frozen_version < 0 || frozen_version < last_frozen_mem_version_)
  {
    TBSYS_LOG(WARN, "invalid froze_version, version=%ld last_frozen_version=%ld",
        frozen_version, last_frozen_mem_version_);
    ret = OB_ERROR;
  }
  else
  {
    // check last frozen version merged safely
    if (!did_replay && is_master() && (!check_all_tablet_safe_merged()))
    {
      TBSYS_LOG(WARN, "merge is too slow, last_version=%ld curr_version=%ld",
          last_frozen_mem_version_, frozen_version); //just warn
    }
  }

  if (OB_SUCCESS == ret)
  {
    atomic_exchange((uint64_t*) &last_frozen_mem_version_, frozen_version);
    last_frozen_time_ = did_replay ? last_frozen_time : tbsys::CTimeUtil::getTime();
    TBSYS_LOG(INFO, "frozen_version=%ld last_frozen_time=%ld did_replay=%d",
        last_frozen_mem_version_, last_frozen_time_, did_replay);
  }

  if (OB_SUCCESS == ret)
  {
    if (is_master() && !did_replay)
    {
      log_worker_->sync_us_frozen_version(last_frozen_mem_version_, last_frozen_time_);
    }
  }
  if (OB_SUCCESS != ret && did_replay && last_frozen_mem_version_ >= frozen_version)
  {
    TBSYS_LOG(ERROR, "RS meet a unlegal forzen_version[%ld] while replay. last_frozen_verson=%ld, ignore the err",
        frozen_version, last_frozen_mem_version_);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObRootServer2::get_last_frozen_version_from_ups(const bool did_replay)
{
  int64_t frozen_version = -1;
  int ret = OB_SUCCESS;
  ObServer ups = get_update_server_info(false);
  if (0 == ups.get_ipv4())
  {
    TBSYS_LOG(INFO, "no ups right now, sleep for next round");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = worker_->get_rpc_stub().get_last_frozen_version(
          ups, config_.network_timeout, frozen_version)))
  {
    TBSYS_LOG(WARN, "failed to get frozen version, err=%d ups=%s", ret, ups.to_cstring());
  }
  else if (0 >= frozen_version)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "invalid frozen version=%ld ups=%s", frozen_version, ups.to_cstring());
  }
  else
  {
    tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
    TBSYS_LOG(INFO, "got last frozen version, ups=%s version=%ld", ups.to_cstring(), frozen_version);
    if (last_frozen_mem_version_ != frozen_version && frozen_version > 0)
    {
      atomic_exchange((uint64_t*) &last_frozen_mem_version_, frozen_version);
      if (is_master() && !did_replay)
      {
        log_worker_->sync_us_frozen_version(last_frozen_mem_version_, 0);
      }
    }
  }
  return ret;
}
int64_t ObRootServer2::get_time_stamp_changing() const
{
  return time_stamp_changing_;
}
int64_t ObRootServer2::get_lease() const
{
  return config_.cs_lease_duration_time;
}

int ObRootServer2::request_cs_report_tablet()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "request cs report tablet start:");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->status_ != ObServerStatus::STATUS_DEAD)
    {
      if (OB_SUCCESS != (ret = worker_->get_rpc_stub().request_report_tablet(it->server_)))
      {
        TBSYS_LOG(WARN, "fail to request cs to report. cs_addr=%s", it->server_.to_cstring());
      }
    }
  }
  return ret;
}

const ObiRole& ObRootServer2::get_obi_role() const
{
  return obi_role_;
}

int ObRootServer2::set_obi_role(const ObiRole& role)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else if (ObiRole::INIT == role.get_role())
  {
    TBSYS_LOG(WARN, "role cannot be INIT");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    obi_role_.set_role(role.get_role());
    TBSYS_LOG(INFO, "set obi role, role=%s", role.get_role_str());
    if (ObRoleMgr::MASTER == worker_->get_role_manager()->get_role())
    {
      if (OB_SUCCESS != (ret = worker_->send_obi_role(obi_role_)))
      {
        TBSYS_LOG(WARN, "fail to set slave_rs's obi role err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ups_manager_->send_obi_role()))
      {
        TBSYS_LOG(WARN, "failed to set updateservers' obi role, err=%d", ret);
      }
    }
  }
  return ret;
}

int ObRootServer2::get_master_ups_config(int32_t &master_master_ups_read_percent, int32_t &slave_master_ups_read_percent) const
{
  int ret = OB_SUCCESS;
  if (ups_manager_ != NULL)
  {
    ups_manager_->get_master_ups_config(master_master_ups_read_percent, slave_master_ups_read_percent);
  }
  return ret;
}

void ObRootServer2::cancel_restart_all_cs()
{
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  server_manager_.cancel_restart_all_cs();
}

void ObRootServer2::restart_all_cs()
{
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  server_manager_.restart_all_cs();
  if(NULL != balancer_thread_)
  {
    balancer_thread_->wakeup();
  }
  else
  {
    TBSYS_LOG(WARN, "balancer_thread_ is null");
  }
}

int ObRootServer2::shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
{
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  ret = server_manager_.shutdown_cs(servers, op);
  if(RESTART == op)
  {
    if(NULL != balancer_thread_)
    {
      balancer_thread_->wakeup();
    }
    else
    {
      TBSYS_LOG(WARN, "balancer_thread_ is null");
    }
  }
  return ret;
}

int ObRootServer2::cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
{
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  ret = server_manager_.cancel_shutdown_cs(servers, op);
  return ret;
}

void ObRootServer2::do_stat_common(char *buf, const int64_t buf_len, int64_t& pos)
{
  do_stat_start_time(buf, buf_len, pos);
  do_stat_local_time(buf, buf_len, pos);
  databuff_printf(buf, buf_len, pos, "prog_version: %s(%s)\n", PACKAGE_STRING, RELEASEID);
  databuff_printf(buf, buf_len, pos, "pid: %d\n", getpid());
  databuff_printf(buf, buf_len, pos, "obi_role: %s\n", obi_role_.get_role_str());
  ob_print_mod_memory_usage();
}

void ObRootServer2::do_stat_start_time(char *buf, const int64_t buf_len, int64_t& pos)
{
  databuff_printf(buf, buf_len, pos, "start_time: %s", ctime(&start_time_));
}

void ObRootServer2::do_stat_local_time(char *buf, const int64_t buf_len, int64_t& pos)
{
  time_t now = time(NULL);
  databuff_printf(buf, buf_len, pos, "local_time: %s", ctime(&now));
}

void ObRootServer2::do_stat_schema_version(char* buf, const int64_t buf_len, int64_t &pos)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
}

void ObRootServer2::do_stat_frozen_time(char* buf, const int64_t buf_len, int64_t &pos)
{
  tbutil::Time frozen_time = tbutil::Time::microSeconds(last_frozen_time_);
  struct timeval frozen_tv(frozen_time);
  struct tm stm;
  localtime_r(&frozen_tv.tv_sec, &stm);
  char time_buf[32];
  strftime(time_buf, sizeof(time_buf), "%F %H:%M:%S", &stm);
  databuff_printf(buf, buf_len, pos, "frozen_time: %lu(%s)", last_frozen_time_, time_buf);
}

void ObRootServer2::do_stat_mem(char* buf, const int64_t buf_len, int64_t &pos)
{
  struct mallinfo minfo = mallinfo();
  databuff_printf(buf, buf_len, pos, "mem: arena=%d ordblks=%d hblkhd=%d uordblks=%d fordblks=%d keepcost=%d",
                  minfo.arena, minfo.ordblks, minfo.hblkhd, minfo.uordblks, minfo.fordblks, minfo.keepcost);
}

void ObRootServer2::do_stat_table_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
}

void ObRootServer2::do_stat_tablet_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int64_t num = -1;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != tablet_manager_)
  {
    num = tablet_manager_->end() - tablet_manager_->begin();
  }
  databuff_printf(buf, buf_len, pos, "tablet_num: %ld", num);
}

int ObRootServer2::table_exist_in_cs(const uint64_t table_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      if (OB_SUCCESS != (ret = worker_->get_rpc_stub().table_exist_in_cs(it->server_,
                                                                         config_.network_timeout,
                                                                         table_id, is_exist)))
      {
        TBSYS_LOG(WARN, "fail to check table info in cs[%s], ret=%d",
                  to_cstring(it->server_), ret);
      }
      else
      {
        if (true == is_exist)
        {
          TBSYS_LOG(INFO, "table already exist in chunkserver."
                    " table_id=%lu, cs_addr=%s", table_id, to_cstring(it->server_));
          break;
        }
      }
    }
  }
  return ret;
}
void ObRootServer2::do_stat_cs(char* buf, const int64_t buf_len, int64_t &pos)
{
  ObServer tmp_server;
  databuff_printf(buf, buf_len, pos, "chunkservers: ");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      tmp_server = it->server_;
      tmp_server.set_port(it->port_cs_);
      databuff_printf(buf, buf_len, pos, "%s ", to_cstring(tmp_server));
    }
  }
}

void ObRootServer2::do_stat_cs_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int num = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      num++;
    }
  }
  databuff_printf(buf, buf_len, pos, "cs_num: %d", num);
}

void ObRootServer2::do_stat_ms(char* buf, const int64_t buf_len, int64_t &pos)
{
  ObServer tmp_server;
  databuff_printf(buf, buf_len, pos, "mergeservers: ");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_ms_ != 0
        && it->ms_status_ != ObServerStatus::STATUS_DEAD)
    {
      tmp_server = it->server_;
      tmp_server.set_port(it->port_ms_);
      databuff_printf(buf, buf_len, pos, "%s ", to_cstring(tmp_server));
    }
  }
}

void ObRootServer2::do_stat_ms_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int num = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_ms_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      num++;
    }
  }
  databuff_printf(buf, buf_len, pos, "cs_num: %d", num);
}
void ObRootServer2::do_stat_all_server(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (NULL != ups_manager_)
  {
    ups_manager_->print(buf, buf_len, pos);
  }
  databuff_printf(buf, buf_len, pos, "\n");
  do_stat_cs(buf, buf_len, pos);
  databuff_printf(buf, buf_len, pos, "\n");
  do_stat_ms(buf, buf_len, pos);
}

void ObRootServer2::do_stat_ups(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (NULL != ups_manager_)
  {
    ups_manager_->print(buf, buf_len, pos);
  }
}

int64_t ObRootServer2::get_stat_value(const int32_t index)
{
  int64_t ret = 0;
  ObStat *stat = NULL;
  OB_STAT_GET(ROOTSERVER, stat);
  if (NULL != stat)
  {
    ret = stat->get_value(index);
  }
  return ret;
}

void ObRootServer2::do_stat_merge(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (last_frozen_time_ == 0)
  {
    databuff_printf(buf, buf_len, pos, "merge: DONE");
  }
  else
  {
    if (check_all_tablet_safe_merged())
    {
      databuff_printf(buf, buf_len, pos, "merge: DONE");
    }
    else
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      int64_t max_merge_duration_us = config_.max_merge_duration_time;
      if (now > last_frozen_time_ + max_merge_duration_us)
      {
        databuff_printf(buf, buf_len, pos, "merge: TIMEOUT");
      }
      else
      {
        databuff_printf(buf, buf_len, pos, "merge: DOING");
      }
    }
  }
}

void ObRootServer2::do_stat_unusual_tablets_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int32_t num = 0;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (root_table_ != NULL)
  {
    root_table_->dump_unusual_tablets(last_frozen_mem_version_,
                                      (int32_t)config_.tablet_replicas_num, num);
  }
  databuff_printf(buf, buf_len, pos, "unusual_tablets_num: %d", num);
}

int ObRootServer2::do_stat(int stat_key, char *buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  switch(stat_key)
  {
    case OB_RS_STAT_COMMON:
      do_stat_common(buf, buf_len, pos);
      break;
    case OB_RS_STAT_START_TIME:
      do_stat_start_time(buf, buf_len, pos);
      break;
    case OB_RS_STAT_LOCAL_TIME:
      do_stat_local_time(buf, buf_len, pos);
      break;
    case OB_RS_STAT_PROGRAM_VERSION:
      databuff_printf(buf, buf_len, pos, "prog_version: %s(%s)", PACKAGE_STRING, RELEASEID);
      break;
    case OB_RS_STAT_PID:
      databuff_printf(buf, buf_len, pos, "pid: %d", getpid());
      break;
    case OB_RS_STAT_MEM:
      do_stat_mem(buf, buf_len, pos);
      break;
    case OB_RS_STAT_RS_STATUS:
      databuff_printf(buf, buf_len, pos, "rs_status: INITED");
      break;
    case OB_RS_STAT_FROZEN_VERSION:
      databuff_printf(buf, buf_len, pos, "frozen_version: %ld", last_frozen_mem_version_);
      break;
    case OB_RS_STAT_SCHEMA_VERSION:
      do_stat_schema_version(buf, buf_len, pos);
      break;
    case OB_RS_STAT_LOG_SEQUENCE:
      databuff_printf(buf, buf_len, pos, "log_seq: %lu", log_worker_->get_cur_log_seq());
      break;
    case OB_RS_STAT_LOG_FILE_ID:
      databuff_printf(buf, buf_len, pos, "log_file_id: %lu", log_worker_->get_cur_log_file_id());
      break;
    case OB_RS_STAT_TABLE_NUM:
      do_stat_table_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_TABLET_NUM:
      do_stat_tablet_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_CS:
      do_stat_cs(buf, buf_len, pos);
      break;
    case OB_RS_STAT_MS:
      do_stat_ms(buf, buf_len, pos);
      break;
    case OB_RS_STAT_UPS:
      do_stat_ups(buf, buf_len, pos);
      break;
    case OB_RS_STAT_ALL_SERVER:
      do_stat_all_server(buf, buf_len, pos);
      break;
    case OB_RS_STAT_FROZEN_TIME:
      do_stat_frozen_time(buf, buf_len, pos);
      break;
    case OB_RS_STAT_SSTABLE_DIST:
      balancer_->nb_print_balance_infos(buf, buf_len, pos);
      break;
    case OB_RS_STAT_OPS_GET:
      databuff_printf(buf, buf_len, pos, "ops_get: %ld", get_stat_value(INDEX_SUCCESS_GET_COUNT));
      break;
    case OB_RS_STAT_OPS_SCAN:
      databuff_printf(buf, buf_len, pos, "ops_scan: %ld", get_stat_value(INDEX_SUCCESS_SCAN_COUNT));
      break;
    case OB_RS_STAT_FAIL_GET_COUNT:
      databuff_printf(buf, buf_len, pos, "fail_get_count: %ld", get_stat_value(INDEX_FAIL_GET_COUNT));
      break;
    case OB_RS_STAT_FAIL_SCAN_COUNT:
      databuff_printf(buf, buf_len, pos, "fail_scan_count: %ld", get_stat_value(INDEX_FAIL_SCAN_COUNT));
      break;
    case OB_RS_STAT_GET_OBI_ROLE_COUNT:
      databuff_printf(buf, buf_len, pos, "get_obi_role_count: %ld", get_stat_value(INDEX_GET_OBI_ROLE_COUNT));
      break;
    case OB_RS_STAT_MIGRATE_COUNT:
      databuff_printf(buf, buf_len, pos, "migrate_count: %ld", get_stat_value(INDEX_MIGRATE_COUNT));
      break;
    case OB_RS_STAT_COPY_COUNT:
      databuff_printf(buf, buf_len, pos, "copy_count: %ld", get_stat_value(INDEX_COPY_COUNT));
      break;
    case OB_RS_STAT_TABLE_COUNT:
      databuff_printf(buf, buf_len, pos, "table_count: %ld", get_stat_value(INDEX_ALL_TABLE_COUNT));
      break;
    case OB_RS_STAT_TABLET_COUNT:
      databuff_printf(buf, buf_len, pos, "tablet_count: %ld", get_stat_value(INDEX_ALL_TABLET_COUNT));
      break;
    case OB_RS_STAT_ROW_COUNT:
      databuff_printf(buf, buf_len, pos, "row_count: %ld", get_stat_value(INDEX_ALL_ROW_COUNT));
      break;
    case OB_RS_STAT_DATA_SIZE:
      databuff_printf(buf, buf_len, pos, "data_size: %ld", get_stat_value(INDEX_ALL_DATA_SIZE));
      break;
    case OB_RS_STAT_CS_NUM:
      do_stat_cs_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_MS_NUM:
      do_stat_ms_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_MERGE:
      do_stat_merge(buf, buf_len, pos);
      break;
    case OB_RS_STAT_UNUSUAL_TABLETS_NUM:
      do_stat_unusual_tablets_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_SHUTDOWN_CS:
      balancer_->nb_print_shutting_down_progress(buf, buf_len, pos);
      break;
    case OB_RS_STAT_REPLICAS_NUM:
    default:
      databuff_printf(buf, buf_len, pos, "unknown or not implemented yet, stat_key=%d", stat_key);
      break;
  }
  return ret;
}

int ObRootServer2::make_checkpointing()
{
  tbsys::CRLockGuard rt_guard(root_table_rwlock_);
  tbsys::CRLockGuard cs_guard(server_manager_rwlock_);
  tbsys::CThreadGuard log_guard(worker_->get_log_manager()->get_log_sync_mutex());
  int ret = worker_->get_log_manager()->do_check_point();
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "failed to make checkpointing, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "made checkpointing");
  }
  return ret;
}

int ObRootServer2::register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease, const char *server_version)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_ALREADY_REGISTERED ==
           (ret = ups_manager_->register_ups(addr, inner_port,
                                             log_seq_num, lease, server_version)))
  {
    /* by design */
  }
  else if (OB_SUCCESS == ret)
  {
    // do nothing
  }
  else
  {
    TBSYS_LOG(ERROR, "register ups error, addr: [%s], inner_port: [%d], "
        "seq_num: [%ld] ret: [%d]", to_cstring(addr), inner_port, log_seq_num, ret);
  }
  return ret;
}

int ObRootServer2::receive_ups_heartbeat_resp(const common::ObServer &addr, ObUpsStatus stat,
                                              const common::ObiRole &obi_role)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->renew_lease(addr, stat, obi_role);
  }
  return ret;
}

int ObRootServer2::ups_slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->slave_failure(addr, slave_addr);
  }
  return ret;
}

int ObRootServer2::set_ups_config(int32_t read_master_master_ups_percentage, int32_t read_slave_master_ups_percentage)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->set_ups_config(read_master_master_ups_percentage, read_slave_master_ups_percentage);
  }
  return ret;
}
int ObRootServer2::set_ups_config(const common::ObServer &ups, int32_t ms_read_percentage, int32_t cs_read_percentage)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->set_ups_config(ups, ms_read_percentage, cs_read_percentage);
  }
  return ret;
}

int ObRootServer2::change_ups_master(const common::ObServer &ups, bool did_force)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->set_ups_master(ups, did_force);
  }
  return ret;
}

int ObRootServer2::get_ups_list(common::ObUpsList &ups_list)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->get_ups_list(ups_list);
  }
  return ret;
}

int ObRootServer2::serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const
{
  return server_manager_.serialize_cs_list(buf, buf_len, pos);
}

int ObRootServer2::serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const
{
  return server_manager_.serialize_ms_list(buf, buf_len, pos);
}

int ObRootServer2::serialize_proxy_list(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;

  const ObServerStatus* it = NULL;
  int ms_num = 0;
  for (it = server_manager_.begin(); it != server_manager_.end(); ++it)
  {
    if (ObServerStatus::STATUS_DEAD != it->ms_status_)
    {
      ms_num++;
    }
  }
  ret = serialization::encode_vi32(buf, buf_len, pos, ms_num);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "serialize error");
  }
  else
  {
    int i = 0;
    for (it = server_manager_.begin();
         it != server_manager_.end() && i < ms_num; ++it)
    {
      if (ObServerStatus::STATUS_DEAD != it->ms_status_)
      {
        ObServer addr = it->server_;
        addr.set_port((int32_t)config_.obconnector_port);
        ret = addr.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObServer serialize error, ret=%d", ret);
          break;
        }
        else
        {
          int64_t reserved = 0;
          ret = serialization::encode_vi64(buf, buf_len, pos, reserved);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "encode_vi64 error, ret=%d", ret);
            break;
          }
          else
          {
            ++i;
          }
        }
      }
    }
  }
  return ret;
}

int ObRootServer2::grant_eternal_ups_lease()
{
  int ret = OB_SUCCESS;
  if (NULL == ups_heartbeat_thread_
      || NULL == ups_check_thread_)
  {
    TBSYS_LOG(ERROR, "ups_heartbeat_thread is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ups_check_thread_->stop();
    ups_heartbeat_thread_->stop();
  }
  return ret;
}

int ObRootServer2::cs_import_tablets(const uint64_t table_id, const int64_t tablet_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id)
  {
    TBSYS_LOG(WARN, "invalid table id");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (0 > tablet_version)
  {
    TBSYS_LOG(WARN, "invalid table version, version=%ld", tablet_version);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    int64_t version = tablet_version;
    if (0 == version)
    {
      version = last_frozen_mem_version_;
    }
    int32_t sent_num = 0;
    ObServer cs;
    ObChunkServerManager::iterator it = server_manager_.begin();
    for (; it != server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        cs = it->server_;
        cs.set_port(it->port_cs_);
        // CS收到消息后立即返回成功，然后再完成实际的导入
        if (OB_SUCCESS != (ret = worker_->get_rpc_stub().import_tablets(cs, table_id, version, config_.network_timeout)))
        {
          TBSYS_LOG(WARN, "failed to send msg to cs, err=%d", ret);
          break;
        }
        sent_num++;
      }
    }
    if (OB_SUCCESS == ret && 0 == sent_num)
    {
      TBSYS_LOG(WARN, "no chunkserver");
      ret = OB_DATA_NOT_SERVE;
    }
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "send msg to import tablets, table_id=%lu version=%ld cs_num=%d",
                table_id, version, sent_num);
    }
    else
    {
      TBSYS_LOG(WARN, "failed to import tablets, err=%d table_id=%lu version=%ld cs_num=%d",
                ret, table_id, version, sent_num);
    }
  }
  return ret;
}

bool ObRootServer2::is_master() const
{
  return worker_->get_role_manager()->is_master();
}

int ObRootServer2::receive_hb(const common::ObServer& server, const int32_t sql_port, const bool is_listen_ms, const ObRole role)
{
  int64_t  now = tbsys::CTimeUtil::getTime();
  int err = server_manager_.receive_hb(server, now, role == OB_MERGESERVER ? true : false, is_listen_ms, sql_port);
  // realive or regist again
  if (2 == err)
  {
    if (role != OB_MERGESERVER)
    {
      TBSYS_LOG(INFO, "receive cs alive heartbeat. need add commit log and force to report tablet");
      if (OB_SUCCESS != (err = log_worker_->regist_cs(server, "hb server version null", now)))
      {
        TBSYS_LOG(WARN, "fail to write log for regist_cs. err=%d", err);
      }
      else
      {
        err = worker_->get_rpc_stub().request_report_tablet(server);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "fail to force cs to report tablet info. server=%s, err=%d", server.to_cstring(), err);
        }
      }
    }
    TBSYS_LOG(INFO, "receive server alive heartbeat without version info:server[%s]", server.to_cstring());
    if (is_master())
    {
      if (OB_CHUNKSERVER == role)
      {
        commit_task(SERVER_ONLINE, OB_CHUNKSERVER, server, 0, "hb server version null");
      }
      // not listener merge server
      else if (!is_listen_ms)
      {
        commit_task(SERVER_ONLINE, OB_MERGESERVER, server, sql_port, "hb server version null");
      }
    }
  }
  return err;
}

int ObRootServer2::force_heartbeat_all_servers(void)
{ // only used in boot strap, so no need to lock last_frozen_mem_version_
  int ret = OB_SUCCESS;
  ObServer tmp_server;
  if (this->is_master())
  {
    tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
    ObChunkServerManager::iterator it = this->server_manager_.begin();
    for (; OB_SUCCESS == ret && it != this->server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD && it->port_cs_ != 0)
      {
        tmp_server = it->server_;
        tmp_server.set_port(it->port_cs_);
        ret = worker_->get_rpc_stub().heartbeat_to_cs(tmp_server,
            config_.cs_lease_duration_time, last_frozen_mem_version_,
            get_schema_version(), get_config_version());
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "force hearbeat to cs %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "foce heartbeat to cs %s failed", to_cstring(tmp_server));
        }
      }
      if (OB_SUCCESS == ret && it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
      {
        tmp_server = it->server_;
        tmp_server.set_port(it->port_ms_);
        ret = worker_->get_rpc_stub().heartbeat_to_ms(tmp_server,
            config_.cs_lease_duration_time, last_frozen_mem_version_,
            get_schema_version(), get_obi_role(),
            get_privilege_version(), get_config_version());
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "force hearbeat to ms %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "foce heartbeat to ms %s failed", to_cstring(tmp_server));
        }
      }
    } //end for
  } //end if master
  return ret;
}

int ObRootServer2::force_sync_schema_all_servers(const ObSchemaManagerV2 &schema)
{
  int ret = OB_SUCCESS;
  ObServer tmp_server;
  if (this->is_master())
  {
    ObChunkServerManager::iterator it = this->server_manager_.begin();
    for (; OB_SUCCESS == ret && it != this->server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD && it->port_cs_ != 0)
      {
        tmp_server = it->server_;
        tmp_server.set_port(it->port_cs_);
        ret = this->worker_->get_rpc_stub().switch_schema(tmp_server,
            schema, config_.network_timeout);
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(DEBUG, "sync schema to cs %s, version[%ld]", to_cstring(tmp_server), schema.get_version());
        }
        else if (OB_OLD_SCHEMA_VERSION == ret)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "no need to sync schema cs %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "sync schema to cs %s failed", to_cstring(tmp_server));
        }
      }
      if (OB_SUCCESS == ret && it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
      {
        //hb to ms
        tmp_server = it->server_;
        tmp_server.set_port(it->port_ms_);
        ret = this->worker_->get_rpc_stub().switch_schema(tmp_server,
            schema, config_.network_timeout);
        if (OB_SUCCESS == ret)
        {
          // do nothing
          TBSYS_LOG(DEBUG, "sync schema to ms %s, version[%ld]", to_cstring(tmp_server), schema.get_version());
        }
        else if (OB_OLD_SCHEMA_VERSION == ret)
        {
          ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "no need to sync schema ms %s", to_cstring(tmp_server));
        }
        else
        {
          TBSYS_LOG(WARN, "sync schema to ms %s failed", to_cstring(tmp_server));
        }
      }
    } //end for
  } //end if master
  return ret;
}
//for bypass
int ObRootServer2::prepare_bypass_process(common::ObBypassTaskInfo &table_name_id)
{
  int ret = OB_SUCCESS;
  uint64_t max_table_id = 0;
  {
    tbsys::CWLockGuard guard(schema_manager_rwlock_);
    tbsys::CThreadGuard mutex_guard(&mutex_lock_);
    ret = schema_service_->get_max_used_table_id(max_table_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get max used table id. ret=%d", ret);
    }
    else
    {
      ret = get_new_table_id(max_table_id, table_name_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to prepare for bypass");
      }
      else
      {
        ret = ddl_tool_.update_max_table_id(max_table_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to update max table id. ret=%d,max_table_id=%ld", ret, max_table_id);
        }
      }
    }
  }
  int64_t count = 0;
  if (OB_SUCCESS != (ret = refresh_new_schema(count)))
  {
    TBSYS_LOG(WARN, "refresh new schema manager after modify table_id failed. err=%d", ret);
  }
  else
  {
    if (OB_SUCCESS != (ret = notify_switch_schema(false)))
    {
      TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "notify switch schema after modify table_id");
      write_schema_to_file();
    }
  }
  return ret;
}
int ObRootServer2::write_schema_to_file()
{
  int ret = OB_SUCCESS;
  ObSchemaManagerV2 *out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == out_schema)
  {
    ret = OB_MEM_OVERFLOW;
    TBSYS_LOG(ERROR, "allocate new schema failed");
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_schema(false, false, *out_schema)))
    {
      TBSYS_LOG(WARN, "fail to get schema. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    uint64_t max_table_id = 0;
    ret = schema_service_->get_max_used_table_id(max_table_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get max used table id, ret=%d", ret);
    }
    else
    {
      out_schema->set_max_table_id(max_table_id);
    }
  }
  if (OB_SUCCESS == ret)
  {

    ret = out_schema->write_to_file(SCHEMA_FILE_NAME);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "write schema to file. ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "write schema to file success.");
    }
  }
  if (out_schema != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
  }
  return ret;
}
int ObRootServer2:: get_new_table_id(uint64_t &max_table_id, ObBypassTaskInfo &table_name_id)
{
  int ret = OB_SUCCESS;
  if (table_name_id.count() <= 0)
  {
    TBSYS_LOG(WARN, "invalid argument. table_name_id.count=%ld", table_name_id.count());
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < table_name_id.count(); i++)
    {
      table_name_id.at(i).second = ++max_table_id;
    }
  }
  return ret;
}

int ObRootServer2::slave_clean_root_table()
{
  int ret = OB_SUCCESS;

  { // lock root table
    tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
    tbsys::CWLockGuard guard(root_table_rwlock_);
    TBSYS_LOG(INFO, "slave rootserver start to clean root table, and set bypass version to zero");
    root_table_->clear();
  }

  { // lock frozen version
    tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
    bypass_process_frozen_men_version_ = 0;
  }
  return ret;
}

int ObRootServer2::clean_root_table()
{
  int ret = OB_SUCCESS;
  ObBypassTaskInfo table_name_id;
  table_name_id.set_operation_type(CLEAN_ROOT_TABLE);
  ret = start_bypass_process(table_name_id);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to start clean roottable. ret=%d", ret);
  }
  else
  {
    //bugfix
    //send clean_root_table status to slave rootserver
    ret = log_worker_->clean_root_table();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to write log for clean_root_table. ret=%d", ret);
    }
  }
  return ret;
}

int ObRootServer2::start_bypass_process(ObBypassTaskInfo &table_name_id)
{
  int ret = OB_SUCCESS;
  bool is_all_merged = false;
  if (CLEAN_ROOT_TABLE != table_name_id.get_operation_type())
  {
    //check rs stat
    ret = check_tablet_version(last_frozen_mem_version_, 1, is_all_merged);
    if (!is_all_merged)
    {
      TBSYS_LOG(WARN, "rootserer refuse to do bypass process. state=%s", state_.get_state_str());
      ret = OB_RS_STATE_NOT_ALLOW;
    }
  }
  //保证导入汇报的过程中串行进行
  if (OB_SUCCESS == ret)
  {
    ret = state_.set_bypass_flag(true);
    if (OB_SUCCESS != ret)
    {
      ret = OB_EAGAIN;
      TBSYS_LOG(WARN, "already have some operaiton task doing. retry later.");
    }
    else
    {
      TBSYS_LOG(INFO, "opration process start. type=%d", table_name_id.get_operation_type());
    }
  }
  if (OB_SUCCESS == ret)
  {
    tbsys::CWLockGuard guard(schema_manager_rwlock_);
    ret = operation_helper_.start_operation(schema_manager_for_cache_, table_name_id, last_frozen_mem_version_);
    if (OB_SUCCESS != ret)
    {
      state_.set_bypass_flag(false);
      TBSYS_LOG(WARN, "fail to start operation process. ,set false.ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(TRACE, "operation_helper start to operaiton success.");
    }
  }
  if (OB_SUCCESS == ret)
  {
    //记录当前的frozen_version，防止RS重启后状态丢失，造成CS开始每日合并
    tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
    if (0 >= bypass_process_frozen_men_version_)
    {
      bypass_process_frozen_men_version_ = last_frozen_mem_version_;
      TBSYS_LOG(INFO, "operation process start. bypass process frozen_mem_version=%ld", bypass_process_frozen_men_version_);
      log_worker_->set_frozen_version_for_brodcast(bypass_process_frozen_men_version_);
    }
  }

  if (OB_SUCCESS == ret)
  {
    operation_duty_.init(worker_);
    if (OB_SUCCESS != (ret = timer_.schedule(operation_duty_, 30 * 1000 * 1000, false)))
    {
      TBSYS_LOG(WARN, "fail to schedule operation duty, ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "start to schedule check_process.");
    }
  }
  return ret;
}

int ObRootServer2::check_bypass_process()
{
  int ret = OB_SUCCESS;
  if (!state_.get_bypass_flag())
  {
    TBSYS_LOG(WARN, "rootserver have no bypass duty now.");
    ret = OB_ERROR;
  }
  OperationType type;
  if (OB_SUCCESS == ret)
  {
    ret = operation_helper_.check_process(type);
    if (OB_BYPASS_TIMEOUT == ret)
    {
      TBSYS_LOG(WARN, "bypass process timeout. check it");
      state_.set_bypass_flag(false);
      tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
      bypass_process_frozen_men_version_ = -1;
      //set_root_server_state();
      log_worker_->set_frozen_version_for_brodcast(bypass_process_frozen_men_version_);
    }
    else if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "operation process finished. operation_type = %d", type);
      state_.set_bypass_flag(false);
      tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
      //set_root_server_state();
      bypass_process_frozen_men_version_ = 0;
      log_worker_->set_frozen_version_for_brodcast(bypass_process_frozen_men_version_);
    }
    else
    {
      if (OB_SUCCESS != timer_.schedule(operation_duty_, 30 * 1000 * 1000, false))
      {
        TBSYS_LOG(WARN, "fail to schedule operation duty, ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "scheule operation_duty success. time=30000000");
      }
    }
  }
  return ret;
}

int ObRootServer2::bypass_meta_data_finished(const OperationType type, ObRootTable2 *root_table,
    ObTabletInfoManager *tablet_manager, common::ObSchemaManagerV2 *schema_mgr)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(INFO, "bypass new meta data is ready. try to refresh root_server's");
  switch(type)
  {
    case IMPORT_TABLE:
      {
        common::ObArray<uint64_t> delete_tables;
        if (OB_SUCCESS != (ret = use_new_root_table(root_table, tablet_manager)))
        {
          TBSYS_LOG(WARN, "operation over, use new root table fail. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = switch_bypass_schema(schema_mgr, delete_tables)))
        {
          TBSYS_LOG(WARN, "swtich bypass schema fail, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = operation_helper_.set_delete_table(delete_tables)))
        {
          TBSYS_LOG(WARN, "fail to set delete table. ret=%d", ret);
        }
      }
      break;
    case IMPORT_ALL:
      {
        if (OB_SUCCESS != (ret = use_new_root_table(root_table, tablet_manager)))
        {
          TBSYS_LOG(WARN, "fail to use new root table. ret=%d", ret);
        }
      }
      break;
    case CLEAN_ROOT_TABLE:
      {
        if (OB_SUCCESS != (ret = use_new_root_table(root_table, tablet_manager)))
        {
          TBSYS_LOG(INFO, "fail to use new root table, ret=%d", ret);
        }
      }
      break;
    default:
      TBSYS_LOG(WARN, "invalid type. type=%d", type);
      break;
  }
  return ret;
}
const char* ObRootServer2::get_bypass_state() const
{
  if (const_cast<ObRootServerState&>(state_).get_bypass_flag())
  {
    return "DOING";
  }
  else if (!const_cast<ObRootServerState&>(state_).get_bypass_flag() && 0 < bypass_process_frozen_men_version_)
  {
    return "RETRY";
  }
  else if (0 == bypass_process_frozen_men_version_)
  {
    return "DONE";
  }
  else if (-1 == bypass_process_frozen_men_version_)
  {
    return "INTERRUPT";
  }
  else
  {
    return "ERROR";
  }
}
int ObRootServer2::switch_bypass_schema(common::ObSchemaManagerV2 *schema_manager, common::ObArray<uint64_t> &delete_tables)
{
  int ret = OB_SUCCESS;
  delete_tables.clear();
  if (NULL == schema_manager)
  {
    TBSYS_LOG(ERROR, "invalid schema_manager. is NULL");
    ret = OB_ERROR;
  }
  ObSchemaManagerV2 * out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == out_schema)
  {
    ret = OB_MEM_OVERFLOW;
    TBSYS_LOG(ERROR, "allocate new schema failed");
  }
  if (OB_SUCCESS == ret)
  {
    ret = get_schema(false, false, *out_schema);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "force refresh schema manager failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = get_deleted_tables(*out_schema, *schema_manager, delete_tables);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get delete tables. ret=%d", ret);
    }
  }
  common::ObArray<TableSchema> table_schema_array;
  common::ObArray<TableSchema> new_table_schema;
  if (OB_SUCCESS == ret)
  {
    ret = common::ObSchemaHelper::transfer_manager_to_table_schema(*out_schema, table_schema_array);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to transfer manager to table_schema, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = common::ObSchemaHelper::transfer_manager_to_table_schema(*schema_manager, new_table_schema);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to transfer manager to table_schema, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    for(int64_t i = 0; i < delete_tables.count(); i++)
    {
      uint64_t new_table_id = 0;
      uint64_t old_table_id = delete_tables.at(i);
      TBSYS_LOG(INFO, "to delete table is=%ld", old_table_id);
      int64_t table_index = -1;
      char* table_name = NULL;
      for (int64_t j = 0; j < table_schema_array.count(); j++)
      {
        if (table_schema_array.at(j).table_id_ == old_table_id)
        {
          table_name = table_schema_array.at(j).table_name_;
          table_index = j;
          break;
        }
      }
      if (table_index == -1 || table_name == NULL)
      {
        TBSYS_LOG(WARN, "fail to find table schema, table_id=%ld, table_name=%p", old_table_id, table_name);
        ret = OB_ERROR;
        break;
      }
      else
      {
        TBSYS_LOG(INFO, "to delete table name=%s, table_id=%ld", table_name, old_table_id);
      }
      bool find = false;
      for (int64_t j = 0; j < new_table_schema.count(); j++)
      {
        if (OB_SUCCESS == strncmp(new_table_schema.at(j).table_name_, table_name, strlen(table_name)))
        {
          find = true;
          new_table_id = new_table_schema.at(j).table_id_;
          break;
        }
      }
      if (find == false)
      {
        TBSYS_LOG(WARN, "fail to find new_table_id in schema_manager. table_name=%s", table_name);
        ret = OB_ERROR;
        break;
      }
      else
      {
        TBSYS_LOG(INFO, "get new table_id for table. table_name=%s, table_id=%ld",
            table_name, new_table_id);
      }
      TBSYS_LOG(INFO, "bypass process start modify table id in inner table. table_index=%ld, table_name=%s, new_table_id=%ld",
          table_index, table_schema_array.at(table_index).table_name_, new_table_id);
      ret = ddl_tool_.modify_table_id(table_schema_array.at(table_index), new_table_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to modify table id. table_name=%s", table_name);
        break;
      }
      else
      {
        TBSYS_LOG(INFO, "modify table id for bypass table success. table_name=%s, new_table_id=%ld",
            table_name, new_table_id);
      }
    }
  }
  int64_t count = 0;
  if (OB_SUCCESS != (ret = refresh_new_schema(count)))
  {
    TBSYS_LOG(WARN, "refresh new schema manager after modify table_id failed. err=%d", ret);
  }
  else
  {
    if (OB_SUCCESS != (ret = notify_switch_schema(false)))
    {
      TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "notify switch schema after modify table_id");
      write_schema_to_file();
    }
  }
  return ret;
}

int ObRootServer2::use_new_root_table(ObRootTable2 *root_table, ObTabletInfoManager* tablet_manager)
{
  int ret = OB_SUCCESS;
  if (NULL == root_table || NULL == tablet_manager)
  {
    TBSYS_LOG(ERROR, "invalid argument. root_table=%p, tablet_manager=%p",
        root_table, tablet_manager);
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    tbsys::CThreadGuard root_guard(&root_table_build_mutex_);
    switch_root_table(root_table, tablet_manager);
    make_checkpointing();
    root_table = NULL;
    tablet_manager = NULL;
    dump_root_table();
  }
  return ret;
}
bool ObRootServer2::is_bypass_process()
{
  return state_.get_bypass_flag();
}
int ObRootServer2::cs_load_sstable_done(const ObServer &cs,
    const common::ObTableImportInfoList &table_list, const bool is_load_succ)
{
  int32_t cs_index = get_server_index(cs);
  return operation_helper_.cs_load_sstable_done(cs_index, table_list, is_load_succ);
}

int ObRootServer2::cs_delete_table_done(const ObServer &cs,
    const uint64_t table_id, const bool is_succ)
{
  int ret = OB_SUCCESS;
  if (is_loading_data())
  {
    if (is_succ)
    {
      TBSYS_LOG(INFO, "cs finish delete table. table_id=%ld, cs=%s", table_id, to_cstring(cs));
    }
    else
    {
      TBSYS_LOG(ERROR, "fail to delete table for bypass. table_id=%ld, cs=%s, ret=%d",
          table_id, to_cstring(cs), ret);
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "rootserver not in bypass process.");
  }
  return ret;
}

void ObRootServer2::set_bypass_version(const int64_t version)
{// should only used in log worker during log replay
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  bypass_process_frozen_men_version_ = version;
}

ObRootOperationHelper* ObRootServer2::get_bypass_operation()
{
  return &operation_helper_;
}

int ObRootServer2::unlock_frozen_version()
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  if (bypass_process_frozen_men_version_ == 0)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "frozen mem version is not locked, no need to unlock");
  }

  bypass_process_frozen_men_version_ = 0;
  if (OB_SUCCESS != log_worker_->set_frozen_version_for_brodcast(0))
  {
    TBSYS_LOG(ERROR, "failed to log unlock_frozen_version");
  }
  return ret;
}

int ObRootServer2::lock_frozen_version()
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  if (bypass_process_frozen_men_version_ > 0)
  {
    ret = OB_EAGAIN;
    TBSYS_LOG(WARN, "frozen mem version is already lock, no need to lock again");
  }
  else
  {
    bypass_process_frozen_men_version_ = last_frozen_mem_version_;
    if (OB_SUCCESS != log_worker_->set_frozen_version_for_brodcast(bypass_process_frozen_men_version_))
    {
      TBSYS_LOG(ERROR, "failed to log lock_frozen_version, last_frozen_mem_version_ =%ld", last_frozen_mem_version_);
    }
  }
  return ret;
}

int64_t ObRootServer2::get_frozen_version_for_cs_heartbeat() const
{
  tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
  int64_t ret = last_frozen_mem_version_;
  if (bypass_process_frozen_men_version_ > 0)
  {
    TBSYS_LOG(DEBUG, "rootserver in bypass process. broadcast version=%ld", bypass_process_frozen_men_version_);
    ret = bypass_process_frozen_men_version_;
  }
  else
  {
    ret = last_frozen_mem_version_;
  }
  return ret;
}

int ObRootServer2::change_table_id(const int64_t table_id, const int64_t new_table_id)
{
  int ret = OB_SUCCESS;
  {
    tbsys::CWLockGuard schema_guard(schema_manager_rwlock_);
    tbsys::CThreadGuard guard(&mutex_lock_);
    uint64_t max_table_id = new_table_id;
    if (max_table_id == 0)
    {
      ret = schema_service_->get_max_used_table_id(max_table_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get max used table id. ret=%d", ret);
      }
      else
      {
        ++max_table_id;
        TBSYS_LOG(INFO, "new max_table_id = %ld, update to inner table.", max_table_id);
        ret = ddl_tool_.update_max_table_id(max_table_id);
        if (OB_SUCCESS!= ret)
        {
          TBSYS_LOG(WARN, "fail to update max table id. ret=%d, max_table_id=%ld", ret, max_table_id);
        }
      }
    }
    if (OB_SUCCESS == ret)
    {
      TableSchema table_schema;
      ret = get_table_schema(table_id, table_schema);
      TBSYS_LOG(INFO, "table name=%s, new_table_id=%ld", table_schema.table_name_, max_table_id);

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get table schema. table_id=%ld, ret=%d", table_id, ret);
      }
      else if (OB_SUCCESS != (ret = ddl_tool_.modify_table_id(table_schema, max_table_id)))
      {
        TBSYS_LOG(WARN, "fail to change table_id.table_id=%ld, ret=%d", max_table_id, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "change table id success. table name=%s, new_table_id=%ld", table_schema.table_name_, max_table_id);
      }
    }
  }
  int64_t count = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = refresh_new_schema(count)))
    {
      TBSYS_LOG(WARN, "refresh new schema manager after modify table_id failed. err=%d", ret);
    }
    else
    {
      if (OB_SUCCESS != (ret = notify_switch_schema(false)))
      {
        TBSYS_LOG(WARN, "switch schema fail:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "notify switch schema after modify table_id");
        write_schema_to_file();
      }
    }
  }
  return ret;
}

int ObRootServer2::add_range_for_load_data(const common::ObList<ObNewRange*> &range_table)
{ // check range list before call this method
  int ret = OB_SUCCESS;
  ObTabletReportInfoList report_info;
  for (common::ObList<ObNewRange*>::const_iterator it = range_table.begin();
      it != range_table.end() && OB_SUCCESS == ret; ++it)
  {
    ObTabletReportInfo tablet;
    tablet.tablet_info_.range_.table_id_ = (*it)->table_id_;
    tablet.tablet_info_.range_.border_flag_ = (*it)->border_flag_;
    tablet.tablet_info_.range_.start_key_ = (*it)->start_key_;
    tablet.tablet_info_.range_.end_key_ = (*it)->end_key_;
    ret = report_info.add_tablet(tablet);
    if (ret == OB_ARRAY_OUT_OF_RANGE)
    {
      TBSYS_LOG(DEBUG, "report info list is full, add to root table new");
      ret = add_range_to_root_table(report_info);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to add range to root table. ret=%d", ret);
        break;
      }
      else
      {
        TBSYS_LOG(DEBUG, "report info list full, clean it");
        report_info.reset();
        ret = report_info.add_tablet(tablet);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to add range to report list, ret=%d", ret);
          break;
        }
      }
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to add range to report list, ret=%d", ret);
      break;
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = add_range_to_root_table(report_info);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to add range to root table. ret=%d", ret);
    }
  }
  return ret;
}

int ObRootServer2::add_range_to_root_table(const ObTabletReportInfoList &tablets, const bool is_replay_log)
{
  int ret = OB_SUCCESS;
  ObServer server;
  int32_t server_index = -1;
  int64_t frozen_mem_version = last_frozen_mem_version_;
  TBSYS_LOG_US(INFO, "[NOTICE] add range for load data, count=%ld, version=%ld",
      tablets.tablet_list_.get_array_index(), frozen_mem_version);
  if (is_master() && !is_replay_log)
  {
    log_worker_->add_range_for_load_data(tablets);
  }
  ret = got_reported(tablets, server_index, frozen_mem_version, true, is_replay_log);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to add tablets range to root table. ret=%d", ret);
  }
  return ret;
}

int ObRootServer2::change_table_id(const ObString& table_name, const uint64_t new_table_id)
{
  int ret = OB_SUCCESS;
  uint64_t old_table_id = 0;
  TableSchema table_schema;
  const bool only_core_tables = false;
  schema_service_->init(schema_service_scan_helper_, only_core_tables);
  ret = schema_service_->get_table_schema(table_name, table_schema);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get table schema. table_name=%s, ret=%d", to_cstring(table_name), ret);
  }
  else if (!is_master() || obi_role_.get_role() != ObiRole::MASTER)
  {
    ret = OB_RS_STATE_NOT_ALLOW;
    TBSYS_LOG(ERROR, "only the master rs of the master cluster can do change table id");
  }
  else
  {
    old_table_id = table_schema.table_id_;
    ret = change_table_id(old_table_id, new_table_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to change table id for load data. new_table_id=%lu, old_table_id=%lu, ret=%d",
          new_table_id, old_table_id, ret);
    }
  }
  return ret;
}

int ObRootServer2::get_table_id(const ObString table_name, uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  const bool only_core_tables = false;
  schema_service_->init(schema_service_scan_helper_, only_core_tables);
  ret = schema_service_->get_table_schema(table_name, table_schema);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get table schema. table_name=%s, ret=%d", to_cstring(table_name), ret);
  }
  else
  {
    table_id = table_schema.table_id_;
  }
  return ret;
}

int ObRootServer2::load_data_done(const ObString table_name, const uint64_t old_table_id)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  const bool only_core_tables = false;
  schema_service_->init(schema_service_scan_helper_, only_core_tables);
  ret = schema_service_->get_table_schema(table_name, table_schema);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get table schema. table_name=%s, ret=%d", to_cstring(table_name), ret);
  }
  else if (!is_master())
  {
    ret = OB_RS_STATE_NOT_ALLOW;
    TBSYS_LOG(ERROR, "only the master rs can do load_data_done");
  }
  else if (table_schema.table_id_ == old_table_id)
  {
    ret = OB_ERR_SYS;
    TBSYS_LOG(ERROR, "old table id must not same as current table id, cant do load_data_done. table_name=%s, table_id=%lu",
        to_cstring(table_name), old_table_id);
  }

  //delete old table in roottable
  if (OB_SUCCESS == ret)
  {
    ObArray<uint64_t> delete_table;
    delete_table.push_back(old_table_id);
    ret = delete_tables(false, delete_table);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "fail to delete tables in roottable for load_data_done, table_id=%lu, ret=%d", old_table_id, ret);
    }
  }

  //delete sstable in chunkserver
  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(server_manager_rwlock_);
    ObChunkServerManager::const_iterator it = server_manager_.begin();
    for (; it != server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        if (OB_SUCCESS != get_rpc_stub().request_cs_delete_table(it->server_, old_table_id, config_.network_timeout))
        {
          TBSYS_LOG(ERROR, "fail to request cs to delete table,table_id=%lu. cs_addr=%s", old_table_id, it->server_.to_cstring());
        }
      }
    }
  }
  return ret;
}

//bypass process error, clean all data
int ObRootServer2::load_data_fail(const uint64_t new_table_id)
{
  int ret = OB_SUCCESS;
  if (!is_master())
  {
    ret = OB_RS_STATE_NOT_ALLOW;
    TBSYS_LOG(ERROR, "only the master rs can do load_data_done");
  }
  else
  {
    //delete roottable
    ObArray<uint64_t> delete_table;
    delete_table.push_back(new_table_id);
    ret = delete_tables(false, delete_table);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to delete tables in roottable, table_id=%ld, ret=%d", new_table_id, ret);
    }
  }

  //delete sstable in chunkserver
  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(server_manager_rwlock_);
    ObChunkServerManager::const_iterator it = server_manager_.begin();
    for (; it != server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        if (OB_SUCCESS != get_rpc_stub().request_cs_delete_table(it->server_, new_table_id, config_.network_timeout))
        {
          TBSYS_LOG(ERROR, "fail to request cs to delete table,table_id=%lu. cs_addr=%s", new_table_id, it->server_.to_cstring());
        }
      }
    }
  }
  return ret;
}

int ObRootServer2::init_first_meta()
{
  int ret = OB_SUCCESS;
  ObTabletMetaTableRow first_meta_row;
  first_meta_row.set_tid(OB_FIRST_TABLET_ENTRY_TID);
  first_meta_row.set_table_name(ObString::make_string(FIRST_TABLET_TABLE_NAME));
  first_meta_row.set_start_key(ObRowkey::MIN_ROWKEY);
  first_meta_row.set_end_key(ObRowkey::MAX_ROWKEY);
  ObTabletReplica r;
  r.version_ = 1;
  ObServer cs;
  r.cs_ = cs;
  r.row_count_ = 0;
  r.occupy_size_ = 0;
  r.checksum_ = 0;
  if (OB_SUCCESS != (ret = first_meta_row.add_replica(r)))
  {
    TBSYS_LOG(ERROR, "failed to add replica for the meta row, err=%d", ret);
  }
  else
  {
    ret = first_meta_->init(first_meta_row);
    if (OB_INIT_TWICE == ret || OB_SUCCESS == ret)
    {
      TBSYS_LOG(WARN, "failed to init first meta, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "init first meta success.");
    }
  }
  return ret;
}

int ObRootServer2::start_import(const ObString& table_name, const uint64_t table_id, ObString& uri)
{
  int ret = OB_SUCCESS;
  ObServer master_rs;
  ObServer slave_rs[OB_MAX_CLUSTER_COUNT];
  int64_t slave_count = OB_MAX_CLUSTER_COUNT;
  ObRootRpcStub& rpc_stub = get_rpc_stub();
  int64_t start_time = tbsys::CTimeUtil::getTime();
  ObServer ms_server;
  master_rs.set_ipv4_addr(config_.master_root_server_ip, static_cast<int32_t>(config_.master_root_server_port));

  TBSYS_LOG(INFO, "[import] receive request: start import table=%.*s with new table_id=%lu, uri=%.*s",
      table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());

  if (!is_master() || obi_role_.get_role() != ObiRole::MASTER)
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is not master of master cluster, cant start import");
  }
  else if (config_.enable_load_data == false)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "load_data is not enabled, cant load table %.*s %lu",
        table_name.length(), table_name.ptr(), table_id);
  }
  else if (OB_SUCCESS != (ret = get_ms(ms_server)))
  {
    TBSYS_LOG(WARN, "failed to get serving ms, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_stub.fetch_slave_cluster_list(
          ms_server, master_rs, slave_rs, slave_count, config_.inner_table_network_timeout)))
  {
    TBSYS_LOG(ERROR, "failed to get slave cluster rs list, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = rpc_stub.import(master_rs, table_name, table_id, uri, start_time, config_.import_rpc_timeout);
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "succeed to start import, master_rs=%s, table_name=%.*s, table_id=%lu, uri=%.*s start_time=%ld",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), start_time);
    }
    else
    {
      TBSYS_LOG(WARN, "failed to start import, master_rs=%s, table_name=%.*s,"
          " table_id=%lu, uri=%.*s, start_time=%ld, ret=%d",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id,
          uri.length(), uri.ptr(), start_time, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < slave_count; ++i)
    {
      ret = rpc_stub.import(slave_rs[i], table_name, table_id, uri, start_time, config_.import_rpc_timeout);
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "succeed to start import, slave_rs[%ld]=%s, "
            "table_name=%.*s, table_id=%lu, uri=%.*s, start_time=%ld",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(),
            table_id, uri.length(), uri.ptr(), start_time);
      }
      else
      {
        TBSYS_LOG(WARN, "failed to start import, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu, uri=%.*s, start_time=%ld, ret=%d",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(),
            table_id, uri.length(), uri.ptr(), start_time, ret);
        break;
      }
    }
  }

  if (OB_SUCCESS != ret)
  { // kill import on all clusters
    int tmp_ret = rpc_stub.kill_import(master_rs, table_name, table_id, config_.import_rpc_timeout);
    if (OB_SUCCESS == tmp_ret)
    {
      TBSYS_LOG(INFO, "succeed to rollback import, master_rs=%s, table_name=%.*s, table_id=%lu, uri=%.*s",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());
    }
    else
    {
      TBSYS_LOG(WARN, "failed to rollback import, master_rs=%s, table_name=%.*s, table_id=%lu, uri=%.*s, ret=%d",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), tmp_ret);
    }

    for (int64_t i = 0; i < slave_count; ++i)
    {
      tmp_ret = rpc_stub.kill_import(slave_rs[i], table_name, table_id, config_.import_rpc_timeout);
      if (OB_SUCCESS == tmp_ret)
      {
        TBSYS_LOG(INFO, "succeed to rollback import, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu, uri=%.*s",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());
      }
      else
      {
        TBSYS_LOG(WARN, "failed to rollback import, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu, uri=%.*s, ret=%d",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), ret);
      }
    }
  }

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "[import] failed to start import table=%.*s with new table_id=%lu, uri=%.*s, start_time=%ld, ret=%d",
        table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), start_time, ret);
  }
  else
  {
    TBSYS_LOG(INFO, "[import] succeed to start import table=%.*s with new table_id=%lu, uri=%.*s, start_time=%ld",
        table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), start_time);
  }

  return ret;
}

int ObRootServer2::import(const ObString& table_name, const uint64_t table_id, ObString& uri, const int64_t start_time)
{
  int ret = OB_SUCCESS;

  TBSYS_LOG(INFO, "receive request: import table=%.*s with new table_id=%lu, uri=%.*s, start_time=%ld",
      table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), start_time);

  if (!is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is not master, cant import, table_name=%.*s, new table_id=%lu, uri=%.*s",
        table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());
  }
  else if (config_.enable_load_data == false)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "load_data is not enabled, cant load table %.*s %lu",
        table_name.length(), table_name.ptr(), table_id);
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (OB_SUCCESS != (ret = balancer_->add_load_table(table_name, table_id, uri, start_time)))
  {
    TBSYS_LOG(ERROR, "fail to add import table=%.*s with new table_id=%lu, uri=%.*s, ret=%d",
        table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr(), ret);
  }

  return ret;
}

int ObRootServer2::start_kill_import(const ObString& table_name, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObServer master_rs;
  ObServer slave_rs[OB_MAX_CLUSTER_COUNT];
  int64_t slave_count = OB_MAX_CLUSTER_COUNT;
  ObRootRpcStub& rpc_stub = get_rpc_stub();
  ObServer ms_server;
  master_rs.set_ipv4_addr(config_.master_root_server_ip, (int32_t)config_.master_root_server_port);

  TBSYS_LOG(INFO, "[import] receive request to kill import table: table_name=%.*s table_id=%lu",
      table_name.length(), table_name.ptr(), table_id);

  if (!is_master() || obi_role_.get_role() != ObiRole::MASTER)
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(WARN, "this rs is not master of marster cluster, cannot kill import task");
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (OB_SUCCESS != (ret = get_ms(ms_server)))
  {
    TBSYS_LOG(WARN, "failed to get serving ms, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_stub.fetch_slave_cluster_list(
          ms_server, master_rs, slave_rs, slave_count, config_.inner_table_network_timeout)))
  {
    TBSYS_LOG(ERROR, "failed to get slave cluster rs list, ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  { // kill import on all clusters
    int tmp_ret = rpc_stub.kill_import(master_rs, table_name, table_id, config_.import_rpc_timeout);
    if (OB_SUCCESS == tmp_ret)
    {
      TBSYS_LOG(INFO, "succeed  to kill import, master_rs=%s, table_name=%.*s, table_id=%lu",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id);
    }
    else
    {
      ret = tmp_ret;
      TBSYS_LOG(WARN, "failed to kill import, master_rs=%s, table_name=%.*s, table_id=%lu, ret=%d",
          to_cstring(master_rs), table_name.length(), table_name.ptr(), table_id, tmp_ret);
    }

    for (int64_t i = 0; i < slave_count; ++i)
    {
      tmp_ret = rpc_stub.kill_import(slave_rs[i], table_name, table_id, config_.import_rpc_timeout);
      if (OB_SUCCESS == tmp_ret)
      {
        TBSYS_LOG(INFO, "succeed to kill import, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id);
      }
      else
      {
        ret = tmp_ret;
        TBSYS_LOG(WARN, "failed to kill import, slave_rs[%ld]=%s, table_name=%.*s, table_id=%lu, ret=%d",
            i, to_cstring(slave_rs[i]), table_name.length(), table_name.ptr(), table_id, ret);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "[import] succeed to kill import table: table_name=%.*s table_id=%lu",
        table_name.length(), table_name.ptr(), table_id);
  }
  else
  {
    TBSYS_LOG(WARN, "[import] failed to kill import table: table_name=%.*s table_id=%lu",
        table_name.length(), table_name.ptr(), table_id);
  }

  return ret;
}

int ObRootServer2::kill_import(const ObString& table_name, const uint64_t table_id)
{
  int ret = OB_SUCCESS;

  TBSYS_LOG(INFO, "receive request to kill import table: table_name=%.*s table_id=%lu",
      table_name.length(), table_name.ptr(), table_id);

  if (!is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is not master, cant kill import");
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (OB_SUCCESS != (ret = balancer_->kill_load_table(table_name, table_id)))
  {
    TBSYS_LOG(WARN, "fail to kill import table %.*s with new table_id=%lu, ret=%d",
        table_name.length(), table_name.ptr(), table_id, ret);
  }
  else
  {
    TBSYS_LOG(INFO, "kill import table done: table_name=%.*s table_id=%lu",
        table_name.length(), table_name.ptr(), table_id);
  }

  return ret;
}

int ObRootServer2::get_import_status(const ObString& table_name,
    const uint64_t table_id, ObLoadDataInfo::ObLoadDataStatus& status)
{
  int ret = OB_SUCCESS;

  if (!is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is not master, cant get import status");
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (OB_SUCCESS != (ret = balancer_->get_import_status(table_name, table_id, status)))
  {
    TBSYS_LOG(ERROR, "[import] fail to get import status table %.*s with table_id=%lu, ret=%d",
        table_name.length(), table_name.ptr(), table_id, ret);
  }

  return ret;
}

int ObRootServer2::set_import_status(const ObString& table_name,
    const uint64_t table_id, const ObLoadDataInfo::ObLoadDataStatus status)
{
  int ret = OB_SUCCESS;

  if (!is_master())
  {
    ret = OB_NOT_MASTER;
    TBSYS_LOG(ERROR, "this rs is not master, cant set import status");
  }
  else if (NULL == balancer_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "balancer_ must not null");
  }
  else if (OB_SUCCESS != (ret = balancer_->set_import_status(table_name, table_id, status)))
  {
    TBSYS_LOG(ERROR, "[import] fail to get import status table %.*s with table_id=%lu, ret=%d",
        table_name.length(), table_name.ptr(), table_id, ret);
  }

  return ret;
}

int ObRootServer2::set_bypass_flag(const bool flag)
{
  return state_.set_bypass_flag(flag);
}

bool ObRootServer2::is_loading_data() const
{
  OB_ASSERT(balancer_);
  return balancer_->is_loading_data();
}

int ObRootServer2::trigger_create_table(const uint64_t table_id/* =0 */)
{
  int ret = OB_SUCCESS;
  ObSchemaManagerV2 * out_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  ObSchemaManagerV2 * new_schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
  if (NULL == out_schema || NULL == new_schema)
  {
    ret = OB_MEM_OVERFLOW;
    TBSYS_LOG(ERROR, "allocate new schema failed");
  }
  if (OB_SUCCESS == ret && 0 == table_id)
  {
    ret = get_schema(false, false, *out_schema);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get schema, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = get_schema(true, false, *new_schema);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get schema. ret=%d", ret);
    }
  }
  common::ObArray<uint64_t> new_tables;
  TableSchema table_schema;
  if (OB_SUCCESS == ret && 0 == table_id)
  {
    //old style, trigger doesn't specify table id to create table
    ret = get_deleted_tables(*new_schema, *out_schema, new_tables);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get new table for trigger create table. ret=%d", ret);
    }
  }
  else if (OB_SUCCESS == ret && table_id > 0)
  {
    //trigger specified table id to create table
    ret = new_tables.push_back(table_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to push_back table id into table list, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (0 == new_tables.count())
    {
      TBSYS_LOG(WARN, "no table to create, create_table_count=0");
      ret = OB_ERROR;
    }
    else if (new_tables.count() > 1)
    {
      TBSYS_LOG(WARN, "not support batch create table! careful!");
      ret = OB_NOT_SUPPORTED;
    }
    else
    {
      TBSYS_LOG(INFO, "trigger create table, input_table_id=%lu, create_table_id=%lu",
          table_id, new_tables.at(0));
      ret = get_table_schema(new_tables.at(0), table_schema);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get table schema. table_id=%ld, ret=%d", new_tables.at(0), ret);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObArray<ObServer> created_cs;
    ret = create_empty_tablet(table_schema, created_cs);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to create emtyp tablet. table_id=%ld, ret=%d", table_schema.table_id_, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    int64_t count = 0;
    ret = renew_user_schema(count);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to renew user schema. ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "renew schema after trigger create table success. table_count=%ld", count);
    }
  }
  if (new_schema != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, new_schema);
  }
  if (out_schema != NULL)
  {
    OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, out_schema);
  }
  return ret;
}
int ObRootServer2::trigger_drop_table(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> delete_table;
  if (OB_SUCCESS == ret)
  {
    delete_table.push_back(table_id);
    ret = delete_tables(false, delete_table);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to delete tables.ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "delete table for trigger drop table success. table_id=%ld", table_id);
    }
  }
  return ret;
}
int ObRootServer2::check_schema()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ret = renew_user_schema(count);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to renew user schema. ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    tbsys::CRLockGuard guard(schema_manager_rwlock_);
    ret = schema_manager_for_cache_->write_to_file(TMP_SCHEMA_LOCATION);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to write schema to file. location = %s, ret=%d", TMP_SCHEMA_LOCATION, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObSchemaManagerV2 *schema = NULL;
    tbsys::CConfig config;
    int64_t now = tbsys::CTimeUtil::getTime();
    if (NULL == (schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, now)))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
    }
    else if (!schema->parse_from_file(TMP_SCHEMA_LOCATION, config))
    {
      TBSYS_LOG(ERROR, "parse schema error chema file is %s ", config_.schema_filename.str());
      ret = OB_SCHEMA_ERROR;
    }
    else
    {
      TBSYS_LOG(INFO, "parse schema file success.");
    }
  }
  if (common::FileDirectoryUtils::exists(TMP_SCHEMA_LOCATION))
  {
    //common::FileDirectoryUtils::delete_file(TMP_SCHEMA_LOCATION);
  }
  return ret;
}
int ObRootServer2::force_drop_table(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  int64_t table_count = 0;
  ret = trigger_drop_table(table_id);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to force drop tablet. table_id=%ld, ret=%d", table_id, ret);
  }
  else if (OB_SUCCESS != (ret = refresh_new_schema(table_count)))
  {
    TBSYS_LOG(WARN, "fail to refresh new schema.ret=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "force to drop table success. table_id=%ld", table_id);
  }
  return ret;
}
int ObRootServer2::force_create_table(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ret = trigger_create_table(table_id);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to force create tablet. table_id=%ld, ret=%d", table_id, ret);
  }
  return ret;
}

int ObRootServer2::get_ms(ObServer& ms_server)
{
  int ret = OB_SUCCESS;

  ObChunkServerManager::const_iterator it = server_manager_.get_serving_ms();
  if (server_manager_.end() == it)
  {
    TBSYS_LOG(WARN, "no serving ms found, failed to do update_data_source_proxy");
    ret = OB_MS_NOT_EXIST;
  }
  else
  {
    ms_server = it->server_;
    ms_server.set_port(it->port_ms_);
  }
  return ret;
}
