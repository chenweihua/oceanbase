/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-09-26
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *          maoqi(maoqi@taobao.com)
 *          xielun.szd(xielun@alipay.com)
 *
 *
 ================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#include <tbsys.h>

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_obi_role.h"
#include "common/ob_ups_info.h"
#include "common/ob_schema_table.h"
#include "common/ob_schema_service.h"
#include "common/ob_table_id_name.h"
#include "common/ob_array.h"
#include "common/ob_list.h"
#include "common/ob_bypass_task_info.h"
#include "common/ob_timer.h"
#include "common/ob_tablet_info.h"
#include "common/ob_trigger_event.h"
#include "common/ob_trigger_msg.h"
#include "ob_root_server_state.h"
#include "common/roottable/ob_root_table_service.h"
#include "common/roottable/ob_first_tablet_entry_meta.h"
#include "common/roottable/ob_scan_helper_impl.h"
#include "common/ob_schema_service.h"
#include "common/ob_spin_lock.h"
#include "common/ob_strings.h"
#include "ob_chunk_server_manager.h"
#include "ob_root_table2.h"
#include "ob_root_log_worker.h"
#include "ob_root_async_task_queue.h"
#include "ob_ups_manager.h"
#include "ob_ups_heartbeat_runnable.h"
#include "ob_ups_check_runnable.h"
#include "ob_root_balancer.h"
#include "ob_root_balancer_runnable.h"
#include "ob_root_ddl_operator.h"
#include "ob_daily_merge_checker.h"
#include "ob_heartbeat_checker.h"
#include "ob_root_server_config.h"
#include "ob_root_ms_provider.h"
#include "ob_rs_after_restart_task.h"
#include "ob_schema_service_ms_provider.h"
#include "ob_schema_service_ups_provider.h"
#include "rootserver/ob_root_operation_helper.h"
#include "rootserver/ob_root_timer_task.h"

class ObBalanceTest;
class ObBalanceTest_test_n_to_2_Test;
class ObBalanceTest_test_timeout_Test;
class ObBalanceTest_test_rereplication_Test;
class ObBalanceTest_test_n_to_2_with_faulty_dest_cs_Test;
class ObDeleteReplicasTest_delete_in_init_Test;
class ObDeleteReplicasTest_delete_when_rereplication_Test;
class ObDeleteReplicasTest_delete_when_report_Test;
class ObBalanceTest_test_shutdown_servers_Test;
class ObRootServerTest;
namespace oceanbase
{
  namespace common
  {
    class ObSchemaManagerV2;
    class ObRange;
    class ObTabletInfo;
    class ObTabletLocation;
    class ObScanner;
    class ObCellInfo;
    class ObTabletReportInfoList;
    class ObTableIdNameIterator;
    class ObConfigManager;
    struct TableSchema;
  }
  namespace rootserver
  {
    class ObBootstrap;
    class ObRootTable2;
    class ObRootServerTester;
    class ObRootWorker;
    class ObRootServer2;
    // 参见《OceanBase自举流程》
    class ObBootState
    {
      public:
        enum State
        {
          OB_BOOT_NO_META = 0,
          OB_BOOT_OK = 1,
          OB_BOOT_STRAP = 2,
          OB_BOOT_RECOVER = 3,
        };
      public:
        ObBootState();
        bool is_boot_ok() const;
        void set_boot_ok();
        void set_boot_strap();
        void set_boot_recover();
        bool can_boot_strap() const;
        bool can_boot_recover() const;
        const char* to_cstring() const;
        ObBootState::State get_boot_state() const;
      private:
        mutable common::ObSpinLock lock_;
        State state_;
    };

    class ObRootServer2
    {
      public:
        friend class ObBootstrap;
        static const int64_t DEFAULT_SAFE_CS_NUMBER = 2;
        static const char* ROOT_TABLE_EXT;
        static const char* CHUNKSERVER_LIST_EXT;
        static const char* LOAD_DATA_EXT;
        static const char* SCHEMA_FILE_NAME;
        static const char* TMP_SCHEMA_LOCATION;
      public:
        ObRootServer2(ObRootServerConfig& config);
        virtual ~ObRootServer2();

        bool init(const int64_t now, ObRootWorker* worker);
        int start_master_rootserver();
        int init_first_meta();
        int init_boot_state();
        void start_threads();
        void stop_threads();
        void start_merge_check();

        // oceanbase bootstrap
        int boot_strap();
        int do_bootstrap(ObBootstrap & bootstrap);
        int boot_recover();
        ObBootState* get_boot();
        ObBootState::State get_boot_state() const;
        int slave_boot_strap();
        int start_notify_switch_schema();
        int notify_switch_schema(bool only_core_tables, bool force_update = false);
        void set_privilege_version(const int64_t privilege_version);
        // commit update inner table task
        void commit_task(const ObTaskType type, const common::ObRole role, const common::ObServer & server, int32_t sql_port,
                         const char* server_version, const int32_t cluster_role = 0);
        // for monitor info
        int64_t get_table_count(void) const;
        void get_tablet_info(int64_t & tablet_count, int64_t & row_count, int64_t & date_size) const;
        int change_table_id(const int64_t table_id, const int64_t new_table_id=0);
        int change_table_id(const ObString& table_name, const uint64_t new_table_id);
        
        // for bypass
        // start_import is called in master cluster by rs_admin, this method will call import on all clusters
        int start_import(const ObString& table_name, const uint64_t table_id, ObString& uri);
        int import(const ObString& table_name, const uint64_t table_id, ObString& uri, const int64_t start_time);
        // start_kill_import is called in master cluster by rs_admin, this method will call kill_import on all clusters
        int start_kill_import(const ObString& table_name, const uint64_t table_id);
        int kill_import(const ObString& table_name, const uint64_t table_id);
        // used by master cluster to check slave cluster's import status
        int get_import_status(const ObString& table_name, const uint64_t table_id, ObLoadDataInfo::ObLoadDataStatus& status);
        int set_import_status(const ObString& table_name, const uint64_t table_id, const ObLoadDataInfo::ObLoadDataStatus status);

        bool is_loading_data() const;

        int write_schema_to_file();
        int trigger_create_table(const uint64_t table_id = 0);
        int trigger_drop_table(const uint64_t table_id);
        int force_create_table(const uint64_t table_id);
        int force_drop_table(const uint64_t table_id);
        int check_schema();
        /*
         * 从本地读取新schema, 判断兼容性
         */
        //int switch_schema(const int64_t time_stamp, common::ObArray<uint64_t> &deleted_tables);
        /*
         * 切换过程中, update server冻结内存表 或者chunk server 进行merge等耗时操作完成
         * 发送消息调用此函数
         */
        int waiting_job_done(const common::ObServer& server, const int64_t frozen_mem_version);
        /*
         * chunk server register
         * @param out status 0 do not start report 1 start report
         */
        int regist_chunk_server(const common::ObServer& server, const char* server_version, int32_t& status, int64_t timestamp = -1);
        /*
         * merge server register
         * @param out status 0 do not start report 1 start report
         */
        int regist_merge_server(const common::ObServer& server, const int32_t sql_port, const bool is_listen_ms,
            const char* server_version, int64_t timestamp = -1);
        /*
         * chunk server更新自己的磁盘情况信息
         */
        int update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used);
        /*
         * 迁移完成操作
         */
        const common::ObServer& get_self() { return my_addr_; }
       ObRootBalancer* get_balancer() { return balancer_; }

       virtual int migrate_over(const int32_t result, const ObDataSourceDesc& desc,
           const int64_t occupy_size, const uint64_t crc_sum, const uint64_t row_checksum, const int64_t row_count);
        /// if (force_update = true && get_only_core_tables = false) then read new schema from inner table
        int get_schema(const bool froce_update, bool get_only_core_tables, common::ObSchemaManagerV2& out_schema);
        int64_t get_schema_version() const;
        const ObRootServerConfig& get_config() const;
        ObConfigManager* get_config_mgr();
        int64_t get_privilege_version() const;
        int get_max_tablet_version(int64_t &version) const;
        int64_t get_config_version() const;
        int64_t get_alive_cs_number();
        common::ObSchemaManagerV2* get_ini_schema() const;
        ObRootRpcStub& get_rpc_stub();
        int fetch_mem_version(int64_t &mem_version);
        int create_empty_tablet(common::TableSchema &tschema, common::ObArray<common::ObServer> &cs);
        int get_table_id_name(common::ObTableIdNameIterator *table_id_name, bool& only_core_tables);
        int get_table_schema(const uint64_t table_id, common::TableSchema &table_schema);
        int find_root_table_key(const uint64_t table_id, const common::ObString& table_name, const int32_t max_key_len,
            const common::ObRowkey& key, common::ObScanner& scanner);

        int find_root_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int find_monitor_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int find_session_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int find_statement_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int find_root_table_range(const common::ObScanParam& scan_param, common::ObScanner& scanner);
        virtual int report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets, const int64_t time_stamp);
        int receive_hb(const common::ObServer& server, const int32_t sql_port, const bool is_listen_ms, const common::ObRole role);
        common::ObServer get_update_server_info(bool use_inner_port) const;
        int add_range_for_load_data(const common::ObList<common::ObNewRange*> &range);
        int load_data_fail(const uint64_t new_table_id);
        int get_table_id(const ObString table_name, uint64_t& table_id);
        int load_data_done(const ObString table_name, const uint64_t old_table_id);
        int get_master_ups(common::ObServer &ups_addr, bool use_inner_port);
        int table_exist_in_cs(const uint64_t table_id, bool &is_exist);
        int create_tablet(const common::ObTabletInfoList& tablets);
        uint64_t get_table_info(const common::ObString& table_name, int32_t& max_row_key_length) const;
        int get_table_info(const common::ObString& table_name, uint64_t& table_id, int32_t& max_row_key_length);

        int64_t get_time_stamp_changing() const;
        int64_t get_lease() const;
        int get_server_index(const common::ObServer& server) const;
        int get_cs_info(ObChunkServerManager* out_server_manager) const;
        // get task queue
        ObRootAsyncTaskQueue * get_task_queue(void);
        const ObChunkServerManager &get_server_manager(void) const;
        void clean_daily_merge_tablet_error();
        void set_daily_merge_tablet_error(const char* msg_buff, const int64_t len);
        char* get_daily_merge_error_msg();
        bool is_daily_merge_tablet_error() const;
        void print_alive_server() const;
        bool is_master() const;
        common::ObFirstTabletEntryMeta* get_first_meta();
        void dump_root_table() const;
        bool check_root_table(const common::ObServer &expect_cs) const;
        int dump_cs_tablet_info(const common::ObServer & cs, int64_t &tablet_num) const;
        void dump_unusual_tablets() const;
        int check_tablet_version(const int64_t tablet_version, const int64_t safe_count, bool &is_merged) const;
        int use_new_schema();
        // dump current root table and chunkserver list into file
        int do_check_point(const uint64_t ckpt_id);
        // recover root table and chunkserver list from file
        int recover_from_check_point(const int server_status, const uint64_t ckpt_id);
        int receive_new_frozen_version(int64_t rt_version, const int64_t frozen_version,
            const int64_t last_frozen_time, bool did_replay);
        int report_frozen_memtable(const int64_t frozen_version, const int64_t last_frozen_time,bool did_replay);
        int get_last_frozen_version_from_ups(const bool did_replay);
        // 用于slave启动过程中的同步
        void wait_init_finished();
        const common::ObiRole& get_obi_role() const;
        int request_cs_report_tablet();
        int set_obi_role(const common::ObiRole& role);
        int get_master_ups_config(int32_t &master_master_ups_read_percent,
            int32_t &slave_master_ups_read_percent) const;
        const common::ObUpsList &get_ups_list() const;
        int set_ups_list(const common::ObUpsList &ups_list);
        int do_stat(int stat_key, char *buf, const int64_t buf_len, int64_t& pos);
        int register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease, const char *server_version_);
        int receive_ups_heartbeat_resp(const common::ObServer &addr, ObUpsStatus stat,
            const common::ObiRole &obi_role);
        int ups_slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr);
        int get_ups_list(common::ObUpsList &ups_list);
        int set_ups_config(const common::ObServer &ups, int32_t ms_read_percentage, int32_t cs_read_percentage);
        int set_ups_config(int32_t read_master_master_ups_percentage, int32_t read_slave_master_ups_percentage);
        int change_ups_master(const common::ObServer &ups, bool did_force);
        int serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_proxy_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int grant_eternal_ups_lease();
        int cs_import_tablets(const uint64_t table_id, const int64_t tablet_version);
        /// force refresh the new schmea manager through inner table scan
        int refresh_new_schema(int64_t & table_count);
        int switch_ini_schema();
        int renew_user_schema(int64_t & table_count);
        int renew_core_schema(void);
        void dump_schema_manager();
        void dump_migrate_info() const; // for monitor
        int shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        void restart_all_cs();
        void cancel_restart_all_cs();
        int cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        void reset_hb_time();
        int remove_replica(const bool did_replay, const common::ObTabletReportInfo &replica);
        int delete_tables(const bool did_replay, const common::ObArray<uint64_t> &deleted_tables);
        int delete_replicas(const bool did_replay, const common::ObServer & cs, const common::ObTabletReportInfoList & replicas);
        int create_table(const bool if_not_exists, const common::TableSchema &tschema);
        int alter_table(common::AlterTableSchema &tschema);
        int drop_tables(const bool if_exists, const common::ObStrings &tables);
        int64_t get_last_frozen_version() const;

        //for bypass process begin
        ObRootOperationHelper* get_bypass_operation();
        bool is_bypass_process();
        int set_bypass_flag(const bool flag);
        int get_new_table_id(uint64_t &max_table_id, ObBypassTaskInfo &table_name_id);
        int clean_root_table();
        int slave_clean_root_table();
        void set_bypass_version(const int64_t version);
        int unlock_frozen_version();
        int lock_frozen_version();
        int prepare_bypass_process(common::ObBypassTaskInfo &table_name_id);
        virtual int start_bypass_process(common::ObBypassTaskInfo &table_name_id);
        int check_bypass_process();
        const char* get_bypass_state()const;
        int cs_delete_table_done(const common::ObServer &cs,
            const uint64_t table_id, const bool is_succ);
        int delete_table_for_bypass();
        int cs_load_sstable_done(const common::ObServer &cs,
            const common::ObTableImportInfoList &table_list, const bool is_load_succ);
        virtual int bypass_meta_data_finished(const OperationType type, ObRootTable2 *root_table,
            ObTabletInfoManager *tablet_manager, common::ObSchemaManagerV2 *schema_mgr);
        int use_new_root_table(ObRootTable2 *root_table, ObTabletInfoManager *tablet_manager);
        int switch_bypass_schema(common::ObSchemaManagerV2 *schema_mgr, common::ObArray<uint64_t> &delete_tables);
        int64_t get_frozen_version_for_cs_heartbeat() const;
        //for bypass process end
        /// check the table exist according the local schema manager
        int check_table_exist(const common::ObString & table_name, bool & exist);
        int delete_dropped_tables(int64_t & table_count);
        void after_switch_to_master();
        int after_restart();
        int make_checkpointing();
        ObRootMsProvider & get_ms_provider() { return ms_provider_; }

        //table_id is OB_INVALID_ID , get all table's row_checksum
        int get_row_checksum(const int64_t tablet_version, const uint64_t table_id, ObRowChecksum &row_checksum);

        friend class ObRootServerTester;
        friend class ObRootLogWorker;
        friend class ObDailyMergeChecker;
        friend class ObHeartbeatChecker;
        friend class ::ObBalanceTest;
        friend class ::ObBalanceTest_test_n_to_2_Test;
        friend class ::ObBalanceTest_test_timeout_Test;
        friend class ::ObBalanceTest_test_rereplication_Test;
        friend class ::ObBalanceTest_test_n_to_2_with_faulty_dest_cs_Test;
        friend class ::ObDeleteReplicasTest_delete_in_init_Test;
        friend class ::ObDeleteReplicasTest_delete_when_rereplication_Test;
        friend class ::ObDeleteReplicasTest_delete_when_report_Test;
        friend class ::ObBalanceTest_test_shutdown_servers_Test;
        friend class ::ObRootServerTest;
        friend class ObRootReloadConfig;
      private:
        bool async_task_queue_empty()
        {
          return seq_task_queue_.size() == 0;
        }
        int after_boot_strap(ObBootstrap & bootstrap);
        /*
         * 收到汇报消息后调用
         */
        int got_reported(const common::ObTabletReportInfoList& tablets, const int server_index,
            const int64_t frozen_mem_version, const bool for_bypass = false, const bool is_replay_log = false);
        /*
         * 旁路导入时，创建新range的时候使用
         */
        int add_range_to_root_table(const ObTabletReportInfoList &tablets, const bool is_replay_log = false);
        /*
         * 处理汇报消息, 直接写到当前的root table中
         * 如果发现汇报消息中有对当前root table的tablet的分裂或者合并
         * 要调用采用写拷贝机制的处理函数
         */
        int got_reported_for_query_table(const common::ObTabletReportInfoList& tablets,
            const int32_t server_index, const int64_t frozen_mem_version, const bool for_bypass = false);
        /*
         * 写拷贝机制的,处理汇报消息
         */
        int got_reported_with_copy(const common::ObTabletReportInfoList& tablets,
            const int32_t server_index, const int64_t have_done_index, const bool for_bypass = false);

        int create_new_table(const bool did_replay, const common::ObTabletInfo& tablet,
            const common::ObArray<int32_t> &chunkservers, const int64_t mem_version);
        int slave_batch_create_new_table(const common::ObTabletInfoList& tablets,
            int32_t** t_server_index, int32_t* replicas_num, const int64_t mem_version);
        void get_available_servers_for_new_table(int* server_index, int32_t expected_num, int32_t &results_num);
        int get_deleted_tables(const common::ObSchemaManagerV2 &old_schema,
            const common::ObSchemaManagerV2 &new_schema, common::ObArray<uint64_t> &deleted_tables);
        int make_out_cell_all_server(ObCellInfo& out_cell, ObScanner& scanner,
            const int32_t max_row_count) const;


        /*
         * 生成查询的输出cell
         */
        int make_out_cell(common::ObCellInfo& out_cell, ObRootTable2::const_iterator start,
            ObRootTable2::const_iterator end, common::ObScanner& scanner, const int32_t max_row_count,
            const int32_t max_key_len) const;

        // stat related functions
        void do_stat_start_time(char *buf, const int64_t buf_len, int64_t& pos);
        void do_stat_local_time(char *buf, const int64_t buf_len, int64_t& pos);
        void do_stat_common(char *buf, const int64_t buf_len, int64_t& pos);
        void do_stat_schema_version(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_mem(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_table_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_tablet_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_cs(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_ms(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_ups(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_all_server(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_frozen_time(char* buf, const int64_t buf_len, int64_t &pos);
        int64_t get_stat_value(const int32_t index);
        void do_stat_cs_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_ms_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_merge(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_unusual_tablets_num(char* buf, const int64_t buf_len, int64_t &pos);

        void switch_root_table(ObRootTable2 *rt, ObTabletInfoManager *ti);
        int switch_schema_manager(const common::ObSchemaManagerV2 & schema_manager);
        /*
         * 在一个tabelt的各份拷贝中, 寻找合适的备份替换掉
         */
        int write_new_info_to_root_table(
            const common::ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index,
            ObRootTable2::const_iterator& first, ObRootTable2::const_iterator& last, ObRootTable2 *p_root_table);
        bool check_all_tablet_safe_merged(void) const;
        int create_root_table_for_build();
        DISALLOW_COPY_AND_ASSIGN(ObRootServer2);
        int get_rowkey_info(const uint64_t table_id, common::ObRowkeyInfo &info) const;
        int select_cs(const int64_t select_num, common::ObArray<std::pair<common::ObServer, int32_t> > &chunkservers);
      private:
        int try_create_new_tables(int64_t fetch_version);
        int try_create_new_table(int64_t frozen_version, const uint64_t table_id);
        int check_tablets_legality(const common::ObTabletInfoList &tablets);
        int split_table_range(const int64_t frozen_version, const uint64_t table_id,
            common::ObTabletInfoList &tablets);
        int create_table_tablets(const uint64_t table_id, const common::ObTabletInfoList & list);
        int create_tablet_with_range(const int64_t frozen_version,
            const common::ObTabletInfoList& tablets);
        int create_empty_tablet_with_range(const int64_t frozen_version,
            ObRootTable2 *root_table, const common::ObTabletInfo &tablet,
            int32_t& created_count, int* t_server_index);

        /// for create and delete table xielun.szd
        int drop_one_table(const bool if_exists, const common::ObString & table_name, bool & refresh);
        /// force sync schema to all servers include ms\cs\master ups
        int force_sync_schema_all_servers(const common::ObSchemaManagerV2 &schema);
        int force_heartbeat_all_servers(void);
        int get_ms(common::ObServer& ms_server);
      private:
        static const int MIN_BALANCE_TOLERANCE = 1;

        common::ObClientHelper client_helper_;
        ObRootServerConfig &config_;
        ObRootWorker* worker_; //who does the net job
        ObRootLogWorker* log_worker_;

        // cs & ms manager
        ObChunkServerManager server_manager_;
        mutable tbsys::CRWLock server_manager_rwlock_;

        mutable tbsys::CThreadMutex root_table_build_mutex_; //any time only one thread can modify root_table
        ObRootTable2* root_table_;
        ObTabletInfoManager* tablet_manager_;
        mutable tbsys::CRWLock root_table_rwlock_; //every query root table should rlock this
        common::ObTabletReportInfoList delete_list_;
        bool have_inited_;
        bool first_cs_had_registed_;
        volatile bool receive_stop_;

        mutable tbsys::CThreadMutex frozen_version_mutex_;
        int64_t last_frozen_mem_version_;
        int64_t last_frozen_time_;
        int64_t next_select_cs_index_;
        int64_t time_stamp_changing_;

        common::ObiRole obi_role_;        // my role as oceanbase instance
        common::ObServer my_addr_;

        time_t start_time_;
        // ups related
        ObUpsManager *ups_manager_;
        ObUpsHeartbeatRunnable *ups_heartbeat_thread_;
        ObUpsCheckRunnable *ups_check_thread_;
        // balance related
        ObRootBalancer *balancer_;
        ObRootBalancerRunnable *balancer_thread_;
        ObRestartServer *restart_server_;
        // schema service
        int64_t schema_timestamp_;
        int64_t privilege_timestamp_;
        common::ObSchemaService *schema_service_;
        common::ObScanHelperImpl *schema_service_scan_helper_;
        ObSchemaServiceMsProvider *schema_service_ms_provider_;
        ObSchemaServiceUpsProvider *schema_service_ups_provider_;
        // new root table service
        mutable tbsys::CThreadMutex rt_service_wmutex_;
        common::ObFirstTabletEntryMeta *first_meta_;
        common::ObRootTableService *rt_service_;
        // sequence async task queue
        ObRootAsyncTaskQueue seq_task_queue_;
        ObDailyMergeChecker merge_checker_;
        ObHeartbeatChecker heart_beat_checker_;
        // trigger tools
        ObRootMsProvider ms_provider_;
        common::ObTriggerEvent root_trigger_;
        // ddl operator
        tbsys::CThreadMutex mutex_lock_;
        ObRootDDLOperator ddl_tool_;
        ObBootState boot_state_;
        //to load local schema.ini file, only use the first_time
        common::ObSchemaManagerV2* local_schema_manager_;
        //used for cache
        common::ObSchemaManagerV2 * schema_manager_for_cache_;
        mutable tbsys::CRWLock schema_manager_rwlock_;

        ObRootServerState state_;  //RS state
        int64_t bypass_process_frozen_men_version_; //旁路导入状态下，可以广播的frozen_version
        ObRootOperationHelper operation_helper_;
        ObRootOperationDuty operation_duty_;
        common::ObTimer timer_;
    };
  }
}

#endif
