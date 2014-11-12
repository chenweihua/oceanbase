/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_balancer.h
 * rebalance and re-replication functions
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_BALANCER_H
#define _OB_ROOT_BALANCER_H 1
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
#include "common/ob_schema_service.h"
#include "common/ob_data_source_desc.h"
#include "ob_root_table2.h"
#include "ob_chunk_server_manager.h"
#include "ob_root_rpc_stub.h"
#include "ob_root_stat.h"
#include "ob_root_log_worker.h"
#include "ob_ups_manager.h"
#include "ob_tablet_info_manager.h"
#include "ob_restart_server.h"
#include "ob_root_server_config.h"
#include "tbsys.h"
#include "ob_data_source_mgr.h"
#include "ob_root_balancer_runnable.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObBootState;
    class ObRootTable2;
    class ObRestartServer;
    class ObRootServer2;
    class ObRootBalancerRunnable;

    enum RereplicationAction
    {
      RA_NOP = 0,
      RA_COPY = 1,
      RA_DELETE = 2
    };

    struct ObLoadDataInfo
    {
      ObLoadDataInfo();
      int clone(ObLoadDataInfo& info);
      enum ObLoadDataStatus
      {
        INIT = 0,
        PREPARE = 1,
        DOING = 2,
        DONE = 3,
        FAILED = 4,
        KILLED = 5,
        COPIED = 6,
      };

      // methods
      const char* get_status() const;
      void reset();
      int set_info(const ObString& table_name, const uint64_t table_id, const uint64_t old_table_id, ObString& uri,
          const int64_t tablet_version, const int64_t start_time, const ObLoadDataStatus status);

      // data
      ObString table_name_;
      uint64_t table_id_;
      uint64_t old_table_id_;
      int64_t start_time_;
      int64_t end_time_;
      int64_t tablet_version_;
      ObString uri_;
      ObLoadDataStatus status_;
      ObDataSourceDesc::ObDataSourceType data_source_type_;
      ObStringBuf buffer_;
    };

    class ObRootBalancer
    {
      public:
        ObRootBalancer();
        virtual ~ObRootBalancer();
        void set_config(ObRootServerConfig *config);
        void set_ddl_operation_mutex(tbsys::CThreadMutex *mutex_lock);
        void set_root_table(ObRootTable2 *root_table);
        void set_root_table_lock(tbsys::CRWLock *root_table_rwlock);
        void set_server_manager(ObChunkServerManager *server_manager);
        void set_server_manager_lock(tbsys::CRWLock *server_manager_rwlock);
        void set_schema_manager(common::ObSchemaManagerV2 *schema_manager);
        void set_schema_manager_lock(tbsys::CRWLock *schema_manager_rwlock);
        void set_rpc_stub(ObRootRpcStub *rpc_stub);
        void set_log_worker(ObRootLogWorker* log_worker);
        void set_restart_server(ObRestartServer* restart_server);
        void set_role_mgr(common::ObRoleMgr *role_mgr);
        void set_boot_state(ObBootState *boot_state);
        void set_tablet_manager(ObTabletInfoManager* tablet_manager);
        void set_ups_manager(ObUpsManager *ups_manager);
        void set_root_server(ObRootServer2 *root_server);
        void set_balancer_thread(ObRootBalancerRunnable *balancer_thread) {balancer_thread_ = balancer_thread;}

        void do_balance_or_load();
        int nb_trigger_next_migrate(const ObDataSourceDesc& desc, int32_t result);
        // monitor functions
        void nb_print_balance_infos(char *buf, const int64_t buf_len, int64_t& pos); // for monitor
        void dump_migrate_info() const; // for monitor
        void nb_print_shutting_down_progress(char *buf, const int64_t buf_len, int64_t& pos); // for monitor

        // testing functions
        bool nb_is_all_tables_balanced(const common::ObServer &except_cs); // only for testing
        bool nb_is_all_tablets_replicated(int32_t expected_replicas_num);    // only for testing
        void nb_find_can_restart_server(int32_t expected_replicas_num);
        int nb_calculate_sstable_count(const uint64_t table_id, int64_t &avg_size, int64_t &avg_count,
            int32_t &cs_num, int64_t &migrate_out_per_cs, int32_t &shutdown_count);
        int nb_calculate_sstable_count(const uint64_t table_id, int64_t &avg_size, int64_t &avg_count,
            int32_t &cs_num, int64_t &migrate_out_per_cs, int32_t &shutdown_count,
            bool& need_replicate, bool& table_found, int64_t& total_tablet_count, int64_t& safe_tablet_count); // public only for testing
        bool nb_did_cs_have_no_tablets(const common::ObServer &cs) const;

        int add_load_table(const ObString& table_name, const uint64_t table_id, ObString& uri, const int64_t start_time);
        int kill_load_table(const ObString& table_name, const uint64_t table_id);
        int handle_load_table_done(const uint64_t table_id);
        void handle_load_table_failed(const uint64_t table_id, const bool need_write_handle_failed_log = true);
        void update_load_table_history(const ObLoadDataInfo& info);
        int get_data_load_info(const uint64_t table_id, ObDataSourceDesc::ObDataSourceType& type,
            ObString*& uri, int64_t& tablet_version);
        int set_import_status(const ObString& table_name, const uint64_t table_id, const ObLoadDataInfo::ObLoadDataStatus& status);
        int get_import_status(const ObString& table_name, const uint64_t table_id, ObLoadDataInfo::ObLoadDataStatus& status);
        int check_import_status_of_all_clusters(const ObString& table_name, const uint64_t table_id, bool& is_finished);
        int start_set_import_status(const ObString& table_name, const uint64_t table_id, const ObLoadDataInfo::ObLoadDataStatus& status);

        bool is_table_loading(uint64_t table_id) const;//check is the table with this table id is been loading
        bool is_loading_data() const { return is_loading_data_;} // check if any table is been loading
        void do_load_data();
        int64_t get_loading_data_infos_count();
        void do_balance();
        int add_load_table_from_log(const ObString& table_name, const uint64_t table_id, const uint64_t old_table_id,
            ObString& uri, const int64_t start_time, const int64_t tablet_version);
        int delete_load_table_from_log(const uint64_t table_id, const int32_t status, const int64_t end_time);
        int write_to_file(const char* filename);
        int read_from_file(const char* filename);
      private:
        //check wether shutdown_cs is migrate clean
        void check_shutdown_process();
        void check_components() const;
        int nb_balance_by_table(const uint64_t table_id, bool &scan_next_table);
        int do_rereplication_by_table(const uint64_t table_id, bool &scan_next_table);
        int do_rereplication_by_table(const uint64_t table_id, bool &scan_next_table,
            bool& need_replicate, bool& table_found, int64_t& total_tablet_count, int64_t& safe_tablet_count);
        int nb_find_dest_cs(ObRootTable2::const_iterator meta, int64_t low_bound, int32_t cs_num,
            int32_t &dest_cs_idx, ObChunkServerManager::iterator &dest_it);
        uint64_t nb_get_next_table_id(int32_t table_count, int32_t seq = -1);
        int32_t nb_get_table_count();
        int send_msg_migrate(const ObServer &dest, const ObDataSourceDesc& data_source_desc);
        int nb_start_batch_migrate();
        int nb_check_migrate_timeout();
        bool nb_is_curr_table_balanced(int64_t avg_sstable_count, const common::ObServer &except_cs) const;
        bool nb_is_curr_table_balanced(int64_t avg_sstable_count) const;
        void nb_print_balance_info() const;
        void nb_print_migrate_infos() const;
        // @return 0 do not copy, 1 copy immediately, -1 delayed copy
        int need_copy(int32_t available_num, int32_t lost_num);
        int nb_add_copy(ObRootTable2::const_iterator it, const common::ObTabletInfo* tablet, int64_t low_bound, int32_t cs_num);
        int nb_del_copy(ObRootTable2::const_iterator it, const common::ObTabletInfo* tablet, int32_t &last_delete_cs_index);
        int nb_select_copy_src(ObRootTable2::const_iterator it,
            int32_t &src_cs_idx, ObChunkServerManager::iterator &src_it, int64_t& tablet_version);
        int nb_check_rereplication(ObRootTable2::const_iterator it, RereplicationAction &act);
        int nb_check_add_migrate(ObRootTable2::const_iterator it, const common::ObTabletInfo* tablet, int64_t avg_count,
            int32_t cs_num, int64_t migrate_out_per_cs);

        bool nb_is_all_tables_balanced(); // only for testing

        bool check_not_ini_table(const uint64_t table_id) const;
        void check_table_rereplication(const uint64_t table_id, const int64_t avg_count,
            const int64_t cs_num, bool & scan_next_table);
        int fetch_range_list(const ObDataSourceDesc::ObDataSourceType data_source_type,
            ObString& uri, const ObString& table_name, const uint64_t table_id,
            ObList<ObNewRange*>& range_table, ModuleArena& allocator);

        void nb_print_balance_info(char *buf, const int64_t buf_len, int64_t& pos) const; // for monitor
        int is_table_replicated(const uint64_t table_id, const int64_t tablet_version, bool& is_replicated);
        int get_ms(ObServer& ms_server);
        int init_load_data_env();
        int add_load_table_task_info(const ObString& table_name, const uint64_t table_id,
            ObString& uri, const int64_t start_time, ObString& simple_uri, ObDataSourceDesc::ObDataSourceType& data_source_type);
        int check_replica_count_for_import(int64_t tablet_version);

        // disallow copy
        ObRootBalancer(const ObRootBalancer &other);
        ObRootBalancer& operator=(const ObRootBalancer &other);
      private:
        // constants
        static const int32_t MAX_LOAD_INFO_CONCURRENCY = 256;

        // data members
        ObRootServerConfig *config_;
        ObRootTable2 *root_table_;
        ObTabletInfoManager* tablet_manager_;
        ObChunkServerManager *server_manager_;
        tbsys::CRWLock *root_table_rwlock_;
        tbsys::CRWLock *server_manager_rwlock_;
        tbsys::CRWLock load_data_lock_;
        ObRootRpcStub *rpc_stub_;
        ObRootLogWorker* log_worker_;
        common::ObRoleMgr *role_mgr_;
        ObUpsManager *ups_manager_;
        ObBootState *boot_state_;
        ObRootServer2 *root_server_;
        ObRestartServer* restart_server_;
        tbsys::CThreadMutex *mutex_lock_;

        common::ObTabletReportInfoList delete_list_;
        int64_t balance_start_time_us_;
        int64_t balance_timeout_us_;
        int64_t balance_last_migrate_succ_time_;
        int32_t balance_next_table_seq_;
        int32_t balance_batch_migrate_count_;
        int32_t balance_batch_migrate_done_num_;
        int32_t balance_select_dest_start_pos_;
        int32_t balance_batch_copy_count_; // for monitor purpose
        int32_t balance_batch_delete_count_;
        ObLoadDataInfo load_data_infos_[MAX_LOAD_INFO_CONCURRENCY];
        ObDataSourceMgr data_source_mgr_;
        bool is_loading_data_;
        ObRootBalancerRunnable *balancer_thread_;
    };

    inline void ObRootBalancer::set_root_server(ObRootServer2 *root_server)
    {
      root_server_ = root_server;
    }
    inline void ObRootBalancer::set_tablet_manager(ObTabletInfoManager* tablet_manager)
    {
      tablet_manager_ = tablet_manager;
    }
    inline void ObRootBalancer::set_boot_state(ObBootState *boot_state)
    {
      boot_state_ = boot_state;
    }
    inline void ObRootBalancer::set_ddl_operation_mutex(tbsys::CThreadMutex *mutex_lock)
    {
      mutex_lock_ = mutex_lock;
    }
    inline void ObRootBalancer::set_config(ObRootServerConfig *config)
    {
      config_ = config;
      data_source_mgr_.set_config(config);
    }
    inline void ObRootBalancer::set_root_table(ObRootTable2 *root_table)
    {
      root_table_ = root_table;
    }
    inline void ObRootBalancer::set_root_table_lock(tbsys::CRWLock *root_table_rwlock)
    {
      root_table_rwlock_ = root_table_rwlock;
    }
    inline void ObRootBalancer::set_server_manager(ObChunkServerManager *server_manager)
    {
      server_manager_ = server_manager;
      data_source_mgr_.set_server_manager(server_manager);
    }
    inline void ObRootBalancer::set_server_manager_lock(tbsys::CRWLock *server_manager_rwlock)
    {
      server_manager_rwlock_ = server_manager_rwlock;
    }
    inline void ObRootBalancer::set_rpc_stub(ObRootRpcStub *rpc_stub)
    {
      rpc_stub_ = rpc_stub;
      data_source_mgr_.set_rpc_stub(rpc_stub);
    }
    inline void ObRootBalancer::set_log_worker(ObRootLogWorker* log_worker)
    {
      log_worker_ = log_worker;
    }
    inline void ObRootBalancer::set_role_mgr(common::ObRoleMgr *role_mgr)
    {
      role_mgr_ = role_mgr;
    }
    inline void ObRootBalancer::set_ups_manager(ObUpsManager *ups_manager)
    {
      ups_manager_ = ups_manager;
    }
    inline void ObRootBalancer::set_restart_server(ObRestartServer* restart_server)
    {
      restart_server_ = restart_server;
    }
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_BALANCER_H */

