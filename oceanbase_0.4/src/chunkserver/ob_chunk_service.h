/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *
 *  Version: 1.0 : 2010/08/22
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 */
#ifndef OCEANBASE_CHUNKSERVER_CHUNKSERVICE_H_
#define OCEANBASE_CHUNKSERVER_CHUNKSERVICE_H_

#include "common/ob_define.h"
#include "common/thread_buffer.h"
#include "common/ob_timer.h"
#include "ob_schema_task.h"
#include "easy_io.h"
#include "common/ob_cur_time.h"
#include "common/ob_ms_list.h"
#include "sql/ob_sql_read_param.h"

namespace oceanbase
{
  namespace common
  {
    class ObDataBuffer;
    class ObSchemaManager;
    class ObSchemaManagerV2;
    class ObMergerSchemaManager;
  }
  namespace chunkserver
  {
    class ObTablet;
    class ObChunkServer;
    class ObQueryService;
    class ObSqlQueryService;

    class ObChunkService
    {
      public:
        ObChunkService();
        ~ObChunkService();
      public:
        int initialize(ObChunkServer* chunk_server);
        int start();
        int destroy();
        int schedule_report_tablet();

      public:
        int do_request(
            const int64_t receive_time,
            const int32_t packet_code,
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time = 0);
      private:
        // warning: fetch schema interval can not be too long
        // because of the heartbeat handle will block tbnet thread
        static const int64_t FETCH_SCHEMA_INTERVAL = 30 * 1000;

        int cs_get(
            const int64_t start_time, 
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_batch_get(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_tablet_read(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_fetch_data(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_sql_scan(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_sql_get(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_sql_read(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time,
            sql::ObSqlReadParam *sql_read_param_ptr);

        int cs_scan(
            const int64_t start_time, 
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

        int cs_drop_old_tablets(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_heart_beat(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_create_tablet(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_accept_schema(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_load_tablet(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_delete_tablets(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_get_migrate_dest_loc(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_dump_tablet_image(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_fetch_stats(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_start_gc(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_reload_conf(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_show_param(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_stop_server(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_force_to_report_tablet(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);
        int cs_change_log_level(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_check_tablet(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_merge_tablets(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_send_file(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_sync_all_images(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_load_bypass_sstables(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_delete_table(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_fetch_sstable_dist(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_set_config(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_get_config(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_get_bloom_filter(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_time);

  
        int cs_disk_maintain(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);

        int cs_show_disk(
            const int32_t version,
            const int32_t channel_id,
            easy_request_t* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer);


      private:
        class LeaseChecker : public common::ObTimerTask
        {
          public:
            LeaseChecker(ObChunkService* service) : service_(service) {}
            ~LeaseChecker() {}
          public:
            virtual void runTimerTask();
          private:
            ObChunkService* service_;
        };

        class StatUpdater : public common::ObTimerTask
        {
          public:
            StatUpdater () : pre_request_count_(0),current_request_count_(0), run_count_(0) {}
            ~StatUpdater() {}
          public:
            virtual void runTimerTask();
          private:
            int64_t pre_request_count_;
            int64_t current_request_count_;
            int64_t run_count_;
        };

        class MergeTask : public common::ObTimerTask
        {
          public:
            MergeTask (ObChunkService* service)
              : frozen_version_(0), task_scheduled_(false), service_(service)  {}
          public:
            inline int64_t get_last_frozen_version() const { return frozen_version_; }
            void set_frozen_version(int64_t version) { frozen_version_ = version; }
            inline bool is_scheduled() const { return task_scheduled_; }
            inline void set_scheduled() { task_scheduled_ = true; }
            inline void unset_scheduled() { task_scheduled_ = false; }
            virtual void runTimerTask();
          private:
            int64_t frozen_version_;
            bool task_scheduled_;
            ObChunkService* service_;
        };

        class FetchUpsTask : public common::ObTimerTask
        {
          public:
            FetchUpsTask (ObChunkService* service) : service_(service) {}
          public:
            virtual void runTimerTask();
          private:
            ObChunkService* service_;
        };

        class ReportTabletTask : public common::ObTimerTask
        {
          public:
            ReportTabletTask(ObChunkService* service) 
              : service_(service), wait_seconds_(1), report_version_(0) {}
            int64_t get_next_wait_useconds();
            void start_retry_round();
          public:
            virtual void runTimerTask();
          private:
            ObChunkService* service_;
            int64_t wait_seconds_;
            int64_t report_version_;
        };

      private:
        int check_compress_lib(const char* compress_name_buf);
        int load_tablets();
        int register_self();
        int register_self_busy_wait(int32_t &status);
        int report_tablets_busy_wait();
        int fetch_schema_busy_wait(common::ObSchemaManagerV2 *schema);
        bool is_valid_lease();
        bool have_frozen_table(const ObTablet& tablet,const int64_t ups_data_version) const;
        int check_update_data(ObTablet& tablet,int64_t ups_data_version,bool& release_tablet);
        int get_sql_query_service(ObSqlQueryService *&service);
        int get_query_service(ObQueryService *&service);
        int reset_internal_status(bool release_table = true);
        int fetch_update_server_list();
        int get_master_master_update_server();
      private:
        DISALLOW_COPY_AND_ASSIGN(ObChunkService);
        ObChunkServer* chunk_server_;
        bool inited_;
        bool service_started_;
        bool in_register_process_;
        int64_t service_expired_time_;
        volatile uint32_t migrate_task_count_;
        volatile uint32_t scan_tablet_image_count_;

        common::ObTimer timer_;
        LeaseChecker lease_checker_;
        StatUpdater  stat_updater_;
        MergeTask    merge_task_;
        FetchUpsTask fetch_ups_task_;
        ObMergerSchemaTask fetch_schema_task_;
        ReportTabletTask report_tablet_task_;
        common::TimeUpdateDuty time_update_duty_;
        common::MsList ms_list_task_;
        common::ThreadSpecificBuffer query_service_buffer_;
    };


  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_CHUNKSERVER_CHUNKSERVICE_H_

