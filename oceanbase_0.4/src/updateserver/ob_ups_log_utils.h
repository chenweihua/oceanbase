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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#ifndef __OB_UPDATESERVER_OB_UPS_LOG_UTILS_H__
#define __OB_UPDATESERVER_OB_UPS_LOG_UTILS_H__

#include "common/ob_define.h"
#include "common/ob_log_entry.h"
#include "common/ob_log_cursor.h"

namespace oceanbase
{
  namespace updateserver
  {
    enum ReplayType
    {
      RT_LOCAL = 0,
      RT_APPLY = 1,
    };

    class IObLogApplier
    {
      public:
        IObLogApplier(){}      
        virtual ~IObLogApplier(){}
        virtual int apply_log(const common::LogCommand cmd, const uint64_t seq,
                              const char* log_data, const int64_t data_len, const ReplayType replay_type) = 0;
    };

    class ObUpsTableMgr;
    class ObUpsMutator;
    class CommonSchemaManagerWrapper;
    class ObLogReplayWorker;

    class ObLogReplayPoint
    {
      public:
        static const char* REPLAY_POINT_FILE;
      public:
        ObLogReplayPoint(): log_dir_(NULL) {}
        ~ObLogReplayPoint(){}
        int init(const char* log_dir);
        int write(const int64_t replay_point);
        int get(int64_t& replay_point);
      private:
        const char* log_dir_;
    };

    int64_t set_counter(tbsys::CThreadCond& cond, volatile int64_t& counter, const int64_t new_counter);
    int64_t wait_counter(tbsys::CThreadCond& cond, volatile int64_t& counter, const int64_t limit, const int64_t timeout_us);

    int get_local_max_log_cursor_func(const char* log_dir, const common::ObLogCursor& start_cursor,
                                      common::ObLogCursor& end_cursor);
    int get_local_max_log_cursor_func(const char* log_dir, const uint64_t log_file_id_by_sst, common::ObLogCursor& log_cursor);

    int replay_single_log_func(ObUpsMutator& mutator, CommonSchemaManagerWrapper& schema, ObUpsTableMgr* table_mgr, common::LogCommand cmd, const char* log_data, int64_t data_len, const int64_t commit_id, const ReplayType replay_type);
    int replay_local_log_func(const volatile bool& stop, const char* log_dir,
                              const common::ObLogCursor& start_cursor, common::ObLogCursor& end_cursor,
                              ObLogReplayWorker& replay_worker);
    int replay_local_log_func(const volatile bool& stop, const char* log_dir, const common::ObLogCursor& start_cursor,
                              common::ObLogCursor& end_cursor, IObLogApplier* log_applier);
    int replay_log_in_buf_func(const char* log_data, int64_t data_len, IObLogApplier* log_applier);


    int serialize_log_entry(char* buf, const int64_t len, int64_t& pos, common::ObLogEntry& entry,
                            const char* log_data, const int64_t data_len);
    int generate_log(char* buf, const int64_t len, int64_t& pos, common::ObLogCursor& cursor, const common::LogCommand cmd,
                     const char* log_data, const int64_t data_len);
    int set_entry(common::ObLogEntry& entry, const int64_t seq, const common::LogCommand cmd,
                  const char* log_data, const int64_t data_len);
    int serialize_log_entry(char* buf, const int64_t len, int64_t& pos, const common::LogCommand cmd, const int64_t seq,
                            const char* log_data, const int64_t data_len);

    int parse_log_buffer(const char* log_data, const int64_t len,
                         int64_t& start_id, int64_t& end_id);
    int trim_log_buffer(const char* log_data, const int64_t len, int64_t& end_pos,
                        int64_t& start_id, int64_t& end_id);
    int trim_log_buffer(const char* log_data, const int64_t len, int64_t& end_pos,
                        int64_t& start_id, int64_t& end_id, bool& is_file_end);
    int trim_log_buffer(const int64_t offset, const int64_t align_bits,
                        const char* log_data, const int64_t len, int64_t& end_pos,
                        int64_t& start_id, int64_t& end_id, bool& is_file_end);
 } // end namespace updateserver
} // end namespace oceanbase
#endif /* __OB_UPDATESERVER_OB_UPS_LOG_UTILS_H__ */
