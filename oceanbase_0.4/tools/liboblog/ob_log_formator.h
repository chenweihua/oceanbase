////===================================================================
 //
 // ob_log_formator.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-06-07 by Yubai (yubai.lk@alipay.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 // 
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_LIBOBLOG_FORMATOR_H_
#define  OCEANBASE_LIBOBLOG_FORMATOR_H_

#include "common/ob_define.h"
#include "common/ob_fixed_queue.h"
#include "common/ob_queue_thread.h"
#include "common/ob_resource_pool.h"
#include "ob_log_router.h"
#include "ob_log_filter.h"
#include "ob_log_meta_manager.h"
#include "ob_log_mysql_adaptor.h"
#include "BinlogRecord.h"

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogBinlogRecord : public BinlogRecord
    {
      public:
        ObLogBinlogRecord() : next_(NULL), mutator_num_(0) {};
      public:
        void set_next(ObLogBinlogRecord *next) {next_ = next;};
        ObLogBinlogRecord *get_next() {return next_;};
        std::string* toParsedString(int version) { UNUSED(version); return NULL; }
        void set_mutator_num(const uint64_t mutator_num) {mutator_num_ = mutator_num;};
        uint64_t get_mutator_num() const {return mutator_num_;};
      private:
        ObLogBinlogRecord *next_;
        uint64_t mutator_num_;
    };

    class IObLogFormator : public IObLogObserver
    {
      public:
        virtual ~IObLogFormator() {};

      public:
        virtual int init(const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter,
            IObLogMetaManager *meta_manager,
            IObLogFilter *log_filter) = 0;

        virtual void destroy() = 0;
        
        virtual int next_record(IBinlogRecord **record, const int64_t timeout_us) = 0;

        virtual void release_record(IBinlogRecord *record) = 0;

        virtual int add_mutator(ObLogMutator &mutator,
            const uint64_t db_partition,
            const uint64_t tb_partition,
            volatile bool &loop_stop) = 0;
    };

    class ObLogString : public std::string
    {
      public:
        void reset() {std::string::clear();};
    };

#ifdef __SPARSE_AVAILABLE__
    class ObLogSparseFormator : public IObLogFormator
    {
      static const int64_t MAX_BINLOG_RECORD_NUM = 10240;
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      static const int64_t WAIT_VLIST_TIMEOUT = 1L * 1000000L;
      static const int64_t COND_SPIN_WAIT_NUM = 40000;
      public:
        ObLogSparseFormator();
        ~ObLogSparseFormator();
      public:
        int init(const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter,
            IObLogMetaManager *meta_manager,
            IObFilter *log_filter);
        void destroy();
        int next_record(IBinlogRecord **record, const int64_t timeout);
        void release_record(IBinlogRecord *record);
        int add_mutator(ObLogMutator &mutator,
            const uint64_t db_partition,
            const uint64_t tb_partition,
            volatile bool &loop_stop);
      private:
        int init_binlogrecord_list_(const int64_t max_binlogrecord_num);
        void destroy_binlogrecord_list_();
        ObLogBinlogRecord *fetch_binlog_record_(volatile bool &loop_stop);
        int fill_trans_barrier_(const RecordType type,
            const int64_t timestamp,
            const uint64_t checkpoint,
            const uint64_t mutator_num,
            volatile bool &loop_stop);
        int fill_binlog_record_(ObLogBinlogRecord &binlog_record,
            const bool irc,
            const common::ObDmlType dml_type,
            const int64_t timestamp,
            const uint64_t checkpoint,
            const uint64_t mutator_num,
            const common::ObCellInfo &cell,
            const uint64_t db_partition,
            const uint64_t tb_partition);
        const char *value2str_(const common::ObObj &v);
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        bool inited_;
        IObLogSchemaGetter *schema_getter_;
        IObLogMetaManager *meta_manager_;
        IObLogFilter *log_filter_;
        common::ObFixedQueue<ObLogBinlogRecord> p_list_;
        common::ObFixedQueue<ObLogBinlogRecord> v_list_;
        common::ObCond p_cond_;
        common::ObCond v_cond_;
        common::ObResourcePool<ObLogString, 0, MAX_BINLOG_RECORD_NUM * 64> string_pool_;
    };
#endif

#ifdef __DENSE_AVAILABLE__
    class ObLogDenseFormator : public IObLogFormator
    {
      struct RowValue
      {
        std::string *columns[OB_MAX_COLUMN_NUMBER];
        int32_t num;
        void reset()
        {
          num = 0;
        };
      };
      static const int64_t MAX_BINLOG_RECORD_NUM = 10240;
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      static const int64_t WAIT_VLIST_TIMEOUT = 1L * 1000000L;
      static const int64_t COND_SPIN_WAIT_NUM = 40000;
      static const int64_t MAX_THREAD_NUM = 128;
      static const int64_t PRINT_BR_USAGE_INTERVAL = 10L * 1000000L;
      public:
        ObLogDenseFormator();
        ~ObLogDenseFormator();
      public:
        int init(const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter,
            IObLogMetaManager *meta_manager,
            IObLogFilter *log_filter);
        void destroy();
        int next_record(IBinlogRecord **record, const int64_t timeout);
        void release_record(IBinlogRecord *record);
        int add_mutator(ObLogMutator &mutator,
            const uint64_t db_partition,
            const uint64_t tb_partition,
            volatile bool &loop_stop);
      private:
        int init_binlogrecord_list_(const int64_t max_binlogrecord_num);
        void destroy_binlogrecord_list_();
        ObLogBinlogRecord *fetch_binlog_record_(volatile bool &loop_stop);
        int fill_trans_barrier_(const RecordType type,
            const int64_t timestamp,
            const uint64_t checkpoint,
            const uint64_t mutator_num,
            ObLogBinlogRecord *&br_list_tail,
            volatile bool &loop_stop);
        int fill_binlog_record_(ObLogBinlogRecord &binlog_record,
            const bool irc,
            common::ObDmlType *dml_type,
            const int64_t timestamp,
            const uint64_t checkpoint,
            const uint64_t mutator_num,
            const common::ObCellInfo &cell,
            const uint64_t db_partition,
            const uint64_t tb_partition);
        const char *value2str_(const common::ObObj &v);
        int fill_binlog_record_(ObLogBinlogRecord &binlog_record,
            ObLogMutator &mutator,
            const bool only_rowkey);
        int fill_row_value_(RowValue *row_value,
            const ObLogSchema *total_schema,
            const uint64_t table_id,
            const common::ObRowkey &rowkey);
        int fill_row_value_(RowValue *row_value,
            const ObLogSchema *total_schema,
            const ObCellInfo &cell);
        int fill_old_value_(ObLogBinlogRecord &binlog_record,
            const uint64_t table_id);
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        bool inited_;
        bool need_query_back_;
        IObLogSchemaGetter *schema_getter_;
        IObLogMetaManager *meta_manager_;
        IObLogFilter *log_filter_;
        common::ObFixedQueue<ObLogBinlogRecord> p_list_;
        common::ObFixedQueue<ObLogBinlogRecord> v_list_;
        common::ObCond p_cond_;
        common::ObCond v_cond_;
        common::ObResourcePool<ObLogString, 0, MAX_BINLOG_RECORD_NUM * 64> string_pool_;
        common::ObResourcePool<RowValue, 0, MAX_THREAD_NUM> row_value_pool_;
        ObLogMysqlAdaptor mysql_adaptor_;
        ObLogBinlogRecord *br_list_iter_;
        std::string null_str_;
    };
#endif
  }
}

#endif //OCEANBASE_LIBOBLOG_FORMATOR_H_

