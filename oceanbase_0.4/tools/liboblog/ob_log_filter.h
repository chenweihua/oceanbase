////===================================================================
 //
 // ob_log_filter.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
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

#ifndef  OCEANBASE_LIBOBLOG_FILTER_H_
#define  OCEANBASE_LIBOBLOG_FILTER_H_

#include "common/ob_define.h"
#include "common/hash/ob_hashset.h"
#include "updateserver/ob_ups_mutator.h"
#include "ob_log_config.h"
#include "ob_log_meta_manager.h"

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogMutator : public updateserver::ObUpsMutator
    {
      public:
        ObLogMutator() {};
        ~ObLogMutator() {};
      public:
        void set_log_id(const uint64_t log_id) { log_id_ = log_id; };
        uint64_t get_log_id() const { return log_id_; };
        void set_db_partition(const uint64_t db_partition) { db_partition_ = db_partition; };
        uint64_t get_db_partition() const { return db_partition_; };
        void set_tb_partition(const uint64_t tb_partition) { tb_partition_ = tb_partition; };
        uint64_t get_tb_partition() const { return tb_partition_; };
        void set_num(const uint64_t num) {num_ = num;};
        uint64_t get_num() const {return num_;};
      public:
        uint64_t log_id_;
        uint64_t db_partition_;
        uint64_t tb_partition_;
        uint64_t num_;
    };

    class IObLogFilter
    {
      public:
        virtual ~IObLogFilter() {};
      public:
        virtual int init(IObLogFilter *filter,
            const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter) {UNUSED(filter); UNUSED(config); UNUSED(schema_getter); return common::OB_SUCCESS;};

        virtual void destroy() = 0;

        virtual int next_mutator(ObLogMutator **mutator, const int64_t timeout) = 0;

        virtual void release_mutator(ObLogMutator *mutator) = 0;

        virtual bool contain(const uint64_t table_id) = 0;
    };

    class ObLogTableFilter : public IObLogFilter
    {
      typedef common::hash::ObHashSet<uint64_t> TIDFilter;
      static const int64_t TID_FILTER_SIZE = 128;
      public:
        ObLogTableFilter();
        ~ObLogTableFilter();
      public:
        // TODO support erase table then re-add the same
        int init(IObLogFilter *filter,
            const ObLogConfig &config,
            IObLogSchemaGetter *schema_getter);
        void destroy();
        int next_mutator(ObLogMutator **mutator, const int64_t timeout);
        void release_mutator(ObLogMutator *mutator);
        bool contain(const uint64_t table_id);
      public:
        int operator ()(const char *tb_name, const ObLogSchema *total_schema);
      private:
        int init_tid_filter_(const ObLogConfig &config, IObLogSchemaGetter &schema_getter);
        bool contain_(ObLogMutator &mutator);
      private:
        bool inited_;
        IObLogFilter *prev_;
        TIDFilter tid_filter_;
    };

  }
}

#endif //OCEANBASE_LIBOBLOG_FILTER_H_

