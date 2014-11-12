////===================================================================
 //
 // ob_log_router.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_ROUTER_H_
#define  OCEANBASE_LIBOBLOG_ROUTER_H_

#include "common/ob_define.h"
#include "common/ob_queue_thread.h"
#include "common/hash/ob_hashmap.h"
#include "ob_log_filter.h"
#include "ob_log_partitioner.h"
#include "ob_log_meta_manager.h"

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogObserver
    {
      public:
        IObLogObserver() : inc_num_(1) {};
        virtual ~IObLogObserver() {};
      public:
        virtual int add_mutator(ObLogMutator &mutator,
            const uint64_t db_partition,
            const uint64_t tb_partition,
            volatile bool &loop_stop) = 0;
        uint64_t inc_num() {return inc_num_++;};
      private:
        uint64_t inc_num_;
    };

    class IObLogRouter
    {
      public:
        virtual ~IObLogRouter() {};
      public:
        virtual int init(ObLogConfig &config,
            IObLogFilter *source,
            IObLogPartitioner *partitioner,
            IObLogSchemaGetter *schema_getter) = 0;

        virtual void destroy() = 0;

        virtual int register_observer(const uint64_t partition, IObLogObserver *observer) = 0;

        virtual void withdraw_observer(const uint64_t partition) = 0;

        virtual int launch() = 0;
    };

    class ObLogRouter : public IObLogRouter, public tbsys::CDefaultRunnable, public common::S2MQueueThread
    {
      typedef common::hash::ObHashMap<uint64_t, IObLogObserver*> ObserverMap;
      static const int64_t MAX_OBSERVER_NUM = 1024;
      static const int64_t WAIT_SOURCE_TIMEOUT = 1000000L;
      static const int64_t SCHEMA_GETTER_REFRESH_INTERVAL = 10L * 1000000L;
      static const int64_t THREAD_QUEUE_SIZE = 100000L;
      public:
        ObLogRouter();
        ~ObLogRouter();
      public:
        int init(ObLogConfig &config,
            IObLogFilter *source,
            IObLogPartitioner *partitioner,
            IObLogSchemaGetter *schema_getter);
        void destroy();
        int register_observer(const uint64_t partition, IObLogObserver *observer);
        void withdraw_observer(const uint64_t partition);
        int launch();
      public:
        virtual void run(tbsys::CThread* thread, void* arg);
        virtual void handle(void *task, void *pdata) {UNUSED(task); UNUSED(pdata);};
        virtual void handle_with_stopflag(void *task, void *pdata, volatile bool &stop_flag);
        virtual void on_end(void *ptr) {UNUSED(ptr); mysql_thread_end(); TBSYS_LOG(INFO, "mysql_thread_end has been called");};
      private:
        bool inited_;
        bool launched_;
        IObLogFilter *source_;
        IObLogPartitioner *partitioner_;
        IObLogSchemaGetter *schema_getter_;
        common::SpinRWLock observer_map_lock_;
        ObserverMap observer_map_;
    };

  }
}

#endif //OCEANBASE_LIBOBLOG_ROUTER_H_

