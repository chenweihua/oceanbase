////===================================================================
 //
 // ob_log_fetcher.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_FETCHER_H_
#define  OCEANBASE_LIBOBLOG_FETCHER_H_

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_mutator.h"
#include "common/ob_fixed_queue.h"
#include "common/ob_queue_thread.h"
#include "ob_log_filter.h"
#include "ob_log_server_selector.h"
#include "ob_log_rpc_stub.h"

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogFetcher : public IObLogFilter
    {
      public:
        virtual ~IObLogFetcher() {};

      public:
        virtual int init(IObLogServerSelector *server_selector, IObLogRpcStub *rpc_stub, const int64_t start_id) = 0;

        virtual void destroy() = 0;

        virtual int next_mutator(ObLogMutator **mutator, const int64_t timeout) = 0;

        virtual void release_mutator(ObLogMutator *mutator) = 0;

        virtual int64_t get_cur_id() const = 0;

        virtual bool contain(const uint64_t table_id) {UNUSED(table_id); return true;};
    };

    class ObLogFetcher : public IObLogFetcher, public tbsys::CDefaultRunnable
    {
      static const int64_t MAX_MUTATOR_NUM = 2048;
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      static const int64_t FETCH_BUFFER_SIZE = common::OB_MAX_LOG_BUFFER_SIZE;
      static const int64_t FETCH_LOG_TIMEOUT = 10L * 1000000L;
      static const int64_t WAIT_VLIST_TIMEOUT = 1L * 1000000L;
      static const int64_t UPS_ADDR_REFRESH_INTERVAL = 10L * 1000000L;
      static const int64_t FETCH_LOG_RETRY_TIMEWAIT = 100L * 1000L;
      static const int64_t COND_SPIN_WAIT_NUM = 40000;
      static const int64_t PRINT_MEMORY_USAGE_INTERVAL = 10L * 60L * 1000000L;
      static const int64_t PRINT_CHECKPOINT_INTERVAL = 10L * 1000000L;
      public:
        ObLogFetcher();
        ~ObLogFetcher();
      public:
        int init(IObLogServerSelector *server_selector, IObLogRpcStub *rpc_stub, const int64_t start_id);
        void destroy();
        int next_mutator(ObLogMutator **mutator, const int64_t timeout);
        void release_mutator(ObLogMutator *mutator);
        int64_t get_cur_id() const;
      public:
        virtual void run(tbsys::CThread* thread, void* arg);
      private:
        int init_mutator_list_(const int64_t max_mutator_num);
        void destroy_mutator_list_();
        void handle_fetched_log_(updateserver::ObFetchedLog &log, uint64_t &seq);
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        bool inited_;
        IObLogServerSelector *server_selector_;
        IObLogRpcStub *rpc_stub_;
        volatile int64_t start_id_;
        common::ObFixedQueue<ObLogMutator> p_list_;
        common::ObFixedQueue<ObLogMutator> v_list_;
        common::ObCond p_cond_;
        common::ObCond v_cond_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_FETCHER_H_

