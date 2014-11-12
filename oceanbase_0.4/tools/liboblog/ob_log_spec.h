////===================================================================
 //
 // ob_log_spec.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_SPEC_H_
#define  OCEANBASE_LIBOBLOG_SPEC_H_

#include "common/ob_define.h"
#include "liboblog.h"
#include "ob_log_config.h"
#include "ob_log_fetcher.h"
#include "ob_log_filter.h"
#include "ob_log_formator.h"
#include "ob_log_meta_manager.h"
#include "ob_log_partitioner.h"
#include "ob_log_router.h"
#include "ob_log_rpc_stub.h"
#include "ob_log_server_selector.h"

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogSpec : public IObLogSpec
    {
      public:
        ObLogSpec();
        ~ObLogSpec();
      public:
        int init(const char *config, const uint64_t checkpoint);
        void destroy();
        int launch();
      public:
        IObLogRouter *get_log_router();
        IObLogMetaManager *get_meta_manager();
        IObLogSchemaGetter *get_schema_getter();
        IObLogFilter *get_log_filter();
        const ObLogConfig &get_config() const {return config_;};
        uint64_t get_start_checkpoint() const {return start_checkpoint_;};
      private:
        int construct_components_();
        int init_components_(const uint64_t checkpoint);
        void destroy_components_();
        void deconstruct_components_();
      private:
        bool inited_;
        ObLogConfig             config_;
        IObLogRpcStub           *rpc_stub_;
        IObLogServerSelector    *server_selector_;
        IObLogSchemaGetter      *schema_getter_;
        IObLogDBNameBuilder     *dbname_builder_;
        IObLogTBNameBuilder     *tbname_builder_;
        IObLogMetaManager       *meta_manager_;
        IObLogPartitioner       *log_partitioner_;
        IObLogFetcher           *log_fetcher_;
        IObLogFilter            *log_filter_;
        IObLogRouter            *log_router_;
        uint64_t start_checkpoint_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_SPEC_H_

