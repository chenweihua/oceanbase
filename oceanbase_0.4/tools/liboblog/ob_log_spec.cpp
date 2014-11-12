////===================================================================
 //
 // ob_log_spec.cpp liboblog / Oceanbase
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

#include "tbsys.h"
#include "ob_log_spec.h"
#include "ob_log_utils.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogSpec::ObLogSpec() : inited_(false),
                             config_(),
                             rpc_stub_(NULL),
                             server_selector_(NULL),
                             schema_getter_(NULL),
                             dbname_builder_(NULL),
                             tbname_builder_(NULL),
                             meta_manager_(NULL),
                             log_partitioner_(NULL),
                             log_fetcher_(NULL),
                             log_filter_(NULL),
                             log_router_(NULL),
                             start_checkpoint_(0)
    {
    }

    ObLogSpec::~ObLogSpec()
    {
      destroy();
    }

    int ObLogSpec::init(const char *config, const uint64_t checkpoint)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == config)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = config_.init(config)))
      {
        TBSYS_LOG(WARN, "config=%s init fail, ret=%d", config, ret);
      }
      else if (OB_SUCCESS != (ret = construct_components_()))
      {
        TBSYS_LOG(WARN, "construct components fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = init_components_(checkpoint)))
      {
        TBSYS_LOG(WARN, "init components fail, ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "init log_spec config=%s checkpoint=%lu", config, checkpoint);
        start_checkpoint_ = checkpoint;
        inited_ = true;
      }
      if (OB_SUCCESS != ret)
      {
        destroy_components_();
        deconstruct_components_();
      }
      return ret;
    }

    void ObLogSpec::destroy()
    {
      inited_ = false;
      destroy_components_();
      deconstruct_components_();
      config_.destroy();
    }

    int ObLogSpec::launch()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = log_router_->launch();
      }
      return ret;
    }

    IObLogRouter *ObLogSpec::get_log_router()
    {
      IObLogRouter *ret = NULL;
      if (inited_)
      {
        ret = log_router_;
      }
      return ret;
    }

    IObLogMetaManager *ObLogSpec::get_meta_manager()
    {
      IObLogMetaManager *ret = NULL;
      if (inited_)
      {
        ret = meta_manager_;
      }
      return ret;
    }

    IObLogSchemaGetter *ObLogSpec::get_schema_getter()
    {
      IObLogSchemaGetter *ret = NULL;
      if (inited_)
      {
        ret = schema_getter_;
      }
      return ret;
    }

    IObLogFilter *ObLogSpec::get_log_filter()
    {
      IObLogFilter *ret = NULL;
      if (inited_)
      {
        ret = log_filter_;
      }
      return ret;
    }

#define CONSTRUCT(v, type) \
    if (OB_SUCCESS == ret \
        && NULL == (v = new(std::nothrow) type())) \
    { \
      TBSYS_LOG(WARN, "construct %s fail", #type); \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
    }

#define RCALL(f, v, args...) \
    if (OB_SUCCESS == ret \
        && OB_SUCCESS != (ret = v->f(args))) \
    { \
      TBSYS_LOG(WARN, "call %s this=%p fail, ret=%d", #f, v, ret); \
    }

#define CALL(f, v) \
    if (NULL != v) \
    { \
      v->f(); \
    }

#define DECONSTRUCT(v) \
    if (NULL != v) \
    { \
      delete v; \
      v = NULL; \
    }

    int ObLogSpec::construct_components_()
    {
      int ret = OB_SUCCESS;
      CONSTRUCT(rpc_stub_,        ObLogRpcStub);
      CONSTRUCT(server_selector_, ObLogServerSelector);
      CONSTRUCT(schema_getter_,   ObLogSchemaGetter);
      CONSTRUCT(dbname_builder_,  ObLogDBNameBuilder);
      CONSTRUCT(tbname_builder_,  ObLogTBNameBuilder);
      CONSTRUCT(meta_manager_,    ObLogMetaManager);
      CONSTRUCT(log_partitioner_, ObLogPartitioner);
      CONSTRUCT(log_fetcher_,     ObLogFetcher);
      CONSTRUCT(log_filter_,      ObLogTableFilter);
      CONSTRUCT(log_router_,      ObLogRouter);
      return ret;
    }

    int ObLogSpec::init_components_(const uint64_t checkpoint)
    {
      int ret = OB_SUCCESS;
      RCALL(init,       rpc_stub_);
      RCALL(init,       server_selector_,   config_);
      RCALL(init,       schema_getter_,     server_selector_, rpc_stub_);
      RCALL(init,       dbname_builder_,    config_);
      RCALL(init,       tbname_builder_,    config_);
      RCALL(init,       meta_manager_,      schema_getter_, dbname_builder_, tbname_builder_);
      RCALL(init,       log_partitioner_,   config_, schema_getter_);
      RCALL(init,       log_fetcher_,       server_selector_, rpc_stub_, checkpoint);
      RCALL(init,       log_filter_,        log_fetcher_, config_, schema_getter_);
      RCALL(init,       log_router_,        config_, log_filter_, log_partitioner_, schema_getter_);
      return ret;
    }

    void ObLogSpec::destroy_components_()
    {
      CALL(destroy,     log_router_);
      CALL(destroy,     log_filter_);
      CALL(destroy,     log_fetcher_);
      CALL(destroy,     log_partitioner_);
      CALL(destroy,     meta_manager_);
      CALL(destroy,     tbname_builder_);
      CALL(destroy,     dbname_builder_);
      CALL(destroy,     schema_getter_);
      CALL(destroy,     server_selector_);
      CALL(destroy,     rpc_stub_);
    }

    void ObLogSpec::deconstruct_components_()
    {
      DECONSTRUCT(log_router_);
      DECONSTRUCT(log_filter_);
      DECONSTRUCT(log_fetcher_);
      DECONSTRUCT(log_partitioner_);
      DECONSTRUCT(meta_manager_);
      DECONSTRUCT(tbname_builder_);
      DECONSTRUCT(dbname_builder_);
      DECONSTRUCT(schema_getter_);
      DECONSTRUCT(server_selector_);
      DECONSTRUCT(rpc_stub_);
    }

  }
}

