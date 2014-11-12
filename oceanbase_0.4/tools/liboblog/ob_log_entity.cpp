////===================================================================
 //
 // ob_log_entity.cpp liboblog / Oceanbase
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

#include "common/ob_define.h"
#include "ob_log_spec.h"
#include "ob_log_entity.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogEntity::ObLogEntity() : inited_(false),
                                 log_spec_(NULL),
                                 log_formator_(NULL),
                                 partitions_()
    {
    }

    ObLogEntity::~ObLogEntity()
    {
      destroy();
    }

    int ObLogEntity::init(IObLogSpec *log_spec, const std::vector<uint64_t> &partitions)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (log_spec_ = dynamic_cast<ObLogSpec*>(log_spec))
              || 0 >= partitions.size())
      {
        TBSYS_LOG(WARN, "invalid param log_spec=%p partitions_size=%ld", log_spec_, partitions.size());
        ret = OB_INVALID_ARGUMENT;
      }
#ifdef __SPARSE_AVAILABLE__
      else if (NULL == (log_formator_ = new(std::nothrow) ObLogSparseFormator()))
      {
        TBSYS_LOG(WARN, "construct log_formator fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
#endif
#ifdef __DENSE_AVAILABLE__
      else if (NULL == (log_formator_ = new(std::nothrow) ObLogDenseFormator()))
      {
        TBSYS_LOG(WARN, "construct log_formator fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
#endif
      else if (OB_SUCCESS != (ret = log_formator_->init(log_spec_->get_config(),
              log_spec_->get_schema_getter(),
              log_spec_->get_meta_manager(),
              log_spec_->get_log_filter())))
      {
        TBSYS_LOG(WARN, "init log_formator fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = seq_map_.init(SEQ_MAP_SIZE)))
      {
        TBSYS_LOG(WARN, "init seq_map fail, ret=%d start_checkpoint=%lu", ret, log_spec_->get_start_checkpoint());
      }
      else
      {
        std::vector<uint64_t>::const_iterator iter = partitions.begin();
        for (; iter != partitions.end(); iter++)
        {
          if (OB_SUCCESS != (ret = log_spec_->get_log_router()->register_observer(*iter, log_formator_)))
          {
            TBSYS_LOG(WARN, "register observer fail, ret=%d partition=%lu log_formator=%p", ret, *iter, log_formator_);
            break;
          }
          else
          {
            TBSYS_LOG(INFO, "register observer succ, ret=%d partition=%lu log_formator=%p", ret, *iter, log_formator_);
            partitions_.push_back(*iter);
          }
        }
        if (OB_SUCCESS == ret)
        {
          seq_map_.set(0, log_spec_->get_start_checkpoint());
          inited_ = true;
        }
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogEntity::destroy()
    {
      inited_ = false;
      if (NULL != log_spec_)
      {
        std::vector<uint64_t>::const_iterator iter = partitions_.begin();
        for (; iter != partitions_.end(); iter++)
        {
          log_spec_->get_log_router()->withdraw_observer(*iter);
        }
      }
      partitions_.clear();

      seq_map_.destroy();

      if (NULL != log_formator_)
      {
        log_formator_->destroy();
        delete log_formator_;
        log_formator_ = NULL;
      }

      log_spec_ = NULL;
    }

    int ObLogEntity::next_record(IBinlogRecord **record, const int64_t timeout_us)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == record)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        *record = NULL;
        ret = log_formator_->next_record(record, timeout_us);
        if (NULL != *record)
        {
          uint64_t ck1 = (*record)->getCheckpoint1();
          uint64_t ck2 = (*record)->getCheckpoint2();
          uint64_t ck = ((ck1 << 32) & 0xffffffff00000000) + (ck2 & 0x00000000ffffffff);
          if (OB_SUCCESS != (ret = seq_map_.set((dynamic_cast<ObLogBinlogRecord*>(*record))->get_mutator_num(), ck)))
          {
            TBSYS_LOG(ERROR, "unexpected error, set ck=%lu to seq_map fail, ret=%d", ck, ret);
            release_record(*record);
            *record = NULL;
          }
          else
          {
            uint64_t cur_ck = seq_map_.next();
            (dynamic_cast<ObLogBinlogRecord*>(*record))->setCheckpoint(
                (uint32_t)((cur_ck & 0xffffffff00000000) >> 32),
                (uint32_t)(cur_ck & 0x00000000ffffffff));
            TBSYS_LOG(DEBUG, "num=%lu ck=%lu cur_ck=%lu", (dynamic_cast<ObLogBinlogRecord*>(*record))->get_mutator_num(), ck, cur_ck);
          }
        }
      }
      return ret;
    }

    void ObLogEntity::release_record(IBinlogRecord *record)
    {
      if (inited_
          && NULL != record)
      {
        log_formator_->release_record(record);
      }
    }

  }
}

