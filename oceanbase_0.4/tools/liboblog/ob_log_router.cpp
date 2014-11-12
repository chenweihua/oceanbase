////===================================================================
 //
 // ob_log_router.cpp liboblog / Oceanbase
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
#include "ob_log_router.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogRouter::ObLogRouter() : inited_(false),
                                 launched_(false),
                                 source_(NULL),
                                 partitioner_(NULL),
                                 schema_getter_(NULL),
                                 observer_map_lock_(),
                                 observer_map_()
    {
    }

    ObLogRouter::~ObLogRouter()
    {
      destroy();
    }

    int ObLogRouter::init(ObLogConfig &config, IObLogFilter *source, IObLogPartitioner *partitioner, IObLogSchemaGetter *schema_getter)
    {
      int ret = OB_SUCCESS;
      bool queue_rebalance = false;
      bool dynamic_rebalance = false;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 != mysql_library_init(0, NULL, NULL))
      {
        TBSYS_LOG(WARN, "init mysql library fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (source_ = source)
              || NULL == (partitioner_ = partitioner)
              || NULL == (schema_getter_ = schema_getter))
      {
        TBSYS_LOG(WARN, "invalid param source=%p partitioner=%p", source, partitioner);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != observer_map_.create(MAX_OBSERVER_NUM))
      {
        TBSYS_LOG(WARN, "init observer_map fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (ret = S2MQueueThread::init(config.get_router_thread_num(), THREAD_QUEUE_SIZE, queue_rebalance, dynamic_rebalance)))
      {
        TBSYS_LOG(WARN, "init queue_thread fail, ret=%d thread_num=%d", ret, config.get_router_thread_num());
      }
      else
      {
        launched_ = false;
        inited_ = true;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogRouter::destroy()
    {
      stop();
      wait();
      S2MQueueThread::destroy();
      inited_ = false;
      launched_ = false;
      observer_map_.destroy();
      schema_getter_ = NULL;
      partitioner_ = NULL;
      source_ = NULL;
      TBSYS_LOG(INFO, "calling mysql_library_end");
      mysql_library_end();
    }

    int ObLogRouter::register_observer(const uint64_t partition, IObLogObserver *observer)
    {
      int ret = OB_SUCCESS;
      int hash_ret = 0;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (launched_)
      {
        TBSYS_LOG(WARN, "thread has been launched, can not register observer partition=%lu observer=%p",
            partition, observer);
        ret = OB_NOT_SUPPORTED;
      }
      else if (hash::HASH_INSERT_SUCC != (hash_ret = observer_map_.set(partition, observer)))
      {
        TBSYS_LOG(WARN, "add observer fail, hash_ret=%d partition=%lu observer=%p",
            hash_ret, partition, observer);
        ret = (hash::HASH_EXIST == hash_ret) ? OB_ENTRY_EXIST : OB_ERR_UNEXPECTED;
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    void ObLogRouter::withdraw_observer(const uint64_t partition)
    {
      if (inited_)
      {
        SpinWLockGuard guard(observer_map_lock_);
        observer_map_.erase(partition);
      }
    }

    int ObLogRouter::launch()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (0 >= observer_map_.size())
      {
        TBSYS_LOG(WARN, "no observer cannot launch");
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (launched_)
      {
        TBSYS_LOG(WARN, "thread has already been launched");
        ret = OB_INIT_TWICE;
      }
      else if (1 != start())
      {
        TBSYS_LOG(WARN, "start thread to route log fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        launched_ = true;
      }
      return ret;
    }

    void ObLogRouter::run(tbsys::CThread* thread, void* arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      while (!_stop)
      {
        if (REACH_TIME_INTERVAL(SCHEMA_GETTER_REFRESH_INTERVAL))
        {
          schema_getter_->refresh();
        }

        ObLogMutator *mutator = NULL;
        int tmp_ret = source_->next_mutator(&mutator, WAIT_SOURCE_TIMEOUT);
        if (OB_SUCCESS != tmp_ret)
        {
          continue;
        }
        if (NULL == mutator)
        {
          // unexpected error
          abort();
        }

        uint64_t db_partition = 0;
        uint64_t tb_partition = 0;
        tmp_ret = partitioner_->partition(*mutator, &db_partition, &tb_partition);
        if (OB_SUCCESS != tmp_ret)
        {
          // unexpected error
          TBSYS_LOG(WARN, "partition fail, ret=%d", tmp_ret);
          abort();
        }
        
        {
          SpinRLockGuard guard(observer_map_lock_);
          IObLogObserver *observer = NULL;
          int hash_ret = observer_map_.get(db_partition, observer);
          if (hash::HASH_EXIST == hash_ret)
          {
            if (NULL == observer)
            {
              // unexpected error
              TBSYS_LOG(WARN, "get observer fail, hash_ret=%d", hash_ret);
              abort();
            }
            mutator->set_num(observer->inc_num());
          }
          else
          {
            source_->release_mutator(mutator);
            continue;
          }
        }

        mutator->set_db_partition(db_partition);
        mutator->set_tb_partition(tb_partition);
        int64_t timestamp = mutator->get_mutate_timestamp();
        // Notice: if 0 == sign, will not spec thread
        tmp_ret = S2MQueueThread::push(mutator, tb_partition + S2MQueueThread::get_thread_num(), PriorityPacketQueueThread::NORMAL_PRIV);
        if (OB_SUCCESS != tmp_ret)
        {
          // unexpected error
          TBSYS_LOG(WARN, "push queue fail, ret=%d", tmp_ret);
          abort();
        }
        else
        {
          TBSYS_LOG(DEBUG, "push task tb_partition=%ld db_partition=%ld timestamp=%ld", tb_partition, db_partition, timestamp);
        }

        //SpinRLockGuard guard(observer_map_lock_);
        //IObLogObserver *observer = NULL;
        //int hash_ret = observer_map_.get(mutator->get_db_partition(), observer);
        //if (hash::HASH_EXIST == hash_ret)
        //{
        //  if (NULL == observer)
        //  {
        //    // unexpected error
        //    TBSYS_LOG(WARN, "get observer fail, hash_ret=%d", hash_ret);
        //    abort();
        //  }

        //  int tmp_ret = observer->add_mutator(*mutator, mutator->get_db_partition(), mutator->get_tb_partition(), _stop);
        //  if (OB_SUCCESS != tmp_ret
        //      && OB_IN_STOP_STATE != tmp_ret)
        //  {
        //    // unexpected error
        //    TBSYS_LOG(WARN, "add mutator fail, ret=%d", tmp_ret);
        //    abort();
        //  }
        //}
        //source_->release_mutator(mutator);

      } // while
      return;
    }

    void ObLogRouter::handle_with_stopflag(void *task, void *pdata, volatile bool &stop_flag)
    {
      UNUSED(pdata);
      ObLogMutator *mutator = (ObLogMutator*)task;
      if (NULL != mutator)
      {
        TBSYS_LOG(DEBUG, "handle task tb_partition=%ld db_partition=%ld timestamp=%ld",
                  mutator->get_tb_partition(), mutator->get_db_partition(), mutator->get_mutate_timestamp());

        SpinRLockGuard guard(observer_map_lock_);
        IObLogObserver *observer = NULL;
        int hash_ret = observer_map_.get(mutator->get_db_partition(), observer);
        if (hash::HASH_EXIST == hash_ret)
        {
          if (NULL == observer)
          {
            // unexpected error
            TBSYS_LOG(WARN, "get observer fail, hash_ret=%d", hash_ret);
            abort();
          }

          int tmp_ret = observer->add_mutator(*mutator, mutator->get_db_partition(), mutator->get_tb_partition(), stop_flag);
          if (OB_SUCCESS != tmp_ret
              && OB_IN_STOP_STATE != tmp_ret)
          {
            // unexpected error
            TBSYS_LOG(WARN, "add mutator fail, ret=%d", tmp_ret);
            abort();
          }
        }
        source_->release_mutator(mutator);
      }
    }

  }
}

