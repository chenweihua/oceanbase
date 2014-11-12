////===================================================================
 //
 // ob_log_fetcher.cpp liboblog / Oceanbase
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
#include "ob_log_fetcher.h"
#include "ob_log_utils.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogFetcher::ObLogFetcher() : mod_(ObModIds::OB_MUTATOR),
                                   allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                                   inited_(false),
                                   server_selector_(NULL),
                                   rpc_stub_(NULL),
                                   start_id_(1),
                                   p_list_(),
                                   v_list_(),
                                   p_cond_(COND_SPIN_WAIT_NUM),
                                   v_cond_(COND_SPIN_WAIT_NUM)
    {
    }

    ObLogFetcher::~ObLogFetcher()
    {
      destroy();
    }
    
    int ObLogFetcher::init(IObLogServerSelector *server_selector, IObLogRpcStub *rpc_stub, const int64_t start_id)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (server_selector_ = server_selector)
              || NULL == (rpc_stub_ = rpc_stub)
              || 0 >= (start_id_ = start_id))
      {
        TBSYS_LOG(WARN, "invalid param, server_selector=%p rpc_stub=%p start_id=%ld",
                  server_selector, rpc_stub, start_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_mutator_list_(MAX_MUTATOR_NUM)))
      {
        TBSYS_LOG(WARN, "prepare mutator list fail, ret=%d", ret);
      }
      else if (1 != start())
      {
        TBSYS_LOG(WARN, "start thread to fetch log fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        inited_ = true;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }
    
    void ObLogFetcher::destroy()
    {
      inited_ = false;
      stop();
      wait();
      destroy_mutator_list_();
      start_id_ = 1;
      rpc_stub_ = NULL;
      server_selector_ = NULL;
      allocator_.reuse();
    }

    int ObLogFetcher::next_mutator(ObLogMutator **mutator, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if (NULL == mutator)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObLogMutator *log_mutator = NULL;
        int64_t start_time = tbsys::CTimeUtil::getTime();
        while (true)
        {
          ret = p_list_.pop(log_mutator);
          if (OB_SUCCESS == ret)
          {
            break;
          }
          int64_t wait_time = timeout - (tbsys::CTimeUtil::getTime() - start_time);
          if (0 >= wait_time)
          {
            break;
          }
          p_cond_.timedwait(timeout);
        }
        if (OB_SUCCESS != ret)
        {
          ret = OB_PROCESS_TIMEOUT;
        }
        else
        {
          if (NULL == log_mutator)
          {
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            *mutator = log_mutator;
          }
        }
      }
      return ret;
    }

    void ObLogFetcher::release_mutator(ObLogMutator *mutator)
    {
      if (NULL != mutator)
      {
        mutator->clear();
        int ret = v_list_.push(mutator);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push mutator to v_list fail, ret=%d", ret);
        }
        else
        {
          v_cond_.signal();
        }
      }
    }

    int64_t ObLogFetcher::get_cur_id() const
    {
      return start_id_;
    }

    void ObLogFetcher::run(tbsys::CThread* thread, void* arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      char *buffer = new char[FETCH_BUFFER_SIZE];
      if (NULL == buffer)
      {
        TBSYS_LOG(WARN, "allocate fetch buffer fail");
      }
      else
      {
        memset(buffer, 0, FETCH_BUFFER_SIZE);

        updateserver::ObFetchLogReq req;
        req.start_id_ = start_id_;
        req.max_data_len_ = FETCH_BUFFER_SIZE;
        
        bool change_ups_addr = false;
        while (!_stop)
        {
          if (REACH_TIME_INTERVAL(PRINT_MEMORY_USAGE_INTERVAL))
          {
            ob_print_mod_memory_usage();
          }

          updateserver::ObFetchedLog res;
          res.max_data_len_ = FETCH_BUFFER_SIZE;
          res.log_data_ = buffer;
          
          if (REACH_TIME_INTERVAL(UPS_ADDR_REFRESH_INTERVAL))
          {
            server_selector_->refresh();
          }

          ObServer ups_addr;
          int tmp_ret = OB_SUCCESS;
          TBSYS_LOG(DEBUG, "fetch start cur_id=%ld", req.start_id_);
          if (OB_SUCCESS != (tmp_ret = server_selector_->get_ups(ups_addr, change_ups_addr)))
          {
            TBSYS_LOG(WARN, "get ups addr failed, ret=%d", tmp_ret);
          }
          else if (OB_SUCCESS != (tmp_ret = rpc_stub_->fetch_log(ups_addr, req, res, FETCH_LOG_TIMEOUT)))
          {
            if (OB_NEED_RETRY == tmp_ret)
            {
              usleep(FETCH_LOG_RETRY_TIMEWAIT);
              continue;
            }
            TBSYS_LOG(WARN, "fetch log from %s fail, ret=%d", to_cstring(ups_addr), tmp_ret);
            change_ups_addr = true;
            req.session_.reset();
          }
          else if (0 == res.data_len_)
          {
            usleep(FETCH_LOG_RETRY_TIMEWAIT);
            continue;
          }
          else 
          {
            TBSYS_LOG(DEBUG, "fetch done %s", to_cstring(res));

            change_ups_addr = false;
            req.session_ = res.next_req_.session_;
            req.max_data_len_ = FETCH_BUFFER_SIZE;

            TIMED_FUNC(handle_fetched_log_, 10L * 1000000L, res, (uint64_t&)req.start_id_);
            start_id_ = req.start_id_;

            req.start_id_ += 1;
          }
          if (REACH_TIME_INTERVAL(PRINT_CHECKPOINT_INTERVAL))
          {
            TBSYS_LOG(INFO, "current checkpoint is %ld", req.start_id_);
          }
        }
        delete[] buffer;
      }
    }

    //////////////////////////////////////////////////

    int ObLogFetcher::init_mutator_list_(const int64_t max_mutator_num)
    {
      int ret = OB_SUCCESS;
      ret = p_list_.init(max_mutator_num);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "init mutator p_list fail, max_mutator_num=%ld", max_mutator_num);
      }
      if (OB_SUCCESS == ret)
      {
        ret = v_list_.init(max_mutator_num);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init mutator v_list fail, max_mutator_num=%ld", max_mutator_num);
        }
      }
      for (int64_t i = 0; OB_SUCCESS == ret && i < max_mutator_num; i++)
      {
        void *buffer = allocator_.alloc(sizeof(ObLogMutator));
        if (NULL == buffer)
        {
          TBSYS_LOG(WARN, "allocator memory to build log_mutator fail, i=%ld", i);
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }

        ObLogMutator *log_mutator = new(buffer) ObLogMutator();
        if (NULL == log_mutator)
        {
          TBSYS_LOG(WARN, "build log_mutator fail, i=%ld", i);
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        ret = v_list_.push(log_mutator);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push mutator to list fail, i=%ld", i);
          break;
        }
      }
      return ret;
    }

    void ObLogFetcher::destroy_mutator_list_()
    {
      int64_t counter = v_list_.get_total() + v_list_.get_free();
      ObLogMutator *log_mutator = NULL;
      while (OB_SUCCESS == p_list_.pop(log_mutator))
      {
        if (NULL != log_mutator)
        {
          log_mutator->~ObLogMutator();
          log_mutator = NULL;
          counter--;
        }
      }
      p_list_.destroy();

      while (OB_SUCCESS == v_list_.pop(log_mutator))
      {
        if (NULL != log_mutator)
        {
          log_mutator->~ObLogMutator();
          log_mutator = NULL;
          counter--;
        }
      }
      v_list_.destroy();

      if (0 != counter)
      {
        TBSYS_LOG(WARN, "still have mutator not release, counter=%ld", counter);
      }
    }

    void ObLogFetcher::handle_fetched_log_(updateserver::ObFetchedLog &log, uint64_t &seq)
    {
      int64_t pos = 0;
      while (!_stop
            && pos < log.data_len_)
      {
        ObLogEntry log_entry;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = log_entry.deserialize(log.log_data_, log.data_len_, pos)))
        {
          TBSYS_LOG(ERROR, "deserialize log_entry fail, ret=%d", tmp_ret);
        }
        else if (OB_SUCCESS != (tmp_ret = log_entry.check_header_integrity(false)))
        {
          TBSYS_LOG(ERROR, "check_header_integrity fail, ret=%d", tmp_ret);
        }
        else if (OB_SUCCESS != (tmp_ret = log_entry.check_data_integrity(log.log_data_ + pos, false)))
        {
          TBSYS_LOG(ERROR, "check_data_integrity fail, ret=%d", tmp_ret);
        }
        else if (OB_LOG_UPS_MUTATOR != log_entry.cmd_)
        {
          pos += log_entry.get_log_data_len();
          seq = log_entry.seq_;
        }
        else
        {
          ObLogMutator *log_mutator = NULL;
          while (!_stop)
          {
            tmp_ret = v_list_.pop(log_mutator);
            if (OB_SUCCESS != tmp_ret)
            {
              v_cond_.timedwait(WAIT_VLIST_TIMEOUT);
            }
            else
            {
              break;
            }
          }
          if (_stop
              && NULL != log_mutator)
          {
            v_list_.push(log_mutator);
            log_mutator = NULL;
          }
          if (NULL == log_mutator)
          {
            if (!_stop)
            {
              tmp_ret = OB_ERR_UNEXPECTED;
            }
            else
            {
              tmp_ret = OB_IN_STOP_STATE;
            }
            break;
          }

          log_mutator->set_log_id(log_entry.seq_);
          int64_t tmp_pos = pos;
          if (OB_SUCCESS != (tmp_ret = log_mutator->deserialize(log.log_data_, log.data_len_, tmp_pos)))
          {
            TBSYS_LOG(WARN, "deserialize log_mutator fail, ret=%d", tmp_ret);
          }
          else if (!log_mutator->is_normal_mutator())
          {
            v_list_.push(log_mutator);
            log_mutator = NULL;
          }
          else if (OB_SUCCESS != (tmp_ret = p_list_.push(log_mutator)))
          {
            TBSYS_LOG(WARN, "push to p_list fail, ret=%d", tmp_ret);
          }
          else
          {
            p_cond_.signal();
          }
          pos += log_entry.get_log_data_len();
          seq = log_entry.seq_;
        }
        if (OB_SUCCESS != tmp_ret
            && OB_IN_STOP_STATE != tmp_ret)
        {
          abort();
        }
      } // end while
      return;
    }

  }
}

