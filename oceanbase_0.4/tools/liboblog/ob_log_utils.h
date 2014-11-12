////===================================================================
 //
 // ob_log_utils.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_UTILS_H_
#define  OCEANBASE_LIBOBLOG_UTILS_H_

#include "tbsys.h"
#include "common/ob_define.h"
#include "ob_log_meta_manager.h"
#include "ob_log_partitioner.h"
#include "ob_log_server_selector.h"

//#define OB_MYSQL_USER     "admin"
//#define OB_MYSQL_PASSWD   "admin"
#define OB_MYSQL_DATABASE ""

#define TIMED_FUNC(func, timeout_us, args...) \
({\
    ObLogClock clock(timeout_us, #func); \
    func(args); \
})

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogClock
    {
      public:
        ObLogClock(const int64_t timeout_us, const char *func_name) : timeout_us_(timeout_us),
                                                                      func_name_(func_name),
                                                                      timeu_(tbsys::CTimeUtil::getTime())
        {
        };
        ~ObLogClock()
        {
          timeu_ = tbsys::CTimeUtil::getTime() - timeu_;
          if (timeout_us_ < timeu_)
          {
            TBSYS_LOG(WARN, "call [%s] long, timeu=%ld", func_name_, timeu_);
          }
        };
      private:
        const int64_t timeout_us_;
        const char *func_name_;
        int64_t timeu_;
    };
    
    /*
    class ObLogServerSelector : public IObLogServerSelector
    {
      public:
        ObLogServerSelector()
        {
        };
        ~ObLogServerSelector()
        {
        };
      public:
        int init(const ObLogConfig &config)
        {
          ups_.set_ipv4_addr(config.get_ups_addr(), config.get_ups_port());
          rs_.set_ipv4_addr(config.get_rs_addr(), config.get_rs_port());
          return common::OB_SUCCESS;
        };
        void destroy()
        {
        };
        int get_ups(common::ObServer &server, const bool change)
        {
          UNUSED(change);
          server = ups_;
          return common::OB_SUCCESS;
        };
        int get_ms(common::ObServer &server, const bool change)
        {
          UNUSED(server);
          UNUSED(change);
          return common::OB_SUCCESS;
        };
        int get_rs(common::ObServer &server, const bool change)
        {
          UNUSED(change);
          server = rs_;
          return common::OB_SUCCESS;
        };
        void refresh()
        {
        };
      private:
        common::ObServer ups_;
        common::ObServer rs_;
    };
    
    class ObLogPartitioner : public IObLogPartitioner
    {
      public:
        ObLogPartitioner()
        {
        };
        ~ObLogPartitioner()
        {
        };
      public:
        int init(ObLogConfig &config)
        {
          UNUSED(config);
          return common::OB_SUCCESS;
        };
        void destroy()
        {
        };
        int partition(ObLogMutator &mutator, uint64_t *db_partition, uint64_t *tb_partition)
        {
          UNUSED(mutator);
          *db_partition = 0;
          *tb_partition = 0;
          return common::OB_SUCCESS;
        };
    };
    
    class ObLogDBNameBuilder : public IObLogDBNameBuilder
    {
      public:
        ObLogDBNameBuilder()
        {
        };
        ~ObLogDBNameBuilder()
        {
        };
      public:
        int init(const ObLogConfig &config)
        {
          UNUSED(config);
          return common::OB_SUCCESS;
        };
        void destroy()
        {
        };
        int get_db_name(const char *src_name,
            const uint64_t partition,
            char *dest_name,
            const int64_t dest_buffer_size)
        {
          snprintf(dest_name, dest_buffer_size, "%s_%04lu", src_name, partition);
          return common::OB_SUCCESS;
        };
    };
    
    class ObLogTBNameBuilder : public IObLogTBNameBuilder
    {
      public:
        ObLogTBNameBuilder()
        {
        };
        ~ObLogTBNameBuilder()
        {
        };
      public:
        int init(const ObLogConfig &config)
        {
          UNUSED(config);
          return common::OB_SUCCESS;
        };
        virtual void destroy()
        {
        };
        int get_tb_name(const char *src_name,
            const uint64_t partition,
            char *dest_name,
            const int64_t dest_buffer_size)
        {
          snprintf(dest_name, dest_buffer_size, "%s_%04lu", src_name, partition);
          return common::OB_SUCCESS;
        };
    };
    */

    class ObLogSeqMap
    {
      struct Item
      {
        uint64_t index;
        uint64_t data;
      };
      public:
        ObLogSeqMap() : items_(NULL),
                        limit_(0),
                        cursor_(0)
        {
        };
        ~ObLogSeqMap()
        {
          destroy();
        };
      public:
        int init(const uint64_t limit)
        {
          int ret = OB_SUCCESS;
          if (NULL != items_)
          {
            ret = OB_INIT_TWICE;
          }
          else if (0 >= limit)
          {
            ret = OB_INVALID_ARGUMENT;
          }
          else if (NULL == (items_ = new(std::nothrow) Item[limit]))
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            TBSYS_LOG(INFO, "init seq_map succ, limit=%lu", limit);
            memset(items_, 0, sizeof(Item) * limit);
            limit_ = limit;
            cursor_ = 1;
          }
          return ret;
        };
        void destroy()
        {
          cursor_ = 0;
          limit_ = 0;
          if (NULL != items_)
          {
            delete[] items_;
            items_ = NULL;
          }
        };
      public:
        int set(const uint64_t index, const uint64_t data)
        {
          int ret = OB_SUCCESS;
          if (NULL == items_)
          {
            ret = OB_NOT_INIT;
          }
          else if (cursor_ + limit_ <= index)
          {
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            items_[index % limit_].index = index;
            items_[index % limit_].data = data;
          }
          return ret;
        };
        uint64_t next()
        {
          uint64_t ret = 0;
          while ((cursor_ + 1) == items_[(cursor_ + 1) % limit_].index)
          {
            cursor_ += 1;
          }
          ret = items_[cursor_ % limit_].data;
          return ret;
        };
      private:
        Item *items_;
        uint64_t limit_;
        uint64_t cursor_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_UTILS_H_

