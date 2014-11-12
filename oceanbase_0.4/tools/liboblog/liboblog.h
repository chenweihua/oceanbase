////===================================================================
 //
 // liboblog.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_H_
#define  OCEANBASE_LIBOBLOG_H_

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include <stdint.h>
#include <vector>
#include "ob_define.h"
#include "MD.h"
#include "BR.h"

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogSpec
    {
      public:
        virtual ~IObLogSpec() {};
      public:
        /*
         * 初始化
         * @param config      配置文件路径或url地址
         * @param checkpoint  从检查点开始抓取日志
         */
        virtual int init(const char *config, const uint64_t checkpoint) = 0;
        virtual void destroy() = 0;
        virtual int launch() = 0;
    };

    class IObLog;
    class ObLogSpecFactory
    {
      public:
        ObLogSpecFactory();
        ~ObLogSpecFactory();
      public:
        IObLogSpec *construct_log_spec();
        IObLog *construct_log();
        void deconstruct(IObLogSpec *log_spec);
        void deconstruct(IObLog *log);
    };

    class IObLog    
    {
      public:
        virtual ~IObLog() {};

      public:
        /*
         * 初始化
         * @param log_spec    全局使用一个log_spec
         * @param partition   抓取指定分区的数据
         * @return 0:成功
         */
        virtual int init(IObLogSpec *log_spec, const std::vector<uint64_t> &partitions) = 0;

        /*
         * 销毁
         */
        virtual void destroy() = 0;

        /*
         * 迭代一个数据库statement
         * @param record      迭代record，地址由内部分配，允许多次调用record之后再调用release
         * @param 0:成功
         */
        virtual int next_record(IBinlogRecord **record, const int64_t timeout_us) = 0;

        /*
         * 释放record
         * @param record
         */
        virtual void release_record(IBinlogRecord *record) = 0;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_H_

