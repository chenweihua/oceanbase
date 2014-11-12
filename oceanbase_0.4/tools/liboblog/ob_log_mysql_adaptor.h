////===================================================================
 //
 // ob_log_mysql_adaptor.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-07-23 by Yubai (yubai.lk@alipay.com) 
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

#ifndef  OCEANBASE_LIBOBLOG_MYSQL_ADAPTOR_H_
#define  OCEANBASE_LIBOBLOG_MYSQL_ADAPTOR_H_

#include <sstream>
#include <string>
#include "mysql/mysql.h"
#include "hash/ob_hashmap.h"
#include "page_arena.h"
#include "ob_log_meta_manager.h"

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogColValue
    {
      public:
        enum ColType
        {
          CT_INT = 0,
          CT_TIME = 1,
          CT_VARCHAR = 2,
        };
      public:
        IObLogColValue(const ColType type) : type_(type), mysql_type_(MYSQL_TYPE_NULL), next_(NULL) {};
        virtual ~IObLogColValue() {};
      public:
        virtual void to_str(std::string &str) const = 0;
        virtual int64_t get_length() const = 0;
        virtual void *get_data_ptr() = 0;
        virtual uint64_t *get_length_ptr() {return NULL;};
      public:
        my_bool *get_isnull_ptr() {return &is_null_;};
        my_bool isnull() const {return is_null_;};
        void set_mysql_type(const int32_t mysql_type) {mysql_type_ = mysql_type;};
        int32_t get_mysql_type() {return mysql_type_;};
        void set_next(IObLogColValue *next) {next_ = next;};
        IObLogColValue *get_next() {return next_;};
        const IObLogColValue *get_next() const {return next_;};
      protected:
        const ColType type_;
        my_bool is_null_;
        int32_t mysql_type_;
        IObLogColValue *next_;
    };

    class ObLogMysqlAdaptor
    {
      class IntColValue : public IObLogColValue
      {
        public:
          IntColValue() : IObLogColValue(CT_INT), i_(0) {};
          ~IntColValue() {};
        public:
          void to_str(std::string &str) const
          {
            if (is_null_)
            {
              str = "";
            }
            else
            {
              oss_.clear();
              oss_.str("");
              oss_ << i_;
              str = oss_.str();
            }
          };
          int64_t get_length() const {return sizeof(i_);};
          void *get_data_ptr() {return &i_;};
        private:
          mutable std::ostringstream oss_;
          int64_t i_;
      };
      class TimeColValue : public IObLogColValue
      {
        public:
          TimeColValue() : IObLogColValue(CT_TIME) {};
          ~TimeColValue() {};
        public:
          void to_str(std::string &str) const
          {
            if (is_null_)
            {
              str = "";
            }
            else
            {
              oss_.str("");
              oss_.clear();
              oss_.fill('0');
              oss_.width(4); oss_ << t_.year   << "-";
              oss_.width(2); oss_ << t_.month  << "-";
              oss_.width(2); oss_ << t_.day    << " ";
              oss_.width(2); oss_ << t_.hour   << ":";
              oss_.width(2); oss_ << t_.minute << ":";
              oss_.width(2); oss_ << t_.second;
              str = oss_.str();
            }
          };
          int64_t get_length() const {return sizeof(t_);};
          void *get_data_ptr() {return &t_;};
        private:
          mutable std::ostringstream oss_;
          MYSQL_TIME t_;
      };
      class VarcharColValue : public IObLogColValue
      {
        public:
          VarcharColValue(const uint64_t length) : IObLogColValue(CT_VARCHAR), length_(length) {};
          ~VarcharColValue() {};
        public:
          void to_str(std::string &str) const
          {
            if (is_null_)
            {
              str = "";
            }
            else
            {
              str = v_;
            }
          };
          int64_t get_length() const {return length_;};
          void *get_data_ptr() {return v_;};
          uint64_t *get_length_ptr() {return &length_;};
        private:
          uint64_t length_;
          char v_[0];
      };

      struct PSInfo
      {
        static const int64_t PS_SQL_BUFFER_SIZE = 4096;
        char sql[PS_SQL_BUFFER_SIZE];
        pthread_key_t tc_key;
        int64_t table_id;
        int64_t rk_column_num;
      };

      struct TCInfo
      {
        MYSQL *mysql;
        MYSQL_STMT *stmt;
        IObLogColValue *res_list;

        MYSQL_BIND *params;
        MYSQL_TIME *tm_data;
        MYSQL_BIND *res_idx;

        PSInfo *ps_info;
        TCInfo *next;

        void clear()
        {
          IObLogColValue *cv_iter = res_list;
          while (NULL != cv_iter)
          {
            IObLogColValue *cv_tmp = cv_iter->get_next();
            cv_iter->~IObLogColValue();
            cv_iter = cv_tmp;
          }
          res_list = NULL;

          if (NULL != stmt)
          {
            mysql_stmt_close(stmt);
            stmt = NULL;
          }

          if (NULL != mysql)
          {
            mysql_close(mysql);
            mysql = NULL;
          }
        };
      };

      typedef common::hash::ObHashMap<uint64_t, PSInfo*> PSMap;
      static const int64_t PS_MAP_SIZE = 128;
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;

      public:
        ObLogMysqlAdaptor();
        ~ObLogMysqlAdaptor();
      public:
        int init(const ObLogConfig &config, IObLogSchemaGetter *schema_getter);
        void destroy();
      public:
        int query_whole_row(const uint64_t table_id, const common::ObRowkey &rowkey, const IObLogColValue *&list);
      public:
        int operator ()(const char *tb_name, const ObLogConfig *config);
        static bool is_time_type(const ObObjType type);
      private:
        TCInfo *get_tc_info_(const uint64_t table_id);
        TCInfo *init_stmt_params_(const uint64_t table_id, TCInfo *tc_info);
        int execute_stmt_(TCInfo *tc_info, const common::ObRowkey &rowkey);
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        common::ObSpinLock allocator_lock_;
        bool inited_;
        const char *mysql_addr_;
        int mysql_port_;
        const char *mysql_user_;
        const char *mysql_password_;
        IObLogSchemaGetter *schema_getter_;
        PSMap ps_map_;
        TCInfo *tc_info_list_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_MYSQL_ADAPTOR_H_

