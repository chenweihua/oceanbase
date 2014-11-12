////===================================================================
 //
 // ob_log_config.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_CONFIG_H_
#define  OCEANBASE_LIBOBLOG_CONFIG_H_

#include "tbsys.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace liboblog
  {
    class ObLogConfig
    {
      public:
        ObLogConfig();
        ~ObLogConfig();
      public:
        int init(const char *fpath);
        void destroy();
      public:
        //const char *get_ups_addr() const;
        //int get_ups_port() const;
        //const char *get_rs_addr() const;
        //int get_rs_port() const;
        const char *get_mysql_addr() const;
        int get_mysql_port() const;
        const char *get_mysql_user() const;
        const char *get_mysql_password() const;
        const char *get_tb_select() const;
        const char *get_lua_conf() const;
        const char *get_dbn_format(const char *tb_name) const;
        const char *get_tbn_format(const char *tb_name) const;
        int get_router_thread_num() const;
        int get_query_back() const;
      public:
        template <class Callback, class Arg>
        static int parse_tb_select(const char *tb_select, Callback &callback, Arg *arg);
        int operator ()(const char *tb_name, void *arg);
      private:
        const char *format_url_(const char *url);
        int load_config_(const char *fpath);
        int load_partition_config_();
      private:
        bool inited_;
        tbsys::CConfig config_;
    };

    template <class Callback, class Arg>
    int ObLogConfig::parse_tb_select(const char *tb_select_cstr, Callback &callback, Arg *arg)
    {
      TBSYS_LOG(INFO, "tb_select=[%s]", tb_select_cstr);
      int ret = common::OB_SUCCESS;
      std::string tb_select = tb_select_cstr;
      tb_select.append(",");
      uint64_t pos = 0;
      while (common::OB_SUCCESS == ret)
      {
        const uint64_t next_pos = tb_select.find(',', pos);
        if (std::string::npos == next_pos)
        {
          break;
        }
        if (pos == next_pos)
        {
          pos = next_pos + 1;
          continue;
        }
        std::string tb_name = tb_select.substr(pos, next_pos - pos);
        
        ret = callback(tb_name.c_str(), arg);

        pos = next_pos + 1;
        if (tb_select.length() <= pos)
        {
          break;
        }
      }
      return ret;
    }
  }
}

#endif //OCEANBASE_LIBOBLOG_CONFIG_H_

