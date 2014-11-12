////===================================================================
 //
 // ob_log_config.cpp liboblog / Oceanbase
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

#include <curl/curl.h>
#include "tbsys.h"
#include "ob_log_config.h"

#define LOG_CONFIG_SECTION    "liboblog"
#define LOG_CONFIG_SECTION_ENTIRE "[liboblog]\n"

#define LOG_CONFIG_OB_URL     "cluster_url"
#define LOG_CONFIG_OB_USER    "cluster_user"
#define LOG_CONFIG_OB_PASSWORD  "cluster_password"
#define LOG_CONFIG_OB_ADDR    "clusterAddress"

#define LOG_CONFIG_UPS_ADDR   "ups_addr"
#define LOG_CONFIG_UPS_PORT   "ups_port"
#define LOG_CONFIG_RS_ADDR    "rs_addr"
#define LOG_CONFIG_RS_PORT    "rs_port"
#define LOG_CONFIG_ROUTER_THREAD_NUM "router_thread_num"
#define LOG_CONFIG_LOG_FPATH  "log_fpath"
#define LOG_CONFIG_QUERY_BACK "query_back"

#define LOG_PARTITION_SECTION   "partition"
#define LOG_PARTITION_TB_SELECT "tb_select"
#define LOG_PARTITION_LUA_CONF  "lua_conf"
#define LOG_PARTITION_DBN_FORMAT_SUFFIX  "_dbn_format"
#define LOG_PARTITION_TBN_FORMAT_SUFFIX  "_tbn_format"

#define DEFAULT_LOG_FPATH     "./log/liboblog.log"

const char* svn_version();
const char* build_date();
const char* build_time();
const char* build_flags();

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogConfig::ObLogConfig() : inited_(false),
                                 config_()
    {
    }

    ObLogConfig::~ObLogConfig()
    {
      destroy();
      TBSYS_LOG(INFO, "====================liboblog end====================");
    }

#define STR_CONFIG_CHECK(session, name) \
    if (OB_SUCCESS == ret)\
    { \
      const char *v = NULL; \
      if (NULL == (v = config_.getString(session, name))) \
      { \
        TBSYS_LOG(WARN, "str config [%s] not exist", name); \
        ret = OB_ENTRY_NOT_EXIST; \
      } \
      else \
      { \
        TBSYS_LOG(INFO, "load str config succ %s=%s", name, v); \
      } \
    }

#define INT_CONFIG_CHECK(session, name, blank) \
    if (OB_SUCCESS == ret) \
    { \
      int v = blank; \
      if (blank == (v = config_.getInt(session, name, blank))) \
      { \
        TBSYS_LOG(WARN, "int config [%s] not exist", name); \
        ret = OB_ENTRY_NOT_EXIST; \
      } \
      else \
      { \
        TBSYS_LOG(INFO, "load int config succ %s=%d", name, v); \
      } \
    }

    int ObLogConfig::init(const char *fpath)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == fpath)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = load_config_(fpath)))
      {
        TBSYS_LOG(WARN, "load config fail, ret=%d fpath=%s", ret, fpath);
      }
      else if (OB_SUCCESS != (ret = load_partition_config_()))
      {
        TBSYS_LOG(WARN, "load partition config fail, ret=%d", ret);
      }
      else
      {
        //STR_CONFIG_CHECK(LOG_CONFIG_SECTION, LOG_CONFIG_UPS_ADDR);
        //STR_CONFIG_CHECK(LOG_CONFIG_SECTION, LOG_CONFIG_RS_ADDR);
        //INT_CONFIG_CHECK(LOG_CONFIG_SECTION, LOG_CONFIG_UPS_PORT, 0);
        //INT_CONFIG_CHECK(LOG_CONFIG_SECTION, LOG_CONFIG_RS_PORT, 0);
        STR_CONFIG_CHECK(LOG_CONFIG_SECTION, LOG_CONFIG_OB_USER);
        STR_CONFIG_CHECK(LOG_CONFIG_SECTION, LOG_CONFIG_OB_PASSWORD);
        INT_CONFIG_CHECK(LOG_CONFIG_SECTION, LOG_CONFIG_ROUTER_THREAD_NUM, 0);
        INT_CONFIG_CHECK(LOG_CONFIG_SECTION, LOG_CONFIG_QUERY_BACK, -1);
        if (OB_SUCCESS == ret)
        {
          inited_ = true;
        }
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogConfig::destroy()
    {
      inited_ = false;
    }

    const char *ObLogConfig::format_url_(const char *url)
    {
      static const int64_t BUF_SIZE = 1024;
      static __thread char BUFFER[BUF_SIZE];
      std::string str(url);
      str.erase(str.find('\"'), 1);
      str.erase(str.rfind('\"'), 1);
      snprintf(BUFFER, BUF_SIZE, "%s", str.c_str());
      TBSYS_LOG(INFO, "format url from [%s] to [%s]", url, BUFFER);
      return BUFFER;
    }

    int ObLogConfig::load_config_(const char *fpath)
    {
      int ret = OB_SUCCESS;
      const char *cluster_url = NULL;

      CURL *curl = NULL;
      CURLcode curl_ret = CURLE_OK;

      const int64_t BUFFER_SIZE = 128;
      char config_file_buffer[BUFFER_SIZE];
      FILE *config_file_fd = NULL;

      if (0 != config_.load(fpath))
      {
        TBSYS_LOG(WARN, "load config fail fpath=%s", fpath);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        const char *log_fpath = config_.getString(LOG_CONFIG_SECTION, LOG_CONFIG_LOG_FPATH) ?: DEFAULT_LOG_FPATH;
        char *p = strrchr(const_cast<char*>(log_fpath), '/');
        if (NULL != p)
        {
          char dir_buffer[OB_MAX_FILE_NAME_LENGTH];
          snprintf(dir_buffer, OB_MAX_FILE_NAME_LENGTH, "%.*s",
                  (int)(p - log_fpath), log_fpath);
          tbsys::CFileUtil::mkdirs(dir_buffer);
        }
        TBSYS_LOGGER.setFileName(log_fpath, true);
        TBSYS_LOGGER.setLogLevel("info");
        TBSYS_LOG(INFO, "logger=%p", &(TBSYS_LOGGER));
        TBSYS_LOG(INFO, "====================liboblog start====================");
        TBSYS_LOG(INFO, "SVN_VERSION: %s", svn_version());
        TBSYS_LOG(INFO, "BUILD_TIME: %s %s", build_date(), build_time());
        TBSYS_LOG(INFO, "BUILD_FLAGS: %s", build_flags());
        TBSYS_LOG(INFO, "config fpath=[%s]", fpath);

        if (NULL == (cluster_url = format_url_(config_.getString(LOG_CONFIG_SECTION, LOG_CONFIG_OB_URL))))
        {
          TBSYS_LOG(WARN, "str config [%s] not exist", LOG_CONFIG_OB_URL);
          ret = OB_ENTRY_NOT_EXIST;
        }
        else if (BUFFER_SIZE <= snprintf(config_file_buffer, BUFFER_SIZE, "/tmp/liboblog.curl.%d", getpid()))
        {
          TBSYS_LOG(WARN, "assemble config_file_buffer fail, buffer=./liboblog.curl.%d", getpid());
          ret = OB_BUF_NOT_ENOUGH;
        }
        else if (NULL == (config_file_fd = fopen(config_file_buffer, "w+")))
        {
          TBSYS_LOG(WARN, "fopen config_file fail, file=%s errno=%u", config_file_buffer, errno);
          ret = OB_ERR_UNEXPECTED;
        }
        else if (sizeof(LOG_CONFIG_SECTION_ENTIRE) != fwrite(LOG_CONFIG_SECTION_ENTIRE,
              1, 
              sizeof(LOG_CONFIG_SECTION_ENTIRE),
              config_file_fd))
        {
          TBSYS_LOG(WARN, "write session string to config file fail, errno=%u", errno);
          ret = OB_ERR_UNEXPECTED;
        }
        else if (NULL == (curl = curl_easy_init()))
        {
          TBSYS_LOG(WARN, "curl_easy_init fail");
          ret = OB_ERR_UNEXPECTED;
        }
        else if (CURLE_OK != (curl_ret = curl_easy_setopt(curl, CURLOPT_URL, cluster_url)))
        {
          TBSYS_LOG(WARN, "curl_easy_setopt set CURLOPT_URL fail, error=%s", curl_easy_strerror(curl_ret));
          ret = OB_ERR_UNEXPECTED;
        }
        else if (CURLE_OK != (curl_ret = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L)))
        {
          TBSYS_LOG(WARN, "curl_easy_setopt set CURLOPT_FOLLOWLOCATION fail, error=%s", curl_easy_strerror(curl_ret));
          ret = OB_ERR_UNEXPECTED;
        }
        else if (CURLE_OK != (curl_ret = curl_easy_setopt(curl, CURLOPT_WRITEDATA, config_file_fd)))
        {
          TBSYS_LOG(WARN, "curl_easy_setopt set CURLOPT_FOLLOWLOCATION fail, error=%s", curl_easy_strerror(curl_ret));
          ret = OB_ERR_UNEXPECTED;
        }
        else if (CURLE_OK != (curl_ret = curl_easy_perform(curl)))
        {
          TBSYS_LOG(WARN, "curl_easy_perform fail, error=%s url=[%s]", curl_easy_strerror(curl_ret), cluster_url);
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          fflush(config_file_fd);
          if (0 != config_.load(config_file_buffer))
          {
            TBSYS_LOG(WARN, "load config fail fpath=%s", config_file_buffer);
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            STR_CONFIG_CHECK(LOG_CONFIG_SECTION, LOG_CONFIG_OB_ADDR);
          }
        }
      }
      if (NULL != curl)
      {
        curl_easy_cleanup(curl);
        curl = NULL;
      }
      if (NULL != config_file_fd)
      {
        remove(config_file_buffer);
        //fclose(config_file_fd);
        config_file_fd = NULL;
      }
      return ret;
    }

    int ObLogConfig::load_partition_config_()
    {
      int ret = OB_SUCCESS;
      STR_CONFIG_CHECK(LOG_PARTITION_SECTION, LOG_PARTITION_TB_SELECT);
      STR_CONFIG_CHECK(LOG_PARTITION_SECTION, LOG_PARTITION_LUA_CONF);
      if (OB_SUCCESS == ret)
      {
        ret = parse_tb_select(config_.getString(LOG_PARTITION_SECTION, LOG_PARTITION_TB_SELECT), *this, (void*)NULL);
      }
      return ret;
    }

    int ObLogConfig::operator ()(const char *tb_name, void *arg)
    {
      UNUSED(arg);
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "parse from tb_name=[%s]", tb_name);

      std::string db_name_format = tb_name;
      db_name_format.append(LOG_PARTITION_DBN_FORMAT_SUFFIX);
      STR_CONFIG_CHECK(LOG_PARTITION_SECTION, db_name_format.c_str());

      std::string tb_name_format = tb_name;
      tb_name_format.append(LOG_PARTITION_TBN_FORMAT_SUFFIX);
      STR_CONFIG_CHECK(LOG_PARTITION_SECTION, tb_name_format.c_str());

      return ret;
    }

    /*
    const char *ObLogConfig::get_ups_addr() const
    {
      const char *ret = NULL;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getString(LOG_CONFIG_SECTION, LOG_CONFIG_UPS_ADDR);
      }
      return ret;
    }

    int ObLogConfig::get_ups_port() const
    {
      int ret = 0;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getInt(LOG_CONFIG_SECTION, LOG_CONFIG_UPS_PORT);
      }
      return ret;
    }

    const char *ObLogConfig::get_rs_addr() const
    {
      const char *ret = NULL;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getString(LOG_CONFIG_SECTION, LOG_CONFIG_RS_ADDR);
      }
      return ret;
    }

    int ObLogConfig::get_rs_port() const
    {
      int ret = 0;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getInt(LOG_CONFIG_SECTION, LOG_CONFIG_RS_PORT);
      }
      return ret;
    }
    */

    const char *ObLogConfig::get_mysql_addr() const
    {
      static const int64_t BUFFER_SIZE = 1024;
      static __thread char buffer[BUFFER_SIZE];
      const char *ret = NULL;
      const char *ob_addr = const_cast<ObLogConfig&>(*this).config_.getString(LOG_CONFIG_SECTION, LOG_CONFIG_OB_ADDR);
      if (inited_
          && NULL != ob_addr)
      {
        snprintf(buffer, BUFFER_SIZE, "%s", ob_addr);
        char *delim = strchr(buffer, ':');
        if (NULL != delim)
        {
          *delim = '\n';
          if (0 < strlen(buffer))
          {
            ret = buffer;
          }
        }
      }
      return ret;
    }

    int ObLogConfig::get_mysql_port() const
    {
      static const int64_t BUFFER_SIZE = 1024;
      static __thread char buffer[BUFFER_SIZE];
      int ret = 0;
      const char *ob_addr = const_cast<ObLogConfig&>(*this).config_.getString(LOG_CONFIG_SECTION, LOG_CONFIG_OB_ADDR);
      if (inited_
          && NULL != ob_addr)
      {
        snprintf(buffer, BUFFER_SIZE, "%s", ob_addr);
        char *delim = strchr(buffer, ':');
        if (NULL != delim)
        {
          delim++;
          ret = atoi(delim);
        }
      }
      return ret;
    }

    const char *ObLogConfig::get_mysql_user() const
    {
      const char *ret = NULL;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getString(LOG_CONFIG_SECTION, LOG_CONFIG_OB_USER);
      }
      return ret;
    }

    const char *ObLogConfig::get_mysql_password() const
    {
      const char *ret = NULL;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getString(LOG_CONFIG_SECTION, LOG_CONFIG_OB_PASSWORD);
      }
      return ret;
    }

    const char *ObLogConfig::get_tb_select() const
    {
      const char *ret = NULL;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getString(LOG_PARTITION_SECTION, LOG_PARTITION_TB_SELECT);
      }
      return ret;
    }

    int ObLogConfig::get_router_thread_num() const
    {
      int ret = 0;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getInt(LOG_CONFIG_SECTION, LOG_CONFIG_ROUTER_THREAD_NUM);
      }
      return ret;
    }

    int ObLogConfig::get_query_back() const
    {
      int ret = 1;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getInt(LOG_CONFIG_SECTION, LOG_CONFIG_QUERY_BACK);
      }
      return ret;
    }

    const char *ObLogConfig::get_lua_conf() const
    {
      const char *ret = NULL;
      if (inited_)
      {
        ret = const_cast<ObLogConfig&>(*this).config_.getString(LOG_PARTITION_SECTION, LOG_PARTITION_LUA_CONF);
      }
      return ret;
    }

    const char *ObLogConfig::get_dbn_format(const char *tb_name) const
    {
      const char *ret = NULL;
      if (inited_)
      {
        std::string db_name_format = tb_name;
        db_name_format.append(LOG_PARTITION_DBN_FORMAT_SUFFIX);
        ret = const_cast<ObLogConfig&>(*this).config_.getString(LOG_PARTITION_SECTION, db_name_format.c_str());
      }
      return ret;
    }

    const char *ObLogConfig::get_tbn_format(const char *tb_name) const
    {
      const char *ret = NULL;
      if (inited_)
      {
        std::string tb_name_format = tb_name;
        tb_name_format.append(LOG_PARTITION_TBN_FORMAT_SUFFIX);
        ret = const_cast<ObLogConfig&>(*this).config_.getString(LOG_PARTITION_SECTION, tb_name_format.c_str());
      }
      return ret;
    }

  }
}

