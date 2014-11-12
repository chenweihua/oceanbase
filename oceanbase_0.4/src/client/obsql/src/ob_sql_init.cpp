#include "common/ob_define.h"
#include "ob_sql_init.h"
#include "ob_sql_define.h"
#include "ob_sql_conn_recycle.h"
#include "ob_sql_update_worker.h"
#include "ob_sql_util.h"
#include "common/ob_malloc.h"
#include <string.h>
#include <malloc.h>

using namespace std;
using namespace oceanbase::common;
//define global var

pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutexattr_t attr;
static int parseValue(char *str, char *key, char *val)
{
  char           *p, *p1, *name, *value;

  if (str == NULL)
    return -1;

  p = str;
  while ((*p) == ' ' || (*p) == '\t' || (*p) == '\r' || (*p) == '\n') p++;
  p1 = p + strlen(p);
  while(p1 > p) {
    p1 --;
    if (*p1 == ' ' || *p1 == '\t' || *p1 == '\r' || *p1 == '\n') continue;
    p1 ++;
    break;
  }
  (*p1) = '\0';
  if (*p == '#' || *p == '\0') return -1;
  p1 = strchr(str, '=');
  if (p1 == NULL) return -2;
  name = p;
  value = p1 + 1;
  while ((*(p1 - 1)) == ' ') p1--;
  (*p1) = '\0';

  while ((*value) == ' ') value++;
  p = strchr(value, '#');
  if (p == NULL) p = value + strlen(value);
  while ((*(p - 1)) <= ' ') p--;
  (*p) = '\0';
  if (name[0] == '\0')
    return -2;

  strcpy(key, name);
  strcpy(val, value);
  return 0;
}

static int get_config(const char *filename, ObSQLConfig *config)
{
  int ret = OB_SQL_SUCCESS;
  FILE *fp;
  char *line = NULL;
  size_t len = 0;
  ssize_t read = 0;
  char key[1024];
  char value[1024];
  if (NULL == (fp = fopen(filename, "r")))
  {
    //fprintf(stderr, "can not open file %s\n", filename);
    ret = OB_SQL_ERROR;
  }
  else
  {
    //set default config
    memcpy(config->username_, OB_SQL_USER, strlen(OB_SQL_USER));
    memcpy(config->passwd_, OB_SQL_PASS, strlen(OB_SQL_PASS));
    while((read = getline(&line, &len, fp)) != -1)
    {
      parseValue(line, key, value);
      if (0 == memcmp(key, OB_SQL_CONFIG_LOG, strlen(OB_SQL_CONFIG_LOG)))
      {
        memcpy(config->logfile_, value, strlen(value));
      }
      else if (0 == memcmp(key, OB_SQL_CONFIG_URL, strlen(OB_SQL_CONFIG_URL)))
      {
        memcpy(config->url_, value, strlen(value));
      }
      else if (0 == memcmp(key, OB_SQL_CONFIG_LOG_LEVEL, strlen(OB_SQL_CONFIG_LOG_LEVEL)))
      {
        memcpy(config->loglevel_, value, strlen(value));
      }
      else if (0 == memcmp(key, OB_SQL_CONFIG_MIN_CONN, strlen(OB_SQL_CONFIG_MIN_CONN)))
      {
        config->min_conn_ = atoi(value);
      }
      else if (0 == memcmp(key, OB_SQL_CONFIG_MAX_CONN, strlen(OB_SQL_CONFIG_MAX_CONN)))
      {
        config->max_conn_ = atoi(value);
      }
      else if (0 == memcmp(key, OB_SQL_CONFIG_IP, strlen(OB_SQL_CONFIG_IP)))
      {
        memcpy(config->ip_, value, strlen(value));
        g_rslist[g_rsnum].ip_ = trans_ip_to_int(config->ip_);
      }
      else if (0 == memcmp(key, OB_SQL_CONFIG_PORT, strlen(OB_SQL_CONFIG_PORT)))
      {
        config->port_ = atoi(value);
        g_rslist[g_rsnum].port_ = config->port_;
      }
      else if (0 == memcmp(key, OB_SQL_CONFIG_USERNAME, strlen(OB_SQL_CONFIG_USERNAME)))
      {
        memset(config->username_, 0, OB_SQL_MAX_USER_NAME_LEN);
        memcpy(config->username_, value, strlen(value));
      }
      else if (0 == memcmp(key, OB_SQL_CONFIG_PASSWD, strlen(OB_SQL_CONFIG_PASSWD)))
      {
        memset(config->passwd_, 0, OB_SQL_MAX_PASSWD_LEN);
        memcpy(config->passwd_, value, strlen(value));
      }
    }
    if (0 != config->port_ && 0 != strlen(config->ip_))
    {
      g_rsnum++;
    }
    fclose(fp);
  }
  return ret;
}

static const char* read_env()
{
  const char* config_file = getenv(OB_SQL_CONFIG_ENV);
  if (NULL == config_file)
  {
    config_file = OB_SQL_CONFIG_DEFAULT_NAME;
  }
  return config_file;
}

static int load_config()
{
  int ret = OB_SQL_SUCCESS;
  const char *config_file = read_env();
  ret = get_config(config_file, &g_sqlconfig);
  if (OB_SQL_SUCCESS != ret)
  {
    //TBSYS_LOG(WARN, "failed to get config %s", config_file);
  }
  return ret;
}

int __attribute__((constructor))ob_sql_init()
{
  int ret = OB_SQL_SUCCESS;
  ::mallopt(M_MMAP_THRESHOLD, OB_SQL_DEFAULT_MMAP_THRESHOLD);
  ob_init_memory_pool();
  //load client config
  ret = load_config();
  if (OB_SQL_SUCCESS == ret)
  {
    //init log
    TBSYS_LOGGER.setFileName(g_sqlconfig.logfile_, true);
    //TBSYS_LOGGER.setFileName(g_sqlconfig.logfile_);
    TBSYS_LOGGER.setLogLevel(g_sqlconfig.loglevel_);
    TBSYS_LOG(INFO, "logger=%p", &(TBSYS_LOGGER));
    //set min max conn
    g_config_using->min_conn_size_ = static_cast<int16_t>(g_sqlconfig.min_conn_);
    g_config_using->max_conn_size_ = static_cast<int16_t>(g_sqlconfig.max_conn_);
    g_config_update->min_conn_size_ = static_cast<int16_t>(g_sqlconfig.min_conn_);
    g_config_update->max_conn_size_ = static_cast<int16_t>(g_sqlconfig.max_conn_);

    //加载libmysqlclient中的函数
    ret = init_func_set(&g_func_set);
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "load real mysql function symbol from libmysqlclient failed");
    }
    else
    {
      TBSYS_LOG(INFO, "new ob_sql_init libmysqlclient native functions loaded");
      g_inited = 1;
      //从配置服务器获取配置 初始化连接池 集群选择表
      if (OB_SQL_SUCCESS != start_update_worker(g_sqlconfig.url_))
      {
        TBSYS_LOG(ERROR, "get config from url failed url is %s", g_sqlconfig.url_);
        ret = OB_SQL_ERROR;
      }
      else
      {
        //初始化连接回收链表启动回收线程
        ob_sql_list_init(&g_delete_ms_list, OBSQLCONNLIST);                     // recycle connection list
        ret = start_recycle_worker();
        if (OB_SQL_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "recycle worker start failed");
        }
      }
    }
  }
  else
  {
    //不打log
    //TBSYS_LOG(ERROR, "load config failed");
  }
  return ret;
}
