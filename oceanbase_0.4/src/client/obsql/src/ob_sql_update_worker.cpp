#include "ob_sql_update_worker.h"
#include "tblog.h"
#include "ob_sql_util.h"
#include "ob_sql_global.h"
#include "ob_sql_ms_select.h"
#include "ob_sql_cluster_config.h"
#include "ob_sql_util.h"
#include <mysql/mysql.h>
#include <string.h>

static void *update_global_config(void *arg)
{
  int ret = OB_SQL_SUCCESS;
  int refreshrs = 0;
  while(1)
  {
    sleep(OB_SQL_UPDATE_INTERVAL);
    refreshrs = 0;
    while (1)
    {
      ret = get_ob_config();
      if (OB_SQL_SUCCESS == ret)
      {
        ret = do_update();
        if (OB_SQL_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "update global config failed");
          dump_config(g_config_update);
        }
        else
        {
          //TBSYS_LOG(WARN, "get config info failed url(%s)", reinterpret_cast<char*>(arg));
        }
      }
      else
      {
        TBSYS_LOG(WARN, "get mslist failed");
        if (0 == refreshrs)
        {
          TBSYS_LOG(WARN, "update rslist and try again");
          ret = get_rs_list(reinterpret_cast<char*>(arg));
          refreshrs++;
          if (OB_SQL_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "get rslist from url(%s) failed", reinterpret_cast<char*>(arg));
            TBSYS_LOG(ERROR, "update global config failed");
            break;
          }
          continue;
        }
      }
      break;
    }
  }
  return NULL;
}

int start_update_worker(const char* url)
{
  int ret = OB_SQL_SUCCESS;
  //query ms list first time immediatly
  if (0 != strlen(url))
  {
    ret = get_rs_list(url);
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "can not get config info from url(%s) using ip/port in config", url);
    }
    else
    {
      TBSYS_LOG(INFO, "Get fake merge server address from %s success", url);
    }
  }

  ret = get_ob_config();
  if (OB_SQL_SUCCESS == ret)
  {
    ret = do_update();
    if (OB_SQL_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "obsql build GroupDataSource select table success");
      pthread_t update_task;
      ret = pthread_create(&update_task, NULL, update_global_config, (void*)url);
      if (OB_SQL_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "start config update worker failed");
      }
      else
      {
        TBSYS_LOG(INFO, "start config update worker");
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "update global config failed");
      dump_config(g_config_update);
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "get config from oceanbse failed");
  }

  return ret;
}
