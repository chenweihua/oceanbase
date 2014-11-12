#include "ob_sql_cluster_config.h"
#include "ob_sql_cluster_select.h"
#include "ob_sql_global.h"
#include "ob_sql_global.h"
#include "ob_sql_util.h"
#include "ob_sql_ms_select.h"
#include "tblog.h"
#include "common/ob_server.h"
#include <mysql/mysql.h>
#include <string.h>

/**
 * 读取OceanBase集群信息
 * 
 * @param server   listen ms information
 *
 * @return int     return OB_SQL_SUCCESS if get cluster information from listen ms success
 *                                       else return OB_SQL_ERROR
 *
 */
static int get_cluster_config(ObServerInfo *server)
{
  int ret = OB_SQL_ERROR;
  int index = 0;
  int sindex = 0;
  MYSQL_RES *results = NULL;
  MYSQL_ROW record;
  TBSYS_LOG(INFO, "real mysql init is %p, mysql_init is %p\n", g_func_set.real_mysql_init, mysql_init);
  MYSQL * mysql = (*g_func_set.real_mysql_init)(NULL);
  if (NULL != mysql)
  {
    mysql = (*g_func_set.real_mysql_real_connect)(mysql, get_ip(server), g_sqlconfig.username_, g_sqlconfig.passwd_,
                                                  OB_SQL_DB, server->port_, NULL, 0);
    if (NULL != mysql)
    {
      //get cluster config
      ret = (*g_func_set.real_mysql_query)(mysql, OB_SQL_QUERY_CLUSTER);
      if (0 == ret)
      {
        results = (*g_func_set.real_mysql_store_result)(mysql);
        if (NULL == results)
        {
          TBSYS_LOG(WARN, "store result failed, query is %s errno is %u", OB_SQL_QUERY_CLUSTER,
                    (*g_func_set.real_mysql_errno)(mysql));
          ret = OB_SQL_ERROR;
        }
        else
        {
          uint64_t cluster_num = (*g_func_set.real_mysql_num_rows)(results);
          TBSYS_LOG(DEBUG, "cluster num is %lu", cluster_num);
          if (cluster_num > OB_SQL_MAX_CLUSTER_NUM)
          {
            TBSYS_LOG(ERROR, "there are %lu cluster info, max cluster supported is %d",
                      cluster_num, OB_SQL_MAX_CLUSTER_NUM);
            ret = OB_SQL_ERROR;
          }
          else
          {
            while (OB_SQL_SUCCESS == ret 
                   &&(record = (*g_func_set.real_mysql_fetch_row)(results)))
            {
              if (NULL == record[0]||NULL == record[1]
                  || NULL == record[2]||NULL == record[3]
                  || NULL == record[4])
              {
                TBSYS_LOG(WARN, "cluster info record not complete");
                ret = OB_SQL_ERROR;
              }
              else
              {
                g_config_update->clusters_[index].cluster_id_ = static_cast<uint32_t>(atoll(record[0]));
                g_config_update->clusters_[index].cluster_type_ = atoll(record[1]);
                g_config_update->clusters_[index].flow_weight_ = static_cast<int16_t>(atoll(record[2]));
                g_config_update->clusters_[index].server_.ip_ = oceanbase::common::ObServer::convert_ipv4_addr(record[3]);
                g_config_update->clusters_[index].server_.port_ = static_cast<uint32_t>(atoll(record[4]));
                if (NULL == record[5])
                {
                  g_config_update->clusters_[index].read_strategy_ = 0;
                }
                else
                {
                  g_config_update->clusters_[index].read_strategy_ = static_cast<uint32_t>(atoll(record[5]));
                }
              
                //insert cluster ip/port to rslist
                insert_rs_list(g_config_update->clusters_[index].server_.ip_,
                               g_config_update->clusters_[index].server_.port_);
                if (MASTER == g_config_update->clusters_[index].cluster_type_)
                {
                  g_config_update->master_cluster_id_ = g_config_update->clusters_[index].cluster_id_;
                }
                TBSYS_LOG(DEBUG, "cluster id is %u", g_config_update->clusters_[index].cluster_id_);
                index++;
              }
            }
            TBSYS_LOG(DEBUG, "update config is %p cluster size is %d", g_config_update, index);
            g_config_update->cluster_size_ = static_cast<int16_t>(index);
          }
          (*g_func_set.real_mysql_free_result)(results);

          //get mslist per cluster
          index = 0;
          for (; index < g_config_update->cluster_size_
                 && OB_SQL_SUCCESS == ret; ++index)
          {
            sindex = 0;
            char querystr[OB_SQL_QUERY_STR_LENGTH];
            memset(querystr, 0, OB_SQL_QUERY_STR_LENGTH);
            snprintf(querystr, OB_SQL_QUERY_STR_LENGTH, "%s%u", OB_SQL_QUERY_SERVER, g_config_update->clusters_[index].cluster_id_);
            ret = (*g_func_set.real_mysql_query)(mysql, querystr);
            if (0 == ret)
            {
              results = (*g_func_set.real_mysql_store_result)(mysql);
              if (NULL == results)
              {
                TBSYS_LOG(WARN, "store result failed, query is %s errno is %u", querystr,
                          (*g_func_set.real_mysql_errno)(mysql));
                ret = OB_SQL_ERROR;
              }
              else
              {
                uint64_t ms_num = (*g_func_set.real_mysql_num_rows)(results);
                if (ms_num > OB_SQL_MAX_MS_NUM)
                {
                  TBSYS_LOG(ERROR, "cluster has %lu ms more than %d(OB_SQL_MAX_MS_NUM)",
                            ms_num, OB_SQL_MAX_MS_NUM);
                  ret = OB_SQL_ERROR;
                }
                else
                {
                  while ((record = (*g_func_set.real_mysql_fetch_row)(results)))
                  {
                    if (NULL == record[0] || NULL == record[1])
                    {
                      TBSYS_LOG(WARN, "ip or port info is null");
                    }
                    else
                    {
                      g_config_update->clusters_[index].merge_server_[sindex].ip_ = oceanbase::common::ObServer::convert_ipv4_addr(record[0]);
                      g_config_update->clusters_[index].merge_server_[sindex].port_ = static_cast<uint32_t>(atoll(record[1]));
                      sindex++;
                    }
                  }
                  g_config_update->clusters_[index].server_num_ = static_cast<int16_t>(sindex);
                }
              }
              (*g_func_set.real_mysql_free_result)(results);
            }
            else
            {
              ret = OB_SQL_ERROR;
              TBSYS_LOG(WARN, "do query (%s) from %s failed", querystr, get_server_str(server));
              break;
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "do query (%s) from %s failed ret is %d", OB_SQL_QUERY_CLUSTER, get_server_str(server), ret);
        ret = OB_SQL_ERROR;
      }
      (*g_func_set.real_mysql_close)(mysql);
    }
    else
    {
      TBSYS_LOG(ERROR, "failed to connect to server %s, Error: %s", get_server_str(server), (*g_func_set.real_mysql_error)(mysql));
      ret = OB_SQL_ERROR;
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "mysql init failed");
  }
  return ret;
}

/* swap g_config_using && g_config_update */
static void swap_config()
{
  ObSQLGlobalConfig * tmp = g_config_using;
  g_config_using = g_config_update;
  g_config_update = tmp;
}

/**
 * 集群个数，流量配置，集群类型是否变化
 */
static bool is_cluster_changed()
{
  bool ret = false;
  int cindex = 0;
  if (g_config_update->cluster_size_ != g_config_using->cluster_size_)
  {
    ret = true;
  }
  else
  {
    for (; cindex < g_config_using->cluster_size_; cindex++)
    {
      if (g_config_update->clusters_[cindex].cluster_id_ != g_config_using->clusters_[cindex].cluster_id_
          ||g_config_update->clusters_[cindex].flow_weight_ != g_config_using->clusters_[cindex].flow_weight_
          ||g_config_update->clusters_[cindex].cluster_type_ != g_config_using->clusters_[cindex].cluster_type_)
      {
        ret = true;
        break;
      }
    }
  }
  return ret;
}

//cluster 没变化
//判断cluster 里面的mslist是否变化
static bool is_mslist_changed()
{
  bool ret = false;
  int cindex = 0;
  int sindex = 0;
  if (g_config_update->cluster_size_ != g_config_using->cluster_size_)
  {
    ret = true;
  }
  else
  {
    for (; cindex < g_config_using->cluster_size_; cindex++)
    {
      if (g_config_update->clusters_[cindex].server_num_ != g_config_using->clusters_[cindex].server_num_)
      {
        ret = true;
        break;
      }
      else
      {
        for (; sindex < g_config_using->clusters_[cindex].server_num_; ++sindex)
        {
          if ((g_config_update->clusters_[cindex].merge_server_[sindex].ip_
               != g_config_using->clusters_[cindex].merge_server_[sindex].ip_)
              || (g_config_update->clusters_[cindex].merge_server_[sindex].port_
                  != g_config_using->clusters_[cindex].merge_server_[sindex].port_))
            {
              ret = true;
              break;
            }
        }
      }
    }
  }
  return ret;
}

static int update_table(ObGroupDataSource *gds)
{
  int ret = OB_SQL_SUCCESS;
  ret = update_select_table(gds);
  if (OB_SQL_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "update select table failed");
    dump_config(g_config_using);
  }
  dump_table();
  return ret;
}

static int rebuild_tables()
{
  int ret = OB_SQL_SUCCESS;

  //如果mslist变化了更新group data source
  if (is_mslist_changed())
  {
    TBSYS_LOG(DEBUG, "cluster size is %d cluste id is %u", g_config_using->cluster_size_, g_config_using->clusters_[0].cluster_id_);
    ret = update_group_ds(g_config_using, &g_group_ds);
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "update ObGroupDataSource failed");
      dump_config(g_config_using);
    }
    else
    {
      dump_config(g_config_using);
      dump_config(g_config_update);
      TBSYS_LOG(DEBUG, "update ObGroupDataSource success");
    }

    if (OB_SQL_SUCCESS == ret)
    {
      ret = update_ms_select_table();
      if (OB_SQL_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "update ms select table failed");
      }
      else
      {
        TBSYS_LOG(DEBUG, "update ms select table success");
      }
    }
  }

  //集群信息流量变化更新集群选择表
  if (OB_SQL_SUCCESS == ret && is_cluster_changed())
  {
    TBSYS_LOG(DEBUG, "update select table");
    ret = update_table(&g_group_ds);
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "update select table failed");
    }
    else
    {
      TBSYS_LOG(DEBUG, "update select table success");
    }
  }
  return ret;
}

// 根据rslist 从Oceanbase 获取ms列表 流量信息
// 更新g_config_update的配置
int get_ob_config()
{
  int ret = OB_SQL_SUCCESS;
  int32_t index = 0;
  TBSYS_LOG(DEBUG, "rsnum is %d", g_rsnum);
  for (; index < g_rsnum; ++index)
  {
    TBSYS_LOG(DEBUG, "listener mergeserver ip is %u, port is %u", g_rslist[index].ip_, g_rslist[index].port_);
    ret = get_cluster_config(g_rslist + index);
    if (OB_SQL_SUCCESS == ret)
    {
      TBSYS_LOG(DEBUG, "Get config information from listen ms(%s) success", get_server_str(g_rslist + index));
      break;
    }
    else
    {
      TBSYS_LOG(WARN, "get cluster info from %s failed", get_server_str(g_rslist + index));
    }
  }
  return ret;
}

int do_update()
{
  int ret = OB_SQL_SUCCESS;
  pthread_rwlock_wrlock(&g_config_rwlock); // wlock for update global config
  swap_config();
  ret = rebuild_tables();
  if (OB_SQL_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "rebuild_table failed");
  }
  pthread_rwlock_unlock(&g_config_rwlock); // unlock
  return ret;
}
