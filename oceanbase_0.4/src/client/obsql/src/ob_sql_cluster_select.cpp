#include "ob_sql_mysql_adapter.h"
#include "ob_sql_cluster_select.h"
#include <algorithm>
#include <stdlib.h>
#include "ob_sql_atomic.h"
#include "ob_sql_global.h"
#include "common/ob_atomic.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;

//SelectTable global_select_table;
//更新配置中的flow_weight
static int update_flow_weight(ObGroupDataSource *gds)
{
  int ret = OB_SQL_SUCCESS;
  int slot = 0;
  int used = 0;
  int index = 0;
  int16_t weight = 0;
  if (NULL == gds)
  {
    TBSYS_LOG(ERROR, "invalid argument gds is %p", gds);
    ret = OB_SQL_ERROR;
  }
  else
  {
    for (; index < gds->csize_; ++index)
    {
      slot += (gds->clusters_ + index)->flow_weight_;
    }
    TBSYS_LOG(INFO, "slot is %d", slot);
    index = 0;
    for (; index < gds->csize_; ++index)
    {
      if (index < gds->csize_ - 1)
      {
        if (0 == slot)
        {
          weight = static_cast<int16_t>(OB_SQL_SLOT_NUM/(gds->csize_));
        }
        else
        {
          weight = (gds->clusters_ + index)->flow_weight_;
          weight = static_cast<int16_t>((weight * OB_SQL_SLOT_NUM) /slot);
        }
        used += weight;
      }
      else
      {
        weight = static_cast<int16_t>(OB_SQL_SLOT_NUM - used);
      }
      (gds->clusters_ + index)->flow_weight_ = weight;
      TBSYS_LOG(DEBUG, "cluster(%u) weight is %d", gds->clusters_[index].cluster_id_, weight);
    }
  }
  return ret;
}

/* 根据全局配置来初始化集群选择对照表 加写锁 */
int update_select_table(ObGroupDataSource *gds)
{
  int ret = OB_SQL_SUCCESS;
  if (NULL == gds)
  {
    TBSYS_LOG(ERROR, "invalid argument gds is %p", gds);
    ret = OB_SQL_ERROR;
  }
  else
  {
    ret = update_flow_weight(gds);
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "update_flow_weigth failed");
    }
    else
    {
      if (NULL == g_table)
      {
        g_table = (ObSQLSelectTable *)ob_malloc(sizeof(ObSQLSelectTable), ObModIds::LIB_OBSQL);
        TBSYS_LOG(DEBUG, "g_table->master_count_ is %u", g_table->master_count_);
        g_table->master_count_ = 0;
        TBSYS_LOG(DEBUG, "g_table->master_count_ is %u", g_table->master_count_);
        if (NULL == g_table)
        {
          TBSYS_LOG(ERROR, "alloc mem for ObSQLSelectTable failed");
          ret = OB_SQL_ERROR;
        }
      }

      if (OB_SQL_SUCCESS == ret)
      {
        g_table->master_cluster_ = get_master_cluster(gds);
        int cindex = 0;
        int sindex = 0;

        for (; cindex < gds->csize_; ++cindex)
        {
          int csindex = 0;
          for (;csindex < gds->clusters_[cindex].flow_weight_; ++csindex, ++sindex)
          {
            g_table->clusters_[sindex]  = gds->clusters_ + cindex;
            TBSYS_LOG(DEBUG, "g_table ids offset is %d, cluster id is %u", sindex, gds->clusters_[cindex].cluster_id_);
          }
        }
        //shuffle global_table->data
        ObClusterInfo **start = g_table->clusters_;
        ObClusterInfo **end = g_table->clusters_ + OB_SQL_SLOT_NUM;
        std::random_shuffle(start, end);
      }
    }
  }
  return ret;
}

ObClusterInfo* select_cluster(ObSQLMySQL* mysql)
{
  ObClusterInfo *cluster = NULL;
  if (g_config_using->cluster_size_ > 1)
  {
    if (is_consistence(mysql))
    {
      //一致性请求
      TBSYS_LOG(DEBUG, "consistence g_table->master_count_ is %u", g_table->master_count_);
      cluster = g_table->master_cluster_;
      TBSYS_LOG(DEBUG, "consistence cluster id = %u flow weight is %d ", cluster->cluster_id_, cluster->flow_weight_);
      atomic_inc(&g_table->master_count_);
    }
    else
    {
      while (1)
      {
        TBSYS_LOG(DEBUG, "select g_table->master_count_ is %u", g_table->master_count_);
        cluster = g_table->clusters_[atomic_inc(&g_table->cluster_index_) % OB_SQL_SLOT_NUM];
        TBSYS_LOG(DEBUG, "select cluster id = %u flow weight is %d ", cluster->cluster_id_, cluster->flow_weight_);
        if (1 == cluster->is_master_)
        {
          if (atomic_dec_positive(&g_table->master_count_) < 0)
          {
            break;
          }
        }
        else
        {
          break;
        }
      }
    }
  }
  else
  {
    cluster = g_table->master_cluster_;
  }
  TBSYS_LOG(DEBUG, "cluster id = %u flow weight is %d ", cluster->cluster_id_, cluster->flow_weight_);
  return cluster;
}
