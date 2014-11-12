#include "common/ob_malloc.h"
#include "ob_sql_ms_select.h"
#include "tblog.h"
#include "ob_sql_util.h"
#include <algorithm>

using namespace oceanbase::common;

static int init_ms_table()
{
  int ret = OB_SQL_SUCCESS;
  int16_t size = g_config_using->cluster_size_;
  int16_t index = 0;
  TBSYS_LOG(DEBUG, "g_config_using cluster size is %d", g_config_using->cluster_size_);
  for(; index < size; ++index)
  {
    ObSQLClusterConfig *config = g_config_using->clusters_ + index;
    int32_t buckets = OB_SQL_BUCKET_PER_SERVER * config->server_num_;
    if (NULL == config->table_)
    {
      TBSYS_LOG(DEBUG, "alloc mem for ms select table index is %d", index);
      config->table_ = (ObSQLSelectMsTable*)ob_malloc(sizeof(ObSQLSelectMsTable) + buckets * sizeof(ObSQLHashItem), ObModIds::LIB_OBSQL);
      if (NULL == config->table_)
      {
        TBSYS_LOG(ERROR, "all mem for ObSQLGlobalMsTable failed");
        ret = OB_SQL_ERROR;
      }
    }
    else
    {
      if (buckets != config->table_->slot_num_)
      {
        ob_free(config->table_);
        config->table_ = NULL;
        config->table_ = (ObSQLSelectMsTable*)ob_malloc(sizeof(ObSQLSelectMsTable) + buckets * sizeof(ObSQLHashItem), ObModIds::LIB_OBSQL);
        if (NULL == config->table_)
        {
          TBSYS_LOG(ERROR, "all mem for ObSQLGlobalMsTable failed");
          ret = OB_SQL_ERROR;
        }
      }
    }

    if (OB_SQL_SUCCESS == ret)
    {
      config->table_->items_ = reinterpret_cast<ObSQLHashItem*>(config->table_ + 1);
      config->table_->slot_num_ = buckets;
      config->table_->offset_ = 0;
    }
  }
  return ret;
}

static int compare_hashitem(const void *a, const void *b)
{
  int ret = 0;
  const ObSQLHashItem * itema = reinterpret_cast<const ObSQLHashItem*>(a);
  const ObSQLHashItem * itemb = reinterpret_cast<const ObSQLHashItem*>(b);
  if (itema->hashvalue_ > itemb->hashvalue_)
  {
    ret = 1;
  }
  else if (itema->hashvalue_ == itemb->hashvalue_)
  {
    ret = 0;
  }
  else
  {
    ret = -1;
  }
  return ret;
}

static bool compare_hash(const uint32_t val, const ObSQLHashItem b)
{
  bool ret = 0;
  if (val < b.hashvalue_)
  {
    ret = true;
  }
  else
  {
    ret = false;
  }
  return ret;
}

//遍历ObGroupDataSource 更新table
static int update_ms_table()
{
  int ret = OB_SQL_SUCCESS;
  int16_t index = 0;
  int16_t bidx = 0;
  int32_t cindex = 0;
  int32_t sindex = 0;
  ObClusterInfo *scluster = NULL;
  ObDataSource *ssource = NULL;
  uint32_t hashval = 0;
  //cal hashval for each cluster
  for (; index < g_config_using->cluster_size_; ++index)
  {
    ObSQLClusterConfig *config = g_config_using->clusters_ + index;
    cindex = 0;
    for (; cindex < g_group_ds.csize_; ++cindex)
    {
      scluster = g_group_ds.clusters_ + cindex;
      TBSYS_LOG(INFO, "ms select table cluster is %p", scluster);
      TBSYS_LOG(INFO, "confile cluster id is %u, sclueter cluster id is %u", config->cluster_id_, scluster->cluster_id_);
      if (config->cluster_id_ == scluster->cluster_id_)
      {
        sindex = 0;
        for (; sindex < scluster->csize_ ; ++sindex)
        {
          ssource = scluster->dslist_ + sindex;
          TBSYS_LOG(INFO, "ds is %p", ssource);
          bidx = 0;
          for ( ;bidx < OB_SQL_BUCKET_PER_SERVER; ++bidx)
          {
            hashval = 0;
            hashval = murmurhash2(&(ssource->server_.ip_), sizeof(ssource->server_.ip_), hashval);
            hashval = murmurhash2(&(ssource->server_.port_), sizeof(ssource->server_.port_), hashval);
            hashval = murmurhash2(&bidx, sizeof(bidx), hashval);
            (config->table_->items_ + config->table_->offset_)->hashvalue_ = hashval;
            (config->table_->items_ + config->table_->offset_)->server_ = ssource;
            config->table_->offset_++;
          }
        }
      }
    }
  }

  //sort ms select table
  index = 0;
  for (; index < g_config_using->cluster_size_; ++index)
  {
    ObSQLClusterConfig *config = g_config_using->clusters_ + index;
    std::qsort(config->table_->items_, config->table_->slot_num_, sizeof(ObSQLHashItem), compare_hashitem);
  }
  g_config_using->ms_table_inited_ = 1;

  //dump for test
  index = 0;
  for (; index < g_config_using->cluster_size_; ++index)
  {
    TBSYS_LOG(DEBUG, "dump cluster ms table %d", index);
    ObSQLClusterConfig *config = g_config_using->clusters_ + index;
    int64_t num = 0;
    for (; num < config->table_->slot_num_; ++num)
    {
      TBSYS_LOG(DEBUG, "offset is %ld, hash vale is %u, ds is %p", num, config->table_->items_[num].hashvalue_, config->table_->items_[num].server_);
    }
  }
  return ret;
}

int update_ms_select_table()
{
  int ret = OB_SQL_SUCCESS;
  ret = init_ms_table();
  if (OB_SQL_SUCCESS == ret)
  {
    ret = update_ms_table();
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "update ms select table failed");
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "reinit ms select table failed");
  }
  return ret;
}

ObDataSource * find_ds_by_val(ObSQLHashItem *first, int64_t num, const uint32_t val)
{
  ObDataSource *ds = NULL;
  ObSQLHashItem *item = std::upper_bound(first, first + num, val, compare_hash);
  TBSYS_LOG(INFO, "hash value is %u", val);
  if (NULL == item)
  {
    TBSYS_LOG(INFO, "item value is %u", first->hashvalue_);
    ds = first->server_;
  }
  else
  {
    if (item - first == num)
    {
      TBSYS_LOG(INFO, "item value is %u", first->hashvalue_);
      ds = first->server_;
    }
    else
    {
      TBSYS_LOG(INFO, "item value is %u", item->hashvalue_);
      ds = item->server_;
    }
  }

  TBSYS_LOG(INFO, "data source is %p, %s", ds, get_server_str(&ds->server_));
  return ds;
}
