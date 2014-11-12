////===================================================================
 //
 // ob_log_filter.cpp liboblog / Oceanbase
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

#include "ob_log_filter.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogTableFilter::ObLogTableFilter() : inited_(false),
                                           prev_(NULL),
                                           tid_filter_()
    {
    }

    ObLogTableFilter::~ObLogTableFilter()
    {
      destroy();
    }

    int ObLogTableFilter::init(IObLogFilter *filter, const ObLogConfig &config, IObLogSchemaGetter *schema_getter)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (prev_ = filter)
              || NULL == schema_getter)
      {
        TBSYS_LOG(WARN, "invalid param filter=%p schema_getter=%p", filter, schema_getter);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_tid_filter_(config, *schema_getter)))
      {
        TBSYS_LOG(WARN, "init_tid_filter fail, ret=%d", ret);
      }
      else
      {
        inited_ = true;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogTableFilter::destroy()
    {
      inited_ = false;
      tid_filter_.destroy();
      prev_ = NULL;
    }

    int ObLogTableFilter::next_mutator(ObLogMutator **mutator, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == mutator)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int64_t start_time = tbsys::CTimeUtil::getTime();
        int64_t end_time = start_time + timeout;
        int64_t cur_timeout = timeout;
        ObLogMutator *tmp_mutator = NULL;
        while (true)
        {
          ret = prev_->next_mutator(&tmp_mutator, cur_timeout);
          if (OB_SUCCESS == ret)
          {
            if (NULL == tmp_mutator)
            {
              // unexpected error
              abort();
            }
            if (contain_(*tmp_mutator))
            {
              *mutator = tmp_mutator;
              break;
            }
            else
            {
              prev_->release_mutator(tmp_mutator);
              tmp_mutator = NULL;
            }
          }
          cur_timeout = end_time - tbsys::CTimeUtil::getTime();
          if (0 >= cur_timeout)
          {
            ret = (NULL == tmp_mutator) ? OB_PROCESS_TIMEOUT : OB_SUCCESS;
            break;
          }
        }
      }
      return ret;
    }

    void ObLogTableFilter::release_mutator(ObLogMutator *mutator)
    {
      if (inited_)
      {
        prev_->release_mutator(mutator);
      }
    }

    bool ObLogTableFilter::contain(const uint64_t table_id)
    {
      return (hash::HASH_EXIST == tid_filter_.exist(table_id));
    }

    int ObLogTableFilter::init_tid_filter_(const ObLogConfig &config, IObLogSchemaGetter &schema_getter)
    {
      int ret = OB_SUCCESS;
      const ObLogSchema *total_schema = schema_getter.get_schema();
      if (NULL == total_schema)
      {
        TBSYS_LOG(WARN, "get total schema fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != tid_filter_.create(TID_FILTER_SIZE))
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = ObLogConfig::parse_tb_select(config.get_tb_select(), *this, total_schema);
      }
      if (NULL != total_schema)
      {
        schema_getter.revert_schema(total_schema);
        total_schema = NULL;
      }
      return ret;
    }

    int ObLogTableFilter::operator ()(const char *tb_name, const ObLogSchema *total_schema)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema *table_schema = total_schema->get_table_schema(tb_name);
      int hash_ret = 0;
      if (NULL == table_schema)
      {
        TBSYS_LOG(WARN, "table_name=%s not find in schema", tb_name);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (hash::HASH_INSERT_SUCC != (hash_ret = tid_filter_.set(table_schema->get_table_id()))
              && hash::HASH_EXIST != hash_ret)
      {
        TBSYS_LOG(WARN, "set to tid_filter fail, hash_ret=%d table_name=%s table_id=%lu",
                  hash_ret, tb_name, table_schema->get_table_id());
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        TBSYS_LOG(INFO, "add tid_filter succ, table_name-%s table_id=%lu",
                  tb_name, table_schema->get_table_id());
      }
      return ret;
    }

    bool ObLogTableFilter::contain_(ObLogMutator &mutator)
    {
      bool bret = false;
      int tmp_ret = OB_SUCCESS;
      while (!bret
            && OB_SUCCESS == tmp_ret
            && OB_SUCCESS == (tmp_ret = mutator.get_mutator().next_cell()))
      {
        ObMutatorCellInfo *cell = NULL;
        if (OB_SUCCESS != (tmp_ret = mutator.get_mutator().get_cell(&cell)))
        {
          TBSYS_LOG(WARN, "mutator get_cell fail, ret=%d", tmp_ret);
        }
        else if (NULL == cell)
        {
          TBSYS_LOG(WARN, "unexpected error cell is null pointer");
        }
        else
        {
          bret = (hash::HASH_EXIST == tid_filter_.exist(cell->cell_info.table_id_));
          //TBSYS_LOG(DEBUG, "table_id=%lu bret=%s", cell->cell_info.table_id_, STR_BOOL(bret));
        }
      }
      mutator.get_mutator().reset_iter();
      return bret;
    }

  }
}

