////===================================================================
 //
 // ob_log_mysql_adaptor.cpp liboblog / Oceanbase
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

#include "ob_log_utils.h"
#include "ob_log_mysql_adaptor.h"

namespace oceanbase
{
  namespace liboblog
  {
    ObLogMysqlAdaptor::ObLogMysqlAdaptor() : mod_(ObModIds::OB_LOG_STMT),
                                             allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                                             inited_(false),
                                             mysql_addr_(NULL),
                                             mysql_port_(0),
                                             mysql_user_(NULL),
                                             mysql_password_(NULL),
                                             schema_getter_(NULL),
                                             ps_map_(),
                                             tc_info_list_(NULL)
    {
    }

    ObLogMysqlAdaptor::~ObLogMysqlAdaptor()
    {
      destroy();
    }

    int ObLogMysqlAdaptor::init(const ObLogConfig &config, IObLogSchemaGetter *schema_getter)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (mysql_addr_ = config.get_mysql_addr())
              || 0 >= (mysql_port_ = config.get_mysql_port())
              || NULL == (mysql_user_ = config.get_mysql_user())
              || NULL == (mysql_password_ = config.get_mysql_password())
              || NULL == (schema_getter_ = schema_getter))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != ps_map_.create(PS_MAP_SIZE))
      {
        TBSYS_LOG(WARN, "ps_map create fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = ObLogConfig::parse_tb_select(config.get_tb_select(), *this, &config)))
      {
        TBSYS_LOG(WARN, "parse_tb_select fail, ret=%d", ret);
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

    void ObLogMysqlAdaptor::destroy()
    {
      inited_ = false;

      TCInfo *info_iter = tc_info_list_;
      while (NULL != info_iter)
      {
        TCInfo *info_tmp = info_iter->next;
        info_iter->clear();
        info_iter = info_tmp;
      }
      tc_info_list_ = NULL;

      PSMap::iterator map_iter;
      for (map_iter = ps_map_.begin(); map_iter != ps_map_.end(); map_iter++)
      {
        if (0 != map_iter->second->tc_key)
        {
          pthread_key_delete(map_iter->second->tc_key);
          map_iter->second->tc_key = 0;
        }
      }
      ps_map_.clear();

      schema_getter_ = NULL;
      mysql_port_ = 0;
      mysql_addr_ = NULL;

      allocator_.reuse();
    }

    int ObLogMysqlAdaptor::operator ()(const char *tb_name, const ObLogConfig *config)
    {
      UNUSED(config);
      int ret = OB_SUCCESS;
      const ObLogSchema *total_schema = NULL;
      const ObTableSchema *table_schema = NULL;
      const ObColumnSchemaV2 *column_schema = NULL;
      int32_t column_num = 0;
      PSInfo *ps_info = NULL;
      int tmp_ret = 0;
      if (NULL == (total_schema = schema_getter_->get_schema()))
      {
        TBSYS_LOG(WARN, "get schema from schema_getter fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (table_schema = total_schema->get_table_schema(tb_name)))
      {
        TBSYS_LOG(WARN, "get_table_schema fail, tb_name=%s", tb_name);
        ret = OB_SCHEMA_ERROR;
      }
      else if (NULL == (column_schema = total_schema->get_table_schema(table_schema->get_table_id(), column_num)))
      {
        TBSYS_LOG(WARN, "get_column_schema fail, table_id=%lu", table_schema->get_table_id());
        ret = OB_SCHEMA_ERROR;
      }
      else if (NULL == (ps_info = (PSInfo*)allocator_.alloc(sizeof(PSInfo))))
      {
        TBSYS_LOG(WARN, "alloc ps_info fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memset(ps_info, 0, sizeof(PSInfo));
        if (0 != (tmp_ret = pthread_key_create(&(ps_info->tc_key), NULL)))
        {
          TBSYS_LOG(WARN, "pthread_key_create, ret=%d", tmp_ret);
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          int64_t pos = 0;

          databuff_printf(ps_info->sql, PSInfo::PS_SQL_BUFFER_SIZE, pos, "select ");
          for (int64_t i = 0; i < column_num; i++)
          {
            databuff_printf(ps_info->sql, PSInfo::PS_SQL_BUFFER_SIZE, pos, "%s", column_schema[i].get_name());
            if (i < (column_num - 1))
            {
              databuff_printf(ps_info->sql, PSInfo::PS_SQL_BUFFER_SIZE, pos, ",");
            }
            else
            {
              databuff_printf(ps_info->sql, PSInfo::PS_SQL_BUFFER_SIZE, pos, " ");
            }
          }

          databuff_printf(ps_info->sql, PSInfo::PS_SQL_BUFFER_SIZE, pos, "from %s where ", table_schema->get_table_name());

          for (int64_t i = 0; i < table_schema->get_rowkey_info().get_size(); i++)
          {
            uint64_t table_id = table_schema->get_table_id();
            uint64_t column_id = table_schema->get_rowkey_info().get_column(i)->column_id_;
            const ObColumnSchemaV2 *rk_column_schema = total_schema->get_column_schema(table_id, column_id);
            databuff_printf(ps_info->sql, PSInfo::PS_SQL_BUFFER_SIZE, pos, "%s=?", rk_column_schema->get_name());
            if (i < (table_schema->get_rowkey_info().get_size() - 1))
            {
              databuff_printf(ps_info->sql, PSInfo::PS_SQL_BUFFER_SIZE, pos, " and ");
            }
            else
            {
              databuff_printf(ps_info->sql, PSInfo::PS_SQL_BUFFER_SIZE, pos, ";");
            }
          }
          
          TBSYS_LOG(INFO, "build sql succ, table_id=%lu [%s]", table_schema->get_table_id(), ps_info->sql);

          if (hash::HASH_INSERT_SUCC != (tmp_ret = ps_map_.set(table_schema->get_table_id(), ps_info)))
          {
            TBSYS_LOG(WARN, "set ps_info to ps_map fail, ret=%d table_id=%lu", tmp_ret, table_schema->get_table_id());
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            ps_info->table_id = table_schema->get_table_id();
            ps_info->rk_column_num = table_schema->get_rowkey_info().get_size();
            TBSYS_LOG(INFO, "construct ps_info succ, table_id=%lu ps_info=%p",
                      table_schema->get_table_id(), ps_info);
          }

          if (OB_SUCCESS != ret)  
          {
            if (0 != ps_info->tc_key)
            {
              pthread_key_delete(ps_info->tc_key);
              ps_info->tc_key = 0;
            }
          }
        }
      }
      if (NULL != total_schema)
      {
        schema_getter_->revert_schema(total_schema);
        total_schema = NULL;
      }
      return ret;
    }

    int ObLogMysqlAdaptor::query_whole_row(const uint64_t table_id, const common::ObRowkey &rowkey, const IObLogColValue *&list)
    {
      int ret = OB_SUCCESS;
      TCInfo *tc_info = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == (tc_info = get_tc_info_(table_id)))
      {
        TBSYS_LOG(INFO, "get_tc_info fail, table_id=%lu", table_id);
        ret = OB_SCHEMA_ERROR;
      }
      else if (OB_SUCCESS != (ret = execute_stmt_(tc_info, rowkey))
          && OB_ENTRY_NOT_EXIST != ret)
      {
        TBSYS_LOG(WARN, "execute_stmt fail, ret=%d tc_info=%p", ret, tc_info);
      }
      else
      {
        list = tc_info->res_list;
      }
      return ret;
    }

    ObLogMysqlAdaptor::TCInfo *ObLogMysqlAdaptor::get_tc_info_(const uint64_t table_id)
    {
      TCInfo *ret = NULL;
      PSInfo *ps_info = NULL;
      int tmp_ret = ps_map_.get(table_id, ps_info);
      if (hash::HASH_EXIST != tmp_ret
          || NULL == ps_info)
      {
        TBSYS_LOG(WARN, "get from ps_map fail, ret=%d table_id=%lu", tmp_ret, table_id);
      }
      else if (NULL == (ret = (TCInfo*)pthread_getspecific(ps_info->tc_key)))
      {
        ObSpinLockGuard guard(allocator_lock_);
        ret = (TCInfo*)allocator_.alloc(sizeof(TCInfo));
        if (NULL != ret)
        {
          memset(ret, 0, sizeof(TCInfo));
          ret->ps_info = ps_info;
          if (NULL != (ret = init_stmt_params_(table_id, ret)))
          {
            ret->next = tc_info_list_;
            tc_info_list_ = ret;
            if (0 != (tmp_ret = pthread_setspecific(ps_info->tc_key, ret)))
            {
              TBSYS_LOG(WARN, "pthread_setspecific fail, key=%d table_id=%lu", ps_info->tc_key, table_id);
              ret = NULL;
            }
          }
        }
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    ObLogMysqlAdaptor::TCInfo *ObLogMysqlAdaptor::init_stmt_params_(const uint64_t table_id, TCInfo *tc_info)
    {
      TCInfo *ret = NULL;
      const ObLogSchema *total_schema = NULL;
      const ObTableSchema *table_schema = NULL;
      const ObColumnSchemaV2 *column_schema = NULL;
      int32_t column_num = 0;
      if (NULL == (tc_info->mysql = mysql_init(NULL)))
      {
        TBSYS_LOG(WARN, "mysql_init fail");
      }
      else if (tc_info->mysql != mysql_real_connect(tc_info->mysql,
            mysql_addr_,
            mysql_user_,
            mysql_password_,
            OB_MYSQL_DATABASE,
            mysql_port_,
            NULL, 0))
      {
        TBSYS_LOG(WARN, "mysql_real_connect fail, %s", mysql_error(tc_info->mysql));
      }
      else if (NULL == (tc_info->stmt = mysql_stmt_init(tc_info->mysql)))
      {
        TBSYS_LOG(WARN, "mysql_stmt_init fail, %s", mysql_error(tc_info->mysql));
      }
      else if (0 != mysql_stmt_prepare(tc_info->stmt, tc_info->ps_info->sql, strlen(tc_info->ps_info->sql)))
      {
        TBSYS_LOG(WARN, "mysql_stmt_prepare fail, %s", mysql_error(tc_info->mysql));
      }
      else if (NULL == (total_schema = schema_getter_->get_schema()))
      {
        TBSYS_LOG(WARN, "get schema from schema_getter fail");
      }
      else if (NULL == (table_schema = total_schema->get_table_schema(table_id)))
      {
        TBSYS_LOG(WARN, "get_table_schema fail, table_id=%lu", table_id);
      }
      else if (NULL == (column_schema = total_schema->get_table_schema(table_id, column_num)))
      {
        TBSYS_LOG(WARN, "get_column_schema fail, table_id=%lu", table_id);
      }
      else if (NULL == (tc_info->params = (MYSQL_BIND*)allocator_.alloc(sizeof(MYSQL_BIND)
              * table_schema->get_rowkey_info().get_size())))
      {
        TBSYS_LOG(WARN, "alloc params fail, size=%ld", table_schema->get_rowkey_info().get_size());
      }
      else if (NULL == (tc_info->tm_data = (MYSQL_TIME*)allocator_.alloc(sizeof(MYSQL_TIME)
              * table_schema->get_rowkey_info().get_size())))
      {
        TBSYS_LOG(WARN, "alloc tm_data fail, size=%ld", table_schema->get_rowkey_info().get_size());
      }
      else if (NULL == (tc_info->res_idx = (MYSQL_BIND*)allocator_.alloc(sizeof(MYSQL_BIND) * column_num)))
      {
        TBSYS_LOG(WARN, "alloc res_idx fail, size=%d", column_num);
      }
      else
      {
        memset(tc_info->params, 0, sizeof(MYSQL_BIND) * table_schema->get_rowkey_info().get_size());
        memset(tc_info->tm_data, 0, sizeof(MYSQL_TIME) * table_schema->get_rowkey_info().get_size());
        memset(tc_info->res_idx, 0, sizeof(MYSQL_BIND) * column_num);
        tc_info->res_list = NULL;

        for (int32_t i = column_num - 1; i >= 0; i--)
        {

          IObLogColValue *cv = NULL;
          if (ObIntType == column_schema[i].get_type())
          {
            cv = (IObLogColValue*)allocator_.alloc(sizeof(IntColValue));
            new(cv) IntColValue();
          }
          else if (is_time_type(column_schema[i].get_type()))
          {
            cv = (IObLogColValue*)allocator_.alloc(sizeof(TimeColValue));
            new(cv) TimeColValue();
          }
          else
          {
            int64_t column_size = column_schema[i].get_size();
            if (0 > column_size)
            {
              column_size = OB_MAX_VARCHAR_LENGTH;
            }
            cv = (IObLogColValue*)allocator_.alloc(sizeof(VarcharColValue) + column_size);
            new(cv) VarcharColValue(column_size);
          }
          
          int tmp_ret = OB_SUCCESS;
          int32_t mysql_type = INT32_MAX;
          uint8_t num_decimals = 0;
          uint32_t length = 0;
          if (NULL != cv
              && OB_SUCCESS == (tmp_ret = obmysql::ObMySQLUtil::get_mysql_type(column_schema[i].get_type(),
                  (obmysql::EMySQLFieldType&)mysql_type,
                  num_decimals,
                  length)))
          {
            tc_info->res_idx[i].buffer_type = (enum_field_types)mysql_type;
            tc_info->res_idx[i].buffer_length = cv->get_length();
            tc_info->res_idx[i].buffer = cv->get_data_ptr();
            tc_info->res_idx[i].length = cv->get_length_ptr();
            tc_info->res_idx[i].is_null = cv->get_isnull_ptr();
            TBSYS_LOG(INFO, "init cv list, buffer_type=%d buffer_length=%ld buffer=%p length=%p is_null=%p",
                      tc_info->res_idx[i].buffer_type,
                      tc_info->res_idx[i].buffer_length,
                      tc_info->res_idx[i].buffer,
                      tc_info->res_idx[i].length,
                      tc_info->res_idx[i].is_null);

            cv->set_mysql_type(mysql_type);
            cv->set_next(tc_info->res_list);
            tc_info->res_list = cv;
          }
          else
          {
            TBSYS_LOG(WARN, "alloc cv fail, or trans ob_type=%d to mysql_type=%d fail, ret=%d",
                      column_schema[i].get_type(), mysql_type, tmp_ret);
            tc_info->clear();
            break;
          }
        }

        for (int64_t i = 0; i < table_schema->get_rowkey_info().get_size(); i++)
        {
          int tmp_ret = OB_SUCCESS;
          int32_t mysql_type = INT32_MAX;
          uint8_t num_decimals = 0;
          uint32_t length = 0;
          if (OB_SUCCESS == (tmp_ret = obmysql::ObMySQLUtil::get_mysql_type(
                  table_schema->get_rowkey_info().get_column(i)->type_,
                  (obmysql::EMySQLFieldType&)mysql_type,
                  num_decimals,
                  length)))
          {
            tc_info->params[i].buffer_type = (enum_field_types)mysql_type;
          }
          else
          {
            TBSYS_LOG(WARN, "alloc cv fail, or trans ob_type=%d to mysql_type=%d fail, ret=%d",
                      column_schema[i].get_type(), mysql_type, tmp_ret);
            tc_info->clear();
            break;
          }
        }

        if (NULL != tc_info->res_list)
        {
          ret = tc_info;
        }
      }
      if (NULL != total_schema)
      {
        schema_getter_->revert_schema(total_schema);
        total_schema = NULL;
      }
      return ret;
    }

    bool ObLogMysqlAdaptor::is_time_type(const ObObjType type)
    {
      bool bret = false;
      if (ObDateTimeType == type
          || ObPreciseDateTimeType == type
          || ObCreateTimeType == type
          || ObModifyTimeType == type)
      {
        bret = true;
      }
      return bret;
    }

    int ObLogMysqlAdaptor::execute_stmt_(TCInfo *tc_info, const common::ObRowkey &rowkey)
    {
      int ret = OB_SUCCESS;
      if (rowkey.get_obj_cnt() != tc_info->ps_info->rk_column_num)
      {
        TBSYS_LOG(WARN, "rowkey column num not match, schema=%ld rowkey=%ld",
                  rowkey.get_obj_cnt(), tc_info->ps_info->rk_column_num);
        ret = OB_SCHEMA_ERROR;
      }
      else
      {
        for (int64_t i = 0; i < rowkey.get_obj_cnt(); i++)
        {
          if (is_time_type(rowkey.get_obj_ptr()[i].get_type()))
          {
            int64_t tm_raw = *(int64_t*)rowkey.get_obj_ptr()[i].get_data_ptr();
            time_t tm_s = (time_t)tm_raw;
            if (ObDateTimeType != rowkey.get_obj_ptr()[i].get_type())
            {
              tm_s = (time_t)(tm_raw / 1000000);
            }
            struct tm tm;
            localtime_r(&tm_s, &tm);
            tc_info->tm_data[i].year = tm.tm_year + 1900;
            tc_info->tm_data[i].month = tm.tm_mon + 1;
            tc_info->tm_data[i].day = tm.tm_mday;
            tc_info->tm_data[i].hour = tm.tm_hour;
            tc_info->tm_data[i].minute = tm.tm_min;
            tc_info->tm_data[i].second = tm.tm_sec;
            tc_info->params[i].buffer = &tc_info->tm_data[i];
            tc_info->params[i].buffer_length = sizeof(tc_info->tm_data[i]);
          }
          else
          {
            tc_info->params[i].buffer = const_cast<void*>(rowkey.get_obj_ptr()[i].get_data_ptr());
            tc_info->params[i].buffer_length = rowkey.get_obj_ptr()[i].get_data_length();
          }
        }
        int tmp_ret = 0;
        if (0 != mysql_stmt_bind_param(tc_info->stmt, tc_info->params))
        {
          TBSYS_LOG(WARN, "mysql_stmt_bind_param fail, %s", mysql_error(tc_info->mysql));
          ret = OB_ERR_UNEXPECTED;
        }
        //TODO oceanbase crash retry
        else if (0 != TIMED_FUNC(mysql_stmt_execute, 1000000, tc_info->stmt))
        {
          TBSYS_LOG(WARN, "mysql_stmt_execute fail, %s", mysql_error(tc_info->mysql));
          ret = OB_ERR_UNEXPECTED;
        }
        else if (0 != mysql_stmt_bind_result(tc_info->stmt, tc_info->res_idx))
        {
          TBSYS_LOG(WARN, "mysql_stmt_bind_result fail, %s", mysql_error(tc_info->mysql));
          ret = OB_ERR_UNEXPECTED;
        }
        else if (0 != (tmp_ret = mysql_stmt_fetch(tc_info->stmt)))
        {
          if (MYSQL_NO_DATA == tmp_ret)
          {
            ret = OB_ENTRY_NOT_EXIST;
          }
          else
          {
            TBSYS_LOG(WARN, "mysql_stmt_fetch fail, %s", mysql_error(tc_info->mysql));
            ret = OB_ERR_UNEXPECTED;
          }
        }
        else
        {
          mysql_stmt_free_result(tc_info->stmt);
        }
      }
      return ret;
    }

  }
}

