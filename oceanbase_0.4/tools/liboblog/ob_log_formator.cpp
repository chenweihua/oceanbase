////===================================================================
 //
 // ob_log_formator.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-06-07 by Yubai (yubai.lk@alipay.com) 
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

#include "tbsys.h"
#include "common/ob_obj_cast.h"
#include "ob_log_formator.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
#ifdef __SPARSE_AVAILABLE__
    ObLogSparseFormator::ObLogSparseFormator() : mod_(ObModIds::OB_LOG_BINLOG_RECORD),
                                                 allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                                                 inited_(false),
                                                 schema_getter_(NULL),
                                                 meta_manager_(NULL),
                                                 log_filter_(NULL),
                                                 p_list_(),
                                                 v_list_(),
                                                 p_cond_(COND_SPIN_WAIT_NUM),
                                                 v_cond_(COND_SPIN_WAIT_NUM)
    {
    }

    ObLogSparseFormator::~ObLogSparseFormator()
    {
      destroy();
    }

    int ObLogSparseFormator::init(const ObLogConfig &config,
        IObLogSchemaGetter *schema_getter,
        IObLogMetaManager *meta_manager,
        IObLogFilter *log_filter)
    {
      UNUSED(config);
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (schema_getter_ = schema_getter)
              || NULL == (meta_manager_ = meta_manager)
              || NULL == (log_filter_ = log_filter))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_binlogrecord_list_(MAX_BINLOG_RECORD_NUM)))
      {
        TBSYS_LOG(WARN, "prepare binglog record list fail, ret=%d", ret);
      }
      else
      {
        inited_ = true;
      }
      return ret;
    }

    void ObLogSparseFormator::destroy()
    {
      inited_ = false;
      destroy_binlogrecord_list_();
      meta_manager_ = NULL;
      schema_getter_ = NULL;
      allocator_.reuse();
    }

    int ObLogSparseFormator::next_record(IBinlogRecord **record, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if (NULL == record)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObLogBinlogRecord *binlog_record = NULL;
        int64_t start_time = tbsys::CTimeUtil::getTime();
        while (true)
        {
          ret = p_list_.pop(binlog_record);
          if (OB_SUCCESS == ret)
          {
            break;
          }
          int64_t wait_time = timeout - (tbsys::CTimeUtil::getTime() - start_time);
          if (0 >= wait_time)
          {
            break;
          }
          p_cond_.timedwait(timeout);
        }
        if (OB_SUCCESS != ret)
        {
          ret = OB_PROCESS_TIMEOUT;
        }
        else
        {
          if (NULL == binlog_record)
          {
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            *record = binlog_record;
          }
        }
      }
      return ret;
    }

    void ObLogSparseFormator::release_record(IBinlogRecord *record)
    {
      ObLogBinlogRecord *sparse_br = dynamic_cast<ObLogBinlogRecord*>(record);
      if (NULL != sparse_br)
      {
        std::vector<std::string *>::const_iterator iter = sparse_br->newCols().begin();
        for (; iter != sparse_br->newCols().end(); iter++)
        {
          if (NULL != *iter)
          {
            string_pool_.free((ObLogString*)(*iter));
          }
        }
        iter = sparse_br->colNames().begin();
        for (; iter != sparse_br->colNames().end(); iter++)
        {
          if (NULL != *iter)
          {
            string_pool_.free((ObLogString*)(*iter));
          }
        }
        iter = sparse_br->colPKs().begin();
        for (; iter != sparse_br->colPKs().end(); iter++)
        {
          if (NULL != *iter)
          {
            string_pool_.free((ObLogString*)(*iter));
          }
        }
        sparse_br->clear();

        int ret = v_list_.push(sparse_br);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push sparse_br to v_list fail, ret=%d", ret);
        }
        else
        {
          v_cond_.signal();
        }
      }
    }

    ObLogBinlogRecord *ObLogSparseFormator::fetch_binlog_record_(volatile bool &loop_stop)
    {
      ObLogBinlogRecord *binlog_record = NULL;
      while (NULL == binlog_record
            && !loop_stop)
      {
        int tmp_ret = v_list_.pop(binlog_record);
        if (OB_SUCCESS != tmp_ret)
        {
          v_cond_.timedwait(WAIT_VLIST_TIMEOUT);
        }
        else
        {
          break;
        }
      }
      if (loop_stop
          && NULL != binlog_record)
      {
        v_list_.push(binlog_record);
        binlog_record = NULL;
      }
      return binlog_record;
    }

    int ObLogSparseFormator::fill_trans_barrier_(const RecordType type,
        const int64_t timestamp,
        const uint64_t checkpoint,
        const uint64_t mutator_num,
        volatile bool &loop_stop)
    {
      int ret = OB_SUCCESS;
      ObLogBinlogRecord *binlog_record = fetch_binlog_record_(loop_stop);
      if (NULL != binlog_record)
      {
        binlog_record->setRecordType(type);
        binlog_record->setTimestamp(timestamp/1000000);
        binlog_record->setCheckpoint(
            (uint32_t)((checkpoint & 0xffffffff00000000) >> 32),
            (uint32_t)(checkpoint & 0x00000000ffffffff));
        binlog_record->set_mutator_num(mutator_num);
        if (OB_SUCCESS != (ret = p_list_.push(binlog_record)))
        {
          TBSYS_LOG(WARN, "push to p_list fail, ret=%d", ret);
        }
        else
        {
          p_cond_.signal();
        }
        binlog_record = NULL;
      }
      else if (!loop_stop)
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = OB_IN_STOP_STATE;
      }
      return ret;
    }

    int ObLogSparseFormator::add_mutator(ObLogMutator &mutator,
        const uint64_t db_partition,
        const uint64_t tb_partition,
        volatile bool &loop_stop)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }

      const int64_t timestamp = mutator.get_mutate_timestamp();
      const uint64_t checkpoint = mutator.get_log_id();
      const uint64_t mutator_num = mutator.get_num();
      if (OB_SUCCESS == ret)
      {
        // Transaction Begin
        ret = fill_trans_barrier_(EBEGIN, timestamp, checkpoint, mutator_num, loop_stop);
      }

      // Transaction Statement
      ObLogBinlogRecord *binlog_record = NULL;
      while (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator.get_mutator().next_cell()))
      {
        ObMutatorCellInfo *cell = NULL;
        bool irc = false;
        bool irf = false;
        ObDmlType dml_type = OB_DML_UNKNOW;
        if (OB_SUCCESS != (ret = mutator.get_mutator().get_cell(&cell, &irc, &irf, &dml_type)))
        {
          TBSYS_LOG(WARN, "mutator get_cell fail, ret=%d", ret);
          break;
        }
        if (NULL == cell)
        {
          TBSYS_LOG(WARN, "unexpected error cell is null pointer");
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        if (NULL == binlog_record
            && NULL == (binlog_record = fetch_binlog_record_(loop_stop)))
        {
          if (!loop_stop)
          {
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            ret = OB_IN_STOP_STATE;
          }
          break;
        }

        if (OB_SUCCESS != (ret = fill_binlog_record_(*binlog_record,
                irc,
                dml_type,
                timestamp,
                checkpoint,
                mutator_num,
                cell->cell_info,
                db_partition,
                tb_partition)))
        {
          TBSYS_LOG(WARN, "fill_binlog_record fail, %s dml_type=%s ret=%d",
              print_cellinfo(&(cell->cell_info)), str_dml_type(dml_type), ret);
          break;
        }

        if (irf)
        {
          if (OB_SUCCESS != (ret = p_list_.push(binlog_record)))
          {
            TBSYS_LOG(WARN, "push to p_list fail, ret=%d", ret);
            break;
          }
          else
          {
            p_cond_.signal();
          }
          binlog_record = NULL;
        }
      }
      mutator.reset_iter();

      if (OB_ITER_END == ret)
      {
        // Transaction Commit
        ret = fill_trans_barrier_(ECOMMIT, timestamp, checkpoint, mutator_num, loop_stop);
      }
      return ret;
    }

    int ObLogSparseFormator::init_binlogrecord_list_(const int64_t max_binlogrecord_num)
    {
      int ret = OB_SUCCESS;
      ret = p_list_.init(max_binlogrecord_num);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "init binlogrecord p_list fail, max_binlogrecord_num=%ld", max_binlogrecord_num);
      }
      if (OB_SUCCESS == ret)
      {
        ret = v_list_.init(max_binlogrecord_num);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init binlogrecord v_list fail, max_binlogrecord_num=%ld", max_binlogrecord_num);
        }
      }
      for (int64_t i = 0; OB_SUCCESS == ret && i < max_binlogrecord_num; i++)
      {
        void *buffer = allocator_.alloc(sizeof(ObLogBinlogRecord));
        if (NULL == buffer)
        {
          TBSYS_LOG(WARN, "allocator memory to build binglog_record fail, i=%ld", i);
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }

        ObLogBinlogRecord *binlog_record = new(buffer) ObLogBinlogRecord();
        if (NULL == binlog_record)
        {
          TBSYS_LOG(WARN, "build binlog_record fail, i=%ld", i);
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        ret = v_list_.push(binlog_record);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push binlog_record to list fail, i=%ld", i);
          break;
        }
      }
      return ret;
    }

    void ObLogSparseFormator::destroy_binlogrecord_list_()
    {
      int64_t counter = v_list_.get_total() + v_list_.get_free();
      ObLogBinlogRecord *binlog_record = NULL;
      while (OB_SUCCESS == p_list_.pop(binlog_record))
      {
        if (NULL != binlog_record)
        {
          release_record(binlog_record);
          binlog_record = NULL;
        }
      }
      p_list_.destroy();

      while (OB_SUCCESS == v_list_.pop(binlog_record))
      {
        if (NULL != binlog_record)
        {
          binlog_record->~ObLogBinlogRecord();
          binlog_record = NULL;
          counter--;
        }
      }
      v_list_.destroy();

      if (0 != counter)
      {
        TBSYS_LOG(WARN, "still have binlog_record not release, counter=%ld", counter);
      }
    }

    int ObLogSparseFormator::fill_binlog_record_(ObLogBinlogRecord &binlog_record,
        const bool irc,
        const ObDmlType dml_type,
        const int64_t timestamp,
        const uint64_t checkpoint,
        const uint64_t mutator_num,
        const ObCellInfo &cell,
        const uint64_t db_partition,
        const uint64_t tb_partition)
    {
      int ret = OB_SUCCESS;

      if (irc)
      {
        for (int64_t i = 0; i < cell.row_key_.get_obj_cnt(); i++)
        {
          const char *pk = value2str_(cell.row_key_.get_obj_ptr()[i]);
          if (NULL == pk)
          {
            TBSYS_LOG(WARN, "trans rowkey column value to string fail, index=%ld %s", i, to_cstring(cell.row_key_));
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          std::string *pkstr = NULL;
          if (NULL == (pkstr = (std::string*)string_pool_.alloc()))
          {
            TBSYS_LOG(WARN, "construct std::string fail, pk=%s", pk);
            ret = OB_ALLOCATE_MEMORY_FAILED;
            break;
          }
          *pkstr = pk;
          binlog_record.putPKValue(pkstr);
        }

        if (OB_SUCCESS == ret)
        {
          ITableMeta *table_meta = NULL;
          if (NULL == (table_meta = meta_manager_->get_table_meta(cell.table_id_, db_partition, tb_partition)))
          {
            TBSYS_LOG(WARN, "get table meta from meta_manager fail");
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            // Oceanbase always set true
            binlog_record.setLastInLogevent(true);
            binlog_record.setDbname(table_meta->getDBMeta()->getName());
            binlog_record.setTbname(table_meta->getName());
            binlog_record.setTableMeta(table_meta);
            binlog_record.setTimestamp(timestamp/1000000);
            binlog_record.setCheckpoint(
                (uint32_t)((checkpoint & 0xffffffff00000000) >> 32),
                (uint32_t)(checkpoint & 0x00000000ffffffff));
            binlog_record.set_mutator_num(mutator_num);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        switch (dml_type)
        {
          case OB_DML_REPLACE:
            binlog_record.setRecordType(EREPLACE);
            break;
          case OB_DML_INSERT:
            binlog_record.setRecordType(EINSERT);
            break;
          case OB_DML_UPDATE:
            binlog_record.setRecordType(EUPDATE);
            break;
          case OB_DML_DELETE:
            binlog_record.setRecordType(EDELETE);
            break;
          default:
            TBSYS_LOG(WARN, "invalid dml_type=%d, %s", dml_type, print_cellinfo(&cell));
            ret = OB_ERR_UNEXPECTED;
            break;
        }
      }

      const ObLogSchema *total_schema = NULL;
      if (OB_SUCCESS == ret)
      {
        if (NULL == (total_schema = schema_getter_->get_schema()))
        {
          TBSYS_LOG(WARN, "get schema from schema_getter fail");
          ret = OB_ERR_UNEXPECTED;
        }
      }

      if ((OB_DML_REPLACE == dml_type
            || OB_DML_INSERT == dml_type
            || OB_DML_UPDATE == dml_type)
          && OB_SUCCESS == ret)
      {
        const ObColumnSchemaV2 *column_schema = total_schema->get_column_schema(cell.table_id_, cell.column_id_);
        const char *str = value2str_(cell.value_);
        std::string *namestr = NULL;
        std::string *strstr = NULL;
        int32_t mysql_type = INT32_MAX;
        uint8_t num_decimals = 0;
        uint32_t length = 0;
        if (NULL == column_schema)
        {
          TBSYS_LOG(WARN, "get column total_schema fail table_id=%lu column_id=%lu", cell.table_id_, cell.column_id_);
          ret = OB_SCHEMA_ERROR;
        }
        else if (NULL == str)
        {
          TBSYS_LOG(WARN, "trans column value to string fail, %s", print_cellinfo(&cell));
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = obmysql::ObMySQLUtil::get_mysql_type(column_schema->get_type(),
                (obmysql::EMySQLFieldType&)mysql_type,
                num_decimals,
                length))
                || 0 > mysql_type || UINT8_MAX < mysql_type)
        {
          TBSYS_LOG(WARN, "trans ob_type=%d to mysql_type=%d fail, ret=%d", column_schema->get_type(), mysql_type, ret);
        }
        else if (NULL == (namestr = (std::string*)string_pool_.alloc()))
        {
          TBSYS_LOG(WARN, "construct std::string fail, name=%s", column_schema->get_name());
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (strstr = (std::string*)string_pool_.alloc()))
        {
          TBSYS_LOG(WARN, "construct std::string fail, str=%s", str);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          *namestr = column_schema->get_name();
          *strstr = str;
          binlog_record.putNew(namestr, strstr, (uint8_t)mysql_type);
          TBSYS_LOG(DEBUG, "table_id=%lu column_id=%lu column_name=%s value=%s", cell.table_id_, cell.column_id_, column_schema->get_name(), str);
        }

        if (OB_SUCCESS != ret)
        {
          if (NULL != namestr)
          {
            string_pool_.free((ObLogString*)namestr);
            namestr = NULL;
          }
          if (NULL != strstr)
          {
            string_pool_.free((ObLogString*)strstr);
            strstr = NULL;
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

    const char *ObLogSparseFormator::value2str_(const ObObj &v)
    {
      static const int64_t DEFAULT_VALUE_STR_BUFFER_SIZE = 65536;
      static __thread char buffer[DEFAULT_VALUE_STR_BUFFER_SIZE];
      const char *ret = NULL;
      int64_t ret_size = 0;
      if (ObVarcharType == v.get_type())
      {
        ObString str;
        int tmp_ret = v.get_varchar(str);
        if (OB_SUCCESS == tmp_ret
            && DEFAULT_VALUE_STR_BUFFER_SIZE > snprintf(buffer, DEFAULT_VALUE_STR_BUFFER_SIZE, "%.*s", str.length(), str.ptr()))
        {
          ret = buffer;
        }
        else
        {
          TBSYS_LOG(WARN, "maybe varchar too long, length=%d", str.length());
        }
      }
      else if (OB_SUCCESS == obj_cast(const_cast<ObObj&>(v), ObVarcharType, buffer, DEFAULT_VALUE_STR_BUFFER_SIZE - 1, ret_size))
      {
        buffer[ret_size] = '\0';
        ret = buffer;
      }
      return ret;
    }
#endif

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObLogDenseFormator::ObLogDenseFormator() : mod_(ObModIds::OB_LOG_BINLOG_RECORD),
                                               allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                                               inited_(false),
                                               need_query_back_(true),
                                               schema_getter_(NULL),
                                               meta_manager_(NULL),
                                               log_filter_(NULL),
                                               p_list_(),
                                               v_list_(),
                                               p_cond_(COND_SPIN_WAIT_NUM),
                                               v_cond_(COND_SPIN_WAIT_NUM),
                                               mysql_adaptor_(),
                                               br_list_iter_(NULL)
    {
      ObObj obj;
      obj.set_null();
      null_str_ = value2str_(obj);
    }

    ObLogDenseFormator::~ObLogDenseFormator()
    {
      destroy();
    }

    int ObLogDenseFormator::init(const ObLogConfig &config,
        IObLogSchemaGetter *schema_getter,
        IObLogMetaManager *meta_manager,
        IObLogFilter *log_filter)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (schema_getter_ = schema_getter)
              || NULL == (meta_manager_ = meta_manager)
              || NULL == (log_filter_ = log_filter))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_binlogrecord_list_(MAX_BINLOG_RECORD_NUM)))
      {
        TBSYS_LOG(WARN, "prepare binglog record list fail, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = mysql_adaptor_.init(config, schema_getter)))
      {
        TBSYS_LOG(WARN, "mysql adaptor init fail, ret=%d", ret);
      }
      else
      {
        need_query_back_ = (0 != config.get_query_back());
        inited_ = true;
      }
      return ret;
    }

    void ObLogDenseFormator::destroy()
    {
      inited_ = false;
      need_query_back_ = true;
      br_list_iter_ = NULL;
      mysql_adaptor_.destroy();
      destroy_binlogrecord_list_();
      meta_manager_ = NULL;
      schema_getter_ = NULL;
      allocator_.reuse();
    }

    int ObLogDenseFormator::next_record(IBinlogRecord **record, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if (NULL == record)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL != br_list_iter_)
      {
        *record = br_list_iter_;
        br_list_iter_ = br_list_iter_->get_next();
      }
      else
      {
        ObLogBinlogRecord *binlog_record = NULL;
        int64_t start_time = tbsys::CTimeUtil::getTime();
        while (true)
        {
          ret = p_list_.pop(binlog_record);
          if (OB_SUCCESS == ret)
          {
            break;
          }
          int64_t wait_time = timeout - (tbsys::CTimeUtil::getTime() - start_time);
          if (0 >= wait_time)
          {
            break;
          }
          p_cond_.timedwait(timeout);
        }
        if (OB_SUCCESS != ret)
        {
          ret = OB_PROCESS_TIMEOUT;
        }
        else
        {
          if (NULL == binlog_record)
          {
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            *record = binlog_record;
            br_list_iter_ = binlog_record->get_next();
          }
        }
      }
      return ret;
    }

    void ObLogDenseFormator::release_record(IBinlogRecord *record)
    {
      ObLogBinlogRecord *dense_br = dynamic_cast<ObLogBinlogRecord*>(record);
      if (NULL != dense_br)
      {
        std::vector<std::string *>::const_iterator iter = dense_br->newCols().begin();
        for (; iter != dense_br->newCols().end(); iter++)
        {
          if (NULL != *iter)
          {
            string_pool_.free((ObLogString*)(*iter));
          }
        }
        iter = dense_br->oldCols().begin();
        for (; iter != dense_br->oldCols().end(); iter++)
        {
          if (NULL != *iter)
          {
            string_pool_.free((ObLogString*)(*iter));
          }
        }
        dense_br->clear();

        int ret = v_list_.push(dense_br);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push dense_br to v_list fail, ret=%d", ret);
        }
        else
        {
          v_cond_.signal();
        }
      }
      if (REACH_TIME_INTERVAL(PRINT_BR_USAGE_INTERVAL))
      {
        TBSYS_LOG(INFO, "free_num=%ld filled_num=%ld", v_list_.get_total(), p_list_.get_total());
      }
    }

    ObLogBinlogRecord *ObLogDenseFormator::fetch_binlog_record_(volatile bool &loop_stop)
    {
      ObLogBinlogRecord *binlog_record = NULL;
      while (NULL == binlog_record
            && !loop_stop)
      {
        int tmp_ret = v_list_.pop(binlog_record);
        if (OB_SUCCESS != tmp_ret)
        {
          v_cond_.timedwait(WAIT_VLIST_TIMEOUT);
        }
        else
        {
          break;
        }
      }
      if (loop_stop
          && NULL != binlog_record)
      {
        v_list_.push(binlog_record);
        binlog_record = NULL;
      }
      return binlog_record;
    }

    int ObLogDenseFormator::fill_trans_barrier_(const RecordType type,
        const int64_t timestamp,
        const uint64_t checkpoint,
        const uint64_t mutator_num,
        ObLogBinlogRecord *&br_list_tail,
        volatile bool &loop_stop)
    {
      int ret = OB_SUCCESS;
      ObLogBinlogRecord *binlog_record = fetch_binlog_record_(loop_stop);
      if (NULL != binlog_record)
      {
        binlog_record->setRecordType(type);
        binlog_record->setTimestamp(timestamp/1000000);
        binlog_record->setCheckpoint(
            (uint32_t)((checkpoint & 0xffffffff00000000) >> 32),
            (uint32_t)(checkpoint & 0x00000000ffffffff));
        binlog_record->set_mutator_num(mutator_num);

        binlog_record->set_next(NULL);
        if (NULL != br_list_tail)
        {
          br_list_tail->set_next(binlog_record);
        }
        br_list_tail = binlog_record;

        binlog_record = NULL;
      }
      else if (!loop_stop)
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = OB_IN_STOP_STATE;
      }
      return ret;
    }

    int ObLogDenseFormator::add_mutator(ObLogMutator &mutator,
        const uint64_t db_partition,
        const uint64_t tb_partition,
        volatile bool &loop_stop)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }

      ObLogBinlogRecord *br_list_head = NULL;
      ObLogBinlogRecord *br_list_tail = NULL;
      const int64_t timestamp = mutator.get_mutate_timestamp();
      const uint64_t checkpoint = mutator.get_log_id();
      const uint64_t mutator_num = mutator.get_num();
      if (OB_SUCCESS == ret)
      {
        // Transaction Begin
        ret = fill_trans_barrier_(EBEGIN, timestamp, checkpoint, mutator_num, br_list_tail, loop_stop);
        br_list_head = br_list_tail;
      }

      // Transaction Statement
      ObLogBinlogRecord *binlog_record = NULL;
      while (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator.get_mutator().next_cell()))
      {
        ObMutatorCellInfo *cell = NULL;
        bool irc = false;
        bool irf = false;
        ObDmlType dml_type = OB_DML_UNKNOW;
        if (OB_SUCCESS != (ret = mutator.get_mutator().get_cell(&cell, &irc, &irf, &dml_type)))
        {
          TBSYS_LOG(WARN, "mutator get_cell fail, ret=%d", ret);
          break;
        }
        if (NULL == cell)
        {
          TBSYS_LOG(WARN, "unexpected error cell is null pointer");
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        if (!log_filter_->contain(cell->cell_info.table_id_))
        {
          continue;
        }

        if (NULL == binlog_record
            && NULL == (binlog_record = fetch_binlog_record_(loop_stop)))
        {
          if (!loop_stop)
          {
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            ret = OB_IN_STOP_STATE;
          }
          break;
        }

        // Notice: will modify dml_type to OB_DML_DELETE while query back empty values
        if (OB_SUCCESS != (ret = fill_binlog_record_(*binlog_record,
                irc,
                &dml_type,
                timestamp,
                checkpoint,
                mutator_num,
                cell->cell_info,
                db_partition,
                tb_partition)))
        {
          TBSYS_LOG(WARN, "fill_binlog_record fail, %s dml_type=%s ret=%d",
              print_cellinfo(&(cell->cell_info)), str_dml_type(dml_type), ret);
          break;
        }

        if (OB_DML_DELETE == dml_type)
        {
          if (!need_query_back_
              && !irf)
          {
            TBSYS_LOG(WARN, "unexpected, delete_row cell is not the last cell of row");
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          if (OB_SUCCESS != (ret = fill_binlog_record_(*binlog_record, mutator, true)))
          {
            TBSYS_LOG(WARN, "fill_binlog_record fail, %s dml_type=%s ret=%d",
                print_cellinfo(&(cell->cell_info)), str_dml_type(dml_type), ret);
            break;
          }
        }
        else if (!need_query_back_
            && irc)
        {
          // handle whole row
          if (OB_SUCCESS != (ret = fill_binlog_record_(*binlog_record, mutator, false)))
          {
            TBSYS_LOG(WARN, "fill_binlog_record fail, %s dml_type=%s ret=%d",
                print_cellinfo(&(cell->cell_info)), str_dml_type(dml_type), ret);
            break;
          }
          else
          {
            irf = true;
          }
        }

        if (irf
            && OB_DML_UPDATE == dml_type)
        {
          if (OB_SUCCESS != (ret = fill_old_value_(*binlog_record, cell->cell_info.table_id_)))
          {
            TBSYS_LOG(WARN, "fill_old_value fail, %s", print_cellinfo(&(cell->cell_info)));
            break;
          }
        }

        if (irf)
        {
          binlog_record->set_next(NULL);
          if (NULL != br_list_tail)
          {
            br_list_tail->set_next(binlog_record);
          }
          br_list_tail = binlog_record;
          binlog_record = NULL;
        }
      }
      mutator.reset_iter();

      if (OB_ITER_END == ret)
      {
        // Transaction Commit
        ret = fill_trans_barrier_(ECOMMIT, timestamp, checkpoint, mutator_num, br_list_tail, loop_stop);
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = p_list_.push(br_list_head)))
          {
            TBSYS_LOG(WARN, "push to p_list fail, ret=%d", ret);
          }
          else
          {
            p_cond_.signal();
          }
        }
      }
      return ret;
    }

    int ObLogDenseFormator::init_binlogrecord_list_(const int64_t max_binlogrecord_num)
    {
      int ret = OB_SUCCESS;
      ret = p_list_.init(max_binlogrecord_num);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "init binlogrecord p_list fail, max_binlogrecord_num=%ld", max_binlogrecord_num);
      }
      if (OB_SUCCESS == ret)
      {
        ret = v_list_.init(max_binlogrecord_num);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init binlogrecord v_list fail, max_binlogrecord_num=%ld", max_binlogrecord_num);
        }
      }
      for (int64_t i = 0; OB_SUCCESS == ret && i < max_binlogrecord_num; i++)
      {
        void *buffer = allocator_.alloc(sizeof(ObLogBinlogRecord));
        if (NULL == buffer)
        {
          TBSYS_LOG(WARN, "allocator memory to build binglog_record fail, i=%ld", i);
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }

        ObLogBinlogRecord *binlog_record = new(buffer) ObLogBinlogRecord();
        if (NULL == binlog_record)
        {
          TBSYS_LOG(WARN, "build binlog_record fail, i=%ld", i);
          ret = OB_ERR_UNEXPECTED;
          break;
        }

        ret = v_list_.push(binlog_record);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "push binlog_record to list fail, i=%ld", i);
          break;
        }
      }
      return ret;
    }

    void ObLogDenseFormator::destroy_binlogrecord_list_()
    {
      int64_t counter = v_list_.get_total() + v_list_.get_free();
      ObLogBinlogRecord *binlog_record = NULL;
      while (OB_SUCCESS == p_list_.pop(binlog_record))
      {
        if (NULL != binlog_record)
        {
          release_record(binlog_record);
          binlog_record = NULL;
          counter--;
        }
      }
      p_list_.destroy();

      while (OB_SUCCESS == v_list_.pop(binlog_record))
      {
        if (NULL != binlog_record)
        {
          binlog_record->~ObLogBinlogRecord();
          binlog_record = NULL;
          counter--;
        }
      }
      v_list_.destroy();

      if (0 != counter)
      {
        TBSYS_LOG(WARN, "still have binlog_record not release, counter=%ld", counter);
      }
    }

    int ObLogDenseFormator::fill_binlog_record_(ObLogBinlogRecord &binlog_record,
        const bool irc,
        ObDmlType *dml_type,
        const int64_t timestamp,
        const uint64_t checkpoint,
        const uint64_t mutator_num,
        const ObCellInfo &cell,
        const uint64_t db_partition,
        const uint64_t tb_partition)
    {
      int ret = OB_SUCCESS;

      if (!irc)
      {
        return ret;
      }

      if (OB_SUCCESS == ret)
      {
        ITableMeta *table_meta = NULL;
        if (NULL == (table_meta = meta_manager_->get_table_meta(cell.table_id_, db_partition, tb_partition)))
        {
          TBSYS_LOG(WARN, "get table meta from meta_manager fail");
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          // Oceanbase always set true
          binlog_record.setLastInLogevent(true);
          binlog_record.setDbname(table_meta->getDBMeta()->getName());
          binlog_record.setTbname(table_meta->getName());
          binlog_record.setTableMeta(table_meta);
          binlog_record.setTimestamp(timestamp/1000000);
          binlog_record.setCheckpoint(
              (uint32_t)((checkpoint & 0xffffffff00000000) >> 32),
              (uint32_t)(checkpoint & 0x00000000ffffffff));
          binlog_record.set_mutator_num(mutator_num);
        }
      }

      if (OB_SUCCESS == ret)
      {
        switch (*dml_type)
        {
          case OB_DML_REPLACE:
            binlog_record.setRecordType(EREPLACE);
            break;
          case OB_DML_INSERT:
            binlog_record.setRecordType(EINSERT);
            break;
          case OB_DML_UPDATE:
            binlog_record.setRecordType(EUPDATE);
            break;
          case OB_DML_DELETE:
            binlog_record.setRecordType(EDELETE);
            break;
          default:
            TBSYS_LOG(WARN, "invalid dml_type=%d, %s", *dml_type, print_cellinfo(&cell));
            ret = OB_ERR_UNEXPECTED;
            break;
        }
      }

      while (OB_SUCCESS == ret)
      {
        if (need_query_back_
            && (OB_DML_REPLACE == *dml_type
                || OB_DML_INSERT == *dml_type
                || OB_DML_UPDATE == *dml_type))
        {
          const IObLogColValue *list = NULL;
          if (OB_SUCCESS != (ret = mysql_adaptor_.query_whole_row(cell.table_id_, cell.row_key_, list))
              || NULL == list)
          {
            if (OB_ENTRY_NOT_EXIST == ret)
            {
              binlog_record.setRecordType(EDELETE);
              *dml_type = OB_DML_DELETE;
              ret = OB_SUCCESS;
              continue;
            }
            TBSYS_LOG(WARN, "mysql_adaptor query_whole_row fail, ret=%d", ret);
            ret = (OB_SUCCESS == ret) ? OB_ERR_UNEXPECTED : ret;
          }
          else
          {
            const IObLogColValue *cv_iter = list;
            while (NULL != cv_iter)
            {
              if (cv_iter->isnull())
              {
                binlog_record.putNew(NULL);
                cv_iter = cv_iter->get_next();
                continue;
              }
              std::string *cv_str = NULL;
              if (NULL == (cv_str = (std::string*)string_pool_.alloc()))
              {
                TBSYS_LOG(WARN, "construct std::string fail, cv_iter=%p", cv_iter);
                ret = OB_ALLOCATE_MEMORY_FAILED;
                break;
              }
              cv_iter->to_str(*cv_str);
              binlog_record.putNew(cv_str);
              cv_iter = cv_iter->get_next();
            }
          }
        }
        //else if (OB_DML_DELETE == dml_type)
        //{
        //  for (int64_t i = 0; i < cell.row_key_.get_obj_cnt(); i++)
        //  {
        //    const char *pk = value2str_(cell.row_key_.get_obj_ptr()[i]);
        //    if (NULL == pk)
        //    {
        //      TBSYS_LOG(WARN, "trans rowkey column value to string fail, index=%ld %s", i, to_cstring(cell.row_key_));
        //      ret = OB_ERR_UNEXPECTED;
        //      break;
        //    }
        //    std::string *pkstr = NULL;
        //    if (NULL == (pkstr = (std::string*)string_pool_.alloc()))
        //    {
        //      TBSYS_LOG(WARN, "construct std::string fail, pk=%s", pk);
        //      ret = OB_ALLOCATE_MEMORY_FAILED;
        //      break;
        //    }
        //    *pkstr = pk;
        //    binlog_record.putOld(pkstr);
        //  }
        //}
        else
        {
          // do nothing
        }
        break;
      }

      return ret;
    }

    const char *ObLogDenseFormator::value2str_(const ObObj &v)
    {
      static const int64_t DEFAULT_VALUE_STR_BUFFER_SIZE = 65536;
      static __thread char buffer[DEFAULT_VALUE_STR_BUFFER_SIZE];
      const char *ret = NULL;
      int64_t ret_size = 0;
      if (ObVarcharType == v.get_type())
      {
        ObString str;
        int tmp_ret = v.get_varchar(str);
        if (OB_SUCCESS == tmp_ret
            && DEFAULT_VALUE_STR_BUFFER_SIZE > snprintf(buffer, DEFAULT_VALUE_STR_BUFFER_SIZE, "%.*s", str.length(), str.ptr()))
        {
          ret = buffer;
        }
        else
        {
          TBSYS_LOG(WARN, "maybe varchar too long, length=%d", str.length());
        }
      }
      else if (OB_SUCCESS == obj_cast(const_cast<ObObj&>(v), ObVarcharType, buffer, DEFAULT_VALUE_STR_BUFFER_SIZE - 1, ret_size))
      {
        buffer[ret_size] = '\0';
        ret = buffer;
      }
      return ret;
    }

    int ObLogDenseFormator::fill_old_value_(ObLogBinlogRecord &binlog_record, const uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      const ObLogSchema *total_schema = NULL;
      int32_t column_count = 0;
      if (NULL == (total_schema = schema_getter_->get_schema()))
      {
        TBSYS_LOG(WARN, "get schema from schema_getter fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == total_schema->get_table_schema(table_id, column_count)
              && 0 >= column_count)
      {
        TBSYS_LOG(WARN, "get_table_schema fail, table_id=%lu column_count=%d", table_id, column_count);
        ret = OB_ERR_UNEXPECTED;
      }
      for (int32_t i = 0; OB_SUCCESS == ret && i < column_count; i++)
      {
        //std::string *null_str = NULL;
        //if (NULL == (null_str = (std::string*)string_pool_.alloc()))
        //{
        //  TBSYS_LOG(WARN, "construct std::string fail");
        //  ret = OB_ALLOCATE_MEMORY_FAILED;
        //}
        //else
        //{
        //  *null_str = null_str_;
        //  binlog_record.putOld(null_str);
        //}
        binlog_record.putOld(NULL);
      }
      if (NULL != total_schema)
      {
        schema_getter_->revert_schema(total_schema);
        total_schema = NULL;
      }
      return ret;
    }

    int ObLogDenseFormator::fill_binlog_record_(ObLogBinlogRecord &binlog_record,
        ObLogMutator &mutator,
        const bool only_rowkey)
    {
      int ret = OB_SUCCESS;

      const ObLogSchema *total_schema = NULL;
      RowValue *row_value = NULL;
      if (NULL == (total_schema = schema_getter_->get_schema()))
      {
        TBSYS_LOG(WARN, "get schema from schema_getter fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (row_value = row_value_pool_.alloc()))
      {
        TBSYS_LOG(WARN, "alloc from row_value_pool fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        row_value->reset();

        common::ObDmlType dml_type = OB_DML_UNKNOW;
        const ObColumnSchemaV2 *column_schema = NULL;
        int32_t column_num = 0;
        do
        {
          ObMutatorCellInfo *cell = NULL;
          bool irc = false;
          bool irf = false;
          if (OB_SUCCESS != (ret = mutator.get_mutator().get_cell(&cell, &irc, &irf, &dml_type)))
          {
            TBSYS_LOG(WARN, "mutator get_cell fail, ret=%d", ret);
            break;
          }
          if (NULL == cell)
          {
            TBSYS_LOG(WARN, "unexpected error cell is null pointer");
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          if (irc)
          {
            if (NULL == (column_schema = total_schema->get_table_schema(cell->cell_info.table_id_, column_num)))
            {
              TBSYS_LOG(WARN, "get_table_schema fail, table_id=%lu", cell->cell_info.table_id_);
              ret = OB_ERR_UNEXPECTED;
              break;
            }
            memset(row_value->columns, 0, sizeof(std::string*) * column_num);

            ret = fill_row_value_(row_value, total_schema, cell->cell_info.table_id_, cell->cell_info.row_key_);
          }

          if (only_rowkey)
          {
            break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = fill_row_value_(row_value, total_schema, cell->cell_info);
          }

          if (irf)
          {
            break;
          }
        }
        while (OB_SUCCESS == ret
              && OB_SUCCESS == (ret = mutator.get_mutator().next_cell()));

        if (row_value->num != column_num
            && !only_rowkey
            && OB_DML_DELETE != dml_type
            && OB_DML_INSERT != dml_type
            && OB_DML_REPLACE != dml_type)
        {
          TBSYS_LOG(WARN, "unexpected, column values from mutator not dense, num=%d schema_num=%d",
              row_value->num, column_num);
          ret = OB_ERR_UNEXPECTED;
        }

        for (int32_t i = 0; OB_SUCCESS == ret && i < column_num; i++)
        {
          if (NULL != row_value->columns[i])
          {
            if (!only_rowkey)
            {
              binlog_record.putNew(row_value->columns[i]);
            }
            else
            {
              binlog_record.putOld(row_value->columns[i]);
            }
          }
          else
          {
            //const char *default_vstr = NULL;
            //std::string *default_vstr_str = NULL;
            //if (NULL == (default_vstr = value2str_(column_schema[i].get_default_value())))
            //{
            //  TBSYS_LOG(WARN, "trans value to string fail, %s", print_obj(column_schema[i].get_default_value()));
            //  ret = OB_ERR_UNEXPECTED;
            //}
            //else if (NULL == (default_vstr_str = (std::string*)string_pool_.alloc()))
            //{
            //  TBSYS_LOG(WARN, "construct std::string fail");
            //  ret = OB_ALLOCATE_MEMORY_FAILED;
            //}
            //else
            //{
            //  *default_vstr_str = default_vstr;
            //  binlog_record.putNew(default_vstr_str);
            //}
            if (!only_rowkey)
            {
              binlog_record.putNew(NULL);
            }
            else
            {
              binlog_record.putOld(NULL);
            }
          }
        }
      }

      if (NULL != row_value)
      {
        row_value_pool_.free(row_value);
      }
      if (NULL != total_schema)
      {
        schema_getter_->revert_schema(total_schema);
        total_schema = NULL;
      }
      return ret;
    }

    int ObLogDenseFormator::fill_row_value_(RowValue *row_value,
        const ObLogSchema *total_schema,
        const uint64_t table_id,
        const ObRowkey &rowkey)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema *table_schema = total_schema->get_table_schema(table_id);
      if (NULL == table_schema)
      {
        TBSYS_LOG(WARN, "get table schema fail, table_id=%lu", table_id);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        for (int64_t i = 0; i < rowkey.get_obj_cnt() && OB_SUCCESS == ret; i++)
        {
          int32_t idx = 0;
          const char *vstr = NULL;
          std::string *vstr_str = NULL;
          uint64_t column_id = table_schema->get_rowkey_info().get_column(i)->column_id_;
          const ObObj &obj = rowkey.get_obj_ptr()[i];
          if (obj.is_null())
          {
            // skip
          }
          else if (NULL == total_schema->get_column_schema(table_id, column_id, &idx)
              || 0 > idx || OB_MAX_COLUMN_NUMBER <= idx)
          {
            TBSYS_LOG(WARN, "get column_schema fail, table_id=%lu column_id=%lu idx=%d",
                table_id, column_id, idx);
            ret = OB_ERR_UNEXPECTED;
          }
          else if (NULL == (vstr = value2str_(obj)))
          {
            TBSYS_LOG(WARN, "trans value to string fail, table_id=%lu column_id=%lu %s",
                table_id, column_id, print_obj(obj));
            ret = OB_ERR_UNEXPECTED;
          }
          else if (NULL == (vstr_str = (std::string*)string_pool_.alloc()))
          {
            TBSYS_LOG(WARN, "construct std::string fail");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            *vstr_str = vstr;
            row_value->columns[idx] = vstr_str;
            row_value->num += 1;
          }
        }
      }
      return ret;
    }

    int ObLogDenseFormator::fill_row_value_(RowValue *row_value,
        const ObLogSchema *total_schema,
        const ObCellInfo &cell)
    {
      int ret = OB_SUCCESS;
      int32_t idx = 0;
      const char *vstr = NULL;
      std::string *vstr_str = NULL;
      if (cell.value_.is_null())
      {
        // skip
      }
      else if (NULL == total_schema->get_column_schema(cell.table_id_, cell.column_id_, &idx)
          || 0 > idx || OB_MAX_COLUMN_NUMBER <= idx)
      {
        TBSYS_LOG(WARN, "get column_schema fail, table_id=%lu column_id=%lu idx=%d",
            cell.table_id_, cell.column_id_, idx);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (vstr = value2str_(cell.value_)))
      {
        TBSYS_LOG(WARN, "trans value to string fail, %s", print_cellinfo(&cell));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (vstr_str = (std::string*)string_pool_.alloc()))
      {
        TBSYS_LOG(WARN, "construct std::string fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        *vstr_str = vstr;
        row_value->columns[idx] = vstr_str;
        row_value->num += 1;
      }
      return ret;
    }

  }
}

