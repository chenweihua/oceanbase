////===================================================================
 //
 // ob_log_meta_manager.cpp liboblog / Oceanbase
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

#include "common/ob_define.h"
#include "common/ob_mod_define.h"
#include "common/ob_obj_cast.h"
#include "ob_log_meta_manager.h"

namespace oceanbase
{
  using namespace common;
  namespace liboblog
  {
    ObLogDBNameBuilder::ObLogDBNameBuilder() : inited_(false),
                                               allocator_(),
                                               name_map_()
    {
    }

    ObLogDBNameBuilder::~ObLogDBNameBuilder()
    {
      destroy();
    }

    int ObLogDBNameBuilder::init(const ObLogConfig &config)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 != name_map_.create(NAME_MAP_SIZE))
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = ObLogConfig::parse_tb_select(config.get_tb_select(), *this, &config);
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

    int ObLogDBNameBuilder::operator ()(const char *tb_name, const ObLogConfig *config)
    {
      int ret = OB_SUCCESS;

      std::string dbn_format = config->get_dbn_format(tb_name);
      char *tb_name_cstr = NULL;
      char *dbn_format_cstr = NULL;
      if (NULL == (tb_name_cstr = allocator_.alloc(strlen(tb_name) + 1))
          || NULL == (dbn_format_cstr = allocator_.alloc(dbn_format.size() + 1)))
      {
        TBSYS_LOG(WARN, "allocate tb_name_cstr or dbn_format_cstr fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        sprintf(tb_name_cstr, "%s", tb_name);
        sprintf(dbn_format_cstr, "%s", dbn_format.c_str());
        int hash_ret = name_map_.set(tb_name_cstr, dbn_format_cstr);
        if (hash::HASH_INSERT_SUCC != hash_ret
            && hash::HASH_EXIST != hash_ret)
        {
          TBSYS_LOG(WARN, "insert into name map fail, hash_ret=%d key=%s value=%s",
                    hash_ret, tb_name_cstr, dbn_format_cstr);
          ret = OB_ERR_UNEXPECTED;
        }
      }

      return ret;
    }

    void ObLogDBNameBuilder::destroy()
    {
      inited_ = false;
      name_map_.destroy();
      allocator_.reuse();
    }

    int ObLogDBNameBuilder::get_db_name(const char *src_name,
        const uint64_t db_partition,
        char *dest_name,
        const int64_t dest_buffer_size)
    {
      int ret = OB_SUCCESS;
      const char *format = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (hash::HASH_EXIST != name_map_.get(src_name, format)
              || NULL == format)
      {
        TBSYS_LOG(WARN, "get from name map fail, tb_name=%s", src_name);
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (dest_buffer_size <= snprintf(dest_name, dest_buffer_size, format, db_partition))
      {
        TBSYS_LOG(WARN, "buffer size not enough, partition=%lu", db_partition);
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObLogTBNameBuilder::ObLogTBNameBuilder() : inited_(false),
                                                     allocator_(),
                                                     name_map_()
    {
    }

    ObLogTBNameBuilder::~ObLogTBNameBuilder()
    {
      destroy();
    }

    int ObLogTBNameBuilder::init(const ObLogConfig &config)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 != name_map_.create(NAME_MAP_SIZE))
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = ObLogConfig::parse_tb_select(config.get_tb_select(), *this, &config);
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

    int ObLogTBNameBuilder::operator ()(const char *tb_name, const ObLogConfig *config)
    {
      int ret = OB_SUCCESS;

      std::string tbn_format = config->get_tbn_format(tb_name);
      char *tb_name_cstr = NULL;
      char *tbn_format_cstr = NULL;
      if (NULL == (tb_name_cstr = allocator_.alloc(strlen(tb_name) + 1))
          || NULL == (tbn_format_cstr = allocator_.alloc(tbn_format.size() + 1)))
      {
        TBSYS_LOG(WARN, "allocate tb_name_cstr or tbn_format_cstr fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (OB_SUCCESS == ret)
      {
        sprintf(tb_name_cstr, "%s", tb_name);
        sprintf(tbn_format_cstr, "%s", tbn_format.c_str());
        int hash_ret = name_map_.set(tb_name_cstr, tbn_format_cstr);
        if (hash::HASH_INSERT_SUCC != hash_ret
            && hash::HASH_EXIST != hash_ret)
        {
          TBSYS_LOG(WARN, "insert into name map fail, hash_ret=%d key=%s value=%s",
                    hash_ret, tb_name_cstr, tbn_format_cstr);
          ret = OB_ERR_UNEXPECTED;
        }
      }

      return ret;
    }

    void ObLogTBNameBuilder::destroy()
    {
      inited_ = false;
      name_map_.destroy();
      allocator_.reuse();
    }

    int ObLogTBNameBuilder::get_tb_name(const char *src_name,
        const uint64_t tb_partition,
        char *dest_name,
        const int64_t dest_buffer_size)
    {
      int ret = OB_SUCCESS;
      const char *format = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (hash::HASH_EXIST != name_map_.get(src_name, format)
              || NULL == format)
      {
        TBSYS_LOG(WARN, "get from name map fail, tb_name=%s", src_name);
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (dest_buffer_size <= snprintf(dest_name, dest_buffer_size, format, tb_partition))
      {
        TBSYS_LOG(WARN, "buffer size not enough, partition=%lu", tb_partition);
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObLogSchemaGetter::ObLogSchemaGetter() : inited_(false),
                                             server_selector_(NULL),
                                             rpc_stub_(NULL),
                                             schema_refresh_lock_(),
                                             cur_schema_(NULL)
    {
    }

    ObLogSchemaGetter::~ObLogSchemaGetter()
    {
      destroy();
    }

    int ObLogSchemaGetter::init(IObLogServerSelector *server_selector, IObLogRpcStub *rpc_stub)
    {
      int ret = OB_SUCCESS;
      ObServer rs_addr;
      bool change_rs_addr = false;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (server_selector_ = server_selector)
              || NULL == (rpc_stub_ = rpc_stub))
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (cur_schema_ = new ObLogSchema()))
      {
        TBSYS_LOG(WARN, "construct schema fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = server_selector_->get_rs(rs_addr, change_rs_addr)))
      {
        TBSYS_LOG(WARN, "get rs addr failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = rpc_stub_->fetch_schema(rs_addr, *cur_schema_, FETCH_SCHEMA_TIMEOUT)))
      {
        TBSYS_LOG(WARN, "fetch schema fail ret=%d", ret);
      }
      else
      {
        cur_schema_->ref();
        inited_ = true;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogSchemaGetter::destroy()
    {
      inited_ = false;
      if (NULL != cur_schema_)
      {
        delete cur_schema_;
        cur_schema_ = NULL;
      }
      rpc_stub_ = NULL;
      server_selector_ = NULL;
    }

    const ObLogSchema *ObLogSchemaGetter::get_schema()
    {
      ObLogSchema *ret = NULL;
      if (inited_)
      {
        SpinRLockGuard guard(schema_refresh_lock_);
        ret = cur_schema_;
        ret->ref();
      }
      return ret;
    }

    void ObLogSchemaGetter::revert_schema(const ObLogSchema *schema)
    {
      ObLogSchema *ret = const_cast<ObLogSchema*>(schema);
      if (NULL != ret)
      {
        SpinRLockGuard guard(schema_refresh_lock_);
        if (0 == ret->deref())
        {
          delete ret;
          ret = NULL;
        }
      }
    }

    void ObLogSchemaGetter::refresh()
    {
      if (inited_)
      {
        if (REACH_TIME_INTERVAL(RS_ADDR_REFRESH_INTERVAL))
        {
          server_selector_->refresh();
        }
        ObServer rs_addr;
        bool change_rs_addr = false;
        int tmp_ret = OB_SUCCESS;
        ObLogSchema *next_schema = NULL;
        if (NULL == (next_schema = new ObLogSchema()))
        {
          TBSYS_LOG(WARN, "construct schema fail");
        }
        else if (OB_SUCCESS != (tmp_ret = server_selector_->get_rs(rs_addr, change_rs_addr)))
        {
          TBSYS_LOG(WARN, "get rs addr failed, ret=%d", tmp_ret);
        }
        else if (OB_SUCCESS != (tmp_ret = rpc_stub_->fetch_schema(rs_addr, *next_schema, FETCH_SCHEMA_TIMEOUT)))
        {
          TBSYS_LOG(WARN, "fetch schema fail ret=%d", tmp_ret);
        }
        else if (next_schema->get_version() <= cur_schema_->get_version())
        {
          TBSYS_LOG(INFO, "need not update schema version [%ld:%ld]", next_schema->get_version(), cur_schema_->get_version());
          delete next_schema;
          next_schema = NULL;
        }
        else
        {
          next_schema->ref();
          SpinWLockGuard guard(schema_refresh_lock_);
          ObLogSchema *prev_schema = cur_schema_;
          cur_schema_ = next_schema;
          if (0 == prev_schema->deref())
          {
            delete prev_schema;
            prev_schema = NULL;
          }
        }
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObLogMetaManager::ObLogMetaManager() : mod_(ObModIds::OB_SCHEMA),
                                           allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                                           db_meta_list_(),
                                           tb_meta_list_(),
                                           meta_collect_(),
                                           inited_(false),
                                           schema_getter_(NULL),
                                           db_name_builder_(NULL),
                                           tb_name_builder_(NULL),
                                           table_meta_lock_(),
                                           table_meta_map_()
    {
    }

    ObLogMetaManager::~ObLogMetaManager()
    {
      destroy();
    }

    int ObLogMetaManager::init(IObLogSchemaGetter *schema_getter,
        IObLogDBNameBuilder *db_name_builder,
        IObLogTBNameBuilder *tb_name_builder)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (schema_getter_ = schema_getter)
              || NULL == (db_name_builder_ = db_name_builder)
              || NULL == (tb_name_builder_ = tb_name_builder))
      {
        TBSYS_LOG(WARN, "invalid param, schema_getter=%p db_name_builder=%p tb_name_builder=%p",
                  schema_getter, db_name_builder, tb_name_builder);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != table_meta_map_.create(MAX_PARTITION_NUM))
      {
        TBSYS_LOG(WARN, "init table_meta_map fail");
        ret = OB_ERR_UNEXPECTED;
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

    void ObLogMetaManager::destroy()
    {
      inited_ = false;
      table_meta_map_.destroy();
      tb_name_builder_ = NULL;
      db_name_builder_ = NULL;
      schema_getter_ = NULL;

      meta_collect_.~MetaDataCollections();
      new(&meta_collect_) MetaDataCollections();

      //ObList<ITableMeta*>::iterator table_meta_iter;
      //for (table_meta_iter = tb_meta_list_.begin(); table_meta_iter != tb_meta_list_.end(); table_meta_iter++)
      //{
      //  if (NULL != (*table_meta_iter))
      //  {
      //    (*table_meta_iter)->~ITableMeta();
      //  }
      //}
      //tb_meta_list_.clear();

      //ObList<IDBMeta*>::iterator db_meta_iter;
      //for (db_meta_iter = db_meta_list_.begin(); db_meta_iter != db_meta_list_.end(); db_meta_iter++)
      //{
      //  if (NULL != (*db_meta_iter))
      //  {
      //    (*db_meta_iter)->~IDBMeta();
      //  }
      //}
      //db_meta_list_.clear();

      allocator_.reuse();
    }

    ITableMeta *ObLogMetaManager::get_table_meta(const uint64_t table_id, const uint64_t db_partition, const uint64_t tb_partition)
    {
      ITableMeta *ret = NULL;
      TableMetaKey table_meta_key(table_id, db_partition, tb_partition);
      int hash_ret = 0;
      if (!inited_)
      {
        // not inited
      }
      //TODO support add new column
      else if (hash::HASH_EXIST == (hash_ret = table_meta_map_.get(table_meta_key, ret)))
      {
        if (NULL == ret)
        {
          TBSYS_LOG(WARN, "unexpected error, %s", table_meta_key.to_cstring());
        }
      }
      else if (hash::HASH_NOT_EXIST != hash_ret)
      {
        TBSYS_LOG(WARN, "unexpected error, hash_ret=%d", hash_ret);
      }
      else
      {
        ObSpinLockGuard guard(table_meta_lock_);
        ITableMeta *tmp_table_meta = NULL;
        if (hash::HASH_EXIST == (hash_ret = table_meta_map_.get(table_meta_key, tmp_table_meta)))
        {
          if (NULL == tmp_table_meta)
          {
            TBSYS_LOG(WARN, "unexpected error, %s", table_meta_key.to_cstring());
          }
        }
        else if (hash::HASH_NOT_EXIST != hash_ret)
        {
          TBSYS_LOG(WARN, "unexpected error, hash_ret=%d", hash_ret);
        }
        else if (NULL == (tmp_table_meta = build_table_meta_(table_id, db_partition, tb_partition)))
        {
          TBSYS_LOG(WARN, "build table_meta fail, %s", table_meta_key.to_cstring());
        }
        else if (hash::HASH_INSERT_SUCC != (hash_ret = table_meta_map_.set(table_meta_key, tmp_table_meta)))
        {
          TBSYS_LOG(WARN, "add table_meta fail, hash_ret=%d %s", hash_ret, table_meta_key.to_cstring());
        }
        else
        {
          ret = tmp_table_meta;
          TBSYS_LOG(INFO, "build and add table_meta succ, %s table_meta=%p", table_meta_key.to_cstring(), ret);
        }
      }
      return ret;
    }

    ITableMeta *ObLogMetaManager::build_table_meta_(const uint64_t table_id, const uint64_t db_partition, const uint64_t tb_partition)
    {
      ITableMeta *ret = NULL;

      const ObLogSchema *total_schema = NULL;;
      const ObTableSchema *table_schema = NULL;

      const int64_t BUFFER_SIZE = 1024;
      char db_name_buffer[BUFFER_SIZE] = {'\0'};
      char table_name_buffer[BUFFER_SIZE] = {'\0'};

      IDBMeta *tmp_db_meta = NULL;
      ITableMeta *tmp_table_meta = NULL;

      bool db_exist = false;
      bool table_exist = false;

      int tmp_ret = OB_SUCCESS;
      if (NULL == (total_schema = schema_getter_->get_schema()))
      {
        TBSYS_LOG(WARN, "get schema mgr fail");
      }
      else if (NULL == (table_schema = total_schema->get_table_schema(table_id)))
      {
        TBSYS_LOG(WARN, "get table schema fail, table_id=%lu", table_id);
      }
      else if (OB_SUCCESS != (tmp_ret = db_name_builder_->get_db_name(table_schema->get_table_name(),
              db_partition,
              db_name_buffer,
              BUFFER_SIZE)))
      {
        TBSYS_LOG(WARN, "build db name fail, ret=%d table_name=%s table_id=%lu db_partition=%lu",
            tmp_ret, table_schema->get_table_name(), table_id, db_partition);
      }
      else if (OB_SUCCESS != (tmp_ret = tb_name_builder_->get_tb_name(table_schema->get_table_name(),
              tb_partition,
              table_name_buffer,
              BUFFER_SIZE)))
      {
        TBSYS_LOG(WARN, "build table name fail, ret=%d table_name=%s table_id=%lu tb_partition=%lu",
            tmp_ret, table_schema->get_table_name(), table_id, tb_partition);
      }
      else if (NULL == (tmp_db_meta = get_db_meta_(db_name_buffer, db_exist)))
      {
        TBSYS_LOG(WARN, "get db_meta fail, db_name=%s", db_name_buffer);
      }
      else if (NULL == (tmp_table_meta = get_table_meta_(db_name_buffer, table_name_buffer, table_exist)))
      {
        TBSYS_LOG(WARN, "get table_meta fail, db_name=%s table_name=%s", db_name_buffer, table_name_buffer);
      }
      else if (!table_exist
              && OB_SUCCESS != prepare_table_column_schema_(*total_schema, *table_schema, *tmp_table_meta))
      {
        TBSYS_LOG(WARN, "set table schema fail, table_name=%s", table_name_buffer);
      }
      else
      {
        tmp_db_meta->setName(db_name_buffer);
        tmp_db_meta->setEncoding(DEFAULT_ENCODING);
        tmp_db_meta->setMetaDataCollections(&meta_collect_);

        tmp_table_meta->setName(table_name_buffer);
        tmp_table_meta->setDBMeta(tmp_db_meta);
        tmp_table_meta->setHasPK(true);
        tmp_table_meta->setEncoding(DEFAULT_ENCODING);

        if (!db_exist
            && 0 != meta_collect_.put(db_name_buffer, tmp_db_meta))
        {
          TBSYS_LOG(ERROR, "put db_meta to meta_collect fail, db_meta=%p db_name=%s", tmp_table_meta, db_name_buffer);
        }
        if (!table_exist
            && 0 != tmp_db_meta->put(table_name_buffer, tmp_table_meta))
        {
          TBSYS_LOG(ERROR, "put table_meta to db_meta fail, table_name=%s db_name=%s", table_name_buffer, db_name_buffer);
        }

        ret = tmp_table_meta;
      }
      if (NULL != total_schema)
      {
        schema_getter_->revert_schema(total_schema);
        total_schema = NULL;
      }
      return ret;
    }

    IDBMeta *ObLogMetaManager::get_db_meta_(const char *db_name, bool &exist)
    {
      IDBMeta *ret = NULL;
      if (NULL == (ret = meta_collect_.get(db_name)))
      {
        void *db_meta_buffer = NULL;
        if (NULL == (db_meta_buffer = allocator_.alloc(sizeof(DBMeta))))
        {
          TBSYS_LOG(WARN, "alloc db_meta_buffer fail");
        }
        //else if (NULL == (ret = new(db_meta_buffer) DBMeta()))
        else if (NULL == (ret = new(std::nothrow) DBMeta()))
        {
          TBSYS_LOG(WARN, "construct db_meta fail");
        }
        else
        {
          db_meta_list_.push_back(ret);
          TBSYS_LOG(INFO, "construct db_meta succ, db_meta=%p db_name=%s", ret, db_name);
        }
        exist = false;
      }
      else
      {
        exist = true;
      }
      return ret;
    }

    ITableMeta *ObLogMetaManager::get_table_meta_(const char *db_name, const char *table_name, bool &exist)
    {
      ITableMeta *ret = NULL;
      void *table_meta_buffer = NULL;
      if (NULL == (ret = meta_collect_.get(db_name, table_name)))
      {
        if (NULL == (table_meta_buffer = allocator_.alloc(sizeof(TableMeta))))
        {
          TBSYS_LOG(WARN, "alloc table_meta_buffer fail");
        }
        //else if (NULL == (ret = new(table_meta_buffer) TableMeta()))
        else if (NULL == (ret = new(std::nothrow) TableMeta()))
        {
          TBSYS_LOG(WARN, "construct table_meta fail");
        }
        else
        {
          tb_meta_list_.push_back(ret);
          TBSYS_LOG(INFO, "construct table_meta succ, table_meta=%p db_name=%s table_name=%s", ret, db_name, table_name);
        }
        exist = false;
      }
      else
      {
        exist = true;
      }
      return ret;
    }

    int ObLogMetaManager::prepare_table_column_schema_(const ObLogSchema &total_schema,
        const ObTableSchema &table_schema,
        ITableMeta &table_meta)
    {
      int ret = OB_SUCCESS;
      
      const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
      std::string pks;
      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_info.get_size(); i++)
      {
        const ObRowkeyColumn *rowkey_column = NULL;
        const ObColumnSchemaV2 *column_schema = NULL;
        if (NULL == (rowkey_column = rowkey_info.get_column(i)))
        {
          TBSYS_LOG(WARN, "get rowkey column fail index=%ld", i);
          ret = OB_ERR_UNEXPECTED;
        }
        else if (NULL == (column_schema = total_schema.get_column_schema(
                table_schema.get_table_id(),
                rowkey_column->column_id_)))
        {
          TBSYS_LOG(WARN, "get column schema fail table_id=%lu column_id=%lu",
              table_schema.get_table_id(), rowkey_column->column_id_);
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          pks.append(column_schema->get_name());
          if (i < (rowkey_info.get_size() - 1))
          {
            pks.append(",");
          }
        }
      }
      table_meta.setPKs(pks.c_str());

      int32_t column_num = 0;
      const ObColumnSchemaV2 *column_schemas = total_schema.get_table_schema(
          table_schema.get_table_id(),
          column_num);
      if (NULL == column_schemas)
      {
        TBSYS_LOG(WARN, "get column schemas fail table_id=%lu", table_schema.get_table_id());
        ret = OB_ERR_UNEXPECTED;
      }
      for (int32_t i = 0; OB_SUCCESS == ret && i < column_num; i++)
      {
        const ObColumnSchemaV2 &column_schema = column_schemas[i];
        void *col_meta_buffer = NULL;
        IColMeta *col_meta = NULL;
        const char *default_value = NULL;
        int mysql_type = DRCMSG_TYPES;
        if (NULL != (col_meta = table_meta.getCol(column_schema.get_name())))
        {
          TBSYS_LOG(INFO, "col_meta has been already add to table_meta succ, table_name=%s column_name=%s col_num=%d col_meta=%p",
                    table_schema.get_table_name(), column_schema.get_name(), table_meta.getColNum(column_schema.get_name()), col_meta);
        }
        else if (NULL == (col_meta_buffer = allocator_.alloc(sizeof(ColMeta))))
        {
          TBSYS_LOG(WARN, "alloc col_meta_buffer fail, col_name=%s", column_schema.get_name());
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        //else if (NULL == (col_meta = new(col_meta_buffer) ColMeta()))
        else if (NULL == (col_meta = new(std::nothrow) ColMeta()))
        {
          TBSYS_LOG(WARN, "construct col_meta fail, col_name=%s", column_schema.get_name());
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (default_value = get_default_value_(column_schema)))
        {
          TBSYS_LOG(WARN, "get default value fail, col_name=%s", column_schema.get_name());
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = type_trans_mysql_(column_schema.get_type(), mysql_type)))
        {
          TBSYS_LOG(WARN, "type trans to mysql fail, ret=%d ob_type=%d, mysql_type=%d", ret, column_schema.get_type(), mysql_type);
        }
        else
        {
          col_meta->setName(column_schema.get_name());
          col_meta->setType(mysql_type);
          col_meta->setLength(column_schema.get_size());
          col_meta->setSigned(true);
          col_meta->setIsPK(rowkey_info.is_rowkey_column(column_schema.get_id()));
          col_meta->setNotNull(column_schema.is_nullable());
          col_meta->setDefault(default_value);
          col_meta->setEncoding(DEFAULT_ENCODING);
          // Do not need
          //col_meta->setDecimals(int decimals);
          //col_meta->setRequired(int required);
          //col_meta->setValuesOfEnumSet(std::vector<std::string> &v);
          //col_meta->setValuesOfEnumSet(std::vector<const char*> &v);
          //col_meta->setValuesOfEnumSet(const char** v, size_t size);
          if (0 != table_meta.append(column_schema.get_name(), col_meta))
          {
            TBSYS_LOG(ERROR, "append col_meta to table_meta fail, table_name=%s column_name=%s", table_schema.get_table_name(), column_schema.get_name());
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            TBSYS_LOG(DEBUG, "append col_meta to table_meta succ, table_name=%s column_name=%s", table_schema.get_table_name(), column_schema.get_name());
          }
        }
      }
      
      return ret;
    }

    const char *ObLogMetaManager::get_default_value_(const ObColumnSchemaV2 &column_schema)
    {
      // OB does not support default value now, all are null type
      UNUSED(column_schema);
      static const int64_t DEFAULT_VALUE_STR_BUFFER_SIZE = 16;
      static __thread char buffer[DEFAULT_VALUE_STR_BUFFER_SIZE];
      const char *ret = NULL;
      ObObj v;
      v.set_null();
      int64_t ret_size = 0;
      if (OB_SUCCESS == obj_cast(v, ObVarcharType, buffer, DEFAULT_VALUE_STR_BUFFER_SIZE - 1, ret_size))
      {
        buffer[ret_size] = '\0';
        ret = buffer;
      }
      return ret;
    }

    int ObLogMetaManager::type_trans_mysql_(const ObObjType ob_type, int &mysql_type)
    {
      uint8_t num_decimals = 0;
      uint32_t length = 0;
      return obmysql::ObMySQLUtil::get_mysql_type(ob_type, (obmysql::EMySQLFieldType&)mysql_type, num_decimals, length);
    }

  }
}

