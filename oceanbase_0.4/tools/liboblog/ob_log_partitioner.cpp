////===================================================================
 //
 // ob_log_partitioner.cpp liboblog / Oceanbase
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
#include "ob_log_partitioner.h"

#define LUA_VAR_TABLE_NAME      "table_name"
#define LUA_VAR_RK_COLUMN_NAME  "rk_column_name"
#define LUA_FUN_DB_PARTITION    "db_partition"
#define LUA_FUN_TB_PARTITION    "tb_partition"

#define LUA_FUN_DB_PARTITION_FORMAT    "%s_db_partition"
#define LUA_FUN_TB_PARTITION_FORMAT    "%s_tb_partition"

#define GET_LUA_VALUE(key, check_type, value, get_type, pop) \
  if (OB_SUCCESS == ret) \
  { \
    lua_getglobal(lua_, key); \
    if (!lua_is##check_type(lua_, -1)) \
    { \
      TBSYS_LOG(WARN, "key=%s is not check_type=%s", key, #check_type); \
      ret = OB_ERR_UNEXPECTED; \
    } \
    else \
    { \
      TBSYS_LOG(INFO, "get_lua_value succ key=%s check_type=%s get_type=%s", key, #check_type, #get_type); \
      value = lua_to##get_type(lua_, -1); \
    } \
    if (pop) \
    { \
      lua_pop(lua_, 1); \
    } \
  }

#define CHECK_LUA_VALUE(key, type) \
  if (OB_SUCCESS == ret) \
  { \
    lua_getglobal(lua_, key); \
    if (!lua_is##type(lua_, -1)) \
    { \
      TBSYS_LOG(WARN, "key=%s is not [%s]", key, #type); \
      ret = OB_ERR_UNEXPECTED; \
    } \
    else \
    { \
      TBSYS_LOG(INFO, "check_lua_value succ key=%s type=%s", key, #type); \
    } \
    lua_pop(lua_, 1); \
  }

namespace oceanbase
{
  namespace liboblog
  {
    ObLogPartitioner::ObLogPartitioner() : inited_(false),
                                           lua_(NULL),
                                           allocator_(),
                                           table_id_map_()
    {
    }

    ObLogPartitioner::~ObLogPartitioner()
    {
      destroy();
    }

    int ObLogPartitioner::init(ObLogConfig &config, IObLogSchemaGetter *schema_getter)
    {
      int ret = OB_SUCCESS;
      const char *tb_select = NULL;
      const ObLogSchema *total_schema = NULL;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == (tb_select = config.get_tb_select()))
      {
        TBSYS_LOG(WARN, "tb_select null pointer");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == schema_getter)
      {
        TBSYS_LOG(WARN, "schema_getter null pointer");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (total_schema = schema_getter->get_schema()))
      {
        TBSYS_LOG(WARN, "get total schema fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (NULL == (lua_ = lua_open()))
      {
        TBSYS_LOG(WARN, "lua_open fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (ret = prepare_partition_functions_(tb_select, config, *total_schema)))
      {
        TBSYS_LOG(WARN, "prepare_partition_functions fail, ret=%d tb_select=[%s]", ret, tb_select);
      }
      else
      {
        inited_ = true;
      }
      if (NULL != total_schema)
      {
        schema_getter->revert_schema(total_schema);
        total_schema = NULL;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void ObLogPartitioner::destroy()
    {
      table_id_map_.destroy();
      allocator_.reuse();
      if (NULL != lua_)
      {
        lua_close(lua_);
        lua_ = NULL;
      }
    }

    int ObLogPartitioner::partition(ObLogMutator &mutator, uint64_t *db_partition, uint64_t *tb_partition)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      while (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator.get_mutator().next_cell()))
      {
        ObMutatorCellInfo *cell = NULL;
        if (OB_SUCCESS != (ret = mutator.get_mutator().get_cell(&cell)))
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

        TableInfo *table_info = NULL;
        int hash_ret = table_id_map_.get(cell->cell_info.table_id_, table_info);
        if (hash::HASH_EXIST != hash_ret)
        {
          continue;
        }
        else
        {
          if (NULL == table_info)
          {
            TBSYS_LOG(WARN, "get from table_id_map fail, hash_ret=%d table_id=%lu table_info=%p",
                      hash_ret, cell->cell_info.table_id_, table_info);
            ret = OB_ERR_UNEXPECTED;
          }
          if (OB_SUCCESS != (ret = calc_partition_(*table_info,
                  cell->cell_info.row_key_,
                  db_partition,
                  tb_partition)))
          {
            TBSYS_LOG(WARN, "calc_partition fail, ret=%d table_id=%lu rowkey=%s",
                      ret, cell->cell_info.table_id_, to_cstring(cell->cell_info.row_key_));
          }
          break;
        }
      }
      mutator.reset_iter();
      return ret;
    }

    int ObLogPartitioner::push_rowkey_values_(lua_State *lua, const ObRowkey &rowkey)
    {
      int ret = OB_SUCCESS;
      int64_t pushed_cnt = 0;
      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey.get_obj_cnt(); i++)
      {
        const ObObj &value = rowkey.get_obj_ptr()[i];
        int64_t int_value = 0;
        switch (value.get_type())
        {
        case ObNullType:
          int_value = 0;
        case ObIntType:
          value.get_int(int_value);
        case ObDateTimeType:
          value.get_datetime(int_value);
        case ObPreciseDateTimeType:
          value.get_precise_datetime(int_value);
        case ObCreateTimeType:
          value.get_createtime(int_value);
        case ObModifyTimeType:
          value.get_modifytime(int_value);
          lua_pushinteger(lua, int_value);
          pushed_cnt += 1;
          break;
        case ObVarcharType:
          {
            ObString varchar_value;
            value.get_varchar(varchar_value);
            char buffer[VARCHAE_BUFFER_SIZE];
            if (VARCHAE_BUFFER_SIZE <= snprintf(buffer,
                  VARCHAE_BUFFER_SIZE,
                  "%.*s",
                  varchar_value.length(),
                  varchar_value.ptr()))
            {
              TBSYS_LOG(WARN, "varchar length too long, value=%.*s",
                        varchar_value.length(), varchar_value.ptr());
              ret = OB_BUF_NOT_ENOUGH;
            }
            else
            {
              lua_pushstring(lua, buffer);
              pushed_cnt += 1;
            }
            break;
          }
        default:
          TBSYS_LOG(WARN, "rowkey column value type=%d not support", value.get_type());
          ret = OB_SCHEMA_ERROR;
          break;
        }
      }
      if (OB_SUCCESS != ret)
      {
        lua_pop(lua, (int)pushed_cnt);
      }
      return ret;
    }

    int ObLogPartitioner::calc_partition_(const TableInfo &table_info,
        const ObRowkey &rowkey,
        uint64_t *db_partition,
        uint64_t *tb_partition)
    {
      int ret = OB_SUCCESS;
      if (NULL != db_partition)
      {
        lua_getglobal(lua_, table_info.db_partition_function);
        int tmp_ret = 0;
        if (OB_SUCCESS != (ret = push_rowkey_values_(lua_, rowkey)))
        {
          TBSYS_LOG(WARN, "push_rowkey_values fail, ret=%d rowkey=%s", ret, to_cstring(rowkey));
        }
        else if(0 != (tmp_ret = lua_pcall(lua_, (int)rowkey.get_obj_cnt(), 1, 0)))
        {
          TBSYS_LOG(WARN, "lua_pcall fail, ret=%d [%s]", tmp_ret, lua_tostring(lua_, -1));
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          *db_partition = lua_tointeger(lua_, -1);
        }
        lua_pop(lua_, 1) ;
      }
      if (OB_SUCCESS == ret
          && NULL != tb_partition)
      {
        lua_getglobal(lua_, table_info.tb_partition_function);
        int tmp_ret = 0;
        if (OB_SUCCESS != (ret = push_rowkey_values_(lua_, rowkey)))
        {
          TBSYS_LOG(WARN, "push_rowkey_values fail, ret=%d rowkey=%s", ret, to_cstring(rowkey));
        }
        else if(0 != (tmp_ret = lua_pcall(lua_, (int)rowkey.get_obj_cnt(), 1, 0)))
        {
          TBSYS_LOG(WARN, "lua_pcall fail, ret=%d [%s]", tmp_ret, lua_tostring(lua_, -1));
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          *tb_partition = lua_tointeger(lua_, -1);
        }
        lua_pop(lua_, 1) ;
      }
      return ret;
    }

    int ObLogPartitioner::prepare_partition_functions_(const char *tb_select,
        const ObLogConfig &config,
        const ObLogSchema &total_schema)
    {
      int ret = OB_SUCCESS;
      int tmp_ret = 0;
      luaL_openlibs(lua_);
      if (0 != (tmp_ret = luaL_loadfile(lua_, config.get_lua_conf())))
      {
        TBSYS_LOG(WARN, "luaL_loadfile fail, ret=%d conf=[%s] [%s]",
                  tmp_ret, config.get_lua_conf(), lua_tostring(lua_, -1));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != (tmp_ret = lua_pcall(lua_, 0, 0, 0)))
      {
        TBSYS_LOG(WARN, "lua_pcall fail, ret=%d [%s]", tmp_ret, lua_tostring(lua_, -1));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 != table_id_map_.create(TABLE_ID_MAP_SIZE))
      {
        TBSYS_LOG(WARN, "create table_id_map fail");
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (ObLogConfig::parse_tb_select(tb_select, *this, &total_schema)))
      {
        TBSYS_LOG(WARN, "parse tb_select to build table_id_map fail, ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "build table_id_map succ, tb_select=[%s]", tb_select);
        TableIDMap::iterator iter;
        for (iter = table_id_map_.begin(); iter != table_id_map_.end(); iter++)
        {
          TBSYS_LOG(INFO, "table_id=%lu db_partition_function=%s tb_partition_function=%s",
                    iter->first, iter->second->db_partition_function, iter->second->tb_partition_function);
        }
      }
      return ret;
    }

    int ObLogPartitioner::operator() (const char *tb_name, const ObLogSchema *total_schema)
    {
      int ret = OB_SUCCESS;
      TableInfo *table_info = NULL;
      const ObTableSchema *table_schema = NULL;
      if (NULL == (table_info = (TableInfo*)allocator_.alloc(sizeof(TableInfo))))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (FUNCTION_NAME_LENGTH <= snprintf(table_info->db_partition_function,
            FUNCTION_NAME_LENGTH,
            LUA_FUN_DB_PARTITION_FORMAT,
            tb_name))
      {
        TBSYS_LOG(WARN, "build db_partition_function name fail");
        ret = OB_BUF_NOT_ENOUGH;
      }
      else if (FUNCTION_NAME_LENGTH <= snprintf(table_info->tb_partition_function,
            FUNCTION_NAME_LENGTH,
            LUA_FUN_TB_PARTITION_FORMAT,
            tb_name))
      {
        TBSYS_LOG(WARN, "build tb_partition_function name fail");
        ret = OB_BUF_NOT_ENOUGH;
      }
      else if (NULL == (table_schema = total_schema->get_table_schema(tb_name)))
      {
        TBSYS_LOG(WARN, "get table schema fail, table_name=%s", tb_name);
        ret = OB_SCHEMA_ERROR;
      }
      else
      {
        for (int64_t i = 0; i < table_schema->get_rowkey_info().get_size(); i++)
        {
          const ObRowkeyColumn *rowkey_column = table_schema->get_rowkey_info().get_column(i);
          if (NULL == rowkey_column)
          {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          ObObjType type = rowkey_column->type_;
          if (ObIntType != type
              && ObDateTimeType != type
              && ObPreciseDateTimeType != type
              && ObVarcharType != type
              && ObCreateTimeType != type
              && ObModifyTimeType != type)
          {
            TBSYS_LOG(WARN, "rowkey column type=%d not support", type);
            ret = OB_SCHEMA_ERROR;
            break;
          }
        }
        CHECK_LUA_VALUE(table_info->db_partition_function, function);
        CHECK_LUA_VALUE(table_info->tb_partition_function, function);
        if (OB_SUCCESS == ret)
        {
          int hash_ret = table_id_map_.set(table_schema->get_table_id(), table_info);
          if (hash::HASH_INSERT_SUCC != hash_ret)
          {
            TBSYS_LOG(WARN, "set to table_id_map fail, hash_ret=%d table_id=%lu table_name=%s",
                      hash_ret, table_schema->get_table_id(), tb_name);
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      return ret;
    }

  }
}

