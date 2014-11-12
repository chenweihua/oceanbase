/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_service.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *   - 表单schema相关数据结构。创建，删除，获取schema描述结构接口
 *
 */

#ifndef _OB_SCHEMA_SERVICE_H
#define _OB_SCHEMA_SERVICE_H

#include "common/ob_define.h"
#include "ob_object.h"
#include "ob_string.h"
#include "ob_array.h"
#include "ob_hint.h"

namespace oceanbase
{
  namespace common
  {
    typedef ObObjType ColumnType;
    /* 表单join关系描述，对应于__all_join_info内部表 */
    struct JoinInfo
    {
      char left_table_name_[OB_MAX_TABLE_NAME_LENGTH];
      uint64_t left_table_id_;
      char left_column_name_[OB_MAX_COLUMN_NAME_LENGTH];
      uint64_t left_column_id_;
      char right_table_name_[OB_MAX_TABLE_NAME_LENGTH];
      uint64_t right_table_id_;
      char right_column_name_[OB_MAX_COLUMN_NAME_LENGTH];
      uint64_t right_column_id_;

      int64_t to_string(char* buf, const int64_t buf_len) const
      {
        int64_t pos = 0;
        databuff_printf(buf, buf_len, pos, "left_table:tname[%s], tid[%lu], cname[%s], cid[%lu]",
          left_table_name_, left_table_id_, left_column_name_, left_column_id_);
        databuff_printf(buf, buf_len, pos, "right_table:tname[%s], tid[%lu], cname[%s], cid[%lu]",
          right_table_name_, right_table_id_, right_column_name_, right_column_id_);
        return pos;
      }

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    /* 表单column描述，对应于__all_all_column内部表 */
    struct ColumnSchema
    {
      char column_name_[OB_MAX_COLUMN_NAME_LENGTH];
      uint64_t column_id_;
      uint64_t column_group_id_;
      int64_t rowkey_id_;
      uint64_t join_table_id_;
      uint64_t join_column_id_;
      ColumnType data_type_;
      int64_t data_length_;
      int64_t data_precision_;
      int64_t data_scale_;
      bool nullable_;
      int64_t length_in_rowkey_; //如果是rowkey列，则表示在二进制rowkey串中占用的字节数；
      int32_t order_in_rowkey_;
      ObCreateTime gm_create_;
      ObModifyTime gm_modify_;
      ColumnSchema():column_id_(OB_INVALID_ID), column_group_id_(OB_INVALID_ID), rowkey_id_(-1),
          join_table_id_(OB_INVALID_ID), join_column_id_(OB_INVALID_ID), data_type_(ObMinType),
          data_precision_(0), data_scale_(0), nullable_(true), length_in_rowkey_(0), order_in_rowkey_(0)
      {
        column_name_[0] = '\0';
      }
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    /* 表单schema描述，对应于__all_all_table内部表 */
    struct TableSchema
    {
      enum TableType
      {
        NORMAL = 1,
        INDEX,
        META,
        VIEW,
      };

      enum LoadType
      {
        INVALID = 0,
        DISK = 1,
        MEMORY
      };

      enum TableDefType
      {
        INTERNAL = 1,
        USER_DEFINED
      };

      enum SSTableVersion
      {
        OLD_SSTABLE_VERSION = 1,
        DEFAULT_SSTABLE_VERSION = 2,
        NEW_SSTABLE_VERSION = 3,
      };

      char table_name_[OB_MAX_TABLE_NAME_LENGTH];
      char compress_func_name_[OB_MAX_TABLE_NAME_LENGTH];
      char expire_condition_[OB_MAX_EXPIRE_CONDITION_LENGTH];
      char comment_str_[OB_MAX_TABLE_COMMENT_LENGTH];
      uint64_t table_id_;
      TableType table_type_;
      LoadType load_type_;
      TableDefType table_def_type_;
      bool is_use_bloomfilter_;
      bool is_pure_update_table_;
      int64_t consistency_level_;
      int64_t rowkey_split_;
      int32_t rowkey_column_num_;
      int32_t replica_num_;
      int64_t max_used_column_id_;
      int64_t create_mem_version_;
      int64_t tablet_block_size_;
      int64_t tablet_max_size_;
      int64_t max_rowkey_length_;
      int64_t merge_write_sstable_version_;
      int64_t schema_version_;
      uint64_t create_time_column_id_;
      uint64_t modify_time_column_id_;
      ObArray<ColumnSchema> columns_;
      ObArray<JoinInfo> join_info_;

    public:
        TableSchema()
          :table_id_(OB_INVALID_ID),
           table_type_(NORMAL),
           load_type_(DISK),
           table_def_type_(USER_DEFINED),
           is_use_bloomfilter_(false),
           is_pure_update_table_(false),
           consistency_level_(NO_CONSISTENCY),
           rowkey_split_(0),
           rowkey_column_num_(0),
           replica_num_(OB_SAFE_COPY_COUNT),
           max_used_column_id_(0),
           create_mem_version_(0),
           tablet_block_size_(OB_DEFAULT_SSTABLE_BLOCK_SIZE),
          tablet_max_size_(OB_DEFAULT_MAX_TABLET_SIZE),
          max_rowkey_length_(0),
          merge_write_sstable_version_(DEFAULT_SSTABLE_VERSION),
          schema_version_(0),
          create_time_column_id_(OB_CREATE_TIME_COLUMN_ID),
          modify_time_column_id_(OB_MODIFY_TIME_COLUMN_ID)
      {
        table_name_[0] = '\0';
        compress_func_name_[0] = '\0';
        expire_condition_[0] = '\0';
        comment_str_[0] = '\0';
      }

      inline void init_as_inner_table()
      {
        // clear all the columns at first
        clear();
        this->table_id_ = OB_INVALID_ID;
        this->table_type_ = TableSchema::NORMAL;
        this->load_type_ = TableSchema::DISK;
        this->table_def_type_ = TableSchema::INTERNAL;
        this->replica_num_ = OB_SAFE_COPY_COUNT;
        this->create_mem_version_ = 1;
        this->tablet_block_size_ = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
        this->tablet_max_size_ = OB_DEFAULT_MAX_TABLET_SIZE;
        strcpy(this->compress_func_name_, OB_DEFAULT_COMPRESS_FUNC_NAME);
        this->is_use_bloomfilter_ = false;
        this->is_pure_update_table_ = false;
        this->consistency_level_ = NO_CONSISTENCY;
        this->rowkey_split_ = 0;
        this->merge_write_sstable_version_ = DEFAULT_SSTABLE_VERSION;
        this->create_time_column_id_ = OB_CREATE_TIME_COLUMN_ID;
        this->modify_time_column_id_ = OB_MODIFY_TIME_COLUMN_ID;
      }

      inline int add_column(const ColumnSchema& column)
      {
        return columns_.push_back(column);
      }

      inline int add_join_info(const JoinInfo& join_info)
      {
        return join_info_.push_back(join_info);
      }

      inline int64_t get_column_count(void) const
      {
        return columns_.count();
      }
      inline ColumnSchema* get_column_schema(const int64_t index)
      {
        ColumnSchema * ret = NULL;
        if ((index >= 0) && (index < columns_.count()))
        {
          ret = &columns_.at(index);
        }
        return ret;
      }
      inline ColumnSchema* get_column_schema(const uint64_t column_id)
      {
        ColumnSchema *ret = NULL;
        for(int64_t i = 0; i < columns_.count(); i++)
        {
          if (columns_.at(i).column_id_ == column_id)
          {
            ret = &columns_.at(i);
            break;
          }
        }
        return ret;
      }
      inline ColumnSchema* get_column_schema(const char * column_name)
      {
        ColumnSchema *ret = NULL;
        for(int64_t i = 0; i < columns_.count(); i++)
        {
          if (strcmp(column_name, columns_.at(i).column_name_) == 0)
          {
            ret = &columns_.at(i);
            break;
          }
        }
        return ret;
      }
      inline const ColumnSchema* get_column_schema(const char * column_name) const
      {
        const ColumnSchema *ret = NULL;
        for(int64_t i = 0; i < columns_.count(); i++)
        {
          if (strcmp(column_name, columns_.at(i).column_name_) == 0)
          {
            ret = &columns_.at(i);
            break;
          }
        }
        return ret;
      }
      inline int get_column_rowkey_index(const uint64_t column_id, int64_t &rowkey_idx)
      {
        int ret = OB_SUCCESS;
        rowkey_idx = -1;
        for (int64_t i = 0; i < columns_.count(); i++)
        {
          if (columns_.at(i).column_id_ == column_id)
          {
            rowkey_idx = columns_.at(i).rowkey_id_;
          }
        }
        if (-1 == rowkey_idx)
        {
          TBSYS_LOG(WARN, "fail to get column. column_id=%lu", column_id);
          ret = OB_ENTRY_NOT_EXIST;
        }
        return ret;
      }
        void clear();
        bool is_valid() const;
        int to_string(char* buffer, const int64_t length) const;

        static bool is_system_table(const common::ObString &tname);
        NEED_SERIALIZE_AND_DESERIALIZE;
    };

    inline bool TableSchema::is_system_table(const common::ObString &tname)
    {
      bool ret = false;
      if (tname.length() >= 1)
      {
        const char *p = tname.ptr();
        if ('_' == p[0])
        {
          ret = true;
        }
      }
      return ret;
    }

    // for alter table add or drop columns rpc construct
    struct AlterTableSchema
    {
      public:
        enum AlterType
        {
          ADD_COLUMN = 0,
          DEL_COLUMN = 1,
          MOD_COLUMN = 2,
        };
        struct AlterColumnSchema
        {
          AlterType type_;
          ColumnSchema column_;
        };
        // get table name
        const char * get_table_name(void) const
        {
          return table_name_;
        }
        // add new column
        int add_column(const AlterType type, const ColumnSchema& column)
        {
          AlterColumnSchema schema;
          schema.type_ = type;
          schema.column_ = column;
          return columns_.push_back(schema);
        }
        // clear all
        void clear(void)
        {
          table_id_ = OB_INVALID_ID;
          table_name_[0] = '\0';
          columns_.clear();
        }
        int64_t get_column_count(void) const
        {
          return columns_.count();
        }
        NEED_SERIALIZE_AND_DESERIALIZE;
        uint64_t table_id_;
        char table_name_[OB_MAX_TABLE_NAME_LENGTH];
        ObArray<AlterColumnSchema> columns_;
    };

    // ob table schema service interface layer
    class ObScanHelper;
    class ObSchemaService
    {
      public:
        virtual ~ObSchemaService() {}
        virtual int init(ObScanHelper* client_proxy, bool only_core_tables) = 0;
        virtual int get_table_schema(const ObString& table_name, TableSchema& table_schema) = 0;
        virtual int create_table(const TableSchema& table_schema) = 0;
        virtual int drop_table(const ObString& table_name) = 0;
        virtual int alter_table(const AlterTableSchema& table_schema, const int64_t old_schema_version) = 0;
        virtual int get_table_id(const ObString& table_name, uint64_t& table_id) = 0;
        virtual int get_table_name(uint64_t table_id, ObString& table_name) = 0;
        virtual int get_max_used_table_id(uint64_t &max_used_tid) = 0;
        virtual int modify_table_id(TableSchema& table_schema, const int64_t new_table_id) = 0;
        virtual int set_max_used_table_id(const uint64_t max_used_tid) = 0;
    };

  }
}


#endif /* _OB_SCHEMA_SERVICE_H */
