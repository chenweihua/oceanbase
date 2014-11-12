/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_service_impl.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_SCHEMA_SERVICE_IMPL_H
#define _OB_SCHEMA_SERVICE_IMPL_H

#include "common/ob_table_id_name.h"
#include "common/ob_schema_service.h"
#include "common/hash/ob_hashmap.h"

class TestSchemaService_assemble_table_Test;
class TestSchemaTable_generate_new_table_name_Test;
class TestSchemaService_assemble_column_Test;
class TestSchemaService_assemble_join_info_Test;
class TestSchemaService_create_table_mutator_Test;
class TestSchemaService_get_table_name_Test;
#define OB_STR(str) \
  ObString(0, static_cast<int32_t>(strlen(str)), const_cast<char *>(str))

namespace oceanbase
{
  namespace common
  {
    static ObString first_tablet_entry_name = OB_STR(FIRST_TABLET_TABLE_NAME);
    static ObString column_table_name = OB_STR(OB_ALL_COLUMN_TABLE_NAME);
    static ObString joininfo_table_name = OB_STR(OB_ALL_JOININFO_TABLE_NAME);
    static ObString privilege_table_name = OB_STR(OB_ALL_TABLE_PRIVILEGE_TABLE_NAME);
    static ObString table_name_str = OB_STR("table_id");
    static const char* const TMP_PREFIX = "tmp_";

    class ObSchemaServiceImpl : public ObSchemaService
    {
      public:
        ObSchemaServiceImpl();
        virtual ~ObSchemaServiceImpl();

        int init(ObScanHelper* client_proxy, bool only_core_tables);

        virtual int get_table_schema(const ObString& table_name, TableSchema& table_schema);
        virtual int create_table(const TableSchema& table_schema);
        virtual int drop_table(const ObString& table_name);
        virtual int alter_table(const AlterTableSchema& table_schema, const int64_t old_schema_version);
        virtual int get_table_id(const ObString& table_name, uint64_t& table_id);
        virtual int get_table_name(uint64_t table_id, ObString& table_name);
        virtual int modify_table_id(TableSchema& table_schema, const int64_t new_table_id);
        virtual int get_max_used_table_id(uint64_t &max_used_tid);
        virtual int set_max_used_table_id(const uint64_t max_used_tid);
        virtual int prepare_privilege_for_table(const nb_accessor::TableRow* table_row,
            ObMutator *mutator, const int64_t table_id);

        friend class ::TestSchemaService_assemble_table_Test;
        friend class ::TestSchemaService_assemble_column_Test;
        friend class ::TestSchemaService_assemble_join_info_Test;
        friend class ::TestSchemaService_create_table_mutator_Test;
        friend class ::TestSchemaService_get_table_name_Test;

      private:
        bool check_inner_stat();
        // for read
        int fetch_table_schema(const ObString& table_name, TableSchema& table_schema);
        int create_table_mutator(const TableSchema& table_schema, ObMutator* mutator);
        int alter_table_mutator(const AlterTableSchema& table_schema, ObMutator* mutator, const int64_t old_schema_version);
        int assemble_table(const nb_accessor::TableRow* table_row, TableSchema& table_schema);
        int assemble_column(const nb_accessor::TableRow* table_row, ColumnSchema& column);
        int assemble_join_info(const nb_accessor::TableRow* table_row, JoinInfo& join_info);
        int init_id_name_map(ObTableIdNameIterator& iterator);
        // for update
        int add_join_info(ObMutator* mutator, const TableSchema& table_schema);
        int add_column(ObMutator* mutator, const TableSchema& table_schema);
        int update_column_mutator(ObMutator* mutator, ObRowkey & rowkey, const ColumnSchema & column);
        int reset_column_id_mutator(ObMutator* mutator, const AlterTableSchema & schema, const uint64_t max_column_id);
        int reset_schema_version_mutator(ObMutator* mutator, const AlterTableSchema & schema, const int64_t old_schema_version);
        int init_id_name_map();
      int generate_new_table_name(char* buf, const uint64_t lenght, const char* table_name, const uint64_t table_name_length);


      private:
        static const int64_t TEMP_VALUE_BUFFER_LEN = 32;
        ObScanHelper* client_proxy_;
        nb_accessor::ObNbAccessor nb_accessor_;
        hash::ObHashMap<uint64_t, ObString> id_name_map_;
        ObStringBuf string_buf_;
        tbsys::CThreadMutex string_buf_write_mutex_;
        tbsys::CThreadMutex mutex_;
        bool is_id_name_map_inited_;
        bool only_core_tables_;
    };
  }
}

#endif /* _OB_SCHEMA_SERVICE_IMPL_H */
