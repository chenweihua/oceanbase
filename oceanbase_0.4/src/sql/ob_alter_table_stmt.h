/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_table_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_ALTER_TABLE_STMT_H_
#define OCEANBASE_SQL_OB_ALTER_TABLE_STMT_H_

#include "common/hash/ob_hashmap.h"
#include "common/ob_string_buf.h"
#include "ob_column_def.h"
#include "ob_basic_stmt.h"
#include "parse_node.h"

namespace oceanbase
{
  namespace sql
  {
    class ObAlterTableStmt : public ObBasicStmt
    {
    public:
      explicit ObAlterTableStmt(common::ObStringBuf* name_pool);
      ObAlterTableStmt();
      virtual ~ObAlterTableStmt();

      int init();
      void set_name_pool(common::ObStringBuf* name_pool);
      int set_table_name(ResultPlan& result_plan, const common::ObString& table_name);
      int set_new_table_name(ResultPlan& result_plan, const common::ObString& table_name);
      int add_column(ResultPlan& result_plan, const ObColumnDef& column_def);
      int drop_column(ResultPlan& result_plan, const ObColumnDef& column_def);
      int rename_column(ResultPlan& result_plan, const ObColumnDef& column_def);
      int alter_column(ResultPlan& result_plan, const ObColumnDef& column_def);
      const uint64_t get_table_id() const;
      const common::ObString& get_table_name() const;
      const common::ObString& get_new_table_name() const;
      common::hash::ObHashMap<common::ObString, ObColumnDef>::iterator column_begin();
      common::hash::ObHashMap<common::ObString, ObColumnDef>::iterator column_end();
      
      virtual void print(FILE* fp, int32_t level, int32_t index = 0);

    protected:
      common::ObStringBuf*        name_pool_;

    private:
      uint64_t                    table_id_;
      common::ObString            table_name_;
      common::ObString            new_table_name_;
      uint64_t                    max_column_id_;
      common::hash::ObHashMap<common::ObString, ObColumnDef> columns_map_;
    };
    inline void ObAlterTableStmt::set_name_pool(common::ObStringBuf* name_pool)
    {
      name_pool_ = name_pool;
    }
    inline const uint64_t ObAlterTableStmt::get_table_id() const
    {
      return table_id_;
    }
    inline const common::ObString& ObAlterTableStmt::get_table_name() const
    {
      return table_name_;
    }
    inline const common::ObString& ObAlterTableStmt::get_new_table_name() const
    {
      return new_table_name_;
    }
    inline common::hash::ObHashMap<common::ObString, ObColumnDef>::iterator ObAlterTableStmt::column_begin()
    {
      return columns_map_.begin();
    }
    inline common::hash::ObHashMap<common::ObString, ObColumnDef>::iterator ObAlterTableStmt::column_end()
    {
      return columns_map_.end();
    }
  }
}

#endif //OCEANBASE_SQL_OB_ALTER_TABLE_STMT_H_


