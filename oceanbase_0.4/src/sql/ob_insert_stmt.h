#ifndef OCEANBASE_SQL_INSERTSTMT_H_
#define OCEANBASE_SQL_INSERTSTMT_H_
#include "ob_stmt.h"
#include <stdio.h>
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace sql
  {
    class ObInsertStmt : public ObStmt
    {
    public:
      ObInsertStmt(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObInsertStmt();

      void set_insert_table(uint64_t id);
      void set_insert_query(uint64_t id);
      void set_replace(bool is_replace);
      void set_values_size(int64_t size);
      int add_value_row(oceanbase::common::ObArray<uint64_t>& value_row);
      bool is_replace() const;
      uint64_t get_table_id() const;
      uint64_t get_insert_query_id() const;
      int64_t get_value_row_size() const;
      const oceanbase::common::ObArray<uint64_t>& get_value_row(int64_t idx) const;

      void print(FILE* fp, int32_t level, int32_t index);

    private:
      uint64_t   table_id_;
      uint64_t   sub_query_id_;
      bool       is_replace_;  // replace semantic
      oceanbase::common::ObArray<oceanbase::common::ObArray<uint64_t> > value_vectors_;
    };

    inline void ObInsertStmt::set_insert_table(uint64_t id)
    {
      table_id_ = id;
    }

    inline void ObInsertStmt::set_insert_query(uint64_t id)
    {
      sub_query_id_ = id;
    }

    inline void ObInsertStmt::set_replace(bool is_replace)
    {
      is_replace_ = is_replace;
      if (is_replace_)
      {
        set_stmt_type(ObBasicStmt::T_REPLACE);
      }
      else
      {
        set_stmt_type(ObBasicStmt::T_INSERT);
      }
    }

    inline void ObInsertStmt::set_values_size(int64_t size)
    {
      return value_vectors_.reserve(size);
    }

    inline bool ObInsertStmt::is_replace() const
    {
      return is_replace_;
    }

    inline int ObInsertStmt::add_value_row(oceanbase::common::ObArray<uint64_t>& value_row)
    {
      return value_vectors_.push_back(value_row);
    }

    inline uint64_t ObInsertStmt::get_table_id() const
    {
      return table_id_;
    }

    inline uint64_t ObInsertStmt::get_insert_query_id() const
    {
      return sub_query_id_;
    }

    inline int64_t ObInsertStmt::get_value_row_size() const
    {
      return value_vectors_.count();
    }

    inline const oceanbase::common::ObArray<uint64_t>& ObInsertStmt::get_value_row(int64_t idx) const
    {
      OB_ASSERT(idx >= 0 && idx < value_vectors_.count());
      return value_vectors_.at(idx);
    }
  }
}

#endif //OCEANBASE_SQL_INSERTSTMT_H_
