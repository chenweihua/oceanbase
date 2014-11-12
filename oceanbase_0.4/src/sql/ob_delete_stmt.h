#ifndef OCEANBASE_SQL_DELETESTMT_H_
#define OCEANBASE_SQL_DELETESTMT_H_
#include "ob_stmt.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace sql
  {
    class ObDeleteStmt : public ObStmt
    {
    public:
      ObDeleteStmt(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObDeleteStmt();

      uint64_t set_delete_table(uint64_t id);
      uint64_t get_delete_table_id(void);

      void print(FILE* fp, int32_t level, int32_t index);

    private:
      uint64_t table_id_;
    };

    inline uint64_t ObDeleteStmt::set_delete_table(uint64_t id)
    {
      table_id_ = id;
      return id;
    }

    inline uint64_t ObDeleteStmt::get_delete_table_id(void)
    {
      return table_id_;
    }
  }
}

#endif //OCEANBASE_SQL_DELETESTMT_H_

