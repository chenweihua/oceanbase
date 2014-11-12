#ifndef OCEANBASE_SQL_LOGICALPLAN_H_
#define OCEANBASE_SQL_LOGICALPLAN_H_
#include "parse_node.h"
#include "ob_raw_expr.h"
#include "ob_stmt.h"
#include "ob_select_stmt.h"
#include "ob_result_set.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_vector.h"
#include "common/ob_stack_allocator.h"
namespace oceanbase
{
  namespace sql
  {
    enum ObCurTimeType {
      CUR_TIME,
      CUR_TIME_UPS,
      NO_CUR_TIME
    };
    class ObSQLSessionInfo;
    class ObLogicalPlan
    {
    public:
      explicit ObLogicalPlan(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObLogicalPlan();

      oceanbase::common::ObStringBuf* get_name_pool() const
      {
        return name_pool_;
      }

      ObBasicStmt* get_query(uint64_t query_id) const;

      ObBasicStmt* get_main_stmt()
      {
       ObBasicStmt *stmt = NULL;
        if (stmts_.size() > 0)
          stmt = stmts_[0];
        return stmt;
      }

      ObSelectStmt* get_select_query(uint64_t query_id) const;

      ObSqlRawExpr* get_expr(uint64_t expr_id) const;

      int add_query(ObBasicStmt* stmt)
      {
        int ret = common::OB_SUCCESS;
        if ((!stmt) || (stmts_.push_back(stmt) != common::OB_SUCCESS))
        {
          TBSYS_LOG(WARN, "fail to allocate space for stmt. %p", stmt);
          ret = common::OB_ERROR;
        }
        return ret;
      }

      int add_expr(ObSqlRawExpr* expr)
      {
        int ret = common::OB_SUCCESS;
        if ((!expr) || (exprs_.push_back(expr) != common::OB_SUCCESS))
        {
          TBSYS_LOG(WARN, "fail to allocate space for expr. %p", expr);
          ret = common::OB_ERROR;
        }
        return ret;
      }

      // Just a storage, only need to add raw expression
      int add_raw_expr(ObRawExpr* expr)
      {
        int ret = common::OB_SUCCESS;
        if ((!expr) || (raw_exprs_store_.push_back(expr) != common::OB_SUCCESS))
        {
          TBSYS_LOG(WARN, "fail to allocate space for raw expr. %p", expr);
          ret = common::OB_ERROR;
        }
        return ret;
      }

      int fill_result_set(ObResultSet& result_set, ObSQLSessionInfo *session_info, common::ObIAllocator &alloc);

      uint64_t generate_table_id()
      {
        return new_gen_tid_--;
      }

      uint64_t generate_column_id()
      {
        return new_gen_cid_--;
      }

      // It will reserve 10 id for the caller
      // In fact is for aggregate functions only,
      // because we need to push part aggregate to tablet and keep top aggregate on all
      uint64_t generate_range_column_id()
      {
        uint64_t ret_cid = new_gen_cid_;
        new_gen_cid_ -= 10;
        return ret_cid;
      }

      uint64_t generate_expr_id()
      {
        return new_gen_eid_++;
      }

      uint64_t generate_query_id()
      {
        return new_gen_qid_++;
      }

      int64_t generate_when_number()
      {
        return new_gen_wid_++;
      }

      int64_t inc_question_mark()
      {
        return question_marks_count_++;
      }

      int64_t get_question_mark_size() const
      {
        return question_marks_count_;
      }

      void set_cur_time_fun()
      {
        if (NO_CUR_TIME == cur_time_fun_type_)
        {
          cur_time_fun_type_ = CUR_TIME ;
        }
      }

      void set_cur_time_fun_ups()
      {
        cur_time_fun_type_ = CUR_TIME_UPS;
      }

      const ObCurTimeType get_cur_time_fun_type()
      {
        return cur_time_fun_type_;
      }

      int32_t get_stmts_count() const
      {
        return stmts_.size();
      }

      ObBasicStmt* get_stmt(int32_t index) const
      {
        OB_ASSERT(index >= 0 && index < get_stmts_count());
        return stmts_.at(index);
      }
      void print(FILE* fp = stderr, int32_t level = 0) const;

    protected:
      oceanbase::common::ObStringBuf* name_pool_;

    private:
      oceanbase::common::ObVector<ObBasicStmt*> stmts_;
      oceanbase::common::ObVector<ObSqlRawExpr*> exprs_;
      oceanbase::common::ObVector<ObRawExpr*> raw_exprs_store_;
      int64_t   question_marks_count_;
      ObCurTimeType cur_time_fun_type_;
      uint64_t  new_gen_tid_;
      uint64_t  new_gen_cid_;
      uint64_t  new_gen_qid_;
      uint64_t  new_gen_eid_;
      int64_t   new_gen_wid_;   // when number
    };
  }
}

#endif //OCEANBASE_SQL_LOGICALPLAN_H_
