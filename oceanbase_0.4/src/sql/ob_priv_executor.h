/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@alipay.com>
 */

#ifndef OB_PRIV_EXECUTOR_H_
#define OB_PRIV_EXECUTOR_H_

#include "common/ob_array.h"
#include "common/page_arena.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_basic_stmt.h"
#include "sql/ob_result_set.h"
#include "sql/ob_no_children_phy_operator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObPrivExecutor : public ObNoChildrenPhyOperator
    {
      public:
        ObPrivExecutor();
        virtual ~ObPrivExecutor();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        virtual ObPhyOperatorType get_type() const { return PHY_PRIV_EXECUTOR; }
        virtual int get_next_row(const ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        void set_stmt(const ObBasicStmt *stmt);
        void set_context(ObSqlContext *context);
      private:
        int do_revoke_privilege(const ObBasicStmt *stmt) ;
        int do_grant_privilege(const ObBasicStmt *stmt);
        int do_drop_user(const ObBasicStmt *stmt);
        int do_create_user(const ObBasicStmt *stmt);
        int do_lock_user(const ObBasicStmt *stmt);
        int do_set_password(const ObBasicStmt *stmt);
        int do_rename_user(const ObBasicStmt *stmt);
      private:
        struct UserInfo
        {
          UserInfo():password_(), comment_(){}
          // 存user_id, 11个权限和1个is_lock字段
          ObArray<int64_t> field_values_;
          ObString password_;
          ObString comment_;
        };
        const ObBasicStmt *stmt_;
        ObSqlContext *context_;
        ObResultSet *result_set_out_;
        common::PageArena<char> page_arena_;
      private:
        // 执行insert，delete，replace等不需要返回行的语句,result是整个权限语句的resultset，
        // 一旦stmt执行出错，用于接住stmt的错误信息
        int execute_stmt_no_return_rows(const ObString &stmt);
        int get_user_id_by_user_name(const ObString &user_name, int64_t &user_id);
        int get_table_ids_by_user_id(int64_t user_id, ObArray<uint64_t> &table_ids);
        int reset_table_privilege(int64_t user_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges);
        int revoke_all_priv_from_users_by_user_name(const ObString &user_name);
        int revoke_all_priv_from_table_privs_by_user_name(const ObString &user_name);
        int execute_delete_user(const ObString &delete_user);
        int execute_update_user(const ObString &update_user);
        int insert_trigger();
        void construct_update_expressions(char *buf, int buf_size, int64_t &pos, const ObArray<ObPrivilegeType> *privileges, bool is_grant);
        void construct_replace_stmt(char *buf, int buf_size, int64_t &pos, int64_t user_id, uint64_t table_id, const ObArray<ObPrivilegeType> *privileges);
        int get_all_columns_by_user_name(const ObString &user_name, UserInfo &user_info);
        int start_transaction();
        // warning:comit then replace into trigger event inner table
        int commit();
        int rollback();
    };
  }// namespace sql
} // namespace oceanbase
#endif
