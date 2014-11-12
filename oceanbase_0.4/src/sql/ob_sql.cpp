/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_values.h"
#include "sql/ob_sql.h"
#include "sql/parse_node.h"
#include "sql/build_plan.h"
#include "sql/ob_transformer.h"
#include "sql/ob_schema_checker.h"
#include "sql/parse_malloc.h"
#include "sql/ob_select_stmt.h"
#include "sql/ob_update_stmt.h"
#include "sql/ob_delete_stmt.h"
#include "common/ob_schema.h"
#include "common/ob_privilege.h"
#include "common/ob_array.h"
#include "common/ob_privilege_type.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_type.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_trace_id.h"
#include "common/ob_encrypted_helper.h"
#include "sql/ob_priv_executor.h"
#include "sql/ob_grant_stmt.h"
#include "sql/ob_create_user_stmt.h"
#include "sql/ob_drop_user_stmt.h"
#include "sql/ob_drop_table_stmt.h"
#include "sql/ob_revoke_stmt.h"
#include "sql/ob_lock_user_stmt.h"
#include "sql/ob_set_password_stmt.h"
#include "sql/ob_rename_user_stmt.h"
#include "sql/ob_show_stmt.h"
#include "common/ob_profile_fill_log.h"
#include "ob_start_trans.h"
#include "ob_end_trans.h"
#include "ob_ups_executor.h"
#include "ob_table_rpc_scan.h"
#include "ob_get_cur_time_phy_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

template <typename OperatorT>
static int init_hook_env(ObSqlContext &context, ObPhysicalPlan *&phy_plan, OperatorT *&op)
{
  int ret = OB_SUCCESS;
  phy_plan = NULL;
  op = NULL;
  StackAllocator& allocator = context.session_info_->get_transformer_mem_pool();
  void *ptr1 = allocator.alloc(sizeof(ObPhysicalPlan));
  void *ptr2 = allocator.alloc(sizeof(OperatorT));
  if (NULL == ptr1 || NULL == ptr2)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "fail to malloc memory for ObValues, op=%p", op);
  }
  else
  {
    op = new(ptr2) OperatorT();
    phy_plan = new(ptr1) ObPhysicalPlan();
    if (OB_SUCCESS != (ret = phy_plan->store_phy_operator(op)))
    {
      TBSYS_LOG(WARN, "failed to add operator, err=%d", ret);
      op->~OperatorT();
      phy_plan->~ObPhysicalPlan();
    }
    else if (OB_SUCCESS != (ret = phy_plan->add_phy_query(op, NULL, true)))
    {
      TBSYS_LOG(WARN, "failed to add query, err=%d", ret);
      phy_plan->~ObPhysicalPlan();
    }
    else
    {
      ob_inc_phy_operator_stat(op->get_type());
    }
  }
  return ret;
}

int ObSql::direct_execute(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  result.set_session(context.session_info_);
  // Step special: process some special statment here, like "show warning" etc
  if (OB_UNLIKELY(no_enough_memory()))
  {
    static int count = 0;
    TBSYS_LOG(WARN, "no memory");
    result.set_message("no memory");
    result.set_errcode(OB_ALLOCATE_MEMORY_FAILED);
    if (__sync_fetch_and_add(&count, 1) == 0)
    {
      ob_print_mod_memory_usage();
    }
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (true == process_special_stmt_hook(stmt, result, context))
  {
    if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
    {
      TBSYS_LOG(TRACE, "execute special sql statement success [%.*s]", stmt.length(), stmt.ptr());
    }
  }
  else
  {
    ResultPlan result_plan;
    ObMultiPhyPlan multi_phy_plan;
    ObMultiLogicPlan *multi_logic_plan = NULL;
    ObLogicalPlan *logic_plan = NULL;
    result_plan.is_prepare_ = context.is_prepare_protocol_ ? 1 : 0;
    //result.set_query_string(stmt);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "store cur query string failed ret=%d", ret);
    }
    else
    {
      ret = generate_logical_plan(stmt, context, result_plan, result);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "generate logical plan failed, ret=%d sql=%.*s", ret, stmt.length(), stmt.ptr());
      }
      else
      {
        TBSYS_LOG(DEBUG, "generate logical plan succ");
        multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
        logic_plan = multi_logic_plan->at(0);
        if (!context.disable_privilege_check_)
        {
          ret = do_privilege_check(context.session_info_->get_user_name(), context.pp_privilege_, logic_plan);
          if (OB_SUCCESS != ret)
          {
            result.set_message("no privilege");
            TBSYS_LOG(WARN, "no privilege,sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          ObStmt *dml_stmt = dynamic_cast<ObStmt*>(logic_plan->get_main_stmt());
          if (dml_stmt)
          {
            result.set_compound_stmt(dml_stmt->get_when_fun_size() > 0);
          }
          ObBasicStmt::StmtType stmt_type = logic_plan->get_main_stmt()->get_stmt_type();
          result.set_stmt_type(stmt_type);
          result.set_inner_stmt_type(stmt_type);
          if (NULL == context.transformer_allocator_)
          {
            OB_ASSERT(!context.is_prepare_protocol_);
            context.transformer_allocator_ = &context.session_info_->get_transformer_mem_pool();
          }
          if (OB_SUCCESS != (ret = logic_plan->fill_result_set(result, context.session_info_,
                                                               *context.transformer_allocator_)))
          {
            TBSYS_LOG(WARN, "fill result set failed,ret=%d", ret);
          }
          else if(OB_SUCCESS != (ret = generate_physical_plan(context, result_plan, multi_phy_plan, result)))
          {
            TBSYS_LOG(WARN, "generate physical plan failed, ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "generete physical plan success, sql=%.*s", stmt.length(), stmt.ptr());
            ObPhysicalPlan *phy_plan = multi_phy_plan.at(0);
            OB_ASSERT(phy_plan);
            if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
            {
              TBSYS_LOG(TRACE, "ExecutionPlan: \n%s", to_cstring(*phy_plan));
            }
            result.set_physical_plan(phy_plan, true);
            multi_phy_plan.clear();
          }
        }
        clean_result_plan(result_plan);
      }
    }
  }
  result.set_errcode(ret);
  return ret;
}

int ObSql::generate_logical_plan(const common::ObString &stmt, ObSqlContext & context, ResultPlan  &result_plan, ObResultSet & result)
{
  int ret = OB_SUCCESS;
  common::ObStringBuf &parser_mem_pool = context.session_info_->get_parser_mem_pool();
  ParseResult parse_result;
  static const int MAX_ERROR_LENGTH = 80;

  parse_result.malloc_pool_ = &parser_mem_pool;
  if (0 != (ret = parse_init(&parse_result)))
  {
    TBSYS_LOG(WARN, "parser init err, err=%s", strerror(errno));
    ret = OB_ERR_PARSER_INIT;
  }
  else
  {
    // generate syntax tree
    PFILL_ITEM_START(sql_to_logicalplan);
    FILL_TRACE_LOG("before_parse");
    if (parse_sql(&parse_result, stmt.ptr(), static_cast<size_t>(stmt.length())) != 0
      || NULL == parse_result.result_tree_)
    {
      TBSYS_LOG(WARN, "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], line_[%d], yycolumn[%d], yylineno_[%d]",
          parse_result.yyscan_info_,
          parse_result.result_tree_,
          parse_result.malloc_pool_,
          parse_result.error_msg_,
          parse_result.start_col_,
          parse_result.end_col_,
          parse_result.line_,
          parse_result.yycolumn_,
          parse_result.yylineno_);

      int64_t error_length = min(stmt.length() - (parse_result.start_col_ - 1), MAX_ERROR_LENGTH);
      snprintf(parse_result.error_msg_, MAX_ERROR_MSG,
          "You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use near '%.*s' at line %d", static_cast<int32_t>(error_length), stmt.ptr() + parse_result.start_col_ - 1, parse_result.line_);
      TBSYS_LOG(WARN, "failed to parse sql=%.*s err=%s", stmt.length(), stmt.ptr(), parse_result.error_msg_);
      result.set_message(parse_result.error_msg_);
      ret = OB_ERR_PARSE_SQL;
    }
    else if (NULL == context.schema_manager_)
    {
      TBSYS_LOG(WARN, "context.schema_manager_ is null");
      ret = OB_ERR_UNEXPECTED;
    }
    else
    {
      FILL_TRACE_LOG("parse");
      result_plan.name_pool_ = &parser_mem_pool;
      ObSchemaChecker *schema_checker =  (ObSchemaChecker*)parse_malloc(sizeof(ObSchemaChecker), result_plan.name_pool_);
      if (NULL == schema_checker)
      {
        TBSYS_LOG(WARN, "out of memory");
        ret = OB_ERR_PARSER_MALLOC_FAILED;
      }
      else
      {
        schema_checker->set_schema(*context.schema_manager_);
        result_plan.schema_checker_ = schema_checker;
        result_plan.plan_tree_ = NULL;
        // generate logical plan
        ret = resolve(&result_plan, parse_result.result_tree_);
        PFILL_ITEM_END(sql_to_logicalplan);
        FILL_TRACE_LOG("resolve");
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to generate logical plan, err=%d sql=%.*s result_plan.err_stat_.err_msg_=[%s]",
              ret, stmt.length(), stmt.ptr(), result_plan.err_stat_.err_msg_);
          result.set_message(result_plan.err_stat_.err_msg_);
        }
        else
        {
          ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
          ////TODO 为了prepare stmtname from select from test a 也使用psstore 需要记录下绑定的语句
          //ObLogicalPlan *logical_plan = NULL;
          //for (int32_t i = 0; ret = OB_SUCCESS && i < multi_logic_plan.size(); ++i)
          //{
          //  if (T_PREPARE == login_plan->get_main_stmt()->get_stmt_type())
          //  {
          //    //TODO
          //    //log sql to ob_prepare_stmt >>>> ob_prepare构造执行计划的时候可以去 ObPsStore里面找 在session上保存一个stmt_name->sql id的映射
          //    //ob_prepare  ob_execute连个操作符号需要修改
          //  }
          //}
          if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG))
          {
            multi_logic_plan->print();
          }
        }
      }
    }
  }
  // destroy syntax tree
  destroy_tree(parse_result.result_tree_);
  parse_terminate(&parse_result);
  result.set_errcode(ret);
  return ret;
}

void ObSql::clean_result_plan(ResultPlan &result_plan)
{
  ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
  multi_logic_plan->~ObMultiLogicPlan();
  destroy_plan(&result_plan);
}

int ObSql::generate_physical_plan(ObSqlContext & context, ResultPlan &result_plan, ObMultiPhyPlan & multi_phy_plan, ObResultSet & result)
{
  int ret = OB_SUCCESS;
  ObTransformer trans(context);
  ErrStat err_stat;
  ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
  PFILL_ITEM_START(logicalplan_to_physicalplan);
  if (OB_SUCCESS != (ret = trans.generate_physical_plans(*multi_logic_plan, multi_phy_plan, err_stat)))
  {
    TBSYS_LOG(WARN, "failed to transform to physical plan");
    result.set_message(err_stat.err_msg_);
  }
  else
  {
    PFILL_ITEM_END(logicalplan_to_physicalplan);
    for (int32_t i = 0; i < multi_phy_plan.size(); ++i)
    {
      multi_phy_plan.at(i)->set_result_set(&result);
    }
  }
  return ret;
}

int ObSql::stmt_prepare(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  bool do_prepare = false;
  ObPsStoreItem * item = NULL;
  ObPsStore *store = context.ps_store_;
  if (NULL == store)
  {
    TBSYS_LOG(ERROR, "ObPsStore in context is null");
    ret = OB_ERROR;
  }
  else
  {
    ret = store->get(stmt, item);  // rlock  防止在close得到写锁的时候还有线程可以得到这个item
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "stmt_prepare failed when get ObPsStoreItem ret=%d sql=\'%.*s\'", ret,
                stmt.length(), stmt.ptr());
    }
    else
    {
      item->unlock();
      result.set_stmt_type(ObBasicStmt::T_PREPARE);
      if (PS_ITEM_VALID == item->get_status())
      {
        TBSYS_LOG(DEBUG, "Get ObPsStore success stmt is %.*s ref count is %ld", stmt.length(),
                  stmt.ptr(), item->get_ps_count());
        if (need_rebuild_plan(context.schema_manager_, item))
        {
          //get wlock of item free old plan construct new plan if possible
          ret = try_rebuild_plan(stmt, result, context, item, do_prepare, false);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "can not rebuild prepare ret=%d", ret);
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "Latest table schema is same with phyplan in ObPsStore");
        }
        //END check schema
      }
      else if (PS_ITEM_INVALID == item->get_status())
      {
        TBSYS_LOG(DEBUG, "Get ObPsStore success but status PS_ITEM_INVALID stmt is %.*s ref count is %ld", stmt.length(),
                  stmt.ptr(), item->get_ps_count());
        //构造执行计划 copy data into psstore  result里面plan的内存还是用原来的方式管理
        ret = do_real_prepare(stmt, result, context, item, do_prepare);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "do_real_prepare failed, stmt is %.*s ret=%d", stmt.length(), stmt.ptr(), ret);
        }
      }
    }

    //read physical plan and copy data needed into result add read lock
    if (!do_prepare && OB_SUCCESS == ret)
    {
      ret = store->get_plan(item->get_sql_id(), item);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "never reach here ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "Get plan succ sql_id is %ld, ref count is %ld",
                  item->get_sql_id(), item->get_ps_count());
        uint64_t new_stmt_id = 0;
        if (OB_SUCCESS != (ret = context.session_info_->store_ps_session_info(item, new_stmt_id)))
        {
          TBSYS_LOG(WARN, "Store result set to session failed ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = copy_plan_from_store(&result, item, &context, new_stmt_id)))
        {
          TBSYS_LOG(WARN, "copy plan to ObPsStore failed ret=%d", ret);
        }
        result.set_plan_from_assign(true);
        result.set_statement_id(new_stmt_id);
        result.set_sql_id(item->get_sql_id());
        item->unlock(); //unlock whaterver
      }
    }
  }
  result.set_errcode(ret);
  return ret;
}

int ObSql::do_real_prepare(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context, ObPsStoreItem *item, bool &do_prepare)
{
  int ret = OB_SUCCESS;
  do_prepare = false;
  int64_t trywcount = 0;
  while(OB_SUCCESS == ret)
  {
    trywcount++;
    if (OB_UNLIKELY(0 == trywcount % 100))
    {
      TBSYS_LOG(ERROR, "try wlock when do real prepare %ld times sql=\"%.*s\"stmt_id=%ld", trywcount,
                stmt.length(), stmt.ptr(), item->get_sql_id());
    }
    if (item->is_valid())
    {
      ret = OB_SUCCESS;
      break;
    }
    else
    {
      if (0 == item->try_wlock())
      {
        //do prepare  result的内存在拷贝到ObPsStore 并返回数据给客户端后可以释放掉 这里使用的是pagearena
        TBSYS_LOG(DEBUG, "do real prepare %.*s stmt_id=%ld", stmt.length(), stmt.ptr(), item->get_sql_id());
        do_prepare = true;
        result.set_statement_id(item->get_sql_id()); //stmt_id equals sql_id when sql first prepared in one session
        result.set_sql_id(item->get_sql_id()); //stmt_id equals sql_id when sql first prepared in one session
        result.set_session(context.session_info_);
        ObArenaAllocator *allocator = NULL;
        context.is_prepare_protocol_ = true;
        if (OB_UNLIKELY(no_enough_memory()))
        {
          TBSYS_LOG(WARN, "no memory");
          result.set_message("no memory");
          result.set_errcode(OB_ALLOCATE_MEMORY_FAILED);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == context.session_info_)
        {
          TBSYS_LOG(WARN, "context.session_info_(null)");
          ret = OB_NOT_INIT;
        }
        else if (NULL == (allocator = context.session_info_->get_transformer_mem_pool_for_ps()))
        {
          TBSYS_LOG(WARN, "failed to get new allocator");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          OB_ASSERT(NULL == context.transformer_allocator_);
          context.transformer_allocator_ = allocator;
          // the transformer's allocator is owned by the result set now, and will be free by the result set
          result.set_ps_transformer_allocator(allocator);

          if (OB_SUCCESS != (ret = direct_execute(stmt, result, context)))
          {
            TBSYS_LOG(WARN, "direct execute failed");
          }
          else if (OB_SUCCESS != (ret = context.session_info_->store_ps_session_info(result)))
          {
            TBSYS_LOG(WARN, "Store result set to session failed");
          }
        }

        if (OB_SUCCESS == ret)
        {
          result.set_stmt_type(ObBasicStmt::T_PREPARE);
          result.set_errcode(ret);
          item->store_ps_sql(stmt);
          TBSYS_LOG(DEBUG, "ExecutionPlan in result: \n%s", to_cstring(*result.get_physical_plan()));
          //copy field/plan from resultset to item
          ret = copy_plan_to_store(&result, item);
          TBSYS_LOG(DEBUG, "ExecutionPlan in item: \n%s", to_cstring(item->get_physical_plan()));
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "copy phy paln from resultset to PbPsStoreItem failed ret=%d", ret);
            //assume prepare success will redo real_prepare next and copy to ObPsStore
            ret = OB_SUCCESS;
          }
          else
          {
            item->set_schema_version(context.schema_manager_->get_version());//for check schema
            item->set_status(PS_ITEM_VALID);
          }
        }
        else //build plan failed dec ps count beacuse ps count inc when get ObPsStoreItem
        {
          item->dec_ps_count();
        }
        item->unlock();
        break;
      }
    }
  }
  return ret;
}

int ObSql::stmt_execute(const uint64_t stmt_id,
                        const common::ObIArray<obmysql::EMySQLFieldType> &params_type,
                        const common::ObIArray<common::ObObj> &params,
                        ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  bool rebuild_plan = false;
  uint64_t inner_stmt_id = stmt_id;
  result.set_session(context.session_info_);
  ObPsStoreItem *item = NULL;
  ObPsSessionInfo *info = NULL;
  if (OB_UNLIKELY(no_enough_memory()))
  {
    static int count = 0;
    if (__sync_fetch_and_add(&count, 1) == 0)
    {
      ob_print_mod_memory_usage();
    }
    TBSYS_LOG(WARN, "no memory");
    result.set_message("no memory");
    result.set_errcode(OB_ALLOCATE_MEMORY_FAILED);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL == context.session_info_)
  {
    TBSYS_LOG(WARN, "context.session_info_(null)");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = context.session_info_->get_ps_session_info(stmt_id, info)))
  {
    TBSYS_LOG(WARN, "can not get ObPsSessionInfo from id_psinfo_map stmt_id = %ld, ret=%d", stmt_id, ret);
  }
  else
  {
    result.set_sql_id(info->sql_id_);
  }

  if (OB_SUCCESS == ret)
  {
    inner_stmt_id = info->sql_id_;
    if(OB_SUCCESS != context.ps_store_->get_plan(inner_stmt_id, item))
    {
      ret = OB_ERR_PREPARE_STMT_UNKNOWN;
      TBSYS_LOG(USER_ERROR, "statement not prepared, stmt_id=%lu ret=%d", inner_stmt_id, ret);
    }
    else
    {
      //check schema
      if (need_rebuild_plan(context.schema_manager_, item))
      {
        item->unlock();
        ret = try_rebuild_plan(item->get_ps_sql(), result, context, item, rebuild_plan, true);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "rebuild plan failed stmt_id=%lu, schema_version=%ld", inner_stmt_id, item->get_schema_version());
          //ret = stmt_close(inner_stmt_id, context);
        }
        else
        {
          TBSYS_LOG(DEBUG, "rebuild plan success stmt_id=%lu, schema_version=%ld", inner_stmt_id, item->get_schema_version());
        }
      }
      //end check schema
      else
      {
        if (OB_SUCCESS != (ret=copy_plan_from_store(&result, item, &context, stmt_id)))
        {
          TBSYS_LOG(ERROR, "Copy plan from ObPsStore to  result set failed ret=%d", ret);
        }
        item->unlock();           // rlocked when get_plan
      }
      if (OB_SUCCESS == ret)
      {
        //for show processlist do_com_execute show ori prepare sql
        if(OB_SUCCESS != context.session_info_->store_query_string(item->get_ps_sql()))
        {
          TBSYS_LOG(WARN, "failed to store cur query string ret=%d", ret);
        }
        else if(OB_SUCCESS != context.session_info_->store_params_type(stmt_id, params_type))
        {
          ret = OB_ERR_PREPARE_STMT_UNKNOWN;
          TBSYS_LOG(USER_ERROR, "statement not prepared, stmt_id=%lu ret=%d", stmt_id, ret);
        }
        // set running param values
        else if (OB_SUCCESS != (ret = result.fill_params(params_type, params)))
        {
          TBSYS_LOG(WARN, "Incorrect arguments to EXECUTE ret=%d", ret);
        }
        else
        {
          if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
          {
            TBSYS_LOG(TRACE, "ExecutionPlan: \n%s", to_cstring(*result.get_physical_plan()->get_main_query()));
          }
        }
        result.set_statement_id(stmt_id);
        if (!rebuild_plan)
        {
          result.set_plan_from_assign(true);
        }
        TBSYS_LOG(DEBUG, "stmt execute end print field info");
        //TBSYS_LOG(DEBUG, "%s", to_cstring(result.get_field_columns()));
        result.set_stmt_type(ObBasicStmt::T_NONE);
        //store errcode in resultset
        result.set_errcode(ret);
      }
    }
  }
  result.set_errcode(ret);
  return ret;
}

int ObSql::stmt_close(const uint64_t stmt_id, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  uint64_t inner_sql_id = stmt_id;
  ObPsSessionInfo *info = NULL;
  if (NULL == context.session_info_)
  {
    TBSYS_LOG(WARN, "context.session_info_(null)");
    ret = OB_NOT_INIT;
  }
  else if(OB_SUCCESS != (ret = context.session_info_->get_ps_session_info(stmt_id, info)))
  {
    TBSYS_LOG(WARN, "can not get ObPsSessionInfo from id_psinfo_map stmt_id = %ld, ret=%d", stmt_id, ret);
  }
  if (OB_SUCCESS == ret)
  {
    inner_sql_id = info->sql_id_;
    if(OB_SUCCESS != (ret = context.ps_store_->remove_plan(inner_sql_id))) //从全局池里面删除执行计划
    {
      TBSYS_LOG(WARN, "close prepared statement failed, sql_id=%lu", inner_sql_id);
    }
    else if(OB_SUCCESS != (ret = context.session_info_->remove_ps_session_info(stmt_id)))//删除session中stmtid对应的ObPsSessionInfo
    {
      TBSYS_LOG(WARN, "remove ps session info failed, stmt_id=%lu", stmt_id);
    }
    else
    {
      TBSYS_LOG(DEBUG, "close stmt, stmt_id=%lu sql_id=%ld", stmt_id, inner_sql_id);
    }
  }
  return ret;
}

bool ObSql::process_special_stmt_hook(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  const char *select_collation = (const char *)"SHOW COLLATION";
  int64_t select_collation_len = strlen(select_collation);
  const char *show_charset = (const char *)"SHOW CHARACTER SET";
  int64_t show_charset_len = strlen(show_charset);
  // SET NAMES latin1/gb2312/utf8/etc...
  const char *set_names = (const char *)"SET NAMES ";
  int64_t set_names_len = strlen(set_names);
  const char *set_session_transaction_isolation = (const char *)"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED";
  int64_t set_session_transaction_isolation_len = strlen(set_session_transaction_isolation);

  ObRow row;
  ObRowDesc row_desc;
  ObValues *op = NULL;
  ObPhysicalPlan *phy_plan = NULL;

  if (stmt.length() >= select_collation_len && 0 == strncasecmp(stmt.ptr(), select_collation, select_collation_len))
  {
    /*
       mysql> SHOW COLLATION;
        +------------------------+----------+-----+---------+----------+---------+
        | Collation              | Charset  | Id  | Default | Compiled | Sortlen |
        +------------------------+----------+-----+---------+----------+---------+
        | big5_chinese_ci    | big5       |   1 | Yes       | Yes         |       1 |
        | ...                        | ...          |  ... |   ...      | ...            |       ... |
       +------------------------+----------+-----+---------+----------+---------+
       */
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {

      // construct table header
      ObResultSet::Field field;
      ObString tname = ObString::make_string("tmp_table");
      field.tname_ = tname;
      field.org_tname_ = tname;
      ObString cname[6];
      cname[0] = ObString::make_string("Collation");
      cname[1] = ObString::make_string("Charset");
      cname[2] = ObString::make_string("Id");
      cname[3] = ObString::make_string("Default");
      cname[4] = ObString::make_string("Compiled");
      cname[5] = ObString::make_string("Sortlen");
      ObObjType type[6];
      type[0] = ObVarcharType;
      type[1] = ObVarcharType;
      type[2] = ObIntType;
      type[3] = ObVarcharType;
      type[4] = ObVarcharType;
      type[5] = ObIntType;
      for (int i = 0; i < 6; i++)
      {
        field.cname_ = cname[i];
        field.org_cname_ = cname[i];
        field.type_.set_type(type[i]);
        if (OB_SUCCESS != (ret = result.add_field_column(field)))
        {
          TBSYS_LOG(WARN, "fail to add field column %d", i);
          break;
        }
      }

      // construct table body
      for (int i = 0; i < 6; ++i)
      {
        ret = row_desc.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      row.set_row_desc(row_desc);
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
      // | binary               | binary   |  63 | Yes     | Yes      |       1 |
      ObObj cells[6];
      ObString cell0 = ObString::make_string("binary");
      cells[0].set_varchar(cell0);
      ObString cell1 = ObString::make_string("binary");
      cells[1].set_varchar(cell1);
      cells[2].set_int(63);
      ObString cell3 = ObString::make_string("Yes");
      cells[3].set_varchar(cell3);
      ObString cell4 = ObString::make_string("Yes");
      cells[4].set_varchar(cell4);
      cells[5].set_int(1);
      ObRow one_row;
      one_row.set_row_desc(row_desc);
      for (int i = 0; i < 6; ++i)
      {
        ret = one_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i, cells[i]);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      ret = op->add_values(one_row);
      OB_ASSERT(OB_SUCCESS == ret);
    }
  }
  else if (stmt.length() >= show_charset_len && 0 == strncasecmp(stmt.ptr(), show_charset, show_charset_len))
  {
    /*
       mysql> SHOW CHARACTER SET
       +----------+-----------------------------+---------------------+--------+
       | Charset  | Description                 | Default collation   | Maxlen |
       +----------+-----------------------------+---------------------+--------+
       | binary   | Binary pseudo charset       | binary              |      1 |
       +----------+-----------------------------+---------------------+--------+
    */
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {

      // construct table header
      ObResultSet::Field field;
      ObString tname = ObString::make_string("tmp_table");
      field.tname_ = tname;
      field.org_tname_ = tname;
      ObString cname[4];
      cname[0] = ObString::make_string("Charset");
      cname[1] = ObString::make_string("Description");
      cname[2] = ObString::make_string("Default collation");
      cname[3] = ObString::make_string("Maxlen");
      ObObjType type[4];
      type[0] = ObVarcharType;
      type[1] = ObVarcharType;
      type[2] = ObVarcharType;
      type[3] = ObIntType;
      for (int i = 0; i < 4; i++)
      {
        field.cname_ = cname[i];
        field.org_cname_ = cname[i];
        field.type_.set_type(type[i]);
        if (OB_SUCCESS != (ret = result.add_field_column(field)))
        {
          TBSYS_LOG(WARN, "fail to add field column %d", i);
          break;
        }
      }

      // construct table body
      for (int i = 0; i < 4; ++i)
      {
        ret = row_desc.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      row.set_row_desc(row_desc);
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
      // | binary   | Binary pseudo charset       | binary              |      1 |
      ObObj cells[4];
      ObString cell0 = ObString::make_string("binary");
      cells[0].set_varchar(cell0);
      ObString cell1 = ObString::make_string("Binary pseudo charset");
      cells[1].set_varchar(cell1);
      ObString cell2 = ObString::make_string("binary");
      cells[2].set_varchar(cell2);
      cells[3].set_int(1);
      ObRow one_row;
      one_row.set_row_desc(row_desc);
      for (int i = 0; i < 4; ++i)
      {
        ret = one_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i, cells[i]);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      ret = op->add_values(one_row);
      OB_ASSERT(OB_SUCCESS == ret);
    }
  }
  else if (stmt.length() >= set_names_len && 0 == strncasecmp(stmt.ptr(), set_names, set_names_len))
  {
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {
      // SET NAMES ...
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
    }
  }
  else if (stmt.length() >= set_session_transaction_isolation_len &&
      0 == strncasecmp(stmt.ptr(), set_session_transaction_isolation, set_session_transaction_isolation_len))
  {
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {
      // SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
    }
  }
  else
  {
    ret = OB_NOT_SUPPORTED;
  }

  if (OB_SUCCESS != ret)
  {
    if (NULL != phy_plan)
    {
      phy_plan->~ObPhysicalPlan(); // will destruct op automatically
    }
  }
  else
  {
    result.set_physical_plan(phy_plan, true);
  }
  return (OB_SUCCESS == ret);
}

int ObSql::do_privilege_check(const ObString & username, const ObPrivilege **pp_privilege, ObLogicalPlan *plan)
{
  int err = OB_SUCCESS;
  ObBasicStmt *stmt = NULL;
  ObArray<ObPrivilege::TablePrivilege> table_privileges;
  for (int32_t i = 0;i < plan->get_stmts_count(); ++i)
  {
    stmt = plan->get_stmt(i);
    switch (stmt->get_stmt_type())
    {
      case ObBasicStmt::T_SELECT:
        {
          ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
          if (OB_UNLIKELY(NULL == select_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObSelectStmt::SetOperator set_op  = select_stmt->get_set_op();
            if (set_op != ObSelectStmt::NONE)
            {
              continue;
            }
            else
            {
              int32_t table_item_size = select_stmt->get_table_size();
              for (int32_t j = 0; j < table_item_size; ++j)
              {
                ObPrivilege::TablePrivilege table_privilege;
                const TableItem &table_item = select_stmt->get_table_item(j);
                uint64_t table_id = OB_INVALID_ID;
                if (table_item.type_ == TableItem::BASE_TABLE || table_item.type_ == TableItem::ALIAS_TABLE)
                {
                  table_id = table_item.ref_id_;
                }
                else
                {
                  continue;
                }
                table_privilege.table_id_ = table_id;
                OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_SELECT));
                err = table_privileges.push_back(table_privilege);
                if (OB_UNLIKELY(OB_SUCCESS != err))
                {
                  TBSYS_LOG(WARN, "push table_privilege to array failed,err=%d", err);
                }
              }
            }
          }
          break;
        }
      case ObBasicStmt::T_INSERT:
      case ObBasicStmt::T_REPLACE:
        {
          ObInsertStmt *insert_stmt = dynamic_cast<ObInsertStmt*>(stmt);
          if (OB_UNLIKELY(NULL == insert_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            table_privilege.table_id_ = insert_stmt->get_table_id();
            if (!insert_stmt->is_replace())
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_INSERT));
            }
            else
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_REPLACE));
            }
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_UPDATE:
        {
          ObUpdateStmt *update_stmt = dynamic_cast<ObUpdateStmt*>(stmt);
          if (OB_UNLIKELY(NULL == update_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            table_privilege.table_id_ = update_stmt->get_update_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_DELETE:
        {
          ObDeleteStmt *delete_stmt = dynamic_cast<ObDeleteStmt*>(stmt);
          if (OB_UNLIKELY(NULL == delete_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            table_privilege.table_id_ = delete_stmt->get_delete_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_DELETE));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_GRANT:
        {
          OB_STAT_INC(SQL, SQL_GRANT_PRIVILEGE_COUNT);

          ObGrantStmt *grant_stmt = dynamic_cast<ObGrantStmt*>(stmt);
          if (OB_UNLIKELY(NULL == grant_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            // if grant priv_xx* on * then table_id == 0
            // if grant priv_xx* on table_name
            table_privilege.table_id_ = grant_stmt->get_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_GRANT_OPTION));
            const common::ObArray<ObPrivilegeType> *privileges = grant_stmt->get_privileges();
            int i = 0;
            for (i = 0;i < privileges->count();++i)
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(privileges->at(i)));
            }
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_REVOKE:
        {
          OB_STAT_INC(SQL, SQL_REVOKE_PRIVILEGE_COUNT);

          ObRevokeStmt *revoke_stmt = dynamic_cast<ObRevokeStmt*>(stmt);
          if (OB_UNLIKELY(NULL == revoke_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            // if revoke priv_xx* on * from user, then table_id == 0
            // elif revoke priv_xx* on table_name from user, then table_id != 0 && table_id != OB_INVALID_ID
            // elif revoke ALL PRIVILEGES, GRANT OPTION from user, then table_id == OB_INVALID_ID
            table_privilege.table_id_ = revoke_stmt->get_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_GRANT_OPTION));
            const common::ObArray<ObPrivilegeType> *privileges = revoke_stmt->get_privileges();
            int i = 0;
            for (i = 0;i < privileges->count();++i)
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(privileges->at(i)));
            }
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_CREATE_USER:
      case ObBasicStmt::T_DROP_USER:
        {
          ObPrivilege::TablePrivilege table_privilege;
          if (ObBasicStmt::T_CREATE_USER == stmt->get_stmt_type())
            OB_STAT_INC(SQL, SQL_CREATE_USER_COUNT);
          else if (ObBasicStmt::T_DROP_USER == stmt->get_stmt_type())
            OB_STAT_INC(SQL, SQL_DROP_USER_COUNT);

          // create user是全局权限
          table_privilege.table_id_ = OB_NOT_EXIST_TABLE_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_CREATE_USER));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_LOCK_USER:
        {
          ObPrivilege::TablePrivilege table_privilege;
          OB_STAT_INC(SQL, SQL_LOCK_USER_COUNT);

          table_privilege.table_id_ = OB_USERS_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_SET_PASSWORD:
        {
          OB_STAT_INC(SQL, SQL_SET_PASSWORD_COUNT);

          ObSetPasswordStmt *set_pass_stmt = dynamic_cast<ObSetPasswordStmt*>(stmt);
          if (OB_UNLIKELY(NULL == set_pass_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            const common::ObStrings* user_pass = set_pass_stmt->get_user_password();
            ObString user;
            err  = user_pass->get_string(0,user);
            OB_ASSERT(OB_SUCCESS == err);
            if (user.length() == 0)
            {
              TBSYS_LOG(DEBUG, "EMPTY");
              // do nothing
            }
            else
            {
              ObPrivilege::TablePrivilege table_privilege;
              table_privilege.table_id_ = OB_USERS_TID;
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
              err = table_privileges.push_back(table_privilege);
              if (OB_UNLIKELY(OB_SUCCESS != err))
              {
                TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
              }
            }
          }
          break;
        }
      case  ObBasicStmt::T_RENAME_USER:
        {
          ObPrivilege::TablePrivilege table_privilege;

          OB_STAT_INC(SQL, SQL_RENAME_USER_COUNT);
          table_privilege.table_id_ = OB_USERS_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_CREATE_TABLE:
        {
          ObPrivilege::TablePrivilege table_privilege;
          OB_STAT_INC(SQL, SQL_CREATE_TABLE_COUNT);
          // create table 是全局权限
          table_privilege.table_id_ = OB_NOT_EXIST_TABLE_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_CREATE));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_DROP_TABLE:
        {
          OB_STAT_INC(SQL, SQL_DROP_TABLE_COUNT);
          ObDropTableStmt *drop_table_stmt = dynamic_cast<ObDropTableStmt*>(stmt);
          if (OB_UNLIKELY(NULL == drop_table_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            int64_t i = 0;
            for (i = 0;i < drop_table_stmt->get_table_count();++i)
            {
              ObPrivilege::TablePrivilege table_privilege;
              // drop table 不是全局权限
              table_privilege.table_id_ = drop_table_stmt->get_table_id(i);
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_DROP));
              err = table_privileges.push_back(table_privilege);
              if (OB_UNLIKELY(OB_SUCCESS != err))
              {
                TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
              }
            }
          }
          break;
        }
      case ObBasicStmt::T_SHOW_GRANTS:
        {
          ObShowStmt *show_grant_stmt = dynamic_cast<ObShowStmt*>(stmt);
          ObString user_name = show_grant_stmt->get_user_name();
          ObPrivilege::TablePrivilege table_privilege;
          ObPrivilege::TablePrivilege table_privilege2;
          OB_STAT_INC(SQL, SQL_SHOW_GRANTS_COUNT);
          // show grants for user
          if (user_name.length() > 0)
          {
            table_privilege.table_id_ = OB_USERS_TID;
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_SELECT));
            table_privilege2.table_id_ = OB_TABLE_PRIVILEGES_TID;
            OB_ASSERT(true == table_privilege2.privileges_.add_member(OB_PRIV_SELECT));
            if (OB_SUCCESS != (err = table_privileges.push_back(table_privilege)))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
            else if (OB_SUCCESS != (err = table_privileges.push_back(table_privilege2)))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          else
          {
            // show grants 当前用户,不需要权限
          }
          break;
        }
      default:
        err = OB_ERR_NO_PRIVILEGE;
        break;
    }
  }
  err = (*pp_privilege)->has_needed_privileges(username, table_privileges);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "username %.*s don't have enough privilege,err=%d", username.length(), username.ptr(), err);
  }
  else
  {
    TBSYS_LOG(DEBUG, "%.*s do privilege check success", username.length(), username.ptr());
  }
  return err;
}

bool ObSql::no_enough_memory()
{
  static const int64_t reserved_mem_size = 512*1024*1024LL; // 512MB
  bool ret = (ob_get_memory_size_limit() > reserved_mem_size)
    && (ob_get_memory_size_limit() - ob_get_memory_size_used() < reserved_mem_size);
  if (OB_UNLIKELY(ret))
  {
    TBSYS_LOG(WARN, "not enough memory, limit=%ld used=%ld",
              ob_get_memory_size_limit(), ob_get_memory_size_used());
    ob_print_mod_memory_usage();
    ob_print_phy_operator_stat();
  }
  return ret;
}

int ObSql::copy_plan_from_store(ObResultSet *result, ObPsStoreItem *item, ObSqlContext *context, uint64_t sql_id)
{
  int ret = OB_SUCCESS;
  if (NULL == result || NULL == item)
  {
    TBSYS_LOG(ERROR, "invalid argument result is %p, item is %p", result, item);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObPsStoreItemValue *value = item->get_item_value();
    if (NULL != value)
    {
      ObPhysicalPlan *phy_plan = ObPhysicalPlan::alloc();
      if (NULL == phy_plan)
      {
        TBSYS_LOG(ERROR, "can not alloc mem for ObPhysicalPlan");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        TBSYS_LOG(DEBUG, "copy from store ob_malloc plan is %p store item %p, store plan %p", phy_plan, item, &item->get_physical_plan());
        phy_plan->set_result_set(result);
        ret = copy_physical_plan(*phy_plan, value->plan_, context);//phy_plan->assign(value->plan_);
        if (OB_SUCCESS != ret)
        {
          phy_plan->clear();
          ObPhysicalPlan::free(phy_plan);
          TBSYS_LOG(ERROR, "Copy Physical plan from ObPsStoreItem to Current ResultSet failed ret=%d", ret);
        }
        else
        {
          result->set_physical_plan(phy_plan, true);
          result->set_session(context->session_info_);

          ObPsSessionInfo *info = NULL;
          if (OB_SUCCESS != (ret = context->session_info_->get_ps_session_info(sql_id, info)))
          {
            TBSYS_LOG(WARN, "Get ps session info failed sql_id=%lu, ret=%d", sql_id, ret);
          }
          else if (OB_SUCCESS != (ret = result->from_store(value, info)))
          {
            TBSYS_LOG(WARN, "Assemble Result from ObPsStoreItem(%p) and ObPsSessionInfo(%p) failed, ret=%d",
                      value, info, ret);
          }
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "ObPsStoreItemValue is null");
      ret = OB_ERROR;
    }
  }
  return ret;
}

int ObSql::copy_plan_to_store(ObResultSet *result, ObPsStoreItem *item)
{
  int ret = OB_SUCCESS;
  if (NULL == result || NULL == item)
  {
    TBSYS_LOG(ERROR, "invalid argument result is %p, item is %p", result, item);
    ret = OB_ERROR;
  }
  else
  {
    ObPhysicalPlan *plan = result->get_physical_plan();
    ObPsStoreItemValue *value = item->get_item_value();
    if (NULL != plan && NULL != value)
    {
      ObPhysicalPlan *storeplan = &item->get_physical_plan();
      ret = storeplan->assign(*plan);
      if (OB_SUCCESS == ret)
      {
        value->field_columns_.clear();
        ObResultSet::Field field;
        ObResultSet::Field ofield;
        for (int i = 0; OB_SUCCESS == ret && i < result->get_field_columns().count(); ++i)
        {
          ofield = result->get_field_columns().at(i);
          ret = field.deep_copy(ofield, &value->str_buf_);
          if (OB_SUCCESS == ret)
          {
            value->field_columns_.push_back(field);
          }
          else
          {
            TBSYS_LOG(WARN, "deep copy field failed, ret=%d", ret);
          }
        }

        value->param_columns_.clear();
        for (int i = 0; OB_SUCCESS == ret && i < result->get_param_columns().count(); ++i)
        {
          ofield = result->get_param_columns().at(i);
          ret = field.deep_copy(ofield, &value->str_buf_);
          if (OB_SUCCESS == ret)
          {
            value->param_columns_.push_back(field);
          }
          else
          {
            TBSYS_LOG(WARN, "deep copy parma failed, ret=%d", ret);
          }
        }
        if (OB_SUCCESS == ret)
        {
          value->param_columns_ = result->get_param_columns();
//        value->stmt_type_ = result->get_stmt_type();
          value->inner_stmt_type_ = result->get_inner_stmt_type();
          value->compound_ = result->is_compound_stmt();
        }

        if (NULL != result->get_cur_time_place())
        {
          value->has_cur_time_ = true;
        }
      }
      else
      {
        storeplan->clear();
        TBSYS_LOG(ERROR, "Copy Physical Plan from ResultSet to ObPsStoreItem failed");
        ret = OB_ERROR;
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "PhysicalPlan is null in ObPsStroreItemg");
    }
  }
  return ret;
}

int ObSql::try_rebuild_plan(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context, ObPsStoreItem *item, bool &flag, bool substitute)
{
  int ret = OB_SUCCESS;
  flag = false;
  TBSYS_LOG(DEBUG, "call try rebuild plan");
  if (0 == item->wlock())//wait for wlock
  {
    TBSYS_LOG(DEBUG, "get wlock");
    if (item->is_valid() && !need_rebuild_plan(context.schema_manager_, item))
    {
      ret = OB_SUCCESS;
      TBSYS_LOG(INFO, "other thread rebuild plan already");
    }
    else
    {
      flag = true;
      result.set_statement_id(item->get_sql_id());
      result.set_sql_id(item->get_sql_id());
      result.set_session(context.session_info_);
      ObArenaAllocator *allocator = NULL;
      context.is_prepare_protocol_ = true;
      if (OB_UNLIKELY(no_enough_memory()))
      {
        TBSYS_LOG(WARN, "no memory");
        result.set_message("no memory");
        result.set_errcode(OB_ALLOCATE_MEMORY_FAILED);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (NULL == context.session_info_)
      {
        TBSYS_LOG(WARN, "context.session_info_(null)");
        ret = OB_NOT_INIT;
      }
      else if (NULL == (allocator = context.session_info_->get_transformer_mem_pool_for_ps()))
      {
        TBSYS_LOG(WARN, "failed to get new allocator");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        OB_ASSERT(NULL == context.transformer_allocator_);
        context.transformer_allocator_ = allocator;
        // the transformer's allocator is owned by the result set now, and will be free by the result set
        result.set_ps_transformer_allocator(allocator);
        if (OB_SUCCESS != (ret = direct_execute(stmt, result, context)))
        {
          TBSYS_LOG(WARN, "direct execute failed");
          TBSYS_LOG(WARN, "try rebuild plan failed ret is %d", ret);
          item->set_status(PS_ITEM_EXPIRED);
        }

        if (OB_SUCCESS == ret)
        {
          item->clear();
          result.set_errcode(ret);
          if (!substitute)
          {
            //insert ps session
            ret = context.session_info_->store_ps_session_info(result);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "store ps session info failed ret=%d", ret);
            }
          }

          if (OB_SUCCESS == ret)
          {
            //copy plan from resultset to item
            ret = copy_plan_to_store(&result, item);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "copy phy paln from resultset to PbPsStoreItem failed ret=%d", ret);
            }
            else
            {
              item->set_status(PS_ITEM_VALID);
            }
          }

        }
      }
    }
    if (NULL != result.get_physical_plan())
    {
      TBSYS_LOG(DEBUG, "new physical plan is %s", to_cstring(*result.get_physical_plan()));
    }
    item->unlock();//unlock whatever happen
  }
  TBSYS_LOG(DEBUG, "end call try rebuild plan");
  return ret;
}

int ObSql::copy_physical_plan(ObPhysicalPlan& new_plan, ObPhysicalPlan& old_plan, ObSqlContext *context)
{
  int ret = OB_SUCCESS;
  if ((ret = new_plan.assign(old_plan)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Assign physical plan failed, ret=%d", ret);
  }
  else if (context && (ret = set_execute_context(new_plan, *context)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Set execute context failed, ret=%d", ret);
  }
  return ret;
}

int ObSql::set_execute_context(ObPhysicalPlan& plan, ObSqlContext& context)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; ret == OB_SUCCESS && i < plan.get_operator_size(); i++)
  {
    ObPhyOperator *op = plan.get_phy_operator(i);
    TBSYS_LOG(DEBUG, "plan %ld op is type=%s %p", i, ob_phy_operator_type_str(op->get_type()), op);
    if (!op)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "Wrong physical plan, ret=%d, operator idx=%ld", ret, i);
    }
    switch (op->get_type())
    {
      case PHY_UPS_EXECUTOR:
      {
        ObUpsExecutor *exe_op = dynamic_cast<ObUpsExecutor*>(op);
        ObPhysicalPlan *inner_plan = exe_op->get_inner_plan();
        if (!inner_plan)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "Empty inner plan of ObUpsExecutor, ret=%d", ret);
        }
        else
        {
          exe_op->set_rpc_stub(context.merger_rpc_proxy_);
          ret = ObSql::set_execute_context(*inner_plan, context);
        }
        break;
      }
      case PHY_START_TRANS:
      {
        ObStartTrans *start_op = dynamic_cast<ObStartTrans*>(op);
        start_op->set_rpc_stub(context.merger_rpc_proxy_);
        break;
      }
      case PHY_END_TRANS:
      {
        ObEndTrans *end_op = dynamic_cast<ObEndTrans*>(op);
        end_op->set_rpc_stub(context.merger_rpc_proxy_);
        break;
      }
      case PHY_TABLE_RPC_SCAN:
      {
        ObTableRpcScan *table_rpc_op = dynamic_cast<ObTableRpcScan*>(op);
        table_rpc_op->init(&context);
        break;
      }
      case PHY_CUR_TIME:
      {
        ObGetCurTimePhyOperator *get_cur_time_op = dynamic_cast<ObGetCurTimePhyOperator*>(op);
        get_cur_time_op->set_rpc_stub(context.merger_rpc_proxy_);
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

bool ObSql::need_rebuild_plan(const common::ObSchemaManagerV2 *schema_manager, ObPsStoreItem *item)
{
  bool ret = false;
  if (NULL == schema_manager || NULL == item)
  {
    TBSYS_LOG(WARN, "invalid argument schema_manager=%p, item=%p", schema_manager, item);
  }
  else
  {
    ObPhysicalPlan &plan = item->get_physical_plan();
    int64_t tcount = plan.get_base_table_version_count();
    TBSYS_LOG(DEBUG, "table_version_count=%ld", tcount);
    for (int64_t idx = 0; idx < tcount; ++idx)
    {
      ObPhysicalPlan::ObTableVersion tversion = plan.get_base_table_version(idx);
      const common::ObTableSchema *ts = schema_manager->get_table_schema(tversion.table_id_);
      TBSYS_LOG(DEBUG, "table_id=%ld schema_version=%ld", tversion.table_id_, tversion.version_);
      if (NULL == ts)
      {
        TBSYS_LOG(DEBUG, "table(id=%ld) no longer exist in schema manager", tversion.table_id_);
        ret = true;
        break;
      }
      else
      {
        if (ts->get_schema_version() != tversion.version_)
        {
          TBSYS_LOG(DEBUG, "table(id=%ld) version not match ObPsStore version=%ld, SchemaManager version=%ld",
                    tversion.table_id_, tversion.version_, ts->get_schema_version());
          ret = true;
          break;
        }
      }
    }
  }
  return ret;
}
