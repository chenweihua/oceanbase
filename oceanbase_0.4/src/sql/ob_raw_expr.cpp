#include "ob_raw_expr.h"
#include "ob_transformer.h"
#include "type_name.c"
#include "ob_prepare.h"
#include "ob_result_set.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

bool ObRawExpr::is_const() const
{
  return (type_ >= T_INT && type_ <= T_NULL);
}

bool ObRawExpr::is_column() const
{
  return (type_ == T_REF_COLUMN);
}

bool ObRawExpr::is_equal_filter() const
{
  bool ret = false;
  if (type_ == T_OP_EQ || type_ == T_OP_IS)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->is_const()
      || binary_expr->get_second_op_expr()->is_const())
      ret = true;
  }
  return ret;
}

bool ObRawExpr::is_range_filter() const
{
  bool ret = false;
  if (type_ >= T_OP_LE && type_ <= T_OP_GT)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->is_const()
      || binary_expr->get_second_op_expr()->is_const())
      ret = true;
  }
  else if (type_ == T_OP_BTW)
  {
    ObTripleOpRawExpr *triple_expr = dynamic_cast<ObTripleOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (triple_expr->get_second_op_expr()->is_const()
      && triple_expr->get_third_op_expr()->is_const())
      ret = true;
  }
  return ret;
}

bool ObRawExpr::is_join_cond() const
{
  bool ret = false;
  if (type_ == T_OP_EQ)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN
      && binary_expr->get_second_op_expr()->get_expr_type() == T_REF_COLUMN)
      ret = true;
  }
  return ret;
}

bool ObRawExpr::is_aggr_fun() const
{
  bool ret = false;
  if (type_ >= T_FUN_MAX && type_ <= T_FUN_AVG)
    ret = true;
  return ret;
}

int ObConstRawExpr::set_value_and_type(const common::ObObj& val)
{
  int ret = OB_SUCCESS;
  switch(val.get_type())
  {
    case ObNullType:   // 空类型
      this->set_expr_type(T_NULL);
      this->set_result_type(ObNullType);
      break;
    case ObIntType:
      this->set_expr_type(T_INT);
      this->set_result_type(ObIntType);
      break;
    case ObFloatType:              // @deprecated
      this->set_expr_type(T_FLOAT);
      this->set_result_type(ObFloatType);
      break;
    case ObDoubleType:             // @deprecated
      this->set_expr_type(T_DOUBLE);
      this->set_result_type(ObDoubleType);
      break;
    case ObPreciseDateTimeType:    // =5
    case ObCreateTimeType:
    case ObModifyTimeType:
      this->set_expr_type(T_DATE);
      this->set_result_type(ObPreciseDateTimeType);
      break;
    case ObVarcharType:
      this->set_expr_type(T_STRING);
      this->set_result_type(ObVarcharType);
      break;
    case ObBoolType:
      this->set_expr_type(T_BOOL);
      this->set_result_type(ObBoolType);
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      TBSYS_LOG(WARN, "obj type not support, type=%d", val.get_type());
      break;
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (ObExtendType != val.get_type())
    {
      value_ = val;
    }
  }
  return ret;
}

void ObConstRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s : ", get_type_name(get_expr_type()));
  switch(get_expr_type())
  {
    case T_INT:
    {
      int64_t i = 0;
      value_.get_int(i);
      fprintf(fp, "%ld\n", i);
      break;
    }
    case T_STRING:
    case T_BINARY:
    {
      ObString str;
      value_.get_varchar(str);
      fprintf(fp, "%.*s\n", str.length(), str.ptr());
      break;
    }
    case T_DATE:
    {
      ObDateTime d = static_cast<ObDateTime>(0L);
      value_.get_datetime(d);
      fprintf(fp, "%ld\n", d);
      break;
    }
    case T_FLOAT:
    {
      float f = 0.0f;
      value_.get_float(f);
      fprintf(fp, "%f\n", f);
      break;
    }
    case T_DOUBLE:
    {
      double d = 0.0f;
      value_.get_double(d);
      fprintf(fp, "%lf\n", d);
      break;
    }
    case T_DECIMAL:
    {
      ObString str;
      value_.get_varchar(str);
      fprintf(fp, "%.*s\n", str.length(), str.ptr());
      break;
    }
    case T_BOOL:
    {
      bool b = false;
      value_.get_bool(b);
      fprintf(fp, "%s\n", b ? "TRUE" : "FALSE");
      break;
    }
    case T_NULL:
    {
      fprintf(fp, "NULL\n");
      break;
    }
    case T_UNKNOWN:
    {
      fprintf(fp, "UNKNOWN\n");
      break;
    }
    default:
      fprintf(fp, "error type!\n");
      break;
  }
}

int ObConstRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  UNUSED(logical_plan);
  UNUSED(physical_plan);
  UNUSED(transformer);
  float f = 0.0f;
  double d = 0.0;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  switch (item.type_)
  {
    case T_STRING:
    case T_BINARY:
      ret = value_.get_varchar(item.string_);
      break;
    case T_FLOAT:
      ret = value_.get_float(f);
      item.value_.float_ = f;
      break;
    case T_DOUBLE:
      ret = value_.get_double(d);
      item.value_.double_ = d;
      break;
    case T_DECIMAL:
      ret = value_.get_varchar(item.string_);
      break;
    case T_INT:
      ret = value_.get_int(item.value_.int_);
      break;
    case T_BOOL:
      ret = value_.get_bool(item.value_.bool_);
      break;
    case T_DATE:
      ret = value_.get_precise_datetime(item.value_.datetime_);
      break;
    case T_QUESTIONMARK:
      ret = value_.get_int(item.value_.int_);
      break;
    case T_SYSTEM_VARIABLE:
    case T_TEMP_VARIABLE:
      ret = value_.get_varchar(item.string_);
      break;
    case T_NULL:
      break;
    default:
      TBSYS_LOG(WARN, "unexpected expression type %d", item.type_);
      ret = OB_ERR_EXPR_UNKNOWN;
      break;
  }
  if (OB_SUCCESS == ret)
  {
    ret = inter_expr.add_expr_item(item);
  }
  return ret;
}

void ObCurTimeExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
}

int ObCurTimeExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  UNUSED(physical_plan);
  UNUSED(transformer);

  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type(); //T_CUR_TIME
  item.data_type_ = ObPreciseDateTimeType;
  item.value_.int_ = logical_plan->get_cur_time_fun_type(); //just place holder
  ret = inter_expr.add_expr_item(item);

  return ret;
}

void ObUnaryRefRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s : %lu\n", get_type_name(get_expr_type()), id_);
}

int ObUnaryRefRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  if (transformer == NULL || logical_plan == NULL || physical_plan == NULL)
  {
    TBSYS_LOG(ERROR, "transformer error");
    ret = OB_ERROR;
  }
  else
  {
    ErrStat err_stat;
    int32_t index = OB_INVALID_INDEX;
    ret = transformer->gen_physical_select(logical_plan, physical_plan, err_stat, id_, &index);
    item.value_.int_ = index;
  }
  if (ret == OB_SUCCESS && OB_INVALID_INDEX == item.value_.int_)
  {
    TBSYS_LOG(ERROR, "generating physical plan for sub-query error");
    ret = OB_ERROR;
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObBinaryRefRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  if (first_id_ == OB_INVALID_ID)
    fprintf(fp, "%s : [table_id, column_id] = [NULL, %lu]\n",
            get_type_name(get_expr_type()), second_id_);
  else
    fprintf(fp, "%s : [table_id, column_id] = [%lu, %lu]\n",
            get_type_name(get_expr_type()), first_id_, second_id_);
}

int ObBinaryRefRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  UNUSED(transformer);
  UNUSED(logical_plan);
  UNUSED(physical_plan);
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();

  if (ret == OB_SUCCESS && get_expr_type() == T_REF_COLUMN)
  {
    item.value_.cell_.tid = first_id_;
    item.value_.cell_.cid = second_id_;
  }
  else
  {
    // No other type
    ret = OB_ERROR;
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObUnaryOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  expr_->print(fp, level + 1);
}

int ObUnaryOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = 1; /* One operator */

  ret = expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObBinaryOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  first_expr_->print(fp, level + 1);
  second_expr_->print(fp, level + 1);
}

void ObBinaryOpRawExpr::set_op_exprs(ObRawExpr *first_expr, ObRawExpr *second_expr)
{
  ObItemType exchange_type = T_MIN_OP;
  switch (get_expr_type())
  {
    case T_OP_LE:
      exchange_type = T_OP_GE;
      break;
    case T_OP_LT:
      exchange_type = T_OP_GT;
      break;
    case T_OP_GE:
      exchange_type = T_OP_LE;
      break;
    case T_OP_GT:
      exchange_type = T_OP_LT;
      break;
    case T_OP_EQ:
    case T_OP_NE:
      exchange_type = get_expr_type();
      break;
    default:
      exchange_type = T_MIN_OP;
      break;
  }
  if (exchange_type != T_MIN_OP
    && first_expr && first_expr->is_const()
    && second_expr && second_expr->is_column())
  {
    set_expr_type(exchange_type);
    first_expr_ = second_expr;
    second_expr_ = first_expr;
  }
  else
  {
    first_expr_ = first_expr;
    second_expr_ = second_expr;
  }
}

int ObBinaryOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  bool dem_1_to_2 = false;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = 2; /* Two operators */

  // all form with 1 dimension and without sub-select will be changed to 2 dimensions
  // c1 in (1, 2, 3) ==> (c1) in ((1), (2), (3))
  if ((ret = first_expr_->fill_sql_expression(
                             inter_expr,
                             transformer,
                             logical_plan,
                             physical_plan)) == OB_SUCCESS
    && (get_expr_type() == T_OP_IN || get_expr_type() == T_OP_NOT_IN))
  {
    if (!first_expr_ || !second_expr_)
    {
      ret = OB_ERR_EXPR_UNKNOWN;
    }
    else if (first_expr_->get_expr_type() != T_OP_ROW
      && first_expr_->get_expr_type() != T_REF_QUERY
      && second_expr_->get_expr_type() == T_OP_ROW)
    {
      dem_1_to_2 = true;
      ExprItem dem2;
      dem2.type_ = T_OP_ROW;
      dem2.data_type_ = ObIntType;
      dem2.value_.int_ = 1;
      ret = inter_expr.add_expr_item(dem2);
    }
    if (OB_LIKELY(ret == OB_SUCCESS))
    {
      ExprItem left_item;
      left_item.type_ = T_OP_LEFT_PARAM_END;
      left_item.data_type_ = ObIntType;
      switch (first_expr_->get_expr_type())
      {
        case T_OP_ROW:
        case T_REF_QUERY:
          left_item.value_.int_ = 2;
          break;
        default:
          left_item.value_.int_ = 1;
          break;
      }
      if (dem_1_to_2)
        left_item.value_.int_ = 2;
      ret = inter_expr.add_expr_item(left_item);
    }
  }
  if (ret == OB_SUCCESS)
  {
    if (!dem_1_to_2)
    {
      ret = second_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
    }
    else
    {
      ExprItem dem2;
      dem2.type_ = T_OP_ROW;
      dem2.data_type_ = ObIntType;
      dem2.value_.int_ = 1;
      ObMultiOpRawExpr *row_expr = dynamic_cast<ObMultiOpRawExpr*>(second_expr_);
      if (row_expr != NULL)
      {
        ExprItem row_item;
        row_item.type_ = row_expr->get_expr_type();
        row_item.data_type_ = row_expr->get_result_type();
        row_item.value_.int_ = row_expr->get_expr_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < row_expr->get_expr_size(); i++)
        {
          if ((ret = row_expr->get_op_expr(i)->fill_sql_expression(
                                                   inter_expr,
                                                   transformer,
                                                   logical_plan,
                                                   physical_plan)) != OB_SUCCESS
            || (ret = inter_expr.add_expr_item(dem2)) != OB_SUCCESS)
          {
            break;
          }
        }
        if (ret == OB_SUCCESS)
          ret = inter_expr.add_expr_item(row_item);
      }
      else
      {
        ret = OB_ERR_EXPR_UNKNOWN;
      }
    }
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObTripleOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  first_expr_->print(fp, level + 1);
  second_expr_->print(fp, level + 1);
  third_expr_->print(fp, level + 1);
}

void ObTripleOpRawExpr::set_op_exprs(
    ObRawExpr *first_expr,
    ObRawExpr *second_expr,
    ObRawExpr *third_expr)
{
  first_expr_ = first_expr;
  second_expr_ = second_expr;
  third_expr_ = third_expr;
}

int ObTripleOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = 3; /* thress operators */

  if (ret == OB_SUCCESS)
    ret = first_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = second_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = third_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObMultiOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  for (int32_t i = 0; i < exprs_.size(); i++)
  {
    exprs_[i]->print(fp, level + 1);
  }
}

int ObMultiOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = exprs_.size();

  for (int32_t i = 0; ret == OB_SUCCESS && i < exprs_.size(); i++)
  {
    ret = exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObCaseOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  if (arg_expr_)
    arg_expr_->print(fp, level + 1);
  for (int32_t i = 0; i < when_exprs_.size() && i < then_exprs_.size(); i++)
  {
    when_exprs_[i]->print(fp, level + 1);
    then_exprs_[i]->print(fp, level + 1);
  }
  if (default_expr_)
  {
    default_expr_->print(fp, level + 1);
  }
  else
  {
    for(int i = 0; i < level; ++i) fprintf(fp, "    ");
    fprintf(fp, "DEFAULT : NULL\n");
  }
}

int ObCaseOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  if (arg_expr_ == NULL)
    item.type_ = T_OP_CASE;
  else
    item.type_ = T_OP_ARG_CASE;
  item.data_type_ = get_result_type();
  item.value_.int_ = (arg_expr_ == NULL ? 0 : 1) + when_exprs_.size() + then_exprs_.size();
  item.value_.int_ += (default_expr_ == NULL ? 0 : 1);

  if (ret == OB_SUCCESS && arg_expr_ != NULL)
    ret = arg_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  for (int32_t i = 0; ret == OB_SUCCESS && i < when_exprs_.size() && i < then_exprs_.size(); i++)
  {
    ret = when_exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
    if (ret != OB_SUCCESS)
      break;
    ret = then_exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  }
  if (ret == OB_SUCCESS && default_expr_ != NULL)
    ret = default_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObAggFunRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  if (distinct_)
  {
    for(int i = 0; i < level; ++i) fprintf(fp, "    ");
    fprintf(fp, "DISTINCT\n");
  }
  if (param_expr_)
    param_expr_->print(fp, level + 1);
}

int ObAggFunRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  inter_expr.set_aggr_func(get_expr_type(), distinct_);
  if (param_expr_)
    ret = param_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  return ret;
}

void ObSysFunRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s : %.*s\n", get_type_name(get_expr_type()), func_name_.length(), func_name_.ptr());
  for (int32_t i = 0; i < exprs_.size(); i++)
  {
    exprs_[i]->print(fp, level + 1);
  }
}

int ObSysFunRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = T_FUN_SYS;
  item.string_ = func_name_;
  item.value_.int_ = exprs_.size();
  for (int32_t i = 0; ret == OB_SUCCESS && i < exprs_.size(); i++)
  {
    ret = exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Add parameters of system function failed, param %d", i + 1);
      break;
    }
  }
  if (ret == OB_SUCCESS && (ret = inter_expr.add_expr_item(item)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add system function %.*s failed", func_name_.length(), func_name_.ptr());
  }
  return ret;
}

ObSqlRawExpr::ObSqlRawExpr()
{
  expr_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  column_id_ = OB_INVALID_ID;
  is_apply_ = false;
  contain_aggr_ = false;
  contain_alias_ = false;
  is_columnlized_ = false;
  expr_ = NULL;
}

ObSqlRawExpr::ObSqlRawExpr(
    uint64_t expr_id, uint64_t table_id, uint64_t column_id, ObRawExpr* expr)
{
  table_id_ = table_id;
  expr_id_ = expr_id;
  column_id_ = column_id;
  is_apply_ = false;
  contain_aggr_ = false;
  contain_alias_ = false;
  is_columnlized_ = false;
  expr_ = expr;
}

int ObSqlRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan)
{
  int ret = OB_SUCCESS;
  if (!(transformer == NULL && logical_plan == NULL && physical_plan == NULL)
    && !(transformer != NULL && logical_plan != NULL && physical_plan != NULL))
  {
    TBSYS_LOG(WARN,"(ObTransformer, ObLogicalPlan, ObPhysicalPlan) should be set together");
  }

  inter_expr.set_tid_cid(table_id_, column_id_);
  if (ret == OB_SUCCESS)
    ret = expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item_end();
  return ret;
}

void ObSqlRawExpr::print(FILE* fp, int32_t level, int32_t index) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObSqlRawExpr %d Begin>\n", index);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "expr_id = %lu\n", expr_id_);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  if (table_id_ == OB_INVALID_ID)
    fprintf(fp, "(table_id : column_id) = (NULL : %lu)\n", column_id_);
  else
    fprintf(fp, "(table_id : column_id) = (%lu : %lu)\n", table_id_, column_id_);
  expr_->print(fp, level);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObSqlRawExpr %d End>\n", index);
}
