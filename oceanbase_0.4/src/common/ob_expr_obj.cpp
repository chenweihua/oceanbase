/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_obj.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <regex.h>
#include <cmath>
#include "utility.h"
#include "ob_expr_obj.h"
#include "ob_string_buf.h"
#include "ob_string_search.h"
#include "ob_malloc.h"
#include "ob_obj_cast.h"
using namespace oceanbase::common;

void ObExprObj::assign(const ObObj &obj)
{
  type_ = obj.get_type();
  switch(obj.get_type())
  {
    case ObNullType:
      break;
    case ObIntType:
      obj.get_int(v_.int_);
      break;
    case ObDateTimeType:
      obj.get_datetime(v_.datetime_);
      break;
    case ObPreciseDateTimeType:
      obj.get_precise_datetime(v_.precisedatetime_);
      break;
    case ObVarcharType:
      obj.get_varchar(varchar_);
      break;
    case ObCreateTimeType:
      obj.get_createtime(v_.createtime_);
      break;
    case ObModifyTimeType:
      obj.get_modifytime(v_.modifytime_);
      break;
    case ObBoolType:
      obj.get_bool(v_.bool_);
      break;
    case ObDecimalType:
      obj.get_decimal(num_);
      break;
    case ObFloatType:
      obj.get_float(v_.float_);
      break;
    case ObDoubleType:
      obj.get_double(v_.double_);
      break;
    case ObExtendType:
      obj.get_ext(v_.ext_);
      break;
    default:
      TBSYS_LOG(ERROR, "invalid value type=%d", obj.get_type());
      break;
  }
}

int ObExprObj::to(ObObj &obj) const
{
  int ret = OB_SUCCESS;
  switch(get_type())
  {
    case ObNullType:
      obj.set_null();
      break;
    case ObIntType:
      obj.set_int(v_.int_);
      break;
    case ObDateTimeType:
      obj.set_datetime(v_.datetime_);
      break;
    case ObPreciseDateTimeType:
      obj.set_precise_datetime(v_.precisedatetime_);
      break;
    case ObVarcharType:
      obj.set_varchar(varchar_);
      break;
    case ObCreateTimeType:
      obj.set_createtime(v_.createtime_);
      break;
    case ObModifyTimeType:
      obj.set_modifytime(v_.modifytime_);
      break;
    case ObBoolType:
      obj.set_bool(v_.bool_);
      break;
    case ObDecimalType:
      ret = obj.set_decimal(num_);
      break;
    case ObFloatType:
      obj.set_float(v_.float_);
      break;
    case ObDoubleType:
      obj.set_double(v_.double_);
      break;
    case ObExtendType:
      obj.set_ext(v_.ext_);
      break;
    default:
      TBSYS_LOG(ERROR, "invalid value type=%d", get_type());
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

inline bool ObExprObj::is_datetime() const
{
  return ((type_ == ObDateTimeType)
          || (type_ == ObPreciseDateTimeType)
          || (type_ == ObCreateTimeType)
          || (type_ == ObModifyTimeType));
}

bool ObExprObj::can_compare(const ObExprObj & other) const
{
  bool ret = false;
  if ((get_type() == ObNullType) || (other.get_type() == ObNullType)
      || (get_type() == other.get_type()) || (is_datetime() && other.is_datetime())
      || (is_numeric() && other.is_numeric()))
  {
    ret = true;
  }
  return ret;
}

inline bool ObExprObj::is_numeric() const
{
  return ((type_ == ObIntType)
          || (type_ == ObDecimalType)
          || (type_ == ObFloatType)
          || (type_ == ObDoubleType)
          || (type_ == ObBoolType));
}

inline int ObExprObj::get_timestamp(int64_t & timestamp) const
{
  int ret = OB_SUCCESS;
  switch(type_)
  {
    case ObDateTimeType:
      timestamp = v_.datetime_ * 1000 * 1000L;
      break;
    case ObPreciseDateTimeType:
      timestamp = v_.precisedatetime_;
      break;
    case ObModifyTimeType:
      timestamp = v_.modifytime_;
      break;
    case ObCreateTimeType:
      timestamp = v_.createtime_;
      break;
    default:
      TBSYS_LOG(ERROR, "unexpected branch");
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

int ObExprObj::compare_same_type(const ObExprObj &other) const
{
  int cmp = 0;
  OB_ASSERT(get_type() == other.get_type()
            || (is_datetime() && other.is_datetime()));
  switch(get_type())
  {
    case ObIntType:
      if (this->v_.int_ < other.v_.int_)
      {
        cmp = -1;
      }
      else if (this->v_.int_ == other.v_.int_)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    case ObDecimalType:
      cmp = this->num_.compare(other.num_);
      break;
    case ObVarcharType:
      cmp = this->varchar_.compare(other.varchar_);
      break;
    case ObFloatType:
    {
      bool float_eq = fabsf(v_.float_ - other.v_.float_) < FLOAT_EPSINON;
      if (float_eq)
      {
        cmp = 0;
      }
      else if (this->v_.float_ < other.v_.float_)
      {
        cmp = -1;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObDoubleType:
    {
      bool double_eq = fabs(v_.double_ - other.v_.double_) < DOUBLE_EPSINON;
      if (double_eq)
      {
        cmp = 0;
      }
      else if (this->v_.double_ < other.v_.double_)
      {
        cmp = -1;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    {
      int64_t ts1 = 0;
      int64_t ts2 = 0;
      get_timestamp(ts1);
      other.get_timestamp(ts2);
      if (ts1 < ts2)
      {
        cmp = -1;
      }
      else if (ts1 == ts2)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObBoolType:
      cmp = this->v_.bool_ - other.v_.bool_;
      break;
    default:
      TBSYS_LOG(ERROR, "invalid type=%d", get_type());
      break;
  }
  return cmp;
}

// @return ObMaxType when not supporting
// do not promote where comparing with the same type
static ObObjType COMPARE_TYPE_PROMOTION[ObMaxType][ObMaxType] =
{
  {
    /*Null compare with XXX*/
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType
  }
  ,
  {
    /*Int compare with XXX*/
    ObNullType/*Null*/, ObIntType/*Int*/, ObFloatType,
    ObDoubleType, ObIntType, ObIntType,
    ObIntType, ObMaxType/*Seq, not_support*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType
  }
  ,
  {
    /*Float compare with XXX*/
    ObNullType/*Null*/, ObFloatType, ObFloatType,
    ObDoubleType, ObFloatType, ObFloatType,
    ObFloatType, ObMaxType/*Seq*/, ObFloatType,
    ObFloatType, ObMaxType/*Extend*/, ObBoolType,
    ObDoubleType
  }
  ,
  {
    /*Double compare with XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObBoolType,
    ObDoubleType
  }
  ,
  {
    /*DateTime compare with XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObDateTimeType, ObPreciseDateTimeType,
    ObDateTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType
  }
  ,
  {
    /*PreciseDateTime compare with XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType
  }
  ,
  {
    /*Varchar compare with XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObDateTimeType, ObPreciseDateTimeType,
    ObVarcharType, ObMaxType/*Seq*/, ObCreateTimeType,
    ObModifyTimeType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType
  }
  ,
  {
    /*Seq compare with XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType
  }
  ,
  {
    /*CreateTime compare with XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    ObCreateTimeType, ObMaxType/*Seq*/, ObCreateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType
  }
  ,
  {
    /*ModifyTime compare with XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    ObModifyTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    ObModifyTimeType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType
  }
  ,
  {
    /*Extend compare with XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType
  }
  ,
  {
    /*Bool compare with XXX*/
    ObNullType/*Null*/, ObBoolType, ObBoolType,
    ObBoolType, ObBoolType, ObBoolType,
    ObBoolType, ObMaxType/*Seq*/, ObBoolType,
    ObBoolType, ObMaxType/*Extend*/, ObBoolType,
    ObBoolType
  }
  ,
  {
    /*Decimal compare with XXX*/
    ObNullType/*Null*/, ObDecimalType, ObDoubleType,
    ObDoubleType, ObDecimalType, ObDecimalType,
    ObDecimalType, ObMaxType/*Seq*/, ObDecimalType,
    ObDecimalType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType
  }
};

struct IntegrityChecker1
{
  IntegrityChecker1()
  {
    for (ObObjType t1 = static_cast<ObObjType>(ObMinType+1);
         t1 < ObMaxType;
         t1 = static_cast<ObObjType>(t1 + 1))
    {
      for (ObObjType t2 = static_cast<ObObjType>(ObMinType+1);
           t2 < ObMaxType;
           t2 = static_cast<ObObjType>(t2 + 1))
      {
        OB_ASSERT(COMPARE_TYPE_PROMOTION[t1][t2] == COMPARE_TYPE_PROMOTION[t2][t1]);
      }
    }
  }
} COMPARE_TYPE_PROMOTION_CHECKER;

inline int ObExprObj::type_promotion(ObObjType type_promote_map[ObMaxType][ObMaxType],
                                     const ObExprObj &this_obj, const ObExprObj &other,
                                     ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                     const ObExprObj *&p_this, const ObExprObj *&p_other)
{
  int ret = OB_SUCCESS;
  ObObjType this_type = this_obj.get_type();
  ObObjType other_type = other.get_type();
  OB_ASSERT(ObMinType < this_type && this_type < ObMaxType);
  OB_ASSERT(ObMinType < other_type && other_type < ObMaxType);
  ObObjType res_type = type_promote_map[this_type][other_type];
  if (ObNullType == res_type)
  {
    ret = OB_RESULT_UNKNOWN;
  }
  else if (ObMaxType == res_type)
  {
    TBSYS_LOG(ERROR, "invalid obj type for type promotion, this=%d other=%d", this_type, other_type);
    ret = OB_ERR_UNEXPECTED;
  }
  else
  {
    p_this = &this_obj;
    p_other = &other;
    if (this_type != res_type)
    {
      ObObjCastParams params;
      if (OB_SUCCESS != (ret = OB_OBJ_CAST[this_type][res_type](params, this_obj, promoted_obj1)))
      {
        TBSYS_LOG(WARN, "failed to cast object, err=%d from_type=%d to_type=%d",
                  ret, this_type, res_type);
      }
      else
      {
        p_this = &promoted_obj1;
      }
    }
    if (OB_SUCCESS == ret && other_type != res_type)
    {
      ObObjCastParams params;
      if (OB_SUCCESS != (ret = OB_OBJ_CAST[other_type][res_type](params, other, promoted_obj2)))
      {
        TBSYS_LOG(WARN, "failed to cast object, err=%d from_type=%d to_type=%d",
                  ret, this_type, res_type);
      }
      else
      {
        p_other = &promoted_obj2;
      }
    }
  }
  return ret;
}

inline int ObExprObj::compare_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                             ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                             const ObExprObj *&p_this, const ObExprObj *&p_other)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = type_promotion(COMPARE_TYPE_PROMOTION, this_obj, other,
                                          promoted_obj1, promoted_obj2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN != ret)
    {
      TBSYS_LOG(WARN, "invalid type promote for compare, err=%d", ret);
    }
  }
  return ret;
}

int ObExprObj::compare(const ObExprObj &other, int &cmp) const
{
  int ret = OB_SUCCESS;
  ObExprObj promoted1;
  ObExprObj promoted2;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (ret = compare_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  {
    ret = OB_RESULT_UNKNOWN;
  }
  else
  {
    cmp = p_this->compare_same_type(*p_other);
  }
  return ret;
}

int ObExprObj::lt(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp < 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::gt(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp > 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::ge(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp >= 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::le(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp <= 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::eq(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp == 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::ne(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp != 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(v1, cmp)))
  {
    if (cmp >= 0)
    {
      if (OB_SUCCESS == (ret = this->compare(v2, cmp)))
      {
        res.set_bool(cmp <= 0);
      }
      else
      {
        res.set_null();
      }
    }
    else
    {
      res.set_bool(false);
    }
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::not_btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(v1, cmp)))
  {
    if (cmp >= 0)
    {
      if (OB_SUCCESS == (ret = this->compare(v2, cmp)))
      {
        res.set_bool(cmp > 0);
      }
      else
      {
        res.set_null();
      }
    }
    else
    {
      res.set_bool(true);
    }
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::is(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left_bool = false;
  if (other.get_type() == ObNullType)
  {
    res.set_bool((get_type() == ObNullType));
  }
  else if (ObBoolType != other.get_type())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid type for is operator, type=%d", other.get_type());
    res.set_bool(false);        // res cannot be UNKNOWN according to the SQL standard
  }
  else
  {
    if (OB_SUCCESS != get_bool(left_bool))
    {
      res.set_bool(false);
    }
    else
    {
      res.set_bool(left_bool == other.v_.bool_);
    }
  }
  return ret;
}

int ObExprObj::is_not(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left_bool = false;
  if (other.get_type() == ObNullType)
  {
    res.set_bool((get_type() != ObNullType));
  }
  else if (ObBoolType != other.get_type())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid type for is operator, type=%d", other.get_type());
    res.set_bool(false);        // res cannot be UNKNOWN according to the SQL standard
  }
  else
  {
    if (OB_SUCCESS != get_bool(left_bool))
    {
      res.set_bool(true);       // NULL is not TRUE/FALSE
    }
    else
    {
      res.set_bool(left_bool != other.v_.bool_);
    }
  }
  return ret;
}

int ObExprObj::get_bool(bool &value) const
{
  int res = OB_SUCCESS;
  switch (type_)
  {
    case ObBoolType:
      value = v_.bool_;
      break;
    case ObVarcharType:
      value = (varchar_.length() > 0);
      break;
    case ObIntType:
      value = (v_.int_ != 0);
      break;
    case ObDecimalType:
      value = !num_.is_zero();
      break;
    case ObFloatType:
      value = (fabsf(v_.float_) > FLOAT_EPSINON);
      break;
    case ObDoubleType:
      value = (fabs(v_.double_) > DOUBLE_EPSINON);
      break;
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    {
      int64_t ts1 = 0;
      get_timestamp(ts1);
      value = (0 != ts1);
      break;
    }
    default:
      res = OB_OBJ_TYPE_ERROR;
      break;
  }
  return res;
}

int ObExprObj::land(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left = false;
  bool right = false;
  int err1 = get_bool(left);
  int err2 = other.get_bool(right);
  switch (err1)
  {
    case OB_SUCCESS:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          res.set_bool(left && right);
          break;
        default:
          if (left)
          {
            // TRUE and UNKNOWN
            res.set_null();
          }
          else
          {
            // FALSE and UNKNOWN
            res.set_bool(false);
          }
          break;
      }
      break;
    }
    default:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          if (right)
          {
            // UNKNOWN and TRUE
            res.set_null();
          }
          else
          {
            // UNKNOWN and FALSE
            res.set_bool(false);
          }
          break;
        default:
          res.set_null();
          break;
      }
      break;
    }
  }
  return ret;
}

int ObExprObj::lor(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left = false;
  bool right = false;
  int err1 = get_bool(left);
  int err2 = other.get_bool(right);
  switch (err1)
  {
    case OB_SUCCESS:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          res.set_bool(left || right);
          break;
        default:
          if (left)
          {
            // TRUE or UNKNOWN
            res.set_bool(true);
          }
          else
          {
            // FALSE or UNKNOWN
            res.set_null();
          }
          break;
      }
      break;
    }
    default:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          if (right)
          {
            // UNKNOWN or TRUE
            res.set_bool(true);
          }
          else
          {
            // UNKNOWN or FALSE
            res.set_null();
          }
          break;
        default:
          res.set_null();
          break;
      }
      break;
    }
  }
  return ret;
}

int ObExprObj::lnot(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool val = false;
  int err1 = get_bool(val);
  if (OB_SUCCESS == err1)
  {
    res.set_bool(!val);
  }
  else
  {
    res.set_null();
  }
  return ret;
}
////////////////////////////////////////////////////////////////
// arithmetic operations
////////////////////////////////////////////////////////////////
// @return ObMaxType when not supporting
static ObObjType ARITHMETIC_TYPE_PROMOTION[ObMaxType][ObMaxType] =
{
  {
    /*Null +/- XXX*/
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType
  }
  ,
  {
    /*Int +/- XXX*/
    ObNullType/*Null*/, ObIntType/*Int*/, ObFloatType,
    ObDoubleType, ObIntType, ObIntType,
    ObIntType, ObMaxType/*Seq, not_support*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType
  }
  ,
  {
    /*Float +/- XXX*/
    ObNullType/*Null*/, ObFloatType, ObFloatType,
    ObDoubleType, ObFloatType, ObFloatType,
    ObFloatType, ObMaxType/*Seq*/, ObFloatType,
    ObFloatType, ObMaxType/*Extend*/, ObFloatType,
    ObDoubleType
  }
  ,
  {
    /*Double +/- XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    ObDoubleType
  }
  ,
  {
    /*DateTime +/- XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObIntType, ObIntType,
    ObIntType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType
  }
  ,
  {
    /*PreciseDateTime +/- XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObIntType, ObIntType,
    ObIntType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType
  }
  ,
  {
    /*Varchar +/- XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObIntType, ObIntType,
    ObIntType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType
  }
  ,
  {
    /*Seq +/- XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType
  }
  ,
  {
    /*CreateTime +/- XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObIntType, ObIntType,
    ObIntType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType
  }
  ,
  {
    /*ModifyTime +/- XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObIntType, ObIntType,
    ObIntType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType
  }
  ,
  {
    /*Extend +/- XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType
  }
  ,
  {
    /*Bool +/- XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObIntType, ObIntType,
    ObIntType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType
  }
  ,
  {
    /*Decimal +/- XXX*/
    ObNullType/*Null*/, ObDecimalType, ObDoubleType,
    ObDoubleType, ObDecimalType, ObDecimalType,
    ObDecimalType, ObMaxType/*Seq*/, ObDecimalType,
    ObDecimalType, ObMaxType/*Extend*/, ObDecimalType,
    ObDecimalType
  }
};

struct IntegrityChecker2
{
  IntegrityChecker2()
  {
    for (ObObjType t1 = static_cast<ObObjType>(ObMinType+1);
         t1 < ObMaxType;
         t1 = static_cast<ObObjType>(t1 + 1))
    {
      for (ObObjType t2 = static_cast<ObObjType>(ObMinType+1);
           t2 < ObMaxType;
           t2 = static_cast<ObObjType>(t2 + 1))
      {
        OB_ASSERT(ARITHMETIC_TYPE_PROMOTION[t1][t2] == ARITHMETIC_TYPE_PROMOTION[t2][t1]);
      }
    }
  }
} ARITHMETIC_TYPE_PROMOTION_CHECKER;

inline int ObExprObj::arith_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                     ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                     const ObExprObj *&p_this, const ObExprObj *&p_other)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = type_promotion(ARITHMETIC_TYPE_PROMOTION, this_obj, other,
                                          promoted_obj1, promoted_obj2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN != ret)
    {
      TBSYS_LOG(WARN, "invalid type promote for arithmetic, err=%d", ret);
    }
  }
  return ret;
}

inline int ObExprObj::add_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_type() == other.get_type()
            && this->is_numeric());
  res.type_ = get_type();
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = this->v_.int_ + other.v_.int_; // overflow is allowed
      break;
    case ObFloatType:
      res.v_.float_ = this->v_.float_ + other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ + other.v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.add(other.num_, res.num_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObExprObj::add(ObExprObj &other, ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObExprObj promoted1;
  ObExprObj promoted2;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (ret = arith_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    ret = p_this->add_same_type(*p_other, res);
  }
  return ret;
}

inline int ObExprObj::sub_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_type() == other.get_type()
            && this->is_numeric());
  res.type_ = get_type();
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = this->v_.int_ - other.v_.int_; // overflow is allowed
      break;
    case ObFloatType:
      res.v_.float_ = this->v_.float_ - other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ - other.v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.sub(other.num_, res.num_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObExprObj::sub(ObExprObj &other, ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObExprObj promoted1;
  ObExprObj promoted2;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (ret = arith_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    ret = p_this->sub_same_type(*p_other, res);
  }
  return ret;
}

inline int ObExprObj::mul_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_type() == other.get_type()
            && this->is_numeric());
  res.type_ = get_type();
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = this->v_.int_ * other.v_.int_; // overflow is allowed
      break;
    case ObFloatType:
      res.v_.float_ = this->v_.float_ * other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ * other.v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.mul(other.num_, res.num_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObExprObj::mul(ObExprObj &other, ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObExprObj promoted1;
  ObExprObj promoted2;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (ret = arith_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    ret = p_this->mul_same_type(*p_other, res);
  }
  return ret;
}

// @return ObMaxType when not supporting
// int div int result in double
static ObObjType DIV_TYPE_PROMOTION[ObMaxType][ObMaxType] =
{
  {
    /*Null div XXX*/
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType
  }
  ,
  {
    /*Int div XXX*/
    ObNullType/*Null*/, ObDoubleType/*Int*/, ObFloatType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq, not_support*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType
  }
  ,
  {
    /*Float div XXX*/
    ObNullType/*Null*/, ObFloatType, ObFloatType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObFloatType,
    ObDoubleType
  }
  ,
  {
    /*Double div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    ObDoubleType
  }
  ,
  {
    /*DateTime div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType
  }
  ,
  {
    /*PreciseDateTime div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType
  }
  ,
  {
    /*Varchar div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType
  }
  ,
  {
    /*Seq div XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType
  }
  ,
  {
    /*CreateTime div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType
  }
  ,
  {
    /*ModifyTime div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType
  }
  ,
  {
    /*Extend div XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType
  }
  ,
  {
    /*Bool div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObFloatType,
    ObDoubleType, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType
  }
  ,
  {
    /*Decimal div XXX*/
    ObNullType/*Null*/, ObDecimalType, ObDoubleType,
    ObDoubleType, ObDecimalType, ObDecimalType,
    ObDecimalType, ObMaxType/*Seq*/, ObDecimalType,
    ObDecimalType, ObMaxType/*Extend*/, ObDecimalType,
    ObDecimalType
  }
};

struct IntegrityChecker3
{
  IntegrityChecker3()
  {
    for (ObObjType t1 = static_cast<ObObjType>(ObMinType+1);
         t1 < ObMaxType;
         t1 = static_cast<ObObjType>(t1 + 1))
    {
      for (ObObjType t2 = static_cast<ObObjType>(ObMinType+1);
           t2 < ObMaxType;
           t2 = static_cast<ObObjType>(t2 + 1))
      {
        OB_ASSERT(DIV_TYPE_PROMOTION[t1][t2] == DIV_TYPE_PROMOTION[t2][t1]);
      }
    }
  }
} DIV_TYPE_PROMOTION_CHECKER;

inline int ObExprObj::cast_to_int(int64_t &val) const
{
  int ret = OB_SUCCESS;
  ObExprObj casted_obj;

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    // TBSYS_LOG(WARN, "should not be null");
    // NB: becareful, it is not real error, the caller need treat it as NULL
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObObjCastParams params;
    if (OB_SUCCESS != (ret = OB_OBJ_CAST[this->get_type()][ObIntType](params, *this, casted_obj)))
    {
      // don't report WARN when type NOT_SUPPORT
      /*
      TBSYS_LOG(WARN, "failed to cast object, err=%d from_type=%d to_type=%d",
          ret, this->get_type(), ObVarcharType);
      */
    }
    else
    {
      val = casted_obj.get_int();
    }
  }
  return ret;
}


inline int ObExprObj::cast_to_varchar(ObString &varchar, ObStringBuf &mem_buf) const
{
  int ret = OB_SUCCESS;
  ObExprObj casted_obj;
  char max_tmp_buf[128]; // other type to varchar won't takes too much space, assume 128, should be enough
  ObString tmp_str(128, 128, max_tmp_buf);

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    //TBSYS_LOG(WARN, "should not be null");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_LIKELY(this->get_type() != ObVarcharType))
  {
    casted_obj.set_varchar(tmp_str);
    ObObjCastParams params;
    if (OB_SUCCESS != (ret = OB_OBJ_CAST[this->get_type()][ObVarcharType](params, *this, casted_obj)))
    {
      // don't report WARN when type NOT_SUPPORT
      /*
      TBSYS_LOG(WARN, "failed to cast object, err=%d from_type=%d to_type=%d",
          ret, this->get_type(), ObVarcharType);
      */
    }
    else if (OB_SUCCESS != (ret = mem_buf.write_string(casted_obj.get_varchar(), &varchar)))
    {
      TBSYS_LOG(WARN, "fail to allocate memory for string. ret=%d", ret);
    }
  }
  else if (OB_UNLIKELY(OB_SUCCESS != (ret = mem_buf.write_string(this->get_varchar(), &varchar))))
  {
    TBSYS_LOG(WARN, "fail to allocate memory for string. ret=%d", ret);
  }
  return ret;
}

int ObExprObj::cast_to(int32_t dest_type, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int err = OB_SUCCESS;
  ObObjCastParams params;
  ObString varchar;
  if (dest_type == ObVarcharType)
  {
    if (OB_SUCCESS != this->cast_to_varchar(varchar, mem_buf))
    {
      result.set_null();
    }
    else
    {
      result.set_varchar(varchar);
    }
  }
  else if (dest_type > ObMinType && dest_type < ObMaxType)
  {
    if (OB_SUCCESS != (err = OB_OBJ_CAST[this->get_type()][dest_type](params, *this, result)))
    {
      err = OB_SUCCESS;
      result.set_null();
    }
  }
  else
  {
    err = OB_INVALID_ARGUMENT;
  }
  return err;
}

inline int ObExprObj::div_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                         ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                         const ObExprObj *&p_this, const ObExprObj *&p_other,
                                         bool int_div_as_double)
{
  int ret = OB_SUCCESS;
  UNUSED(int_div_as_double);
  if (OB_SUCCESS != (ret = type_promotion(DIV_TYPE_PROMOTION, this_obj, other,
                                          promoted_obj1, promoted_obj2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN != ret)
    {
      TBSYS_LOG(WARN, "invalid type promote for compare, err=%d", ret);
    }
  }
  return ret;
}

inline int ObExprObj::div_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_type() == other.get_type()
            && this->is_numeric());
  res.type_ = get_type();
  switch(get_type())
  {
    case ObFloatType:
      res.v_.float_ = this->v_.float_ / other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ / other.v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.div(other.num_, res.num_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch, type=%d", get_type());
      break;
  }
  return ret;
}

int ObExprObj::div(ObExprObj &other, ObExprObj &res, bool int_div_as_double)
{
  int ret = OB_SUCCESS;
  if(OB_UNLIKELY(other.is_zero()))
  {
    res.set_null();
    ret = OB_DIVISION_BY_ZERO;
  }
  else
  {
    ObExprObj promoted_value1;
    ObExprObj promoted_value2;
    const ObExprObj *p_this = NULL;
    const ObExprObj *p_other = NULL;
    if (OB_SUCCESS != (ret = div_type_promotion(*this, other, promoted_value1,
                                                promoted_value2, p_this, p_other, int_div_as_double)))
    {
      if (OB_RESULT_UNKNOWN == ret)
      {
        ret = OB_SUCCESS;
      }
      res.set_null();
    }
    else
    {
      ret = p_this->div_same_type(*p_other, res);
    }
  }
  return ret;
}

inline int ObExprObj::mod_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                         ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                         const ObExprObj *&p_this, const ObExprObj *&p_other)
{
  int ret = OB_SUCCESS;
  ObObjType this_type = this_obj.get_type();
  ObObjType other_type = other.get_type();
  OB_ASSERT(ObMinType < this_type && this_type < ObMaxType);
  OB_ASSERT(ObMinType < other_type && other_type < ObMaxType);
  p_this = &this_obj;
  p_other = &other;
  if (ObNullType == this_type
      || ObNullType == other_type)
  {
    ret = OB_RESULT_UNKNOWN;
  }
  if (OB_SUCCESS == ret && ObIntType != this_type)
  {
    ObObjCastParams params;
    if (OB_SUCCESS != (ret = OB_OBJ_CAST[this_type][ObIntType](params, this_obj, promoted_obj1)))
    {
      TBSYS_LOG(WARN, "failed to cast obj to int, err=%d this_type=%d",
                ret, this_type);
    }
    else
    {
      p_this = &promoted_obj1;
    }
  }
  if (OB_SUCCESS == ret && ObIntType != other_type)
  {
    ObObjCastParams params;
    if (OB_SUCCESS != (ret = OB_OBJ_CAST[other_type][ObIntType](params, other, promoted_obj2)))
    {
      TBSYS_LOG(WARN, "failed to cast obj to int, err=%d this_type=%d",
                ret, other_type);
    }
    else
    {
      p_other = &promoted_obj2;
    }
  }
  return ret;
}

/* 取模运算结果为整数 */
int ObExprObj::mod(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  ObExprObj promoted_obj1;
  ObExprObj promoted_obj2;
  const ObExprObj *p_this = this;
  const ObExprObj *p_other = &other;
  if (OB_SUCCESS != (ret = mod_type_promotion(*this, other, promoted_obj1, promoted_obj2,
                                              p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    OB_ASSERT(ObIntType == p_this->type_);
    OB_ASSERT(ObIntType == p_other->type_);
    if (p_other->is_zero())
    {
      res.set_null();
      ret = OB_DIVISION_BY_ZERO;
    }
    else
    {
      res.type_ = ObIntType;
      res.v_.int_ = p_this->v_.int_ % p_other->v_.int_;
    }
  }
  return ret;
}

int ObExprObj::negate(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  res.type_ = get_type();
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = -this->v_.int_; // overflow is allowd
      break;
    case ObFloatType:
      res.v_.float_ = -this->v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = -this->v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.negate(res.num_);
      break;
    default:
      res.set_null();
      ret = OB_INVALID_ARGUMENT;
      break;
  }
  return ret;
}

int ObExprObj::old_like(const ObExprObj &pattern, ObExprObj &result) const
{
  int err = OB_SUCCESS;
  if (this->get_type() == ObNullType || pattern.get_type() == ObNullType)
  {
    result.set_null();
  }
  else if (ObVarcharType != this->get_type() || ObVarcharType != pattern.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    result.set_null();
  }
  else if (pattern.varchar_.length() <= 0)
  {
    result.set_bool(true);
  }
  else
  {
    // TODO: 对于常量字符串，此处可以优化。无需每次计算sign
    uint64_t pattern_sign = ObStringSearch::cal_print(pattern.varchar_);
    int64_t pos = ObStringSearch::kr_search(pattern.varchar_, pattern_sign, this->varchar_);
    result.set_bool(pos >= 0);
  }
  return err;
}

int ObExprObj::substr(const ObExprObj &start_pos_obj, const ObExprObj &expect_length_obj, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int ret = OB_SUCCESS;
  int64_t start_pos = 0;
  int64_t expect_length = 0;
  ObString varchar;

  // get start pos value
  if (start_pos_obj.is_null() || expect_length_obj.is_null())
  {
    result.set_null();
  }
  else if (OB_SUCCESS != start_pos_obj.cast_to_int(start_pos))
  {
    result.set_null();
  }
  else if (OB_SUCCESS != expect_length_obj.cast_to_int(expect_length))
  {
    result.set_null();
  }
  else
  {
    ret = this->substr(static_cast<int32_t>(start_pos), static_cast<int32_t>(expect_length), result, mem_buf);
  }
  return ret;
}

int ObExprObj::substr(const ObExprObj &start_pos, ObExprObj &result, ObStringBuf &mem_buf) const
{
  ObExprObj max_guess_len;
  max_guess_len.set_int(OB_MAX_VALID_COLUMN_ID);
  return substr(start_pos, max_guess_len, result, mem_buf);
}



int ObExprObj::substr(const int32_t start_pos, const int32_t expect_length_of_str, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int err = OB_SUCCESS;
  ObString varchar;

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    result.set_null();
  }
  else if (OB_SUCCESS != this->cast_to_varchar(varchar, mem_buf))
  {
    result.set_null();
  }
  else
  {
    if (OB_UNLIKELY(varchar.length() <= 0 || expect_length_of_str <= 0))
    {
      // empty result string
      varchar.assign(NULL, 0);
    }
    else
    {
      int32_t start = start_pos;
      int32_t res_len = 0;
      start  = (start > 0) ? start : ((start == 0) ? 1 : start + varchar.length() + 1);
      if (OB_UNLIKELY(start <= 0 || start > varchar.length()))
      {
        varchar.assign(NULL, 0);
      }
      else
      {
        if (start + expect_length_of_str - 1 > varchar.length())
        {
          res_len = varchar.length() - start + 1;
        }
        else
        {
          res_len = expect_length_of_str;
        }
        varchar.assign(const_cast<char*>(varchar.ptr() + start - 1), res_len);
      }
    }
    result.set_varchar(varchar);
  }
  return err;
}


int ObExprObj::upper_case(ObExprObj &result, ObStringBuf &mem_buf) const
{
  ObString varchar;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    result.set_null();
  }
  else
  {
    if (OB_SUCCESS != this->cast_to_varchar(varchar, mem_buf))
    {
      result.set_null();
    }
    else
    {
      for (int i = 0; i < varchar.length(); i++)
      {
        varchar.ptr()[i] = static_cast<char>(toupper(varchar.ptr()[i]));
      }
      result.set_varchar(varchar);
    }
  }
  return ret;
}

int ObExprObj::lower_case(ObExprObj &result, ObStringBuf &mem_buf) const
{
  ObString varchar;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    result.set_null();
  }
  else
  {
    if (OB_SUCCESS != this->cast_to_varchar(varchar, mem_buf))
    {
      result.set_null();
    }
    else
    {
      for (int i = 0; i < varchar.length(); i++)
      {
        varchar.ptr()[i] = static_cast<char>(tolower(varchar.ptr()[i]));
      }
      result.set_varchar(varchar);
    }
  }
  return ret;
}

int ObExprObj::concat(const ObExprObj &other, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int ret = OB_SUCCESS;
  char tmp_buf[OB_MAX_VARCHAR_LENGTH];
  ObString this_varchar;
  ObString that_varchar;
  //__thread char tmp_buf[OB_MAX_VARCHAR_LENGTH];
  if (get_type() == ObNullType || other.get_type() == ObNullType)
  {
    result.set_null();
  }
  else
  {
    if (OB_SUCCESS != this->cast_to_varchar(this_varchar, mem_buf))
    {
      // TBSYS_LOG(WARN, "fail to cast obj to varchar. ret=%d", ret);
      result.set_null();
    }
    else if (OB_SUCCESS != other.cast_to_varchar(that_varchar, mem_buf))
    {
      // TBSYS_LOG(WARN, "fail to cast obj to varchar. ret=%d", ret);
      result.set_null();
    }
    else
    {
      int32_t this_len = this_varchar.length();
      int32_t other_len = that_varchar.length();
      ObString tmp_varchar;
      ObString varchar;
      if (this_len + other_len > OB_MAX_VARCHAR_LENGTH)
      {
        //FIXME: 合并后的字符串长度超过了最大限制，结果设置为NULL
        result.set_null();
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        memcpy(tmp_buf, this_varchar.ptr(), this_len);
        memcpy(tmp_buf + this_len, that_varchar.ptr(), other_len);
        tmp_varchar.assign(tmp_buf, this_len + other_len);
        if (OB_SUCCESS != (ret = mem_buf.write_string(tmp_varchar, &varchar)))
        {
          result.set_null();
          TBSYS_LOG(WARN, "fail to write string to membuf. ret=%d", ret);
        }
        else
        {
          result.set_varchar(varchar);
        }
      }
    }
  }
  return ret;
}

int ObExprObj::trim(const ObExprObj &trimType, const ObExprObj &trimPattern, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int err = OB_SUCCESS;
  int64_t type = 0;
  ObString pattern;
  ObString src;
  ObString varchar;
  int32_t start = 0;
  int32_t end = 0;

  if (OB_SUCCESS != (err = trimType.get_int(type)))
  {
    TBSYS_LOG(WARN, "fail to get trim type. err=%d", err);
  }
  else if (OB_SUCCESS != (err = trimPattern.cast_to_varchar(pattern, mem_buf)))
  {
    // TBSYS_LOG(WARN, "fail to get trim pattern. err=%d", err);
  }
  else if (pattern.length() <= 0)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "trim pattern is empty");
  }
  else if (OB_SUCCESS != (err = this->cast_to_varchar(src, mem_buf)))
  {
    // TBSYS_LOG(WARN, "fail to get trim source. err=%d", err);
  }
  else
  {
    if (type == 0) // both
    {
      lrtrim(src, pattern, start, end);
    }
    else if (type == 1) // leading
    {
      ltrim(src, pattern, start);
      end = src.length();
    }
    else if (type == 2) // trailing
    {
      start = 0;
      rtrim(src, pattern, end);
    }
    else
    {
      err = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err)
  {
    varchar.assign(src.ptr() + start, end - start);
    result.set_varchar(varchar);
  }
  else
  {
    result.set_null();
    err = OB_SUCCESS;
  }
  return err;
}


int ObExprObj::lrtrim(const ObString src, const ObString pattern, int32_t &start, int32_t &end) const
{
  int32_t i = 0;
  start = 0;
  end = src.length();
  for (i = 0; i <= src.length() - pattern.length(); i += pattern.length())
  {
    if (0 == memcmp(src.ptr() + i, pattern.ptr(), pattern.length()))
    {
      start += pattern.length();
    }
    else
    {
      break;
    }
  }
  for (i = src.length() - pattern.length(); i >= start; i -= pattern.length())
  {
    if (0 == memcmp(src.ptr() + i, pattern.ptr(), pattern.length()))
    {
      end -= pattern.length();
    }
    else
    {
      break;
    }
  }
  return OB_SUCCESS;
}


int ObExprObj::ltrim(const ObString src, const ObString pattern, int32_t &start) const
{
  int32_t i = 0;
  start = 0;
  for (i = 0; i <= src.length() - pattern.length(); i += pattern.length())
  {
    if (0 == memcmp(src.ptr() + i, pattern.ptr(), pattern.length()))
    {
      start += pattern.length();
    }
    else
    {
      break;
    }
  }
  return OB_SUCCESS;
}

int ObExprObj::rtrim(const ObString src, const ObString pattern, int32_t &end) const
{
  int32_t i = 0;
  end = src.length();
  for (i = src.length() - pattern.length(); i >= 0; i -= pattern.length())
  {
    if (0 == memcmp(src.ptr() + i, pattern.ptr(), pattern.length()))
    {
      end -= pattern.length();
    }
    else
    {
      break;
    }
  }
  return OB_SUCCESS;
}

int ObExprObj::like(const ObExprObj &pattern, ObExprObj &result) const
{
  int err = OB_SUCCESS;
  if (this->get_type() == ObNullType || pattern.get_type() == ObNullType)
  {
    result.set_null();
  }
  else if (ObVarcharType != this->get_type() || ObVarcharType != pattern.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    result.set_null();
  }
  else if (pattern.varchar_.length() <= 0 && varchar_.length() <= 0)
  {
    // empty string
    result.set_bool(true);
  }
  else
  {
    bool b = ObStringSearch::is_matched(pattern.varchar_, this->varchar_);
    result.set_bool(b == true);
  }
  return err;
}

int ObExprObj::not_like(const ObExprObj &pattern, ObExprObj &result) const
{
  int err = OB_SUCCESS;
  if (this->get_type() == ObNullType || pattern.get_type() == ObNullType)
  {
    result.set_null();
  }
  else if (ObVarcharType != this->get_type() || ObVarcharType != pattern.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    result.set_null();
  }
  else if (pattern.varchar_.length() <= 0 && varchar_.length() <= 0)
  {
    // empty string
    result.set_bool(false);
  }
  else
  {
    bool b = ObStringSearch::is_matched(pattern.varchar_, this->varchar_);
    result.set_bool(b != true);
  }
  return err;
}

int ObExprObj::set_decimal(const char* dec_str)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  ObNumber num;
  static const int8_t TEST_PREC = 38;
  static const int8_t TEST_SCALE = 4;
  if (OB_SUCCESS != (ret = num.from(dec_str)))
  {
    TBSYS_LOG(WARN, "failed to construct decimal from string, err=%d str=%s", ret, dec_str);
  }
  else if (OB_SUCCESS != (ret = obj.set_decimal(num, TEST_PREC, TEST_SCALE)))
  {
    TBSYS_LOG(WARN, "obj failed to set decimal, err=%d", ret);
  }
  else
  {
    this->assign(obj);
  }
  return ret;
}

void ObExprObj::set_varchar(const char* value)
{
  ObString str;
  str.assign_ptr(const_cast<char*>(value), static_cast<int32_t>(strlen(value)));
  ObObj obj;
  obj.set_varchar(str);
  this->assign(obj);
}

int ObExprObj::get_int(int64_t& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_int(value);
  }
  return ret;
}

int ObExprObj::get_datetime(ObDateTime& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_datetime(value);
  }
  return ret;
}

int ObExprObj::get_precise_datetime(ObPreciseDateTime& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_precise_datetime(value);
  }
  return ret;
}

int ObExprObj::get_varchar(ObString& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_varchar(value);
  }
  return ret;
}

int ObExprObj::get_float(float &f) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_float(f);
  }
  return ret;
}

int ObExprObj::get_double(double &d) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_double(d);
  }
  return ret;
}

int ObExprObj::get_decimal(char * buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (type_ != ObDecimalType)
  {
    ret = OB_OBJ_TYPE_ERROR;
  }
  else
  {
    num_.to_string(buf, buf_len);
  }
  return ret;
}

int ObExprObj::unhex(ObExprObj &res, ObStringBuf & mem_buf)
{
  int ret = OB_SUCCESS;
  ObString result;
  ObObj obj;
  if (get_type() != ObVarcharType)
  {
    res.set_null();
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "type not match, ret=%d", ret);
  }
  else if (varchar_.length() % 2 != 0)
  {
    res.set_null();
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "length is odd, ret=%d", ret);
  }
  else
  {
    int i = 0;
    char value = 0;
    char *buf = reinterpret_cast<char*>(mem_buf.alloc(varchar_.length() / 2 + 1));
    if (NULL == buf)
    {
      res.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "alloc memory failed, ret=%d", ret);
    }
    else
    {
      for (i = 0;i < varchar_.length();i = i + 2)
      {
        char &c1 = (varchar_.ptr())[i];
        char &c2 = (varchar_.ptr())[i + 1];
        if (isxdigit(c1) && isxdigit(c2))
        {
          if (c1 >= 'a' && c1 <= 'f')
          {
            value = (char)((c1 - 'a' + 10) * 16);
          }
          else if (c1 >= 'A' && c1 <= 'F')
          {
            value = (char)((c1 - 'A' + 10) * 16);
          }
          else
          {
            value = (char)((c1 - '0') * 16);
          }
          if (c2 >= 'a' && c2 <= 'f')
          {
            value = (char)(value + (c2 - 'a' + 10));
          }
          else if (c2 >= 'A' && c2 <= 'F')
          {
            value = (char)(value + (c2 - 'A' + 10));
          }
          else
          {
            value = (char)(value + (c2 - '0'));
          }
          buf[i / 2] = value;

        }
        else
        {
          ret = OB_ERR_UNEXPECTED;
          res.set_null();
          TBSYS_LOG(WARN, "data invalid, ret=%d", ret);
          break;
        }
      }
      if (OB_SUCCESS == ret)
      {
        result.assign_ptr(buf, varchar_.length() / 2);
        obj.set_varchar(result);
        res.assign(obj);
      }
    }
  }
  return ret;
}
int ObExprObj::ip_to_int(ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_UNLIKELY(get_type() != ObVarcharType))
  {
    res.set_null();
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "type not match, ret=%d", ret);
  }
  else
  {
    char buf[16];
    if (varchar_.length() > 15)
    {
      res.set_null();
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "ip format invalid, ret=%d", ret);
    }
    else
    {
      memcpy(buf, varchar_.ptr(), varchar_.length());
      int len = varchar_.length();
      buf[len] = '\0';
      int cnt = 0;
      for (int i = 0;i < len; ++i)
      {
        if (varchar_.ptr()[i] == '.')
        {
          cnt++;
        }
      }
      if (cnt != 3)
      {
        obj.set_null();
        TBSYS_LOG(WARN, "ip format invalid");
      }
      else
      {
        struct in_addr addr;
        int err = inet_aton(buf, &addr);
        if (err != 0)
        {
          obj.set_int(addr.s_addr);
        }
        else
        {
          obj.set_null();
          TBSYS_LOG(WARN, "ip format invalid");
        }
      }
      res.assign(obj);
    }
  }
  return ret;
}
int ObExprObj::int_to_ip(ObExprObj &res, ObStringBuf &mem_buf)
{
  int ret = OB_SUCCESS;
  ObString result;
  ObObj obj;
  if (OB_UNLIKELY(get_type() != ObIntType))
  {
    res.set_null();
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "type not match, ret=%d", ret);
  }
  else
  {
    // 255.255.255.255  共15 bit
    char *buf = reinterpret_cast<char*>(mem_buf.alloc(16));
    if (NULL == buf)
    {
      res.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "alloc memory failed, ret=%d", ret);
    }
    else
    {
      int cnt = snprintf(buf, 16, "%ld.%ld.%ld.%ld",
          (v_.int_ & 0xFF),
          (v_.int_ >> 8) & 0xFF,
          (v_.int_ >> 16) & 0xFF,
          (v_.int_ >> 24) & 0xFF);
      OB_ASSERT(cnt > 0);
      result.assign_ptr(buf, cnt);
      obj.set_varchar(result);
      res.assign(obj);
    }
  }
  return ret;
}
int ObExprObj::hex(ObExprObj &res, ObStringBuf & mem_buf)
{
  int ret = OB_SUCCESS;
  int cnt = 0;
  ObString result;
  ObObj obj;
  if (get_type() == ObVarcharType)
  {
    int i = 0;
    char *buf = reinterpret_cast<char*>(mem_buf.alloc(varchar_.length() * 2 + 1));
    if (NULL == buf)
    {
      res.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "alloc memory failed, ret=%d", ret);
    }
    else
    {
      for (i = 0;i < varchar_.length(); ++i)
      {
        char &c = (varchar_.ptr())[i];
        cnt = snprintf(buf + i * 2, 3, "%02hhx", c);
        OB_ASSERT(cnt == 2);
      }
      result.assign_ptr(buf, varchar_.length() * 2);
      obj.set_varchar(result);
      res.assign(obj);
    }
  }
  else if (get_type() == ObIntType)
  {
    char *buf = reinterpret_cast<char*>(mem_buf.alloc(16 + 1));
    if (NULL == buf)
    {
      res.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "alloc memory failed, ret=%d", ret);
    }
    else
    {
      cnt = snprintf(buf, 16 + 1, "%lx", v_.int_);
      OB_ASSERT(cnt > 0);
      result.assign_ptr(buf, cnt);
      obj.set_varchar(result);
      res.assign(obj);
    }
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    res.set_null();
    TBSYS_LOG(WARN, "type not match, ret=%d", ret);
  }
  return ret;
}
int ObExprObj::varchar_length(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  if (ObVarcharType != get_type())
  {
    ret = OB_INVALID_ARGUMENT;
    res.set_null();
  }
  else
  {
    res.type_ = ObIntType;
    res.v_.int_ = varchar_.length();
  }
  return ret;
}

ObObj ObExprObj::type_arithmetic(const ObObj& t1, const ObObj& t2)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  int err = OB_SUCCESS;
  ObExprObj v1;
  v1.type_ = t1.get_type();
  ObExprObj v2;
  v2.type_ = t2.get_type();
  ObExprObj promoted1;
  ObExprObj promoted2;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (err = arith_type_promotion(v1, v2, promoted1, promoted2, p_this, p_other)))
  {
    TBSYS_LOG(WARN, "failed to promote type, err=%d", err);
  }
  else
  {
    ret.meta_.type_ = p_this->type_;
    if (ObDecimalType == p_this->type_)
    {
      // @todo decimal precision and scale
    }
  }
  return ret;
}

ObObj ObExprObj::type_add(const ObObj& t1, const ObObj& t2)
{
  return type_arithmetic(t1, t2);
}

ObObj ObExprObj::type_sub(const ObObj& t1, const ObObj& t2)
{
  return type_arithmetic(t1, t2);
}

ObObj ObExprObj::type_mul(const ObObj& t1, const ObObj& t2)
{
  return type_arithmetic(t1, t2);
}

ObObj ObExprObj::type_div(const ObObj& t1, const ObObj& t2, bool int_div_as_double)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  int err = OB_SUCCESS;
  ObExprObj v1;
  v1.type_ = t1.get_type();
  ObExprObj v2;
  v2.type_ = t2.get_type();
  ObExprObj promoted_value1;
  ObExprObj promoted_value2;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (err = div_type_promotion(v1, v2, promoted_value1, promoted_value2,
                                                  p_this, p_other, int_div_as_double)))
  {
    TBSYS_LOG(WARN, "failed to promote type, err=%d", err);
  }
  else
  {
    ret.meta_.type_ = p_this->type_;
    if (ObDecimalType == p_this->type_)
    {
      // @todo decimal precision and scale
    }
  }
  return ret;
}

ObObj ObExprObj::type_mod(const ObObj& t1, const ObObj& t2)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  int err = OB_SUCCESS;
  ObExprObj v1;
  v1.type_ = t1.get_type();
  ObExprObj v2;
  v2.type_ = t2.get_type();
  ObExprObj promoted_obj1;
  ObExprObj promoted_obj2;
  const ObExprObj *p_this = &v1;
  const ObExprObj *p_other = &v2;
  if (OB_SUCCESS != (err = mod_type_promotion(v1, v2, promoted_obj1, promoted_obj2,
                                              p_this, p_other)))
  {
    TBSYS_LOG(WARN, "failed to promote type, err=%d type1=%d type2=%d", err, t1.get_type(), t2.get_type());
  }
  else
  {
    ret.meta_.type_ = ObIntType;
  }
  return ret;
}

ObObj ObExprObj::type_negate(const ObObj& t1)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  switch(t1.get_type())
  {
    case ObIntType:
    case ObFloatType:
    case ObDoubleType:
    case ObDecimalType:
      ret.meta_.type_ = static_cast<uint8_t>(t1.get_type());
      break;
    default:
      TBSYS_LOG(WARN, "not supported type for negate, type=%d", t1.get_type());
      break;
  }
  return ret;
}

ObObj ObExprObj::type_varchar_length(const ObObj& t1)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  if (ObVarcharType != t1.get_type())
  {
    TBSYS_LOG(WARN, "not supported type for varchar_length, type=%d", t1.get_type());
  }
  else
  {
    ret.meta_.type_ = ObIntType;
  }
  return ret;
}

int64_t ObExprObj::to_string(char* buffer, const int64_t length) const
{
  ObObj tmp;
  this->to(tmp);
  return tmp.to_string(buffer, length);
}
