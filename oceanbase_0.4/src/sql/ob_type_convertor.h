/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_type_convertor.h
 *
 * Authors:
 *
 */
#ifndef _OB__TYPE_CONVERTOR_H
#define _OB__TYPE_CONVERTOR_H 1
#include "ob_item_type.h"

namespace oceanbase
{
  namespace sql
  {
    inline ObObjType convert_item_type_to_obj_type(const ObItemType item_type)
    {
      ObObjType dest_type = ObNullType;
      switch(item_type)
      {
        // TODO: optimize to an array map 
        case T_TYPE_INTEGER:
          dest_type = ObIntType;
          break;
        case T_TYPE_FLOAT:
          dest_type = ObFloatType;
          break;
        case T_TYPE_DOUBLE:
          dest_type = ObDoubleType;
          break;
        case T_TYPE_DECIMAL:
          dest_type = ObDecimalType;
          break;
        case T_TYPE_BOOLEAN:
          dest_type = ObBoolType;
          break;
        case T_TYPE_DATE:
        case T_TYPE_TIME:
        case T_TYPE_DATETIME:
          dest_type = ObDateTimeType;
          break;
        case T_TYPE_TIMESTAMP:
          dest_type = ObPreciseDateTimeType;
          break;
        case T_TYPE_CHARACTER:
        case T_TYPE_VARCHAR:
          dest_type = ObVarcharType;
          break;                
        case T_TYPE_CREATETIME:
          dest_type = ObCreateTimeType;
          break;
        case T_TYPE_MODIFYTIME:
          dest_type = ObModifyTimeType;
          break;
        default:
          break;
      }
      return dest_type;
    }


  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB__TYPE_CONVERTOR_H */
