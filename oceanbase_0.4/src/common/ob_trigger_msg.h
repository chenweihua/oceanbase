/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_trigger_msg.h,  12/14/2012 11:09:36 AM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *
 *
 */
#ifndef __COMMON_OB_TRIGGER_MSG_
#define __COMMON_OB_TRIGGER_MSG_

#include "ob_define.h"
#include "ob_string.h"

namespace oceanbase
{
  namespace common
  {
    const static int64_t REFRESH_NEW_SCHEMA_TRIGGER = 1;
    const static int64_t UPDATE_PRIVILEGE_TIMESTAMP_TRIGGER = 2;
    const static int64_t SLAVE_BOOT_STRAP_TRIGGER = 3;
    const static int64_t REFRESH_NEW_CONFIG_TRIGGER = 4;
    const static int64_t CREATE_TABLE_TRIGGER = 5;
    const static int64_t DROP_TABLE_TRIGGER = 6;

    class ObTriggerMsg{
     public:
       ObTriggerMsg(){}
       ~ObTriggerMsg(){}
       NEED_SERIALIZE_AND_DESERIALIZE;
     public:
       ObString src;
       int64_t type;
       int64_t param;
    };
  }; // end namespace common
};

#endif

