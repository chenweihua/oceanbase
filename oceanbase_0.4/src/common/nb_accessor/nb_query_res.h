/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * nb_query_res.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _NB_QUERY_RES_H
#define _NB_QUERY_RES_H 1

#include "common/ob_scanner.h"
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_object.h"
#include "common/ob_rowkey.h"
#include "common/ob_simple_condition.h"
#include "common/ob_easy_array.h"

#include "nb_table_row.h"

namespace oceanbase
{
namespace common
{
  namespace nb_accessor
  {
    typedef EasyArray<const char*> SC;

    //scan和get操作的返回的对象，包含了一个或者多个TableRow
    class QueryRes
    {
    public:
      QueryRes();
      virtual ~QueryRes();

      int get_row(TableRow** table_row);
      int next_row();

      int add_row(TableRow * table_row);
      TableRow* get_only_one_row(); //取出第一个TableRow

      int init(const SC& sc);

      inline ObScanner* get_scanner()
      {
        return &scanner_;
      }
      
    private:
      TableRow cur_row_;
      ObScanner scanner_;
      ObScannerIterator scanner_iter_;
      hash::ObHashMap<const char*,int64_t> cell_map_; //保存列名到列序号的对应关系
      bool first_row_;
    };
  }
}
}

#endif /* _NB_QUERY_RES_H */

