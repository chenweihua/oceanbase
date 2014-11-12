/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_scan_helper.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SCAN_HELPER_H
#define _OB_SCAN_HELPER_H 1

namespace oceanbase
{
  namespace common
  {
    class ObScanParam;
    class ObScanner;
    class ObMutator;
    
    class ObScanHelper
    {
      public:
        ObScanHelper(){}
        virtual ~ObScanHelper(){}

        virtual int scan(const ObScanParam& scan_param, ObScanner &out) const = 0;
        virtual int mutate(ObMutator& mutator) = 0;

      private:
        // disallow copy
        ObScanHelper(const ObScanHelper &other);
        ObScanHelper& operator=(const ObScanHelper &other);
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_SCAN_HELPER_H */

