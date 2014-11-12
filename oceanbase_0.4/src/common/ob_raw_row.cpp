/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_raw_row.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_raw_row.h"
using namespace oceanbase::common;
ObRawRow::ObRawRow()
  : cells_((ObObj*)cells_buffer_), cells_count_(0), reserved1_(0), reserved2_(0)
{
  memset(cells_buffer_, 0, sizeof(cells_buffer_));
}

ObRawRow::~ObRawRow()
{
}

void ObRawRow::assign(const ObRawRow &other)
{
  if (this != &other)
  {
    for (int16_t i = 0; i < other.cells_count_; ++i)
    {
      this->cells_[i] = other.cells_[i];
    }
    this->cells_count_ = other.cells_count_;
  }
}

int ObRawRow::add_cell(const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  if (cells_count_ >= MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(WARN, "array overflow, cells_count=%hd", cells_count_);
    ret = OB_SIZE_OVERFLOW;
  }
  else
  {
    cells_[cells_count_++] = cell;
  }
  return ret;
}
