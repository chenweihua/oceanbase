#include "ob_table_id_name.h"
#include "utility.h"

using namespace oceanbase;
using namespace common;
using namespace nb_accessor;

ObTableIdNameIterator::ObTableIdNameIterator()
   :need_scan_(false), only_core_tables_(true),
   table_idx_(-1), client_proxy_(NULL), res_(NULL)
{
}

ObTableIdNameIterator::~ObTableIdNameIterator()
{
  this->destroy();
  need_scan_ = true;
}

bool ObTableIdNameIterator::check_inner_stat()
{
  bool ret = true;
  if (NULL == client_proxy_)
  {
    ret = false;
    TBSYS_LOG(ERROR, "not init");
  }
  else
  {
    if (need_scan_)
    {
      ObNewRange range;
      range.border_flag_.unset_inclusive_start();
      range.border_flag_.set_inclusive_end();
      range.set_whole_range();
      int err = OB_SUCCESS;
      if(OB_SUCCESS != (err = nb_accessor_.scan(res_, FIRST_TABLET_TABLE_NAME, range, SC("table_name")("table_id"))))
      {
        TBSYS_LOG(WARN, "nb accessor scan fail:ret[%d]", err);
        ret = false;
      }
      else
      {
        TBSYS_LOG(DEBUG, "scan first_tablet_table success. scanner row count =%ld", res_->get_scanner()->get_row_num());
        need_scan_ = false;
      }
    }
    //need add core_schema to res_
  }
  return ret;
}

int ObTableIdNameIterator::init(ObScanHelper* client_proxy, bool only_core_tables)
{
  int ret = OB_SUCCESS;
  if(NULL == client_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "client_proxy is null");
  }

  if(OB_SUCCESS == ret)
  {
    this->only_core_tables_ = only_core_tables;
    this->client_proxy_ = client_proxy;
    table_idx_ = -1;
    if (only_core_tables == false)
    {
      need_scan_ = true;
      ret = nb_accessor_.init(client_proxy);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "init nb_accessor fail:ret[%d]", ret);
      }
      else
      {
        nb_accessor_.set_is_read_consistency(true);
      }
    }
  }
  return ret;
}

int ObTableIdNameIterator::next()
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  if(OB_SUCCESS == ret)
  {
    if (only_core_tables_ == true)
    {
      ++table_idx_;
      TBSYS_LOG(DEBUG, "table_idx=%d", table_idx_);
      if (table_idx_ < 3)
      {
        // we have three basic tables: __first_tablet_entry, __all_all_column, __all_all_join
      }
      else
      {
        ret = OB_ITER_END;
      }
    }
    else
    {
      ++table_idx_;
      TBSYS_LOG(DEBUG, "table_idx=%d", table_idx_);
      if (table_idx_ < 3)
      {
        // we have three basic tables: __first_tablet_entry, __all_all_column, __all_all_join
      }
      else if (NULL == res_)
      {
        TBSYS_LOG(ERROR, "results is NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = res_->next_row();
        if(OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "next row fail:ret[%d]", ret);
        }
      }
    }
  }
  return ret;
}

int ObTableIdNameIterator::get(ObTableIdName** table_info)
{
  int ret = OB_SUCCESS;
  if (0 > table_idx_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "get failed");
  }
  else if (table_idx_ < 3)
  {
    ret = internal_get(table_info);
  }
  else
  {
    if (only_core_tables_ == true)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(WARN, "get core tables but table_idx[%d]_ >= 3", table_idx_);
    }
    else
    {
      ret = normal_get(table_info);
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "table_name: [%.*s], table_id: [%ld]",
        (*table_info)->table_name_.length(),
        (*table_info)->table_name_.ptr(), (*table_info)->table_id_);
  }
  return ret;
}

int ObTableIdNameIterator::internal_get(ObTableIdName** table_info)
{
  int ret = OB_SUCCESS;
  switch(table_idx_)
  {
    case 0:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(FIRST_TABLET_TABLE_NAME),
          static_cast<int32_t>(strlen(FIRST_TABLET_TABLE_NAME)));
      table_id_name_.table_id_ = OB_FIRST_TABLET_ENTRY_TID;
      *table_info = &table_id_name_;
      break;
    case 1:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(OB_ALL_COLUMN_TABLE_NAME),
          static_cast<int32_t>(strlen(OB_ALL_COLUMN_TABLE_NAME)));
      table_id_name_.table_id_ = OB_ALL_ALL_COLUMN_TID;
      *table_info = &table_id_name_;
      break;
    case 2:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(OB_ALL_JOININFO_TABLE_NAME),
          static_cast<int32_t>(strlen(OB_ALL_JOININFO_TABLE_NAME)));
      table_id_name_.table_id_ = OB_ALL_JOIN_INFO_TID;
      *table_info = &table_id_name_;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObTableIdNameIterator::normal_get(ObTableIdName** table_id_name)
{
  int ret = OB_SUCCESS;

  TableRow* table_row = NULL;
  if (NULL == res_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "results is NULL");
  }

  if(OB_SUCCESS == ret)
  {
    ret = res_->get_row(&table_row);
    if(OB_SUCCESS != ret && OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "get row fail:ret[%d]", ret);
    }
  }

  ObCellInfo* cell_info = NULL;
  if(OB_SUCCESS == ret)
  {
    cell_info = table_row->get_cell_info("table_name");
    if(NULL != cell_info)
    {
      if (cell_info->value_.get_type() == ObNullType
          && cell_info->row_key_.get_obj_cnt() > 0
          && cell_info->row_key_.get_obj_ptr()[0].get_type() == ObVarcharType)
      {
        TBSYS_LOG(WARN, "value is null,  get table name from cell rowkey:%s", print_cellinfo(cell_info));
        cell_info->row_key_.get_obj_ptr()[0].get_varchar(table_id_name_.table_name_);
      }
      else
      {
        cell_info->value_.get_varchar(table_id_name_.table_name_);
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "get table_name cell info fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    cell_info = table_row->get_cell_info("table_id");
    if(NULL != cell_info)
    {
      int64_t tmp = 0;
      cell_info->value_.get_int(tmp);
      table_id_name_.table_id_ = static_cast<uint64_t>(tmp);
      if (tmp == 0)
      {
        TBSYS_LOG(INFO, "get table_id = 0");
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "get table_id cell info fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    *table_id_name = &table_id_name_;
  }
  return ret;
}

void ObTableIdNameIterator::destroy()
{
  if(NULL != res_)
  {
    nb_accessor_.release_query_res(res_);
    res_ = NULL;
  }
}

