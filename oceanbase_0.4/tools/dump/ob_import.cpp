#include "ob_import.h"
#include "db_utils.h"
#include "common/ob_object.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/ob_tsi_factory.h"
#include <string>
using namespace std;

using namespace oceanbase::common;

extern bool g_gbk_encoding;
extern bool g_print_lineno_taggle;

ObRowBuilder::ObRowBuilder(ObSchemaManagerV2 *schema,  
                           const TableParam &table_param
                           ) : table_param_(table_param)
{
  schema_ = schema;
  memset(columns_desc_, 0, sizeof(columns_desc_));
  //memset(rowkey_desc_, 0, sizeof(rowkey_desc_));
  atomic_set(&lineno_, 0);
  rowkey_max_size_ = OB_MAX_ROW_KEY_LENGTH;
  columns_desc_nr_ = 0;
}


ObRowBuilder::~ObRowBuilder()
{
  TBSYS_LOG(INFO, "Processed lines = %d", atomic_read(&lineno_));
}

int ObRowBuilder::set_column_desc(const std::vector<ColumnDesc> &columns)
{
  int ret = OB_SUCCESS;

  //columns_desc_nr_ = static_cast<int32_t>(columns.size());
  for (size_t i = 0; i < columns.size(); i++) {
    const ObColumnSchemaV2 *col_schema = 
      schema_->get_column_schema(table_param_.table_name.c_str(), columns[i].name.c_str());

    int offset = columns[i].offset;
    if (offset >= table_param_.input_column_nr) {
      TBSYS_LOG(ERROR, "wrong config table=%s, columns=%s", table_param_.table_name.c_str(), columns[i].name.c_str());
      ret = OB_ERROR;
      break;
    }

    if (col_schema) {
      //ObModifyTimeType, ObCreateTimeType update automaticly, skip
      if (col_schema->get_type() != ObModifyTimeType &&
          col_schema->get_type() != ObCreateTimeType) {
        columns_desc_[columns_desc_nr_].schema = col_schema;
        columns_desc_[columns_desc_nr_].offset = offset;
        columns_desc_nr_++;
      }
    } else {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "column:%s is not a legal column in table %s", 
                columns[i].name.c_str(), table_param_.table_name.c_str());
      break;
    }
  }

  //schema_->print_info();
  const ObTableSchema *table_schema = schema_->get_table_schema(table_param_.table_name.c_str());
  if (table_schema != NULL) {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    rowkey_desc_nr_ = rowkey_info.get_size();
    assert(rowkey_desc_nr_ < (int64_t)kMaxRowkeyDesc);

    for(int64_t i = 0;i < rowkey_info.get_size(); i++) {
      ObRowkeyColumn rowkey_column;
      rowkey_info.get_column(i, rowkey_column);

      TBSYS_LOG(INFO, "rowkey_info idx=%ld, column_id = %ld", i, rowkey_column.column_id_);
      int64_t idx = 0;
      for(;idx < columns_desc_nr_; idx++) {
        const ObColumnSchemaV2 *schema = columns_desc_[idx].schema;
        if (NULL == schema) {
          continue;
        }
        if (rowkey_column.column_id_ == schema->get_id()) {
          break;
        }
      }
      if (idx >= columns_desc_nr_) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "row_desc in config file is not correct");
      }
      else {
        rowkey_offset_[i] = idx;
        if (idx == OB_MAX_COLUMN_NUMBER) {
          TBSYS_LOG(ERROR, "%ldth rowkey column is not specified", i);
          ret = OB_ERROR;
          break;
        }
      }
    }
  } else {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "no such table %s in schema", table_param_.table_name.c_str());
  }

  return ret;
}


/*
int ObRowBuilder::set_rowkey_desc(const std::vector<RowkeyDesc> &rowkeys)
{
  int ret = OB_SUCCESS;

  const ObTableSchema *tab_schema = schema_->get_table_schema(table_param_.table_name.c_str());
  if (tab_schema == NULL) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "no such table in schema");
  } else {
    rowkey_max_size_  = tab_schema->get_rowkey_max_length();
  }

  if (ret == OB_SUCCESS) {
    rowkey_desc_nr_ = rowkeys.size();

    for (size_t i = 0;i < rowkey_desc_nr_;i++) {
      rowkey_desc_[i].pos = -1;
    }

    for(size_t i = 0;i < rowkeys.size(); i++) {
      int pos = rowkeys[i].pos;
      int offset = rowkeys[i].offset;

      if (offset >= table_param_.input_column_nr) {
        TBSYS_LOG(ERROR, "wrong config, table=%s, offset=%d", table_param_.table_name.c_str(), offset);
        ret = OB_ERROR;
        break;
      }
      rowkey_desc_[pos] = rowkeys[i];
    }

    for (size_t i = 0;i < rowkey_desc_nr_;i++) {
      if (rowkey_desc_[i].pos == -1) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "rowkey config error, intervals in it, pos=%ld, please check it", i);
        break;
      }
    }
  }

  return ret;
}
*/

bool ObRowBuilder::check_valid()
{
  bool valid = true;

  if (table_param_.input_column_nr <= 0) {
    TBSYS_LOG(ERROR, "input data file has 0 column");
    valid = false;
  }

  return valid;
}


int ObRowBuilder::build_tnx(RecordBlock &block, DbTranscation *tnx) const
{
  int ret = OB_SUCCESS;
  int token_nr = kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;
  TokenInfo tokens[token_nr];

  Slice slice;
  block.reset();
  while (block.next_record(slice)) {
    token_nr = kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;
    if (g_gbk_encoding) {
      if (RecordDelima::CHAR_DELIMA == table_param_.delima.delima_type() && table_param_.delima.get_char_delima() < 128u) {
        Tokenizer::tokenize_gbk(slice, static_cast<char>(table_param_.delima.get_char_delima()), token_nr, tokens);
      }
      else {
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(ERROR, "short delima or delima great than 128 is not support in gbk encoding");
        break;
      }
    }
    else {
      Tokenizer::tokenize(slice, table_param_.delima, token_nr, tokens);
    }
    if (token_nr != table_param_.input_column_nr) {
      TBSYS_LOG(ERROR, "corrupted data files, please check it, input column number=%d, real nr=%d, slice[%.*s]", 
        table_param_.input_column_nr, token_nr, static_cast<int>(slice.size()), slice.data());
      ret = OB_ERROR;
      break;
    }

    atomic_add(1, &lineno_);
    if (g_print_lineno_taggle) {
      g_print_lineno_taggle = false;
      TBSYS_LOG(INFO, "proccessed line no [%d]", atomic_read(&lineno_));
    }

    ObRowkey rowkey;
    ret = create_rowkey(rowkey, tokens);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't rowkey, quiting");
    }

    RowMutator mutator;
    if (ret == OB_SUCCESS) {
      ret = tnx->insert_mutator(table_param_.table_name.c_str(), rowkey, &mutator);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't create insert mutator , table=%s", table_param_.table_name.c_str());
      }
    }

    if (table_param_.is_delete) {
      if (OB_SUCCESS != (ret = mutator.delete_row())) {
        TBSYS_LOG(WARN, "fail to delete row:ret[%d]", ret);
      }
    }
    else {
      if (ret == OB_SUCCESS) {
        ret = setup_content(&mutator, tokens);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't transcation content, table=%s", table_param_.table_name.c_str());
          for (int i = 0; i < token_nr; i++) {
            TBSYS_LOG(ERROR, "token seq[%d] [%.*s]", i, static_cast<int>(tokens[i].len), tokens[i].token);
          }
        }
      }
    }

    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "failed slice[%.*s]", static_cast<int>(slice.size()), slice.data());
      break;
    }
  }

  return ret;
}

int ObRowBuilder::create_rowkey(ObRowkey &rowkey, TokenInfo *tokens) const
{
  int ret = OB_SUCCESS;
  //static __thread ObObj buffer[kMaxRowkeyDesc];
  ObMemBuf *mbuf = GET_TSI_MULT(ObMemBuf, TSI_MBUF_ID);

  if (mbuf == NULL || mbuf->ensure_space(rowkey_desc_nr_ * sizeof(ObObj))) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "can't create object from TSI, or ObMemBuff can't alloc memory");
  }

  if (ret == OB_SUCCESS) {
    ObObj *buffer = (ObObj *)mbuf->get_buffer();
    for(int64_t i = 0;i < rowkey_desc_nr_; i++) {
      ObObj obj;
      int token_idx = columns_desc_[rowkey_offset_[i]].offset;
      if (table_param_.has_nop_flag) {
        if (table_param_.nop_flag == tokens[token_idx].token[0]) {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "There is nop flag in rowkey. rowkey seq[%ld]", i);
          return ret;
        }
      }

      if ((ret = make_obobj(columns_desc_[rowkey_offset_[i]], obj, tokens)) != OB_SUCCESS) {
        TBSYS_LOG(WARN, "create rowkey failed, pos = %ld", i);
        break;
      }
      buffer[i] = obj;
    }
    rowkey.assign(buffer, rowkey_desc_nr_);
  }

  return ret;
}

int ObRowBuilder::make_obobj(const ColumnInfo &column_info, ObObj &obj, TokenInfo *tokens) const
{
  int ret = OB_SUCCESS;
  if (NULL == column_info.schema) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "column_info schema is null");
    return ret;
  }

  int token_idx = column_info.offset;
  int type = column_info.schema->get_type();
  const char *token = tokens[token_idx].token;
  int token_len = static_cast<int32_t>(tokens[token_idx].len);
  bool is_null = false;

  if (1 == token_len && NULL != token) {
    if (table_param_.null_flag == token[0]) {
      is_null = true;
    }
  }

  if (token_len == 0 || (table_param_.has_null_flag && is_null)) {
    obj.set_null();
  }
  else {
    switch(type) {
     case ObIntType:
       {
         int64_t lv = 0;
         if (token_len != 0)
           lv = atol(token);

         obj.set_int(lv);
       }
       break;
     case ObFloatType:
       if (token_len != 0)
         obj.set_float(strtof(token, NULL));
       else
         obj.set_float(0);
       break;

     case ObDoubleType:
       if (token_len != 0)
         obj.set_double(strtod(token, NULL));
       else
         obj.set_double(0);
       break;
     case ObDateTimeType:
       {
         ObDateTime t = 0;
         ret = transform_date_to_time(token, token_len, t);
         if (OB_SUCCESS != ret)
         {
           TBSYS_LOG(ERROR,"transform_date_to_time error");
         }
         else
         {
           obj.set_datetime(t);
         }
       }
       break;
     case ObVarcharType:
       {
         ObString bstring;
         bstring.assign_ptr(const_cast<char *>(token),token_len);
         obj.set_varchar(bstring);
       }
       break;
     case ObPreciseDateTimeType:
       {
         ObDateTime t = 0;
         ret = transform_date_to_time(token, token_len, t);
         if (OB_SUCCESS != ret)
         {
           TBSYS_LOG(ERROR,"transform_date_to_time error");
         }
         else
         {
           obj.set_precise_datetime(t * 1000 * 1000L); //seconds -> ms
         }
       }
       break;
     default:
       TBSYS_LOG(ERROR,"unexpect type index: %d", type);
       ret = OB_ERROR;
       break;
    }
  }

  return ret;
}

int ObRowBuilder::setup_content(RowMutator *mutator, TokenInfo *tokens) const
{
  int ret = OB_SUCCESS;

  const ObRowkeyInfo &rowkey_info = schema_->get_table_schema(table_param_.table_name.c_str())->get_rowkey_info();
  for(int i = 0;i < columns_desc_nr_ && ret == OB_SUCCESS; i++) {
    const ObColumnSchemaV2 *schema = columns_desc_[i].schema;
    if (NULL == schema) {
      continue;
    }

    if (rowkey_info.is_rowkey_column(schema->get_id())) {
      continue;
    }

    ObObj obj;

    const char *token = tokens[columns_desc_[i].offset].token;
    int token_len = static_cast<int32_t>(tokens[columns_desc_[i].offset].len);
    bool is_nop = false;

    if (token_len == 1 && NULL != token) {
      if (token[0] == table_param_.nop_flag) {
        is_nop = true;
      }
    }

    if (!table_param_.has_nop_flag || !is_nop) {
      ret = make_obobj(columns_desc_[i], obj, tokens);
      if (ret == OB_SUCCESS) {
        ret = mutator->add(schema->get_name(), obj);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't add column to row mutator");
          break;
        } else {
          TBSYS_LOG(DEBUG, "obj idx=%d, name=%s", columns_desc_[i].offset, schema->get_name());
          obj.dump();
        }
      } else {
        TBSYS_LOG(ERROR, "can't make obobj, column=%s", schema->get_name());
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "nop token, offset[%d]", columns_desc_[i].offset);
    }
  }

  return ret;
}

