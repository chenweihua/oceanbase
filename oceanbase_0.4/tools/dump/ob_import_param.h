#ifndef __OB_IMPORT_PARAM_H__
#define  __OB_IMPORT_PARAM_H__

#include <vector>
#include <string>
#include "tokenizer.h"
#include "common/ob_schema.h"

using namespace oceanbase::common;

struct ColumnDesc {
  std::string name;
  int offset;
  int len;
};

struct RowkeyDesc {
  int offset;
  int type;
  int pos;
};

struct ColumnInfo {
  const ObColumnSchemaV2 *schema;
  int offset;
};


struct TableParam {
  TableParam() 
    : input_column_nr(0), 
      delima('\01'), 
      rec_delima('\n'), 
      has_nop_flag(false), 
      has_null_flag(false), 
      concurrency(10), 
      has_table_title(false),
      bad_file_(NULL),
      is_delete(false)
      { }

  std::vector<ColumnDesc> col_descs;
  std::string table_name;
  std::string data_file;
  int input_column_nr;
  RecordDelima delima;
  RecordDelima rec_delima;
  bool has_nop_flag;
  char nop_flag;
  bool has_null_flag;
  char null_flag;
  int concurrency;                              /* default 5 threads */
  bool has_table_title;
  const char *bad_file_;
  bool is_delete;
};

class ImportParam {
  public:
    ImportParam();

    int load(const char *file);

    int get_table_param(const char *table_name, TableParam &param);

    void PrintDebug();
  private:
    std::vector<TableParam> params_;
};

#endif
