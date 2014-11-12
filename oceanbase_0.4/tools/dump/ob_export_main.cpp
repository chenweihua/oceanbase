#include <string>
#include <vector>
#include <inttypes.h>
#include "db_utils.h"
#include "oceanbase_db.h"
#include "db_log_monitor.h"
#include "db_dumper_config.h"
#include "common/utility.h"
#include "common/file_utils.h"
#include "common/ob_string.h"
#include "common/serialization.h"
#include "ob_data_set.h"
#include "tokenizer.h"

using namespace oceanbase::api;
using namespace oceanbase::common;
using namespace std;

void usage(const char *program)
{
  fprintf(stdout, "Usage: %s -t table -c column -h host -p port\n"
      "\t\t-d delima -r rec_delima -m consistency\n"
      "\t\t-s start_key -e end_key \n"
      "\t\t-o output -l count_limit must be set on ob0.3\n"
      "\t\t-k rowkey_split_str\n\n"
      "rowkey_split_str:\tsplit rowkey strategy, rule:\n"
      "\tcolumn_type(i or s, i for int, s for string) + byte_count + comma + next if exist.\n"
      "\te.g. There's rowkey with 21 bytes, which's composed by a int64,\n"
      "\t12 bytes string and a int8. Then `rowkey_split_str' should set `i8,s12,i1'.\n",
      program);
}

struct ExportParam {
  const char *table;
  vector<string> columns;
  const char *host;
  const char *output;
  unsigned short port;
  ObRowkey start_key;
  ObRowkey end_key;
  vector<int> rowkey_split_lens;
  vector<char> rowkey_split_types;
  char *rowkey_split;
  const char *start_key_str;
  const char *end_key_str;
  int64_t count;

  RecordDelima delima;
  RecordDelima rec_delima;

  ObMemBuf start_key_buff;
  ObMemBuf end_key_buff;

  bool consistency;

  ExportParam() : delima(9), rec_delima(10) {   /* default delima TAB -- rec_delima ENTER */
    table = NULL;
    host = NULL;
    output = NULL;
    port = static_cast<uint16_t>(-1);
    consistency = false;
    count = 0;
  }

  int make_obobj(const ObColumnSchemaV2 *col_schema, ObObj &obj, const char *token) const
  {
    int type = col_schema->get_type();
    int token_len = strlen(token);
    int ret = OB_SUCCESS;

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

    return ret;
  }

  int parse_rowkey(const ObSchemaManagerV2 &schema_mgr, const char *key, ObRowkey &rowkey, bool start_key_flag) {
    char * key_buf = strdup(key);
    const ObTableSchema *tab_schema = schema_mgr.get_table_schema(table);
    if (tab_schema == NULL) {
      TBSYS_LOG(ERROR, "no such table[%s]", table);
      return OB_ERROR;
    }

    int ret = OB_SUCCESS;
    vector<char *> list;
    tbsys::CStringUtil::split(key_buf, ",", list);
    const ObRowkeyInfo &rowkey_info = tab_schema->get_rowkey_info();

    ObObj *obj_ptr = new ObObj[rowkey_info.get_size()];
    int64_t i = 0;
    for(;i < rowkey_info.get_size();i++) {
      ObRowkeyColumn rowkey_column;
      rowkey_info.get_column(i, rowkey_column);

      const ObColumnSchemaV2 *col_schema = 
        schema_mgr.get_column_schema(tab_schema->get_table_id(), rowkey_column.column_id_);
      ObObj obj;
      TBSYS_LOG(INFO, "rowkey[%ld] column_id:[%ld] column_name[%s]", i, 
          rowkey_column.column_id_, col_schema->get_name());
      if (i < static_cast<int64_t>(list.size())) {
        ret = make_obobj(col_schema, obj, list[i]);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "make obj failed, value=%s, column=%s", list[i], col_schema->get_name());
          break;
        }
        obj_ptr[i] = obj;
      } else {
        if (start_key_flag) {
          obj.set_min_value();
        } else {
          obj.set_max_value();
        }
        obj_ptr[i] = obj;
        //break;
      }
    }
    if (ret == OB_SUCCESS) {
      rowkey.assign(obj_ptr, i);
    }

    return ret;
  }

  int build_rowkeys(const ObSchemaManagerV2 &schema_mgr) {
    int ret = parse_rowkey(schema_mgr, start_key_str, start_key, true);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "parse start key failed");
    } else {
      ret = parse_rowkey(schema_mgr, end_key_str, end_key, false);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "parse end key failed");
      }
    }

    if (ret == OB_SUCCESS) {
      static char buf[1024 * 1024];
      int64_t len = start_key.to_string(buf, 1024 * 1024);
      buf[len] = 0;
      TBSYS_LOG(INFO, "start key: [%s]", buf);
      len = end_key.to_string(buf, 1024 * 1024);
      buf[len] = 0;
      TBSYS_LOG(INFO, "end key: [%s]", buf);
    }

    return ret;
  }

  void parse_delima(const char *str_delima, RecordDelima &out_delima) {
    const char *end_pos = str_delima + strlen(str_delima);

    if (find(str_delima, end_pos, ',') == end_pos) {
      out_delima = RecordDelima(static_cast<char>(atoi(str_delima)));
    } else {
      int part1, part2;

      sscanf(str_delima, "%d,%d", &part1, &part2);
      out_delima = RecordDelima(static_cast<char>(part1), static_cast<char>(part2));
    }
  }

  void set_startkey(const char *str_key) {
    start_key_str = str_key;
  }

  void set_endkey(const char *str_key) {
    end_key_str = str_key;
  }

  void parse_rowkey_split(char *split_str) {
    int len;
    char *p;

    p = strtok(split_str, ",");
    while (NULL != p) {
      len = atoi(p + 1);
      if ((p[0] != 's' && p[0] != 'i') || len == 0) {
        fprintf(stderr, "rowkey split parameter parsed error!");
        exit(-1);
      }
      rowkey_split_types.push_back(p[0]);
      rowkey_split_lens.push_back(len);

      p = strtok(NULL, ",");
    }

  }

  int check_param() {
    int ret = 0;
    if (host == NULL || table == NULL || columns.size() == 0 ||
        start_key.length() == 0 || end_key.length() == 0) {
      ret = -1;
    }

    return ret;
  }

  void PrintDebug() {
    fprintf(stderr, "#######################################################################\n");

    fprintf(stderr, "host=%s, port=%d, table name=[%s]\n", host, port, table);
    fprintf(stderr, "delima type = %d, part1 = %d, part2 = %d\n", delima.type_, 
        delima.part1_, delima.part2_ );
    fprintf(stderr, "rec_delima type = %d, part1 = %d, part2 = %d\n", rec_delima.type_, 
        rec_delima.part1_, rec_delima.part2_ );

    fprintf(stderr, "columns list as follows\n");
    for(size_t i = 0;i < columns.size(); i++) {
      fprintf(stderr, "ID:%ld\t%s\n", i, columns[i].c_str());
    }

    //fprintf(stderr, "start rowkey = %s\n", print_string(start_key));
    //fprintf(stderr, "end rowkey = %s\n", print_string(end_key));
  }
};

static ExportParam export_param;

int parse_options(int argc, char *argv[])
{
  int ret = 0;
  while ((ret = getopt(argc, argv, "o:t:c:h:p:d:r:s:e:m:k:l:")) != -1) {
    switch(ret) {
     case 't':
       export_param.table = optarg;
       break;
     case 'c':
       export_param.columns.push_back(optarg);
       break;
     case 'h':
       export_param.host = optarg;
       break;
     case 'p':
       export_param.port = static_cast<unsigned short>(atol(optarg));
       break;
     case 'd':
       export_param.parse_delima(optarg, export_param.delima);
       break;
     case 'r':
       export_param.parse_delima(optarg, export_param.rec_delima);
       break;
     case 's':
       export_param.set_startkey(optarg);
       break;
     case 'e':
       export_param.set_endkey(optarg);
       break;
     case 'm':
       export_param.consistency = static_cast<bool>(atol(optarg));
       break;
     case 'o':
       export_param.output = optarg;
       break;
     case 'k':
       export_param.parse_rowkey_split(optarg);
       break;
     case 'l':
       export_param.count = atol(optarg);
       break;
     default:
       usage(argv[0]);
       exit(0);
    }
  }

  OceanbaseDb db(export_param.table, export_param.port); db.init();
  ObSchemaManagerV2 schema_mgr;

  RPC_WITH_RETIRES(db.fetch_schema(schema_mgr), 5, ret);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "fetch schema failed, ret=%d", ret);
  } else {
    ret = export_param.build_rowkeys(schema_mgr);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "build rowkeys failed");
    }
  }
  
  if (ret == OB_SUCCESS) {
    ret = export_param.check_param();
  }

  return ret;
}

static int append_delima(ObDataBuffer &buffer, const RecordDelima &delima)
{
  if (buffer.get_remain() < delima.length()) {
    return OB_ERROR;
  }

  delima.append_delima(buffer.get_data(), buffer.get_position(), buffer.get_capacity());
  delima.skip_delima(buffer.get_position());
  return OB_SUCCESS;
}

int write_record(FILE *out, DbRecord *recp)
{
#define MAX_RECORD_LEN 2 * 1024 * 1024
  static char write_buffer[MAX_RECORD_LEN];
  ObDataBuffer buffer(write_buffer, MAX_RECORD_LEN);

  int ret = OB_SUCCESS;
  vector<string>::iterator itr = export_param.columns.begin();
  ObCellInfo *cell = NULL;
  ObRowkey rowkey;
  ret = recp->get_rowkey(rowkey);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "can't find rowkey from record, currupted data or bugs!\n");
    return ret;
  }

  for (int64_t i = 0;i < rowkey.length();i++) {
    const ObObj &obj = rowkey.ptr()[i];
    int len = append_obj(obj, buffer);
    if (len < 0 ) {
      ret = OB_ERROR;
      break;
    }
    if ((ret = append_delima(buffer, export_param.delima)) != OB_SUCCESS) {
      break;
    }
  }

  if (ret == OB_SUCCESS) {
    while (itr != export_param.columns.end()) {
      ret = recp->get(itr->c_str(), &cell);
      if (ret != OB_SUCCESS) {
        fprintf(stderr, "can't get column [%s]\n", itr->c_str());
        break;
      }

      if (serialize_cell(cell, buffer) < 0) {
        fprintf(stderr, "can't serialize_cell\n");
        break;
      }

      if (itr != export_param.columns.end() - 1) {
        ret = append_delima(buffer, export_param.delima);
        if (ret != OB_SUCCESS) {
          fprintf(stderr, "can't append delima, length=%ld\n", buffer.get_position());
          break;
        }
      }
      itr++;
    }

    ret = append_delima(buffer, export_param.rec_delima);
    if (ret != OB_SUCCESS) {
      fprintf(stderr, "can't append record delima, pos=%ld\n", buffer.get_position());
    } else {
      fwrite(buffer.get_data(), buffer.get_position(), 1, out);
    }
  }

  return ret;
}

void scan_and_dump()
{
  OceanbaseDb db(export_param.host, export_param.port, 8 * kDefaultTimeout);
  if (db.init() != OB_SUCCESS) {
    fprintf(stderr, "can't init database, host=%s, port=%d\n", export_param.host, export_param.port);
    return;
  }
  db.set_consistency(export_param.consistency);
  ObDataSet data_set(&db);
  data_set.set_inclusie_start(false);
  data_set.set_scan_limit(export_param.count);

  int ret = data_set.set_data_source(export_param.table, export_param.columns, 
      export_param.start_key, export_param.end_key);
  if (ret != OB_SUCCESS) {
    fprintf(stderr, "can't init data source, please your config\n");
    return;
  }

  FILE *out = NULL;
  if (export_param.output == NULL) {
    out = stdout;
  } else {
    out = fopen(export_param.output, "w");
    if (out == NULL) {
      fprintf(stderr, "can't create output file, %s", export_param.output);
      return;
    }
  }

  DbRecord *recp = NULL;
  while (data_set.has_next()) {
    ret = data_set.get_record(recp);
    if (ret != OB_SUCCESS) {
      fprintf(stderr, "get record from data set failed\n");
      break;
    }

    ret = write_record(out, recp);
    if (ret != OB_SUCCESS) {
      fprintf(stderr, "can't write to output file, %s\n", export_param.output);
      break;
    }

    data_set.next();
  }

  fclose(out);
}

int main(int argc, char *argv[])
{
  OceanbaseDb::global_init(NULL, "info");

  int ret = parse_options(argc, argv);
  if (ret != 0) {
    fprintf(stderr, "wrong params is detected, please check it\n");
    usage(argv[0]);
    exit(1);
  }

  export_param.PrintDebug();
  scan_and_dump();
}
