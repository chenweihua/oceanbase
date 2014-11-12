/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-26
*
*   Authors:
*          daoan(daoan@taobao.com)
*          maoqi(maoqi@taobao.com)
*          fangji.hcm(fangji.hcm@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_COMMON_OB_SCHEMA_H_
#define OCEANBASE_COMMON_OB_SCHEMA_H_
#include <stdint.h>

#include <tbsys.h>

#include "ob_define.h"
#include "ob_object.h"
#include "ob_string.h"
#include "ob_array.h"
#include "hash/ob_hashutils.h"
#include "hash/ob_hashmap.h"
#include "ob_postfix_expression.h"
#include "ob_hint.h"

#define PERM_TABLE_NAME "__perm_info"
#define USER_TABLE_NAME "__user_info"
#define SKEY_TABLE_NAME "__skey_info"
#define PERM_COL_NAME "perm_desc"
#define USER_COL_NAME "password"
#define SKEY_COL_NAME "secure_key"

#define PERM_TABLE_ID 100
#define USER_TABLE_ID 101
#define SKEY_TABLE_ID 102
#define PERM_COL_ID 4
#define USER_COL_ID 4
#define SKEY_COL_ID 4

namespace oceanbase
{
  namespace common
  {
    const int64_t OB_SCHEMA_VERSION = 1;
    const int64_t OB_SCHEMA_VERSION_TWO = 2;
    const int64_t OB_SCHEMA_VERSION_THREE = 3;
    const int64_t OB_SCHEMA_VERSION_FOUR = 4;
    const int64_t OB_SCHEMA_VERSION_FOUR_FIRST = 401;
    const int64_t OB_SCHEMA_VERSION_FOUR_SECOND = 402;

    struct TableSchema;
    //these classes are so close in logical, so I put them together to make client have a easy life
    typedef ObObjType ColumnType;
    const char* convert_column_type_to_str(ColumnType type);
    struct ObRowkeyColumn
    {
      enum Order
      {
        ASC = 1,
        DESC = -1,
      };
      int64_t length_;
      uint64_t column_id_;
      ObObjType type_;
      Order order_;
      NEED_SERIALIZE_AND_DESERIALIZE;
      int64_t to_string(char* buf, const int64_t buf_len) const;
    };

    class ObRowkeyInfo
    {

    public:
      ObRowkeyInfo();
      ~ObRowkeyInfo();

      inline int64_t get_size() const
      {
        return size_;
      }

      /**
       * get sum of every column's length.
       */
      int64_t get_binary_rowkey_length() const;

      /**
       * Get rowkey column by index
       * @param[in]  index   column index in RowkeyInfo
       * @param[out] column
       *
       * @return int  return OB_SUCCESS if get the column, otherwist return OB_ERROR
       */
      int get_column(const int64_t index, ObRowkeyColumn& column) const;
      const ObRowkeyColumn *get_column(const int64_t index) const;

      /**
       * Get rowkey column id by index
       * @param[in]  index   column index in RowkeyInfo
       * @param[out] column_id in ObRowkeyInfo
       *
       * @return int  return OB_SUCCESS if get the column, otherwist return OB_ERROR
       */
      int get_column_id(const int64_t index, uint64_t & column_id) const;

      /**
       * Add column to rowkey info
       * @param column column to add
       * @return itn  return OB_SUCCESS if add success, otherwise return OB_ERROR
       */
      int add_column(const ObRowkeyColumn& column);

      int get_index(const uint64_t column_id, int64_t &index, ObRowkeyColumn& column) const;
      bool is_rowkey_column(const uint64_t column_id) const;
      int set_column(int64_t idx, const ObRowkeyColumn& column);

      int64_t to_string(char* buf, const int64_t buf_len) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    private:
      ObRowkeyColumn columns_[OB_MAX_ROWKEY_COLUMN_NUMBER];
      int64_t size_;
    };


    class ObOperator;
    class ObSchemaManagerV2;

    class ObColumnSchemaV2
    {
      public:

        struct ObJoinInfo
        {
          ObJoinInfo() : join_table_(OB_INVALID_ID),left_column_count_(0) {}
          uint64_t join_table_;   // join table id
          uint64_t correlated_column_;  // column in joined table
          //this means which part of left_column_ to be used make rowkey
          uint64_t left_column_offset_array_[OB_MAX_ROWKEY_COLUMN_NUMBER];
          uint64_t left_column_count_;

          int64_t to_string(char *buf, const int64_t buf_len) const
          {
            int64_t pos = 0;
            databuff_printf(buf, buf_len, pos, "join_table[%lu], correlated_column[%lu], left_column_count[%lu]",
              join_table_, correlated_column_, left_column_count_);
            return pos;
          }
        };

        ObColumnSchemaV2();
        ~ObColumnSchemaV2() {}
        ObColumnSchemaV2& operator=(const ObColumnSchemaV2& src_schema);

        uint64_t    get_id()   const;
        const char* get_name() const;
        ColumnType  get_type() const;
        int64_t     get_size() const;
        uint64_t    get_table_id()        const;
        bool        is_maintained()       const;
        uint64_t    get_column_group_id() const;
        bool        is_join_column() const;

        void set_table_id(const uint64_t id);
        void set_column_id(const uint64_t id);
        void set_column_name(const char *name);
        void set_column_name(const ObString& name);
        void set_column_type(const ColumnType type);
        void set_column_size(const int64_t size); //only used when type is varchar
        void set_column_group_id(const uint64_t id);
        void set_maintained(bool maintained);

        void set_join_info(const uint64_t join_table, const uint64_t* left_column_id,
            const uint64_t left_column_count, const uint64_t correlated_column);

        const ObJoinInfo* get_join_info() const;

        bool operator==(const ObColumnSchemaV2& r) const;

        static ColumnType convert_str_to_column_type(const char* str);

        //this is for test
        void print_info() const;
        void print(FILE* fd) const;

        NEED_SERIALIZE_AND_DESERIALIZE;

        int deserialize_v3(const char* buf, const int64_t data_len, int64_t& pos);
        int deserialize_v4(const char* buf, const int64_t data_len, int64_t& pos);

        inline bool is_nullable() const { return is_nullable_; }
        inline void set_nullable(const bool null) { is_nullable_ = null; }
        inline const ObObj & get_default_value()  const { return default_value_; }
        inline void set_default_value(const ObObj& value) { default_value_ = value; }

      private:
        friend class ObSchemaManagerV2;
      private:
        bool maintained_;
        bool is_nullable_;

        uint64_t table_id_;
        uint64_t column_group_id_;
        uint64_t column_id_;

        int64_t size_;  //only used when type is char or varchar
        ColumnType type_;
        char name_[OB_MAX_COLUMN_NAME_LENGTH];

        ObObj default_value_;
        //join info
        ObJoinInfo join_info_;

        //in mem
        ObColumnSchemaV2* column_group_next_;
    };

    inline static bool column_schema_compare(const ObColumnSchemaV2& lhs, const ObColumnSchemaV2& rhs)
    {
      bool ret = false;
      if ( (lhs.get_table_id() < rhs.get_table_id()) ||
           (lhs.get_table_id() == rhs.get_table_id() && lhs.get_column_group_id() < rhs.get_column_group_id()) ||
           (lhs.get_table_id() == rhs.get_table_id() &&
            (lhs.get_column_group_id() == rhs.get_column_group_id()) && lhs.get_id() < rhs.get_id()) )
      {
        ret = true;
      }
      return ret;
    }

    struct ObColumnSchemaV2Compare
    {
      bool operator()(const ObColumnSchemaV2& lhs, const ObColumnSchemaV2& rhs)
      {
        return column_schema_compare(lhs, rhs);
      }
    };


    class ObTableSchema
    {
      public:
        ObTableSchema();
        ~ObTableSchema() {}
        ObTableSchema& operator=(const ObTableSchema& src_schema);
        enum TableType
        {
          INVALID = 0,
          SSTABLE_IN_DISK,
          SSTABLE_IN_RAM,
        };
        bool is_merge_dynamic_data() const;
        uint64_t    get_table_id()   const;
        TableType   get_table_type() const;
        const char* get_table_name() const;
        const char* get_compress_func_name() const;
        const char* get_comment_str() const;
        uint64_t    get_max_column_id() const;
        const char* get_expire_condition() const;
        const ObRowkeyInfo&  get_rowkey_info() const;
        ObRowkeyInfo& get_rowkey_info();
        int64_t     get_version() const;

        int32_t get_split_pos() const;
        int32_t get_rowkey_max_length() const;

        bool is_pure_update_table() const;
        bool is_use_bloomfilter()   const;
        bool is_read_static()   const;
        bool has_baseline_data() const;
        bool is_expire_effect_immediately() const;
        int32_t get_block_size()    const;
        int64_t get_max_sstable_size() const;
        int64_t get_expire_frequency()    const;
        int64_t get_query_cache_expire_time() const;
        int64_t get_max_scan_rows_per_tablet() const;
        int64_t get_internal_ups_scan_size() const;
        int64_t get_merge_write_sstable_version() const;
        int64_t get_replica_count() const;
        int64_t get_schema_version() const;

        uint64_t get_create_time_column_id() const;
        uint64_t get_modify_time_column_id() const;

        void set_table_id(const uint64_t id);
        void set_max_column_id(const uint64_t id);
        void set_version(const int64_t version);

        void set_table_type(TableType type);
        void set_split_pos(const int64_t split_pos);

        void set_rowkey_max_length(const int64_t len);
        void set_block_size(const int64_t block_size);
        void set_max_sstable_size(const int64_t max_sstable_size);

        void set_table_name(const char* name);
        void set_table_name(const ObString& name);
        void set_compressor_name(const char* compressor);
        void set_compressor_name(const ObString& compressor);

        void set_pure_update_table(bool is_pure);
        void set_use_bloomfilter(bool use_bloomfilter);
        void set_read_static(bool read_static);
        void set_has_baseline_data(const bool base_data);
        void set_expire_effect_immediately(const int64_t expire_effect_immediately);

        void set_expire_condition(const char* expire_condition);
        void set_expire_condition(const ObString& expire_condition);

        void set_comment_str(const char* comment_str);
        void set_expire_frequency(const int64_t expire_frequency);
        void set_query_cache_expire_time(const int64_t expire_time);
        void set_rowkey_info(ObRowkeyInfo& rowkey_info);
        void set_max_scan_rows_per_tablet(const int64_t max_scan_rows);
        void set_internal_ups_scan_size(const int64_t scan_size);
        void set_merge_write_sstable_version(const int64_t version);
        void set_replica_count(const int64_t count) ;
        void set_schema_version(const int64_t version);

        void set_create_time_column(uint64_t id);
        void set_modify_time_column(uint64_t id);
        ObConsistencyLevel get_consistency_level() const;
        void set_consistency_level(int64_t consistency_level);

        bool operator ==(const ObTableSchema& r) const;
        bool operator ==(const ObString& table_name) const;
        bool operator ==(const uint64_t table_id) const;

        NEED_SERIALIZE_AND_DESERIALIZE;
        //this is for test
        void print_info() const;
        void print(FILE* fd) const;
      private:
        int deserialize_v3(const char* buf, const int64_t data_len, int64_t& pos);
        int deserialize_v4(const char* buf, const int64_t data_len, int64_t& pos);
      private:
        static const int64_t TABLE_SCHEMA_RESERVED_NUM = 1;
        uint64_t table_id_;
        uint64_t max_column_id_;
        int64_t rowkey_split_;
        int64_t rowkey_max_length_;

        int32_t block_size_; //KB
        TableType table_type_;

        char name_[OB_MAX_TABLE_NAME_LENGTH];
        char compress_func_name_[OB_MAX_TABLE_NAME_LENGTH];
        char expire_condition_[OB_MAX_EXPIRE_CONDITION_LENGTH];
        char comment_str_[OB_MAX_TABLE_COMMENT_LENGTH];
        bool is_pure_update_table_;
        bool is_use_bloomfilter_;
        bool is_merge_dynamic_data_;
        ObConsistencyLevel consistency_level_;
        bool has_baseline_data_;
        ObRowkeyInfo rowkey_info_;
        int64_t expire_frequency_;  // how many frozen version passed before do expire once
        int64_t max_sstable_size_;
        int64_t query_cache_expire_time_;
        int64_t is_expire_effect_immediately_;
        int64_t max_scan_rows_per_tablet_;
        int64_t internal_ups_scan_size_;
        int64_t merge_write_sstable_version_;
        int64_t replica_count_;
        int64_t reserved_[TABLE_SCHEMA_RESERVED_NUM];
        int64_t version_;
        int64_t schema_version_;
        //in mem
        uint64_t create_time_column_id_;
        uint64_t modify_time_column_id_;
    };

    class ObSchemaSortByIdHelper;
    class ObSchemaManagerV2
    {
      public:
        enum Status{
          INVALID,
          CORE_TABLES,
          ALL_TABLES,
        };

        friend class ObSchemaSortByIdHelper;
      public:
        ObSchemaManagerV2();
        explicit ObSchemaManagerV2(const int64_t timestamp);
        ~ObSchemaManagerV2();
      public:
        ObSchemaManagerV2& operator=(const ObSchemaManagerV2& schema); //ugly,for hashMap
        ObSchemaManagerV2(const ObSchemaManagerV2& schema);
      public:
        ObSchemaManagerV2::Status get_status() const;
        const ObColumnSchemaV2* column_begin() const;
        const ObColumnSchemaV2* column_end() const;
        const char* get_app_name() const;
        int set_app_name(const char* app_name);

        int64_t get_column_count() const;
        int64_t get_table_count() const;

        uint64_t get_max_table_id() const;
        /**
         * @brief timestap is the version of schema,version_ is used for serialize syntax
         *
         * @return
         */
        int64_t get_version() const;
        void set_version(const int64_t version);
        void set_max_table_id(const uint64_t version);

        /**
         * @brife return code version not schema version
         *
         * @return int32_t
         */
        int32_t get_code_version() const;

        const ObColumnSchemaV2* get_column_schema(const int32_t index) const;

        /**
         * @brief get a column accroding to table_id/column_group_id/column_id
         *
         * @param table_id
         * @param column_group_id
         * @param column_id
         *
         * @return column or null
         */
        const ObColumnSchemaV2* get_column_schema(const uint64_t table_id,
                                                  const uint64_t column_group_id,
                                                  const uint64_t column_id) const;

        /**
         * @brief get a column accroding to table_id/column_id, if this column belongs to
         * more than one column group,then return the column in first column group
         *
         * @param table_id the id of table
         * @param column_id the column id
         * @param idx[out] the index in table of this column
         *
         * @return column or null
         */
        const ObColumnSchemaV2* get_column_schema(const uint64_t table_id,
                                                  const uint64_t column_id,
                                                  int32_t* idx = NULL) const;


        const ObColumnSchemaV2* get_column_schema(const char* table_name,
                                                  const char* column_name,
                                                  int32_t* idx = NULL) const;

        const ObColumnSchemaV2* get_column_schema(const ObString& table_name,
                                                  const ObString& column_name,
                                                  int32_t* idx = NULL) const;


        const ObColumnSchemaV2* get_table_schema(const uint64_t table_id, int32_t& size) const;

        const ObColumnSchemaV2* get_group_schema(const uint64_t table_id,
                                                 const uint64_t column_group_id,
                                                 int32_t& size) const;

        const ObTableSchema* table_begin() const;
        const ObTableSchema* table_end() const;

        const ObTableSchema* get_table_schema(const char* table_name) const;
        const ObTableSchema* get_table_schema(const ObString& table_name) const;
        const ObTableSchema* get_table_schema(const uint64_t table_id) const;
        ObTableSchema* get_table_schema(const char* table_name);
        ObTableSchema* get_table_schema(const uint64_t table_id);
        int64_t get_table_query_cache_expire_time(const ObString& table_name) const;

        uint64_t get_create_time_column_id(const uint64_t table_id) const;
        uint64_t get_modify_time_column_id(const uint64_t table_id) const;

        int get_column_index(const char *table_name,const char* column_name,int32_t index_array[],int32_t& size) const;
        int get_column_index(const uint64_t table_id, const uint64_t column_id, int32_t index_array[],int32_t& size) const;

        int get_column_schema(const uint64_t table_id, const uint64_t column_id,
                              ObColumnSchemaV2* columns[],int32_t& size) const;

        int get_column_schema(const char *table_name, const char* column_name,
                              ObColumnSchemaV2* columns[],int32_t& size) const;

        int get_column_schema(const ObString& table_name,
                              const ObString& column_name,
                              ObColumnSchemaV2* columns[],int32_t& size) const;


        int get_column_groups(uint64_t table_id,uint64_t column_groups[],int32_t& size) const;

        bool is_compatible(const ObSchemaManagerV2& schema_manager) const;

        int add_column(ObColumnSchemaV2& column);
        int add_column_without_sort(ObColumnSchemaV2& column);
        int add_table(ObTableSchema& table);
        void del_column(const ObColumnSchemaV2& column);

        /**
         * @brief if you don't want to use column group,set drop_group to true and call this
         *        method before deserialize
         *
         * @param drop_group true - don't use column group,otherwise not
         *
         */
        void set_drop_column_group(bool drop_group = true);

        // convert new table schema into old one and insert it into the schema_manager
        // zhuweng.yzf@taobao.com
        int add_new_table_schema(const TableSchema& tschema);
        //convert new table schema into old one and insert it into the schema_manager
        //rongxuan.lc@taobao.com
        int add_new_table_schema(const ObArray<TableSchema>& schema_array);

      public:
        bool parse_from_file(const char* file_name, tbsys::CConfig& config);
        bool parse_one_table(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
        bool parse_column_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
        bool parse_join_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
        bool parse_rowkey_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
        int change_table_id(const uint64_t table_id, const uint64_t new_table_id);
        int write_to_file(const char* file_name);
        int write_table_to_file(FILE *fd, const int64_t table_index);
        int write_column_group_info_to_file(FILE *fd, const int64_t table_index);
        int write_column_info_to_file(FILE *fd, const ObColumnSchemaV2 *column_schema);
        int write_rowkey_info_to_file(FILE *fd, const uint64_t table_id, const ObRowkeyInfo &rowkey);

      private:
        /**
         * parse rowkey column description into ObRowkeyColumn
         * rowkey column description may contains compatible old binary rowkey structure.
         * @param column_str column description string.
         * @param [out] column rowkey column id, length, data type.
         * @param [in, out] schema check column if exist, and set rowkey info.
         */
        bool parse_rowkey_column(const char* column_str, ObRowkeyColumn& column,  ObTableSchema& schema);

        /**
         * check join column if match with joined table's rowkey column.
         */
        bool check_join_column(const int32_t column_index, const char* column_name, const char* join_column_name,
            ObTableSchema& schema, const ObTableSchema& join_table_schema, uint64_t& column_offset);

      public:
        void print_info() const;
        void print(FILE* fd) const;
      public:
        struct ObColumnNameKey
        {
          int64_t hash() const;
          bool operator==(const ObColumnNameKey& key) const;
          ObString table_name_;
          ObString column_name_;
        };

        struct ObColumnIdKey
        {
          int64_t hash() const;
          bool operator==(const ObColumnIdKey& key) const;
          uint64_t table_id_;
          uint64_t column_id_;
        };

        struct ObColumnInfo
        {
          ObColumnSchemaV2* head_;
          int32_t table_begin_index_;
          ObColumnInfo() : head_(NULL), table_begin_index_(-1) {}
        };

        struct ObColumnGroupHelper
        {
          uint64_t table_id_;
          uint64_t column_group_id_;
        };

        struct ObColumnGroupHelperCompare
        {
          bool operator() (const ObColumnGroupHelper& l,const ObColumnGroupHelper& r) const;
        };

      public:
        NEED_SERIALIZE_AND_DESERIALIZE;
        int sort_column();
        bool check_table_expire_condition() const;
        bool check_compress_name() const;
        static const int64_t MAX_COLUMNS_LIMIT = OB_MAX_TABLE_NUMBER * OB_MAX_COLUMN_NUMBER;
        static const int64_t DEFAULT_MAX_COLUMNS = 16 * OB_MAX_COLUMN_NUMBER;;

      private:
        int replace_system_variable(char* expire_condition, const int64_t buf_size) const;
        int check_expire_dependent_columns(const ObString& expr,
          const ObTableSchema& table_schema, ObExpressionParser& parser) const;
        int ensure_column_storage();
        int prepare_column_storage(const int64_t column_num, bool need_reserve_space = false);

      private:
        int32_t   schema_magic_;
        int32_t   version_;
        int64_t   timestamp_;
        uint64_t  max_table_id_;
        int64_t   column_nums_;
        int64_t   table_nums_;

        char app_name_[OB_MAX_APP_NAME_LENGTH];

        ObTableSchema    table_infos_[OB_MAX_TABLE_NUMBER];
        ObColumnSchemaV2 *columns_;
        int64_t   column_capacity_; // current %columns_ occupy size.

        //just in mem
        bool drop_column_group_; //
        volatile bool hash_sorted_;       //after deserialize,will rebuild the hash maps
        hash::ObHashMap<ObColumnNameKey,ObColumnInfo,hash::NoPthreadDefendMode> column_hash_map_;
        hash::ObHashMap<ObColumnIdKey,ObColumnInfo,hash::NoPthreadDefendMode> id_hash_map_;

        int64_t column_group_nums_;
        ObColumnGroupHelper column_groups_[OB_MAX_COLUMN_GROUP_NUMBER * OB_MAX_TABLE_NUMBER];
    };

    class ObSchemaSortByIdHelper
    {
      public:
        explicit ObSchemaSortByIdHelper(const ObSchemaManagerV2* schema_manager)
        {
          init(schema_manager);
        }
        ~ObSchemaSortByIdHelper()
        {
        }
      public:
        struct Item
        {
          int64_t table_id_;
          int64_t index_;
          bool operator<(const Item& rhs) const
          {
            return table_id_ < rhs.table_id_;
          }
        };
      public:
        const ObTableSchema* get_table_schema(const Item* item) const
        {
          return &schema_manager_->table_infos_[item->index_];
        }
        const Item* begin() const { return table_infos_index_; }
        const Item* end() const { return table_infos_index_ + table_nums_; }

      private:
        inline int init(const ObSchemaManagerV2* schema_manager)
        {
          int ret = OB_SUCCESS;
          if (NULL == schema_manager)
          {
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            schema_manager_ = schema_manager;
            memset(table_infos_index_, 0, sizeof(table_infos_index_));
            int64_t i = 0;
            for (; i < schema_manager_->table_nums_; ++i)
            {
              table_infos_index_[i].table_id_ = schema_manager_->table_infos_[i].get_table_id();
              table_infos_index_[i].index_ = i;
            }
            table_nums_ = i;
            std::sort(table_infos_index_, table_infos_index_ + table_nums_);
          }
          return ret;
        }

        Item table_infos_index_[OB_MAX_TABLE_NUMBER];
        int64_t table_nums_;
        const ObSchemaManagerV2* schema_manager_;

    };
  }
}
#endif
