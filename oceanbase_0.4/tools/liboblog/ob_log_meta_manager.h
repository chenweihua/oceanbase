////===================================================================
 //
 // ob_log_meta_manager.h liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-05-23 by Yubai (yubai.lk@alipay.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 // 
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_LIBOBLOG_META_MANAGER_H_
#define  OCEANBASE_LIBOBLOG_META_MANAGER_H_

#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "common/ob_spin_lock.h"
#include "common/ob_spin_rwlock.h"
#include "common/ob_list.h"
#include "common/hash/ob_hashmap.h"
#include "obmysql/ob_mysql_util.h"
#include "ob_log_config.h"
#include "ob_log_server_selector.h"
#include "MetaData.h"

#define DEFAULT_ENCODING "UTF-8"

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogDBNameBuilder
    {
      public:
        virtual ~IObLogDBNameBuilder() {};
      public:
        virtual int init(const ObLogConfig &config) = 0;

        virtual void destroy() = 0;

        virtual int get_db_name(const char *src_name,
            const uint64_t db_partition,
            char *dest_name,
            const int64_t dest_buffer_size) = 0;
    };

    class IObLogTBNameBuilder
    {
      public:
        virtual ~IObLogTBNameBuilder() {};
      public:
        virtual int init(const ObLogConfig &config) = 0;

        virtual void destroy() = 0;

        virtual int get_tb_name(const char *src_name,
            const uint64_t tb_partition,
            char *dest_name,
            const int64_t dest_buffer_size) = 0;
    };

    class ObLogSchema : public common::ObSchemaManagerV2
    {
      public:
        ObLogSchema() : ref_(0) {};
      public:
        void ref() {ATOMIC_ADD(&ref_, 1);};
        int64_t deref() {return ATOMIC_ADD(&ref_, -1);};
      private:
        volatile int64_t ref_;
    };

    class IObLogSchemaGetter
    {
      public:
        virtual ~IObLogSchemaGetter() {};
      public:
        virtual int init(IObLogServerSelector *server_selector, IObLogRpcStub *rpc_stub) = 0;

        virtual void destroy() = 0;

        virtual const ObLogSchema *get_schema() = 0;

        virtual void revert_schema(const ObLogSchema *schema) = 0;

        virtual void refresh() = 0;
    };

    class IObLogMetaManager
    {
      public:
        virtual ~IObLogMetaManager() {};
      public:
        virtual int init(IObLogSchemaGetter *schema_getter,
            IObLogDBNameBuilder *db_name_builder,
            IObLogTBNameBuilder *tb_name_builder) = 0;
        
        virtual void destroy() = 0;

        virtual ITableMeta *get_table_meta(const uint64_t table_id,
            const uint64_t db_partition,
            const uint64_t tb_partition) = 0;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    class ObLogDBNameBuilder : public IObLogDBNameBuilder
    {
      typedef common::hash::ObHashMap<const char*, const char*> NameMap;
      static const int64_t NAME_MAP_SIZE = 128;
      public:
        ObLogDBNameBuilder();
        ~ObLogDBNameBuilder();
      public:
        int init(const ObLogConfig &config);
        void destroy();
        int get_db_name(const char *src_name,
            const uint64_t db_partition,
            char *dest_name,
            const int64_t dest_buffer_size);
      public:
        int operator ()(const char *tb_name, const ObLogConfig *config);
      private:
        bool inited_;
        common::CharArena allocator_;
        NameMap name_map_;
    };

    class ObLogTBNameBuilder : public IObLogTBNameBuilder
    {
      typedef common::hash::ObHashMap<const char*, const char*> NameMap;
      static const int64_t NAME_MAP_SIZE = 128;
      public:
        ObLogTBNameBuilder();
        ~ObLogTBNameBuilder();
      public:
        int init(const ObLogConfig &config);
        void destroy();
        int get_tb_name(const char *src_name,
            const uint64_t tb_partition,
            char *dest_name,
            const int64_t dest_buffer_size);
      public:
        int operator ()(const char *tb_name, const ObLogConfig *config);
      private:
        bool inited_;
        common::CharArena allocator_;
        NameMap name_map_;
    };

    class ObLogSchemaGetter : public IObLogSchemaGetter
    {
      static const int64_t FETCH_SCHEMA_TIMEOUT = 10L * 1000000L;
      static const int64_t RS_ADDR_REFRESH_INTERVAL = 60L * 1000000L;
      public:
        ObLogSchemaGetter();
        ~ObLogSchemaGetter();
      public:
        int init(IObLogServerSelector *server_selector, IObLogRpcStub *rpc_stub);
        void destroy();
        const ObLogSchema *get_schema();
        void revert_schema(const ObLogSchema *schema);
        void refresh();
      private:
        bool inited_;
        IObLogServerSelector *server_selector_;
        IObLogRpcStub *rpc_stub_;
        common::SpinRWLock schema_refresh_lock_;
        ObLogSchema *cur_schema_;
    };

    class ObLogMetaManager : public IObLogMetaManager
    {
      struct TableMetaKey
      {
        uint64_t table_id;
        uint64_t db_partition;
        uint64_t tb_partition;
        TableMetaKey() : table_id(common::OB_INVALID_ID), db_partition(0), tb_partition(0)
        {
        };
        TableMetaKey(const uint64_t t, const uint64_t dp, const uint64_t tp) : table_id(t), db_partition(dp), tb_partition(tp)
        {
        };
        int64_t hash() const
        {
          return common::murmurhash2(this, sizeof(*this), 0);
        };
        bool operator== (const TableMetaKey &other) const
        {
          return (other.table_id == table_id && other.db_partition == db_partition && other.tb_partition == tb_partition);
        };
        const char *to_cstring() const
        {
          static const int64_t BUFFER_SIZE = 64;
          static __thread char buffers[2][BUFFER_SIZE];
          static __thread uint64_t i = 0;
          char *buffer = buffers[i++ % 2];
          buffer[0] = '\0';
          snprintf(buffer, BUFFER_SIZE, "table_id=%lu db_partition=%lu tb_partition=%lu", table_id, db_partition, tb_partition);
          return buffer;
        };
      };
      typedef common::hash::ObHashMap<TableMetaKey, ITableMeta*> TableMetaMap;
      static const int64_t MAX_PARTITION_NUM = 1024;
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      public:
        ObLogMetaManager();
        ~ObLogMetaManager();
      public:
        int init(IObLogSchemaGetter *schema_getter,
            IObLogDBNameBuilder *db_name_builder,
            IObLogTBNameBuilder *tb_name_builder);
        void destroy();
        ITableMeta *get_table_meta(const uint64_t table_id,
            const uint64_t db_partition,
            const uint64_t tb_partition);
      private:
        ITableMeta *build_table_meta_(const uint64_t table_id, const uint64_t db_partition, const uint64_t tb_partition);
        IDBMeta *get_db_meta_(const char *db_name, bool &exist);
        ITableMeta *get_table_meta_(const char *db_name, const char *table_name, bool &exist);
        int prepare_table_column_schema_(const ObLogSchema &total_schema,
            const common::ObTableSchema &table_schema,
            ITableMeta &table_meta);
        const char *get_default_value_(const common::ObColumnSchemaV2 &column_schema);
        int type_trans_mysql_(const ObObjType ob_type, int &mysql_type);
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        common::ObList<IDBMeta*> db_meta_list_;
        common::ObList<ITableMeta*> tb_meta_list_;
        MetaDataCollections meta_collect_;
        bool inited_;
        IObLogSchemaGetter *schema_getter_;
        IObLogDBNameBuilder *db_name_builder_;
        IObLogTBNameBuilder *tb_name_builder_;
        common::ObSpinLock table_meta_lock_;
        TableMetaMap table_meta_map_;
    };
  }
}

#endif //OCEANBASE_LIBOBLOG_META_MANAGER_H_

