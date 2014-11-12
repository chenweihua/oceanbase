////===================================================================
 //
 // ob_log_partitioner.h liboblog / Oceanbase
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

#ifndef  OCEANBASE_LIBOBLOG_PARTITIONER_H_
#define  OCEANBASE_LIBOBLOG_PARTITIONER_H_

#ifdef __cplusplus
# define OB_LOG_CPP_START extern "C" {
# define OB_LOG_CPP_END }
#else
# define OB_LOG_CPP_START
# define OB_LOG_CPP_END
#endif

OB_LOG_CPP_START
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
OB_LOG_CPP_END

#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "ob_log_config.h"
#include "ob_log_filter.h"
#include "ob_log_meta_manager.h"

namespace oceanbase
{
  namespace liboblog
  {
    class IObLogPartitioner
    {
      public:
        virtual ~IObLogPartitioner() {};
      public:
        virtual int init(ObLogConfig &config, IObLogSchemaGetter *schema_getter) = 0;

        virtual void destroy() = 0;

        virtual int partition(ObLogMutator &mutator, uint64_t *db_partition, uint64_t *tb_partition) = 0;
    };

    class ObLogPartitioner : public IObLogPartitioner
    {
      static const int64_t FUNCTION_NAME_LENGTH = 1024;
      static const int64_t TABLE_ID_MAP_SIZE = 128;
      static const int64_t VARCHAE_BUFFER_SIZE = 65536;
      struct TableInfo
      {
        char db_partition_function[FUNCTION_NAME_LENGTH];
        char tb_partition_function[FUNCTION_NAME_LENGTH];
      };
      typedef common::hash::ObHashMap<uint64_t, TableInfo*> TableIDMap;
      public:
        ObLogPartitioner();
        ~ObLogPartitioner();
      public:
        int init(ObLogConfig &config, IObLogSchemaGetter *schema_getter);
        void destroy();
        int partition(ObLogMutator &mutator, uint64_t *db_partition, uint64_t *tb_partition);
        int operator() (const char *tb_name, const ObLogSchema *total_schema);
      private:
        int prepare_partition_functions_(const char *tb_select,
            const ObLogConfig &config,
            const ObLogSchema &total_schema);
        static int push_rowkey_values_(lua_State *lua, const ObRowkey &rowkey);
        int calc_partition_(const TableInfo &table_info,
            const common::ObRowkey &rowkey,
            uint64_t *db_partition,
            uint64_t *tb_partition);
      private:
        bool inited_;
        lua_State *lua_;
        common::CharArena allocator_;
        TableIDMap table_id_map_;
    };

  }
}

#endif //OCEANBASE_LIBOBLOG_PARTITIONER_H_

