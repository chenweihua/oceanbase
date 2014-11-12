#ifndef OCEANBASE_ROOT_RPC_STUB_H_
#define OCEANBASE_ROOT_RPC_STUB_H_

#include "common/ob_fetch_runnable.h"
#include "common/ob_common_rpc_stub.h"
#include "common/ob_server.h"
#include "common/ob_schema.h"
#include "common/ob_range.h"
#include "common/ob_tablet_info.h"
#include "common/ob_tablet_info.h"
#include "common/ob_rs_ups_message.h"
#include "common/ob_data_source_desc.h"
#include "common/ob_list.h"
#include "common/ob_range2.h"
#include "ob_chunk_server_manager.h"
#include "common/ob_new_scanner.h"
#include "sql/ob_sql_scan_param.h"
#include "ob_daily_merge_checker.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObDataSourceProxyList;

    class ObRootRpcStub : public common::ObCommonRpcStub
    {
      public:
        ObRootRpcStub();
        virtual ~ObRootRpcStub();
        int init(const common::ObClientManager *client_mgr, common::ThreadSpecificBuffer* tsbuffer);
        // synchronous rpc messages
        virtual int slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout);
        virtual int set_obi_role(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us);
        virtual int switch_schema(const common::ObServer& server, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us);
        virtual int migrate_tablet(const common::ObServer& src_cs, const common::ObDataSourceDesc& desc, const int64_t timeout_us);
        virtual int create_tablet(const common::ObServer& cs, const common::ObNewRange& range, const int64_t mem_version, const int64_t timeout_us);
        virtual int delete_tablets(const common::ObServer& cs, const common::ObTabletReportInfoList &tablets, const int64_t timeout_us);
        virtual int get_last_frozen_version(const common::ObServer& ups, const int64_t timeout_us, int64_t &frozen_version);
        virtual int get_obi_role(const common::ObServer& master, const int64_t timeout_us, common::ObiRole &obi_role);
        virtual int get_boot_state(const common::ObServer& master, const int64_t timeout_us, bool &boot_ok);
        virtual int revoke_ups_lease(const common::ObServer &ups, const int64_t lease, const common::ObServer& master, const int64_t timeout_us);
        virtual int import_tablets(const common::ObServer& cs, const uint64_t table_id, const int64_t version, const int64_t timeout_us);
        virtual int get_ups_max_log_seq(const common::ObServer& ups, uint64_t &max_log_seq, const int64_t timeout_us);
        virtual int shutdown_cs(const common::ObServer& cs, bool is_restart, const int64_t timeout_us);
        virtual int get_row_checksum(const common::ObServer& server, const int64_t data_version, const uint64_t table_id, ObRowChecksum &row_checksum, int64_t timeout_us);

        virtual int get_split_range(const common::ObServer& ups, const int64_t timeout_us,
             const uint64_t table_id, const int64_t frozen_version, common::ObTabletInfoList &tablets);
        virtual int table_exist_in_cs(const common::ObServer &cs, const int64_t timeout_us,
            const uint64_t table_id, bool &is_exist_in_cs);
        // asynchronous rpc messages
        virtual int heartbeat_to_cs(const common::ObServer& cs,
                                    const int64_t lease_time,
                                    const int64_t frozen_mem_version,
                                    const int64_t schema_version,
                                    const int64_t config_version);
        virtual int heartbeat_to_ms(const common::ObServer& ms,
                                    const int64_t lease_time,
                                    const int64_t frozen_mem_version,
                                    const int64_t schema_version,
                                    const common::ObiRole &role,
                                    const int64_t privilege_version,
                                    const int64_t config_version);

        virtual int grant_lease_to_ups(const common::ObServer& ups,
                                       common::ObMsgUpsHeartbeat &msg);

        virtual int request_report_tablet(const common::ObServer& chunkserver);
        virtual int execute_sql(const common::ObServer& ms,
                                const common::ObString sql, int64_t timeout);
        virtual int request_cs_load_bypass_tablet(const common::ObServer& chunkserver,
            const common::ObTableImportInfoList &import_info, const int64_t timeout_us);
        virtual int request_cs_delete_table(const common::ObServer& chunkserver, const uint64_t table_id, const int64_t timeout_us);
        virtual int fetch_ms_list(const common::ObServer& rs, common::ObArray<common::ObServer> &ms_list, const int64_t timeout_us);

        // fetch range table from datasource, uri is used when datasouce need uri info to generate range table(e.g. datasource proxy server)
        virtual int fetch_range_table(const common::ObServer& data_source, const common::ObString& table_name,
            common::ObList<common::ObNewRange*>& range_table , common::ModuleArena& allocator, int64_t timeout);
        virtual int fetch_range_table(const common::ObServer& data_source, const common::ObString& table_name, const common::ObString& uri,
            common::ObList<common::ObNewRange*>& range_table ,common::ModuleArena& allocator, int64_t timeout);
        virtual int fetch_proxy_list(const common::ObServer& ms, const common::ObString& table_name,
            const int64_t cluster_id, ObDataSourceProxyList& proxy_list, int64_t timeout);
        virtual int fetch_slave_cluster_list(const common::ObServer& ms, const common::ObServer& master_rs,
            common::ObServer* slave_cluster_rs, int64_t& rs_count, int64_t timeout);
        virtual int import(const common::ObServer& rs, const common::ObString& table_name,
            const uint64_t table_id, const common::ObString& uri, const int64_t start_time, const int64_t timeout);
        virtual int kill_import(const common::ObServer& rs, const common::ObString& table_name,
            const uint64_t table_id, const int64_t timeout);
        virtual int get_import_status(const common::ObServer& rs, const common::ObString& table_name,
            const uint64_t table_id, int32_t& status, const int64_t timeout);
        virtual int set_import_status(const common::ObServer& rs, const common::ObString& table_name,
            const uint64_t table_id, const int32_t status, const int64_t timeout);
        virtual int notify_switch_schema(const common::ObServer& rs, const int64_t timeout);
      private:
        int fill_proxy_list(ObDataSourceProxyList& proxy_list, common::ObNewScanner& scanner);
        int fill_slave_cluster_list(common::ObNewScanner& scanner, const common::ObServer& master_rs,
            common::ObServer* slave_cluster_rs, int64_t& rs_count);
        int get_thread_buffer_(common::ObDataBuffer& data_buffer);
      private:
        static const int32_t DEFAULT_VERSION = 1;
        common::ThreadSpecificBuffer *thread_buffer_;
    };
  } /* rootserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOT_RPC_STUB_H_ */
