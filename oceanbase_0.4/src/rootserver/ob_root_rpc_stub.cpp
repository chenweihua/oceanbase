#include "rootserver/ob_root_worker.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "common/ob_schema.h"
#include "common/ob_define.h"
#include "sql/ob_sql_result_set.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootRpcStub::ObRootRpcStub()
  :thread_buffer_(NULL)
{
}

ObRootRpcStub::~ObRootRpcStub()
{
}

int ObRootRpcStub::init(const ObClientManager *client_mgr, common::ThreadSpecificBuffer* tsbuffer)
{
  OB_ASSERT(NULL != client_mgr);
  OB_ASSERT(NULL != tsbuffer);
  ObCommonRpcStub::init(client_mgr);
  thread_buffer_ = tsbuffer;
  return OB_SUCCESS;
}

int ObRootRpcStub::slave_register(const ObServer& master, const ObServer& slave_addr,
    ObFetchParam& fetch_param, const int64_t timeout)
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  }
  else
  {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. serialize slave addr
  if (OB_SUCCESS == err)
  {
    err = slave_addr.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
  }

  // step 2. send request to register
  if (OB_SUCCESS == err)
  {
    err = client_mgr_->send_request(master,
        OB_SLAVE_REG, DEFAULT_VERSION, timeout, data_buff);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to register failed"
          "err[%d].", err);
    }
  }

  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }

  // step 3. deserialize fetch param
  if (OB_SUCCESS == err)
  {
    err = fetch_param.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "deserialize fetch param failed, err[%d]", err);
    }
  }

  return err;
}

int ObRootRpcStub::get_thread_buffer_(common::ObDataBuffer& data_buffer)
{
  int ret = OB_SUCCESS;
  if (NULL == thread_buffer_)
  {
    TBSYS_LOG(ERROR, "thread_buffer_ = NULL");
    ret = OB_ERROR;
  }
  else
  {
    common::ThreadSpecificBuffer::Buffer* buff = thread_buffer_->get_buffer();
    if (NULL == buff)
    {
      TBSYS_LOG(ERROR, "thread_buffer_ = NULL");
      ret = OB_ERROR;
    }
    else
    {
      buff->reset();
      data_buffer.set_data(buff->current(), buff->remain());
    }
  }
  return ret;
}

int ObRootRpcStub::get_row_checksum(const common::ObServer& server, const int64_t data_version, const uint64_t table_id, ObRowChecksum &row_checksum, int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), data_version)))
  {
    TBSYS_LOG(WARN, "failed to serialize sql scan param, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
  {
    TBSYS_LOG(WARN, "failed to serialize sql scan param, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(server, OB_GET_ROW_CHECKSUM, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result_code;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result_code.result_code_)
    {
      TBSYS_LOG(WARN, "failed to sql scan, err=%d", result_code.result_code_);
      ret = result_code.result_code_;
    }
    else if (OB_SUCCESS != (ret = row_checksum.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(WARN, "failed to deserialize scanner, err=%d", ret);
    }
  }

  return ret;
}

int ObRootRpcStub::set_obi_role(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(WARN, "failed to serialize role, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_SET_OBI_ROLE, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result_code;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result_code.result_code_)
    {
      TBSYS_LOG(WARN, "failed to set obi role, err=%d", result_code.result_code_);
      ret = result_code.result_code_;
    }
  }
  return ret;
}

int ObRootRpcStub::switch_schema(const common::ObServer& server, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_manager.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize schema, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(server, OB_SWITCH_SCHEMA, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to switch schema, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else
    {
      TBSYS_LOG(INFO, "send switch_schema, server=%s schema_version=%ld", to_cstring(server), schema_manager.get_version());
    }
  }
  return ret;
}

int ObRootRpcStub::migrate_tablet(const common::ObServer& dest_server, const common::ObDataSourceDesc& desc, const int64_t timeout_us)
{
  const int32_t MIGRATE_TABLET_VERSION = 3;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObResultCode result_code;
  ObDataBuffer data_buff;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(data_buff)))
  {
    TBSYS_LOG(WARN, "failed to get thread buffer, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = desc.serialize(data_buff.get_data(), data_buff.get_capacity(),
          data_buff.get_position())))
  {
    TBSYS_LOG(WARN, "failed to serialize desc[%s], ret=%d", to_cstring(desc), ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(dest_server, OB_CS_MIGRATE, MIGRATE_TABLET_VERSION, timeout_us, data_buff)))
  {
    TBSYS_LOG(WARN, "failed to send OB_FETCH_RANGE_TABLE request to cs[%s], ret=%d", to_cstring(dest_server), ret);
  }
  else if (OB_SUCCESS != (ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
  {
    TBSYS_LOG(WARN, "deserialize result_code failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != result_code.result_code_)
  {
    ret = result_code.result_code_;
    TBSYS_LOG(WARN, "cs_fetch_data failed, cs=%s, desc=%s, ret=%d", to_cstring(dest_server), to_cstring(desc), ret);
  }

  return ret;
}

int ObRootRpcStub::create_tablet(const common::ObServer& cs, const common::ObNewRange& range,
    const int64_t mem_version, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  static char buff[OB_MAX_PACKET_LENGTH];
  msgbuf.set_data(buff, OB_MAX_PACKET_LENGTH);
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = range.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize range, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
          msgbuf.get_position(), mem_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize key_src, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_CS_CREATE_TABLE, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    static char range_buff[OB_MAX_ROW_KEY_LENGTH * 2];
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      range.to_string(range_buff, OB_MAX_ROW_KEY_LENGTH * 2);
      TBSYS_LOG(WARN, "failed to create tablet, err=%d, cs=%s, range=%s", result.result_code_, cs.to_cstring(), range_buff);
      ret = result.result_code_;
    }
    else
    {
    }
  }
  return ret;
}

int ObRootRpcStub::delete_tablets(const common::ObServer& cs, const common::ObTabletReportInfoList &tablets, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = tablets.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serializ, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_CS_DELETE_TABLETS, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to delete tablets, err=%d", result.result_code_);
      ret = result.result_code_;
    }
  }
  return ret;
}

int ObRootRpcStub::import_tablets(const common::ObServer& cs, const uint64_t table_id, const int64_t version, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
  {
    TBSYS_LOG(ERROR, "failed to serialize keey_src, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize keey_src, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_CS_IMPORT_TABLETS, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to create tablet, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else
    {
    }
  }
  return ret;
}

int ObRootRpcStub::get_last_frozen_version(const common::ObServer& ups, const int64_t timeout_us, int64_t &frozen_version)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  frozen_version = -1;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_UPS_GET_LAST_FROZEN_VERSION, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to get frozon version, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else if (OB_SUCCESS != (ret = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_position(), pos, &frozen_version)))
    {
      TBSYS_LOG(WARN, "failed to deserialize frozen version ,err=%d", ret);
      frozen_version = -1;
    }
    else
    {
      TBSYS_LOG(INFO, "last_frozen_version=%ld", frozen_version);
    }
  }
  return ret;
}

int ObRootRpcStub::get_obi_role(const common::ObServer& master, const int64_t timeout_us, common::ObiRole &obi_role)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(master, OB_GET_OBI_ROLE, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to get obi_role, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else if (OB_SUCCESS != (ret = obi_role.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(WARN, "failed to deserialize frozen version ,err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "get obi_role from master, obi_role=%s", obi_role.get_role_str());
    }
  }
  return ret;
}

int ObRootRpcStub::heartbeat_to_cs(const common::ObServer& cs, const int64_t lease_time, const int64_t frozen_mem_version,
    const int64_t schema_version, const int64_t config_version)
{
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 3;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), lease_time)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), frozen_mem_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), schema_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), config_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize config_version, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->post_request(cs, OB_REQUIRE_HEARTBEAT, MY_VERSION, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
  }
  return ret;
}

int ObRootRpcStub::heartbeat_to_ms(const common::ObServer& ms, const int64_t lease_time, const int64_t frozen_mem_version,
    const int64_t schema_version, const common::ObiRole &role, const int64_t privilege_version, const int64_t config_version)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  static const int MY_VERSION = 4;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), lease_time)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), frozen_mem_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), schema_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), privilege_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize privilege version, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), config_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize config_version, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->post_request(ms, OB_REQUIRE_HEARTBEAT, MY_VERSION, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
  }
  return ret;
}

int ObRootRpcStub::grant_lease_to_ups(const common::ObServer& ups, ObMsgUpsHeartbeat &msg)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = msg.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize msg, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->post_request(ups, OB_RS_UPS_HEARTBEAT, msg.MY_VERSION, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
  }
  return ret;
}

int ObRootRpcStub::request_report_tablet(const common::ObServer& chunkserver)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    ret = get_thread_buffer_(msgbuf);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get thread buffer. err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = client_mgr_->post_request(chunkserver, OB_RS_REQUEST_REPORT_TABLET, DEFAULT_VERSION, msgbuf);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to post request to chunkserver. err=%d, chunkserver_addr=%s", ret, chunkserver.to_cstring());
    }
  }
  return ret;
}

int ObRootRpcStub::revoke_ups_lease(const common::ObServer& ups, const int64_t lease, const common::ObServer& master, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  ObMsgRevokeLease msg;
  msg.lease_ = lease;
  msg.ups_master_ = master;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = msg.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_RS_UPS_REVOKE_LEASE, msg.MY_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to revoke lease, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else
    {
    }
  }
  return ret;
}

int ObRootRpcStub::get_ups_max_log_seq(const common::ObServer& ups, uint64_t &max_log_seq, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_RS_GET_MAX_LOG_SEQ, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to revoke lease, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else if (OB_SUCCESS != (ret = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_position(),
                                                             pos, (int64_t*)&max_log_seq)))
    {
      TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "get ups max log seq, ups=%s seq=%lu", ups.to_cstring(), max_log_seq);
    }
  }
  return ret;
}

int ObRootRpcStub::shutdown_cs(const common::ObServer& cs, bool is_restart, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), is_restart ? 1 : 0)))
  {
    TBSYS_LOG(ERROR, "encode is_restart fail:ret[%d], is_restart[%d]", ret, is_restart ? 1 : 0);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_STOP_SERVER, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to restart, err=%d server=%s", result.result_code_, cs.to_cstring());
      ret = result.result_code_;
    }
  }
  return ret;
}

int ObRootRpcStub::get_split_range(const common::ObServer& ups, const int64_t timeout_us,
    const uint64_t table_id, const int64_t forzen_version, ObTabletInfoList &tablets)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
    {
      TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
            msgbuf.get_position(), forzen_version)))
    {
      TBSYS_LOG(WARN, "fail to encode forzen_version. forzen_version=%ld, ret=%d", forzen_version, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
            msgbuf.get_position(), table_id)))
    {
      TBSYS_LOG(WARN, "fail to encode table_id. table_id=%lu, ret=%d", table_id, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_RS_FETCH_SPLIT_RANGE, DEFAULT_VERSION, timeout_us, msgbuf)))
    {
      TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
    }
  }
  ObResultCode result;
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to fetch split range, err=%d", result.result_code_);
      ret = result.result_code_;
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = tablets.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(WARN, "failed to deserialize tablets, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "fetch split range from ups succ.");
  }
  else
  {
    TBSYS_LOG(WARN, "fetch split range from ups fail, ups_addr=%s, version=%ld", ups.to_cstring(), forzen_version);
  }
  return ret;
}

int ObRootRpcStub::table_exist_in_cs(const ObServer &cs, const int64_t timeout_us,
    const uint64_t table_id, bool &is_exist_in_cs)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
    {
      TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
            msgbuf.get_position(), table_id)))
    {
      TBSYS_LOG(WARN, "fail to encode table_id, table_id=%ld, ret=%d", table_id, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_CS_CHECK_TABLET, DEFAULT_VERSION, timeout_us, msgbuf)))
    {
      TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
    }
  }
  ObResultCode result;
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_CS_TABLET_NOT_EXIST == result.result_code_)
    {
      ret = OB_SUCCESS;
      is_exist_in_cs = false;
    }
    else if (OB_SUCCESS == result.result_code_)
    {
      is_exist_in_cs = true;
    }
    else
    {
      ret = result.result_code_;
      TBSYS_LOG(WARN, "fail to check cs tablet. table_id=%lu, cs_addr=%s, err=%d",
          table_id, cs.to_cstring(), ret);
    }
  }
  return ret;
}

int ObRootRpcStub::execute_sql(const ObServer& ms, const ObString sql, int64_t timeout)
{
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ObDataBuffer data_buff;
    get_thread_buffer_(data_buff);
    if (OB_SUCCESS != (ret = sql.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position())))
    {
      TBSYS_LOG(WARN, "serialize sql fail, ret: [%d], sql: [%.*s]",
          ret, sql.length(), sql.ptr());
    }
    else if (OB_SUCCESS != (ret = client_mgr_->send_request(ms, OB_SQL_EXECUTE,
            MY_VERSION, timeout, data_buff)))
    {
      TBSYS_LOG(WARN, "send sql request to [%s] fail, ret: [%d], sql: [%.*s]",
          to_cstring(ms), ret, sql.length(), sql.ptr());
    }
    else
    {
      int64_t pos = 0;
      ObResultCode result_code;
      ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed: pos[%ld], ret[%d]", pos, ret);
      }
      else if (result_code.result_code_ != OB_SUCCESS)
      {
        ret = result_code.result_code_;
        TBSYS_LOG(WARN, "execute sql at[%s] fail, ret: [%d], sql: [%.*s]",
            to_cstring(ms), ret, sql.length(), sql.ptr());
      }
    }
  }
  return ret;
}
int ObRootRpcStub::request_cs_load_bypass_tablet(const common::ObServer& chunkserver,
    const common::ObTableImportInfoList &import_info, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int MY_VERSION = 1;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    ret = get_thread_buffer_(msgbuf);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get thread buffer. err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = import_info.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to serialize table_name_id, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = client_mgr_->send_request(chunkserver, OB_CS_LOAD_BYPASS_SSTABLE, MY_VERSION, timeout_us, msgbuf);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to post request to chunkserver. err=%d, chunkserver_addr=%s", ret, chunkserver.to_cstring());
    }
  }
  ObResultCode result;
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "fail to request cs load bypass tablet. ret=%d", result.result_code_);
      ret = result.result_code_;
    }
  }
  return ret;
}
int ObRootRpcStub::request_cs_delete_table(const common::ObServer& chunkserver,
    const uint64_t table_id, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int MY_VERSION = 1;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    ret = get_thread_buffer_(msgbuf);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get thread buffer. err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to serialize table_name_id, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = client_mgr_->send_request(chunkserver, OB_CS_DELETE_TABLE, MY_VERSION, timeout_us, msgbuf);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to post request to chunkserver. err=%d, chunkserver_addr=%s", ret, chunkserver.to_cstring());
    }
  }
  ObResultCode result;
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      ret = result.result_code_;
      TBSYS_LOG(WARN, "fail to request cs delete table. table_id=%lu, ret=%d", table_id, ret);
    }
  }
  return ret;
}
int ObRootRpcStub::fetch_ms_list(const ObServer &rs, ObArray<ObServer> &ms_list, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int MY_VERSION = 1;
  ObDataBuffer data_buff;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    ret = get_thread_buffer_(data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get thread buffer. err=%d", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    ret = client_mgr_->send_request(rs, OB_GET_MS_LIST, MY_VERSION, timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    }
  }
  ObResultCode res;
  if (OB_SUCCESS == ret)
  {
    data_buff.get_position() = 0;
    ret= res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
    }
    else if (OB_SUCCESS != res.result_code_)
    {
      ret = res.result_code_;
      TBSYS_LOG(WARN, "fail to fetch ms list. rs=%s, ret=%d", to_cstring(rs), ret);
    }
  }

  int32_t ms_num = 0;
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi32(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), &ms_num);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "decode ms num fail:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "ms server number[%d]", ms_num);
    }
  }

  ObServer ms;
  int64_t reserved = 0;
  for(int32_t i = 0;i<ms_num && OB_SUCCESS == ret;i++)
  {
    if (OB_SUCCESS != (ret = ms.deserialize(data_buff.get_data(),
            data_buff.get_capacity(),
            data_buff.get_position())))
    {
      TBSYS_LOG(WARN, "deserialize merge server fail, ret: [%d]", ret);
    }
    else if (OB_SUCCESS !=
        (ret = serialization::decode_vi64(data_buff.get_data(),
                                          data_buff.get_capacity(),
                                          data_buff.get_position(),
                                          &reserved)))
    {
      TBSYS_LOG(WARN, "deserializ merge server"
          " reserver int64 fail, ret: [%d]", ret);
    }
    else
    {
      ms_list.push_back(ms);
    }
  }
  return ret;
}
int ObRootRpcStub::get_boot_state(const common::ObServer& master, const int64_t timeout_us, bool &boot_ok)
{
  int ret = OB_SUCCESS;
  boot_ok = false;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(master, OB_GET_BOOT_STATE, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to get boot state, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else if (OB_SUCCESS != (ret = serialization::decode_bool(msgbuf.get_data(), msgbuf.get_position(), pos, &boot_ok)))
    {
      TBSYS_LOG(WARN, "failed to deserialize book label ,err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "get boot_state from master, state=%s", boot_ok ? "true":"false");
    }
  }
  return ret;
}
int ObRootRpcStub::fetch_range_table(const ObServer& data_source, const ObString& table_name,
    ObList<ObNewRange*>& range_table ,ModuleArena& allocator, int64_t timeout)
{
  ObString uri;
  return fetch_range_table(data_source, table_name, uri, range_table, allocator, timeout);
}

int ObRootRpcStub::fetch_range_table(const ObServer& data_source, const ObString& table_name, const ObString& uri,
    ObList<ObNewRange*>& range_table ,common::ModuleArena& allocator, int64_t timeout)
{ //TODO: refact this method after the root table refact, ranges received in a package could be inserted to root table
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  int64_t pos = 0;
  int64_t package_count = 0;
  int64_t session_id = 0;
  ObResultCode result_code; // ths result code of the packet
  bool has_more = true; // has_more flag of the packet
  int64_t count = 0;//range count of the packet
  ObNewRange range;
  ObDataBuffer data_buff;
  int64_t cost_time = 0;
  bool is_first = true;
  ObNewRange* last_range = NULL;

  ObObj start_key_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObObj end_key_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    if (OB_SUCCESS != (ret = get_thread_buffer_(data_buff)))
    {
      TBSYS_LOG(WARN, "failed to get thread buffer, ret=%d", ret);
    }
    if (OB_SUCCESS != (ret = table_name.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position())))
    {
      TBSYS_LOG(WARN, "serialize table name fail, table_name: [%.*s], ret=%d",
          table_name.length(), table_name.ptr(), ret);
    }
    else if (OB_SUCCESS != (ret = uri.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position())))
    {
      TBSYS_LOG(WARN, "serialize uri fail, uri: [%.*s], ret=%d",
          uri.length(), uri.ptr(), ret);
    }
    else if (OB_SUCCESS != (ret = client_mgr_->send_request(data_source, OB_FETCH_RANGE_TABLE,
            MY_VERSION, timeout, data_buff, session_id)))
    {
      TBSYS_LOG(WARN, "fetch range table from [%s] fail, table_name: [%.*s], uri: [%.*s], ret=%d",
          to_cstring(data_source), table_name.length(), table_name.ptr(), uri.length(), uri.ptr(), ret);
    }
  }

  while (OB_SUCCESS == ret && has_more)
  {
    ++package_count;
    pos = 0;

    if (OB_SUCCESS != (ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(WARN, "deserialize result_code failed: pos[%ld], buf_len[%ld] ret=%d",
          pos, data_buff.get_position(), ret);
    }
    else if (OB_SUCCESS != result_code.result_code_)
    {
      ret = result_code.result_code_;
      TBSYS_LOG(WARN, "fetch range table from [%s] fail, table_name: [%.*s], uri: [%.*s], "
          "package_count=%ld ret=%d",
          to_cstring(data_source), table_name.length(), table_name.ptr(), uri.length(), uri.ptr(), package_count, ret);
    }
    else if (OB_SUCCESS != (ret = serialization::decode_bool(data_buff.get_data(), data_buff.get_position(), pos, &has_more)))
    {
      TBSYS_LOG(WARN, "failed to decode has more, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = serialization::decode_i64(data_buff.get_data(), data_buff.get_position(), pos, &count)))
    {
      TBSYS_LOG(WARN, "failed to decode range count, ret=%d", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "has_more=%c, count=%ld", has_more?'Y':'N', count);
      for(int64_t i=0; i < count; ++i)
      {
        range.start_key_.assign(start_key_objs, OB_MAX_ROWKEY_COLUMN_NUMBER);
        range.end_key_.assign(end_key_objs, OB_MAX_ROWKEY_COLUMN_NUMBER);
        if (OB_SUCCESS != (ret = range.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
        {
          TBSYS_LOG(WARN, "failed to decode range, ret=%d", ret);
        }
        void *buffer = allocator.alloc(sizeof(ObNewRange));
        ObNewRange *new_range = NULL;
        if (NULL == buffer)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "failed to alloc new_range");
        }
        else
        {
          new_range = new(buffer) ObNewRange();
          if (OB_SUCCESS != (ret = deep_copy_range(allocator, range, *new_range)))
          {
            TBSYS_LOG(WARN, "failed to deep copy range: %s, ret=%d", to_cstring(range), ret);
          }
          else if (OB_SUCCESS != (ret = range_table.push_back(new_range)))
          {
            TBSYS_LOG(WARN, "failed to add range [%s] to range_table, ret=%d", to_cstring(*new_range), ret);
          }
          else
          {
            if (is_first)
            {
              is_first = false;
              if(!new_range->start_key_.is_min_row())
              {
                ret = OB_ROOT_RANGE_NOT_CONTINUOUS;
                TBSYS_LOG(WARN, "the start key of the first range[%s] is not min", to_cstring(*new_range));
              }
            }
            else if (NULL == last_range)
            {
              ret = OB_ERR_UNEXPECTED;
              TBSYS_LOG(ERROR, "last range must not NULL");
            }
            else if (last_range->end_key_ != new_range->start_key_)
            {
              ret = OB_ROOT_RANGE_NOT_CONTINUOUS;
              TBSYS_LOG(WARN, "cur range[%s] and last range[%s] not continuous", to_cstring(*new_range), to_cstring(*last_range));
            }
            last_range = new_range;
          }
        }
      }

      if (OB_SUCCESS == ret && !has_more)
      {
        if (NULL == last_range)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "last range must not NULL");
        }
        else if (!last_range->end_key_.is_max_row())
        {
          ret = OB_ROOT_RANGE_NOT_CONTINUOUS;
          TBSYS_LOG(WARN, "the end key of the last range[%s] is not max", to_cstring(*last_range));
        }
      }
    }

    if (OB_SUCCESS == ret && has_more)
    {
      data_buff.get_position() = 0;
      cost_time = tbsys::CTimeUtil::getTime() - start_time;
      if (OB_SUCCESS != (ret = client_mgr_->get_next(data_source, session_id, timeout - cost_time, data_buff, data_buff)))
      {
        TBSYS_LOG(WARN, "failed to get next package for fetch range table, package_count=%ld, range_count=%ld, ret=%d",
            package_count, range_table.size(), ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "fetch range for table[%.*s] from %s succeed, range count=%ld",
         table_name.length(), table_name.ptr(), to_cstring(data_source), range_table.size());
  }
  else
  {
    TBSYS_LOG(WARN, "fetch range for table[%.*s] from %s failed, range count=%ld, ret=%d",
        table_name.length(), table_name.ptr(), to_cstring(data_source), range_table.size(), ret);
  }

  return ret;
}

int ObRootRpcStub::fetch_proxy_list(const common::ObServer& ms, const common::ObString& table_name,
     const int64_t cluster_id, ObDataSourceProxyList& proxy_list, int64_t timeout)
{
  int ret = OB_SUCCESS;
  char sql_buf[OB_SQL_LENGTH];
  ObString select_stmt;
  ObDataBuffer msgbuf;
  ObResultCode rs;
  int64_t session_id;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr is NULL");
    ret = OB_NOT_INIT;
  }

  if (OB_SUCCESS == ret)
  {
    int n = snprintf(sql_buf, OB_SQL_LENGTH, "select ip, port, supported_data_source_name from %.*s where cluster_id=%ld",
        table_name.length(), table_name.ptr(), cluster_id);
    if (n < 0 || n >= OB_SQL_LENGTH)
    {
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "failed to generate the sql of fetch_proxy_list, table_name=%.*s, cluster_id=%ld, ret=%d",
          table_name.length(), table_name.ptr(), cluster_id, ret);
    }
    else
    {
      select_stmt.assign_ptr(sql_buf, static_cast<ObString::obstr_size_t>(strlen(sql_buf)));
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
    {
      TBSYS_LOG(WARN, "failed to get thread buffer, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = select_stmt.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
    {
      TBSYS_LOG(WARN, "failed to serialize select stmt, ret=%d", ret);
    }
    else if (OB_SUCCESS !=
        (ret = client_mgr_->send_request(ms, OB_SQL_EXECUTE, DEFAULT_VERSION, timeout, msgbuf, session_id)))
    {
      TBSYS_LOG(WARN, "failed to send request sql %.*s to ms %s, ret=%d",
          select_stmt.length(), select_stmt.ptr(), to_cstring(ms), ret);
    }
    else
    {
      bool fullfilled = true;
      sql::ObSQLResultSet rs;
      do
      {
        msgbuf.get_position() = 0;
        if (OB_SUCCESS !=
            (ret = rs.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          TBSYS_LOG(WARN, "fail to deserialize result buffer, ret=%d", ret);
        }
        else if (OB_SUCCESS != rs.get_errno())
        {
          TBSYS_LOG(WARN, "fail to exeucte sql: [%s], errno: [%d]", to_cstring(rs.get_sql_str()), rs.get_errno());
          ret = rs.get_errno();
          break;
        }
        else
        {
          ObNewScanner& scanner = rs.get_new_scanner();

          if (OB_SUCCESS != (ret = fill_proxy_list(proxy_list, scanner)))
          {
            TBSYS_LOG(WARN, "failted to parse and update proxy list, ret=%d", ret);
            break;
          }

          rs.get_fullfilled(fullfilled);
          if (fullfilled || scanner.is_empty())
          {
            break;
          }
          else
          {
            msgbuf.get_position() = 0;
            if (OB_SUCCESS != (ret = client_mgr_->get_next(ms, session_id, timeout, msgbuf, msgbuf)))
            {
              TBSYS_LOG(WARN, "failted to send get_next, ret = [%d]", ret);
              break;
            }
          }
        }
      } while (OB_SUCCESS == ret);
    }
  }

  return ret;
}

int ObRootRpcStub::fill_proxy_list(ObDataSourceProxyList& proxy_list, common::ObNewScanner& scanner)
{
  int ret = OB_SUCCESS;
  ObRow row;
  const ObObj *cell = NULL;
  uint64_t tid = 0;
  uint64_t cid = 0;
  int64_t cell_idx = 0;

  char ip_buffer[OB_IP_STR_BUFF];

  ObString ip;
  int64_t port = 0;
  ObString supported_data_source_name;
  ObServer server;

  while (OB_SUCCESS == (ret = scanner.get_next_row(row)))
  {
    // ip                         varchar
    // port                       int
    // supported_data_source_name varchar
    cell_idx = 0;
    if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to get cell, ret=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell->get_varchar(ip)))
    {
      TBSYS_LOG(WARN, "failed to get ip, ret=%d", ret);
      break;
    }
    if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to get cell, ret=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell->get_int(port)))
    {
      TBSYS_LOG(WARN, "failed to get port, ret=%d", ret);
      break;
    }
    if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to get cell, ret=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell->get_varchar(supported_data_source_name)))
    {
      TBSYS_LOG(WARN, "failed to get upported_data_source_name, ret=%d", ret);
      break;
    }
    else
    {
      char *p = ip.ptr();
      while (p <= ip.length() + ip.ptr() && *p == ' ')
      {
        ++p;
      }
      int32_t length = static_cast<int32_t>(ip.length() - (p - ip.ptr()));
      int n = snprintf(ip_buffer, sizeof(ip_buffer), "%.*s", length, p);
      if (n<0 || n >= static_cast<int64_t>(sizeof(ip_buffer)))
      {
        ret = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "ip buffer not enough, ip=%.*s", ip.length(), ip.ptr());
      }
      else if (!server.set_ipv4_addr(ip_buffer, static_cast<int32_t>(port)))
      {
        ret = OB_ERR_SYS;
        TBSYS_LOG(WARN, "failed to parse ip=%.*s", ip.length(), ip.ptr());
      }
      else if (server.get_ipv4() == 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "wrong proxy ip input ip=[%.*s] port=%ld, after parsed server=%s",
            ip.length(), ip.ptr(), port, to_cstring(server));
      }
      else if (OB_SUCCESS != (ret = proxy_list.add_proxy(server, supported_data_source_name)))
      {
        TBSYS_LOG(WARN, "failed to add proxy %s (support types %.*s) to proxy list, ret=%d",
            to_cstring(server), supported_data_source_name.length(), supported_data_source_name.ptr(), ret);
      }
    }
  }

  if (OB_ITER_END == ret)
  {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObRootRpcStub::fetch_slave_cluster_list(const common::ObServer& ms,
    const ObServer& master_rs, ObServer* slave_cluster_rs, int64_t& rs_count, int64_t timeout)
{
  int ret = OB_SUCCESS;
  char sql_buf[OB_SQL_LENGTH];
  ObString select_stmt;
  ObDataBuffer msgbuf;
  ObResultCode rs;
  int64_t session_id;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr is NULL");
    ret = OB_NOT_INIT;
  }

  if (OB_SUCCESS == ret)
  {
    int n = snprintf(sql_buf, OB_SQL_LENGTH, "select cluster_vip, rootserver_port from %s where cluster_role = 2;",
        OB_ALL_CLUSTER);
    if (n < 0 || n >= OB_SQL_LENGTH)
    {
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "failed to generate the sql of fetch_slave_cluster_list");
    }
    else
    {
      select_stmt.assign_ptr(sql_buf, static_cast<ObString::obstr_size_t>(strlen(sql_buf)));
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
    {
      TBSYS_LOG(WARN, "failed to get thread buffer, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = select_stmt.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
    {
      TBSYS_LOG(WARN, "failed to serialize select stmt, ret=%d", ret);
    }
    else if (OB_SUCCESS !=
        (ret = client_mgr_->send_request(ms, OB_SQL_EXECUTE, DEFAULT_VERSION, timeout, msgbuf, session_id)))
    {
      TBSYS_LOG(WARN, "failed to send request sql %.*s to ms %s, ret=%d",
          select_stmt.length(), select_stmt.ptr(), to_cstring(ms), ret);
    }
    else
    {
      bool fullfilled = true;
      sql::ObSQLResultSet rs;
      do
      {
        msgbuf.get_position() = 0;
        if (OB_SUCCESS !=
            (ret = rs.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          TBSYS_LOG(WARN, "fail to deserialize result buffer, ret=%d", ret);
        }
        else if (OB_SUCCESS != rs.get_errno())
        {
          TBSYS_LOG(WARN, "fail to exeucte sql: [%s], errno: [%d]", to_cstring(rs.get_sql_str()), rs.get_errno());
          ret = rs.get_errno();
          break;
        }
        else
        {
          ObNewScanner& scanner = rs.get_new_scanner();

          if (OB_SUCCESS != (ret = fill_slave_cluster_list(scanner, master_rs, slave_cluster_rs, rs_count)))
          {
            TBSYS_LOG(WARN, "failted to parse and update proxy list, ret=%d", ret);
            break;
          }

          rs.get_fullfilled(fullfilled);
          if (fullfilled || scanner.is_empty())
          {
            break;
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "slave cluster should not have more than one package");
          }
        }
      } while (OB_SUCCESS == ret);
    }
  }

  return ret;
}

int ObRootRpcStub::fill_slave_cluster_list(ObNewScanner& scanner, const ObServer& master_rs,
    ObServer* slave_cluster_rs, int64_t& rs_count)
{
  int ret = OB_SUCCESS;
  ObRow row;
  const ObObj *cell = NULL;
  uint64_t tid = 0;
  uint64_t cid = 0;
  int64_t cell_idx = 0;

  char ip_buffer[OB_IP_STR_BUFF];
  int64_t slave_rs_count = 0;

  ObString ip;
  int64_t port = 0;
  ObServer server;

  while (OB_SUCCESS == (ret = scanner.get_next_row(row)))
  {
    // ip                         varchar
    // port                       int
    cell_idx = 0;
    if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to get cell, ret=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell->get_varchar(ip)))
    {
      TBSYS_LOG(WARN, "failed to get ip, ret=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to get cell, ret=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell->get_int(port)))
    {
      TBSYS_LOG(WARN, "failed to get port, ret=%d", ret);
      break;
    }
    else
    {
      char *p = ip.ptr();
      while (p <= ip.length() + ip.ptr() && *p == ' ')
      {
        ++p;
      }
      int32_t length = static_cast<int32_t>(ip.length() - (p - ip.ptr()));
      int n = snprintf(ip_buffer, sizeof(ip_buffer), "%.*s", length, p);
      if (n<0 || n >= static_cast<int64_t>(sizeof(ip_buffer)))
      {
        ret = OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(WARN, "ip buffer not enough, ip=%.*s", ip.length(), ip.ptr());
      }
      else if (!server.set_ipv4_addr(ip_buffer, static_cast<int32_t>(port)))
      {
        ret = OB_ERR_SYS;
        TBSYS_LOG(WARN, "failed to parse ip=%.*s", ip.length(), ip.ptr());
      }
      else if (server.get_ipv4() == 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "wrong proxy ip input ip=[%.*s] port=%ld, after parsed server=%s",
            ip.length(), ip.ptr(), port, to_cstring(server));
      }
      else if (server == master_rs)
      {
        ret = OB_RS_STATE_NOT_ALLOW;
        TBSYS_LOG(ERROR, "master_rs's cluster_role should not be 2! ip=%s", to_cstring(master_rs));
      }
      else
      {
        slave_cluster_rs[slave_rs_count++] = server;
      }
    }
  }

  if (OB_ITER_END == ret)
  {
    ret = OB_SUCCESS;
    rs_count = slave_rs_count;
  }

  return ret;
}

int ObRootRpcStub::get_import_status(const common::ObServer& rs, const common::ObString& table_name,
    const uint64_t table_id, int32_t& status, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  int32_t version = 1;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = table_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize table_name, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
  {
    TBSYS_LOG(ERROR, "failed to serialize table_id, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(rs, OB_RS_GET_IMPORT_STATUS, version, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to get import status, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else if (OB_SUCCESS != (ret = serialization::decode_i32(msgbuf.get_data(), msgbuf.get_position(), pos, &status)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize status, err=%d", ret);
    }
  }
  return ret;
}

int ObRootRpcStub::set_import_status(const common::ObServer& rs, const common::ObString& table_name,
    const uint64_t table_id, const int32_t status, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const int32_t version = 1;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = table_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize table_name, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
  {
    TBSYS_LOG(ERROR, "failed to serialize table_id, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), status)))
  {
    TBSYS_LOG(ERROR, "failed to serialize status_i32, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(rs, OB_RS_SET_IMPORT_STATUS, version, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to get import status, err=%d", result.result_code_);
      ret = result.result_code_;
    }
  }
  return ret;
}

int ObRootRpcStub::import(const common::ObServer& rs, const common::ObString& table_name,
    const uint64_t table_id, const common::ObString& uri,  const int64_t start_time, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const int32_t version = 1;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = table_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize table_name, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
  {
    TBSYS_LOG(ERROR, "failed to serialize table_id, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = uri.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize uri, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), start_time)))
  {
    TBSYS_LOG(ERROR, "failed to serialize start_time, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(rs, OB_RS_IMPORT, version, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to call import, err=%d", result.result_code_);
      ret = result.result_code_;
    }
  }
  return ret;
}

int ObRootRpcStub::kill_import(const common::ObServer& rs, const common::ObString& table_name,
    const uint64_t table_id, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const int32_t version = 1;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = table_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize table_name, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
  {
    TBSYS_LOG(ERROR, "failed to serialize table_id, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(rs, OB_RS_KILL_IMPORT, version, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to kill import, err=%d", result.result_code_);
      ret = result.result_code_;
    }
  }
  return ret;
}

int ObRootRpcStub::notify_switch_schema(const common::ObServer& rs, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const int32_t version = 1;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(rs, OB_RS_NOTIFY_SWITCH_SCHEMA, version, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to notify_switch_schema, err=%d", result.result_code_);
      ret = result.result_code_;
    }
  }
  return ret;
}

