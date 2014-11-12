#ifdef __rs_debug__
oceanbase::rootserver::ObRootServer2* __rs = NULL;
oceanbase::common::ObRoleMgr* __role_mgr = NULL;
oceanbase::common::ObiRole* __obi_role = NULL;
#define __debug_init__() __rs = &root_server_; __role_mgr = &role_mgr_;  __obi_role = (typeof(__obi_role))&root_server_.get_obi_role();
#endif

#ifdef __ms_debug__
oceanbase::mergeserver::ObMergeServer * __ms = NULL;
oceanbase::common::ObMergerSchemaManager * __ms_schema = NULL;
oceanbase::mergeserver::ObMergeServerService * __ms_service = NULL;
#define __debug_init__() __ms = merge_server_; __ms_schema = schema_mgr_; __ms_service =  this;
#endif

#ifdef __ups_debug__
oceanbase::updateserver::ObUpdateServer* __ups = NULL;
oceanbase::updateserver::ObUpsRoleMgr* __role_mgr = NULL;
oceanbase::updateserver::ObUpsLogMgr* __log_mgr = NULL;
oceanbase::common::ObiRole* __obi_role = NULL;
#define __debug_init__() __ups = this; __role_mgr = &role_mgr_; __obi_role = &obi_role_; __log_mgr = &log_mgr_;
#endif
