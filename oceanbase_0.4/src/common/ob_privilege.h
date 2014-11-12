/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@taobao.com>
 */
#ifndef OB_PRIVILEGE_H_
#define OB_PRIVILEGE_H_

#include "ob_string.h"
#include "hash/ob_hashmap.h"
#include "ob_string_buf.h"
#include "ob_row.h"
#include "ob_array.h"
#include "ob_bit_set.h"
#define USERNAME_MAP_BUCKET_NUM 100
#define USERTABLE_MAP_BUCKET_NUM 100
namespace oceanbase
{
  namespace common
  {
    // do not need serialization and deserialization
    class ObPrivilege
    {
      public:
        struct TablePrivilege
        {
          TablePrivilege();
          uint64_t table_id_;
          ObBitSet<> privileges_;
        };
        struct User
        {
          User();
          ObString user_name_;
          uint64_t user_id_;
          ObString password_;
          ObString comment_;
          ObBitSet<> privileges_;
          bool locked_;
        };
        struct UserPrivilege
        {
          UserPrivilege();
          uint64_t user_id_;
          TablePrivilege table_privilege_;
        };
        struct UserIdTableId
        {
          UserIdTableId();
          uint64_t user_id_;
          uint64_t table_id_;
          int64_t hash() const;
          bool operator==(const UserIdTableId & other) const;
        };
        // typedef hash::ObHashMap<ObString, User, hash::NoPthreadDefendMode,
        //                         hash::hash_func<ObString>, hash::equal_to<ObString>,
        //                         hash::SimpleAllocer<hash::HashMapTypes<ObString, User>::AllocType, 384> > NameUserMap;
        // typedef hash::ObHashMap<UserIdTableId, UserPrivilege, hash::NoPthreadDefendMode,
        //                         hash::hash_func<UserIdTableId>, hash::equal_to<UserIdTableId>,
        //                         hash::SimpleAllocer<hash::HashMapTypes<UserIdTableId, UserPrivilege>::AllocType, 384> > UserPrivMap;
        typedef hash::ObHashMap<ObString, User, hash::NoPthreadDefendMode> NameUserMap;
        typedef hash::ObHashMap<UserIdTableId, UserPrivilege, hash::NoPthreadDefendMode> UserPrivMap;
      public:
        static void privilege_to_string(const ObBitSet<> &privileges, char *buf, const int64_t buf_len, int64_t &pos);
        ObPrivilege();
        ~ObPrivilege();
        int init();
        /**
         * @synopsis  user_is_valid
         *
         * @param username
         * @param password
         *
         * @returns OB_SUCCESS if username is valid
         */
        int user_is_valid(const ObString & username, const ObString & password) const;
        /**
         * @synopsis  has_needed_privileges
         *
         * @param username
         * @param table_privileges
         *
         * @returns OB_SUCCESS if username has the needed privileges
         */
        int has_needed_privileges(const ObString &username, const ObArray<TablePrivilege> & table_privileges) const;
        /**
         * @synopsis  全量导入__users表中内容
         *
         * @param row: 从ObResultSet迭代出来的，一行一行的往里加
         *
         * @returns OB_SUCCESS if add row succeed
         */
        int add_users_entry(const ObRow & row);
        int add_table_privileges_entry(const ObRow & row);
        /**
         * @synopsis  比较两个privilege
         *
         * @param privilege1
         * @param privilege2
         *
         * @returns 如果privilege2包含(包括相等)privilege1，则返回true，否则返回false
         */
        bool compare_privilege(const ObBitSet<> & privilege1, const ObBitSet<> & privilege2) const;
        void set_version(const int64_t version);
        int64_t get_version() const;
        void print_info(void) const;
        NameUserMap* get_username_map() { return &username_map_;}
        UserPrivMap* get_user_table_map()  {return &user_table_map_;}
      private:
        // key:username
        NameUserMap username_map_;
        UserPrivMap user_table_map_;
        ObStringBuf string_buf_;
        int64_t version_;
    };
  }// namespace common
}//namespace oceanbase
#endif
