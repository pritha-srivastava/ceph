/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#pragma once

#include "d4n_directory.h"
#include "rgw/ceph_fdb.h"

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include "include/random.h"

#include <chrono>
#include <vector>

using fmt::format;
using fmt::println;
using std::end;
using std::begin;
using std::string;
using std::string_view;
using std::to_string;
using std::vector;

using namespace std::literals::string_literals;
namespace lfdb = ceph::libfdb;

namespace rgw { namespace d4n {

using fdb_conn = lfdb::database;

class FDBDirectory : virtual public Directory {
  public:
	std::shared_ptr<fdb_conn> FDBconn; // FDB data base
    void set_fdb_database(std::shared_ptr<fdb_conn> db) {
      	FDBconn = db;
    }
    FDBDirectory(std::shared_ptr<FDBConnection> fdb_conn) : FDBconn(fdb_conn->get_fdb_conn()) {}
    virtual ~FDBDirectory() = default;

    virtual int get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val);

    virtual int set_kv(const DoutPrefixProvider* dpp, optional_yield y,
                        const std::string& key,
                        const std::string& field,
                        const std::string& val);

    virtual int get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                          const std::string& key,
                          const std::vector<std::string>& fields,
                          std::map<std::string, std::string>& out_vals);

    virtual int set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& key,
                            const std::map<std::string, std::string>& vals);

    virtual int set_kv_multi_init_field(const DoutPrefixProvider* dpp, optional_yield y,
                                        const std::string& key,
                                        const std::map<std::string, std::string>& always_set,
                                        const std::string& init_field,
                                        const std::string& init_val);
};

class FDBBucketDirectory: public FDBDirectory, public BucketDirectory {
  public:
	FDBBucketDirectory(std::shared_ptr<FDBConnection> fdb_conn) : FDBDirectory(fdb_conn) {}

    virtual int add_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, optional_yield y, Pipeline* pipeline=nullptr) override;
    virtual int remove_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, optional_yield y) override;
    virtual int scan_objects(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t start_pos, const std::string& pattern, uint64_t count, std::vector<std::string>& objects, std::optional<CacheObject>& params, uint64_t& next_pos, optional_yield y) override;
    virtual int get_range(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& objects, std::optional<CacheObject>& params, optional_yield y) override;
};

class FDBObjectDirectory: public FDBDirectory, public ObjectDirectory {
  public:
	FDBObjectDirectory(std::shared_ptr<FDBConnection> fdb_conn) : FDBDirectory(fdb_conn) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, optional_yield y) override;	

    virtual int add_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, optional_yield y, Pipeline* pipeline=nullptr);
    virtual int remove_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, optional_yield y);
    virtual int remove_version_by_creation_time(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const double& creation_time, optional_yield y);
    virtual int list_versions(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& start, const std::string& stop, std::vector<CacheObjectVersion>& versions, optional_yield y);
    virtual int get_version_index(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::string& index, optional_yield y) override;
};

class FDBBlockDirectory: public FDBDirectory, public BlockDirectory {
  public:
	FDBBlockDirectory(std::shared_ptr<FDBConnection> fdb_conn) : FDBDirectory(fdb_conn) {}
    virtual int exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) override;

    virtual int set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y) override;
    virtual int set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline=nullptr) override;
    virtual int get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) override;
    //Pipelined version of get using boost::redis::generic_response
    virtual int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y) override;

    virtual int copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y) override;
    virtual int del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) override;
    virtual int update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y) override;
    virtual int remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y) override;

  private:
    template<SeqContainer Container>
    int set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& fdbValues, optional_yield y);
};

} } // namespace rgw::d4n
