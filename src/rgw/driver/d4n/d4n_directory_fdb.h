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

class FDBDirectory {
  public:
	std::shared_ptr<fdb_conn> FDBconn{nullptr}; // FDB data base
    void set_fdb_database(std::shared_ptr<fdb_conn> db) {
      	FDBconn = db;
    }
    FDBDirectory() {}
};

class FDBBucketDirectory: public FDBDirectory, public BucketDirectory {
  public:
	FDBBucketDirectory(std::shared_ptr<FDBConnection> fdb_conn) : FDBconn(fdb_conn->conn) {}

    virtual int zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr) override;
    virtual int zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y) override;
    virtual int zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y) override;
    virtual int zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y) override;
    virtual int zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y) override;

  private:
    std::shared_ptr<fdb_conn> FDBconn;
};

class FDBObjectDirectory: public FDBDirectory, public ObjectDirectory {
  public:
	FDBObjectDirectory(std::shared_ptr<FDBConnection> fdb_conn) : FDBconn(fdb_conn->conn) {}

    virtual int exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) override;

    virtual int set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) override; /* If nx is true, set only if key doesn't exist */
    virtual int get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) override;
    virtual int copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y) override;
    virtual int del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) override;
    virtual int update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y) override;
    virtual int zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr) override;
    virtual int zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y) override;
    virtual int zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y) override;
    virtual int zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y) override;
    virtual int zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y) override;
    virtual int zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y) override;
    //Return value is the incremented value, else return error
    virtual int incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) override;

  private:
    std::shared_ptr<fdb_conn> FDBconn;
};

class FDBBlockDirectory: public FDBDirectory, public BlockDirectory {
  public:
	FDBBlockDirectory(std::shared_ptr<FDBConnection> fdb_conn) : FDBconn(fdb_conn->conn) {}
    
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
    virtual int zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y) override;
    virtual int zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y) override;
    virtual int zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y) override;
    virtual int zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y) override;

  private:
    std::shared_ptr<fdb_conn> FDBconn;

    template<SeqContainer Container>
    int set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& fdbValues, optional_yield y);
};

} } // namespace rgw::d4n
