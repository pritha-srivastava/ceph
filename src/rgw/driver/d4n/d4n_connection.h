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

#include "rgw/ceph_fdb.h"
#include <boost/redis/connection.hpp>

namespace lfdb = ceph::libfdb;

using boost::redis::connection;
using fdbase= lfdb::database;

namespace rgw { namespace d4n {

class D4NConnection {
public:
    virtual ~D4NConnection() = default;

    virtual void get() = 0;
    virtual void put() = 0;

	virtual std::shared_ptr<void> get_conn() = 0;
};


class RedisConnection : public D4NConnection {
public:
    std::shared_ptr<connection> conn;
    RedisConnection(std::shared_ptr<connection> c) : conn(c) {}

    void get() override {
    }

    void put() override {
    }

    std::shared_ptr<void> get_conn() override {
        return conn;
    }
};


class FDBConnection : public D4NConnection {
public:
    std::shared_ptr<fdbase> conn;
    FDBConnection(std::shared_ptr<fdbase> c) : conn(c) {}

    void get() override {
    }

    void put() override {
    }

    std::shared_ptr<void> get_conn() override {
        return conn;
    }
};

}} //namespace rgw::d4n
