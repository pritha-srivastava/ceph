// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <chrono>

#include <gtest/gtest.h>

#include "common/ceph_time.h"
#include "common/iso_8601.h"

using std::chrono::minutes;
using std::chrono::seconds;
using std::chrono::time_point_cast;

using ceph::from_iso_8601;
using ceph::iso_8601_format;
using ceph::real_clock;
using ceph::real_time;
using ceph::to_iso_8601;

TEST(iso_8601, epoch) {
  const auto epoch = real_clock::from_time_t(0);

  ASSERT_EQ(to_iso_8601(epoch, iso_8601_format::Y), "1970");
  ASSERT_EQ(to_iso_8601(epoch, iso_8601_format::YM), "1970-01");
  ASSERT_EQ(to_iso_8601(epoch, iso_8601_format::YMD), "1970-01-01");
  ASSERT_EQ(to_iso_8601(epoch, iso_8601_format::YMDh), "1970-01-01T00Z");
  ASSERT_EQ(to_iso_8601(epoch, iso_8601_format::YMDhm), "1970-01-01T00:00Z");
  ASSERT_EQ(to_iso_8601(epoch, iso_8601_format::YMDhms),
	    "1970-01-01T00:00:00Z");
  ASSERT_EQ(to_iso_8601(epoch, iso_8601_format::YMDhmsn),
	    "1970-01-01T00:00:00.000000000Z");

  ASSERT_EQ(*from_iso_8601("1970"), epoch);
  ASSERT_EQ(*from_iso_8601("1970-01"), epoch);
  ASSERT_EQ(*from_iso_8601("1970-01-01"), epoch);
  ASSERT_EQ(*from_iso_8601("1970-01-01T00:00Z"), epoch);
  ASSERT_EQ(*from_iso_8601("1970-01-01T00:00:00Z"), epoch);
  ASSERT_EQ(*from_iso_8601("1970-01-01T00:00:00.000000000Z"), epoch);
}

TEST(iso_8601, now) {
  const auto now = real_clock::now();

  ASSERT_EQ(*from_iso_8601(
	      to_iso_8601(now, iso_8601_format::YMDhm)),
	    real_time(time_point_cast<minutes>(now)));
  ASSERT_EQ(*from_iso_8601(
	      to_iso_8601(now, iso_8601_format::YMDhms)),
	    real_time(time_point_cast<seconds>(now)));
  ASSERT_EQ(*from_iso_8601(
	      to_iso_8601(now, iso_8601_format::YMDhmsn)),
	    now);
}
