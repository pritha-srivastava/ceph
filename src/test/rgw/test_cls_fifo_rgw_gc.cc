// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <cerrno>
#include <iostream>
#include <string_view>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "include/scope_guard.h"
#include "include/types.h"
#include "include/rados/librados.hpp"

#include "cls/fifo/cls_fifo_ops.h"
#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include "rgw/rgw_tools.h"
#include "rgw/cls_fifo_legacy.h"
#include "rgw/cls_fifo_rgw_gc.h"

#include "gtest/gtest.h"

namespace R = librados;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;
namespace RCf = rgw::cls::fifo;
namespace RCGC = rgw::cls::gc::fifo;

class GCFIFO : public testing::Test {
protected:
  const std::string pool_name = get_temp_pool_name();
  const std::string fifo_id = "gc";
  R::Rados rados;
  librados::IoCtx ioctx;

  void SetUp() override {
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  void TearDown() override {
    destroy_one_pool_pp(pool_name, rados);
  }
};

using GCClsFIFO = GCFIFO;

namespace {
void create_obj(cls_rgw_obj& obj, int i, int j)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "-%d.%d", i, j);
  obj.pool = "pool";
  obj.pool.append(buf);
  obj.key.name = "oid";
  obj.key.name.append(buf);
  obj.loc = "loc";
  obj.loc.append(buf);
}
}

TEST_F(GCFIFO, TestPushListTrimNonExpired)
{
  std::unique_ptr<RCGC::RGW_GC_FIFO> f;
  auto r = RCGC::RGW_GC_FIFO::create(ioctx, fifo_id, &f, null_yield);
  ASSERT_EQ(0, r);
  static constexpr auto max_entries = 10u;
  for (uint32_t i = 0; i < max_entries; ++i) {
    string tag = "chain-" + to_string(i);
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;

    r = f->push(info, 300, null_yield);
    ASSERT_EQ(0, r);
  }

  std::optional<std::string> marker;
  /* get entries one by one */
  std::vector<cls_rgw_gc_obj_info> result;
  bool more = false;
  bool expired_only = false;
	string next_marker;
  for (auto i = 0u; i < max_entries; ++i) {
    r = f->list(1, marker, expired_only, &result, &more, next_marker, null_yield);
    ASSERT_EQ(0, r);

    bool expected_more = (i != (max_entries - 1));
    ASSERT_EQ(expected_more, more);
    ASSERT_EQ(1, result.size());

		string tag = "chain-" + to_string(i);
		ASSERT_EQ(tag, result[0].tag);
	
    result.clear();
		marker = next_marker;
		next_marker.clear();
  }

  /* get all entries at once */
  r = f->list(max_entries * 10, std::nullopt, expired_only, &result, &more, next_marker, null_yield);
  ASSERT_EQ(0, r);

  ASSERT_FALSE(more);
  ASSERT_EQ(max_entries, result.size());
  for (auto i = 0u; i < max_entries; ++i) {
    string tag = "chain-" + to_string(i);
		ASSERT_EQ(tag, result[i].tag);
  }

  /* trim all entries */
  r = f->trim(next_marker, false, null_yield);
  ASSERT_EQ(0, r);

  r = f->list(max_entries * 10, std::nullopt, expired_only, &result, &more, next_marker, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_FALSE(more);
  ASSERT_EQ(0, result.size());
}

TEST_F(GCFIFO, TestPushListTrimExpired)
{
  std::unique_ptr<RCGC::RGW_GC_FIFO> f;
  auto r = RCGC::RGW_GC_FIFO::create(ioctx, fifo_id, &f, null_yield);
  ASSERT_EQ(0, r);
  static constexpr auto max_entries = 10u;
  for (uint32_t i = 0; i < (max_entries/2); ++i) {
    string tag = "chain-" + to_string(i);
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;

    r = f->push(info, 0, null_yield);
    ASSERT_EQ(0, r);
  }

  for (uint32_t i = (max_entries/2); i < max_entries; ++i) {
    string tag = "chain-" + to_string(i);
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;

    r = f->push(info, 300, null_yield);
    ASSERT_EQ(0, r);
  }

  std::optional<std::string> marker;
  /* get entries one by one */
  std::vector<cls_rgw_gc_obj_info> result;
  bool more = false;
  bool expired_only = true;
	string next_marker;
  
  /* get all expired entries */
  r = f->list(max_entries * 10, std::nullopt, expired_only, &result, &more, next_marker, null_yield);
  ASSERT_EQ(0, r);

  ASSERT_FALSE(more);
  ASSERT_EQ((max_entries/2), result.size());
  for (auto i = 0u; i < (max_entries/2); ++i) {
    string tag = "chain-" + to_string(i);
		ASSERT_EQ(tag, result[i].tag);
  }

  /* trim all entries */
  r = f->trim(next_marker, false, null_yield);
  ASSERT_EQ(0, r);

  /* check to see if there are any expired entries */
  result.clear();
  r = f->list(max_entries * 10, std::nullopt, expired_only, &result, &more, next_marker, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_FALSE(more);
  ASSERT_EQ(0, result.size());

  /* check to see if there are non expired entries */
  expired_only = false;
  result.clear();
  r = f->list(max_entries * 10, std::nullopt, expired_only, &result, &more, next_marker, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_FALSE(more);
  ASSERT_EQ((max_entries/2), result.size());
}


TEST_F(GCFIFO, TestBenchmarkPushListTrimNonExpired)
{
  std::unique_ptr<RCGC::RGW_GC_FIFO> f;
  auto r = RCGC::RGW_GC_FIFO::create(ioctx, fifo_id, &f, null_yield);
  ASSERT_EQ(0, r);
  static constexpr auto max_entries = 512u;

  auto start_push = std::chrono::steady_clock::now();
  for (uint32_t i = 0; i < max_entries; ++i) {
    string tag = "chain-" + to_string(i);
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;

    r = f->push(info, 300, null_yield);
    ASSERT_EQ(0, r);
  }
  auto finish_push = std::chrono::steady_clock::now();
  std::optional<std::string> marker;
  /* get entries one by one */
  std::vector<cls_rgw_gc_obj_info> result;
  bool more = false;
  bool expired_only = false;
	string next_marker;

  auto start_list = std::chrono::steady_clock::now();
  /* get all entries at once */
  r = f->list(max_entries * 10, std::nullopt, expired_only, &result, &more, next_marker, null_yield);
  ASSERT_EQ(0, r);
  auto finish_list = std::chrono::steady_clock::now();

  //ASSERT_FALSE(more);
  ASSERT_EQ(max_entries, result.size());
  for (auto i = 0u; i < max_entries; ++i) {
    string tag = "chain-" + to_string(i);
		ASSERT_EQ(tag, result[i].tag);
  }
  
  auto start_trim = std::chrono::steady_clock::now();
  /* trim all entries */
  r = f->trim(next_marker, false, null_yield);
  ASSERT_EQ(0, r);
  auto finish_trim = std::chrono::steady_clock::now();

  r = f->list(max_entries * 10, std::nullopt, expired_only, &result, &more, next_marker, null_yield);
  ASSERT_EQ(0, r);
  ASSERT_FALSE(more);
  ASSERT_EQ(0, result.size());
  

  fmt::print(std::cerr, "Time taken to push {} entries {} \n", max_entries, (finish_push - start_push));
  fmt::print(std::cerr, "Time taken to list {} entries {} \n", max_entries, (finish_list - start_list));
  fmt::print(std::cerr, "Time taken to trim {} entries {} \n", max_entries, (finish_trim - start_trim));
}
