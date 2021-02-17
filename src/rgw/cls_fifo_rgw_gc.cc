// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat <contact@redhat.com>
 * Author: Pritha Srivastava
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <cstdint>
#include <numeric>
#include <optional>
#include <string_view>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/rados/librados.hpp"

#include "include/buffer.h"

#include "common/async/yield_context.h"
#include "common/random_string.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/rgw_gc_fifo/cls_rgw_gc_fifo_ops.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_types.h"

#include "librados/AioCompletionImpl.h"

#include "rgw_tools.h"

#include "cls_fifo_legacy.h"
#include "cls_fifo_rgw_gc.h"

namespace rgw::cls::gc::fifo {
static constexpr auto dout_subsys = ceph_subsys_rgw;
namespace cb = ceph::buffer;
namespace fifo = rados::cls::fifo;
namespace gc = rados::cls::rgw::gc::fifo;

using ceph::from_error_code;

int list_part(lr::IoCtx& ioctx, const std::string& oid,
	      std::optional<std::string_view> tag, std::uint64_t ofs,
	      std::uint64_t max_entries,
        bool expired_only,
	      std::vector<fifo::part_list_entry>* entries,
	      bool* more, bool* full_part, std::string* ptag,
	      std::uint64_t tid, optional_yield y)
{
  lr::ObjectReadOperation op;
  cls_rgw_gc_list_op list_op;

  list_op.marker = std::to_string(ofs);
  list_op.max = max_entries;
  list_op.expired_only = expired_only;

  cb::list in;
  encode(list_op, in);
  cb::list bl;
  op.exec(gc::op::CLASS, gc::op::GC_LIST_PART, in, &bl, nullptr);
  auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
  if (r >= 0) try {
      fifo::op::list_part_reply reply;
      auto iter = bl.cbegin();
      decode(reply, iter);
      if (entries) *entries = std::move(reply.entries);
      if (more) *more = reply.more;
      if (full_part) *full_part = reply.full_part;
      if (ptag) *ptag = reply.tag;
    } catch (const cb::error& err) {
      lderr(static_cast<CephContext*>(ioctx.cct()))
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " decode failed: " << err.what()
      << " tid=" << tid << dendl;
      r = from_error_code(err.code());
    } else if (r != -ENOENT) {
    lderr(static_cast<CephContext*>(ioctx.cct()))
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << " fifo::op::LIST_PART failed r=" << r << " tid=" << tid
      << dendl;
  }
  return r;
}

int RGW_GC_FIFO::open(lr::IoCtx ioctx, std::string oid, std::unique_ptr<RGW_GC_FIFO>* gc_fifo,
          optional_yield y, std::optional<fifo::objv> objv,
          bool probe)
{
  std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
  auto r = rgw::cls::fifo::FIFO::open(ioctx, oid, &fifo, y, objv, probe);
  if (r < 0) {
    return r;
  }
  std::unique_ptr<RGW_GC_FIFO> gc(static_cast<RGW_GC_FIFO*>(fifo.release()));
  *gc_fifo = std::move(gc);

  return 0;
}

int RGW_GC_FIFO::create(lr::IoCtx ioctx, std::string oid, std::unique_ptr<RGW_GC_FIFO>* gc_fifo,
		 optional_yield y, std::optional<fifo::objv> objv,
		 std::optional<std::string_view> oid_prefix,
		 bool exclusive, std::uint64_t max_part_size,
		 std::uint64_t max_entry_size)
{
  std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
  auto r = rgw::cls::fifo::FIFO::create(ioctx, oid, &fifo, y, objv, oid_prefix, exclusive, max_part_size, max_entry_size);
  if (r < 0) {
    return r;
  }
  std::unique_ptr<RGW_GC_FIFO> gc(static_cast<RGW_GC_FIFO*>(fifo.release()));
  *gc_fifo = std::move(gc);

  return 0;
}

int RGW_GC_FIFO::push(cls_rgw_gc_obj_info& info, uint32_t expiration_secs, optional_yield y)
{
  info.time = ceph::real_clock::now();
  info.time += make_timespan(expiration_secs);
  cb::list bl;
  encode(info, bl);
  auto ret = rgw::cls::fifo::FIFO::push(bl, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGW_GC_FIFO::list(int max_entries,
          std::optional<std::string_view> markstr,
          bool expired_only,
          std::vector<cls_rgw_gc_obj_info>* presult, bool* pmore,
          std::string& next_marker,
	        optional_yield y)
{
  std::unique_lock l(m);
  auto tid = ++next_tid;
  std::int64_t part_num = info.tail_part_num;
  l.unlock();
  ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " entering: tid=" << tid << dendl;
  std::uint64_t ofs = 0;
  if (markstr) {
    auto marker = to_marker(*markstr);
    if (!marker) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " invalid marker string: " << markstr
		 << " tid= "<< tid << dendl;
      return -EINVAL;
    }
    part_num = marker->num;
    ofs = marker->ofs;
  }

  std::vector<cls_rgw_gc_obj_info> result;
  result.reserve(max_entries);
  bool more = false;

  std::vector<fifo::part_list_entry> entries;
  fifo::part_list_entry last_entry;
  int r = 0;
  while (max_entries > 0) {
    ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " max_entries=" << max_entries << " tid=" << tid << dendl;
    bool part_more = false;
    bool part_full = false;

    std::unique_lock l(m);
    auto part_oid = info.part_oid(part_num);
    l.unlock();

    r = list_part(ioctx, part_oid, {}, ofs, max_entries, expired_only, &entries,
		  &part_more, &part_full, nullptr, tid, y);
    if (r == -ENOENT) {
      ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " missing part, rereading metadata"
		     << " tid= "<< tid << dendl;
      r = read_meta(tid, y);
      if (r < 0) {
	lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << " read_meta failed: r=" << r
		   << " tid= "<< tid << dendl;
	return r;
      }
      if (part_num < info.tail_part_num) {
	/* raced with trim? restart */
	ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << " raced with trim, restarting: tid=" << tid << dendl;
	max_entries += result.size();
	result.clear();
	std::unique_lock l(m);
	part_num = info.tail_part_num;
	l.unlock();
	ofs = 0;
	continue;
      }
      ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " assuming part was not written yet, so end of data: "
		     << "tid=" << tid << dendl;
      more = false;
      r = 0;
      break;
    }
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " list_entries failed: r=" << r
		 << " tid= "<< tid << dendl;
      return r;
    }
    more = part_full || part_more;
    for (auto& entry : entries) {
      cls_rgw_gc_obj_info info;
      auto iter = entry.data.cbegin();
      decode(info, iter);
      result.push_back(std::move(info));
      --max_entries;
      if (max_entries == 0)
	break;
    }
    if (entries.size() > 0) {
      last_entry = entries.back();
      next_marker = rgw::cls::fifo::marker{part_num, last_entry.ofs}.to_string();
    }
    entries.clear();
    if (max_entries > 0 &&
	    part_more) {
    }

    if (!part_full) {
      ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " head part is not full, so we can assume we're done: "
		     << "tid=" << tid << dendl;
      break;
    }
    if (!part_more) {
      ++part_num;
      ofs = 0;
    }
  }
  if (pmore)
    *pmore =  more;

  if (presult)
    *presult = std::move(result);
  
  return 0;
}

}
