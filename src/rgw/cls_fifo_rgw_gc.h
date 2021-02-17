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

#ifndef CEPH_RGW_CLS_FIFO_RGW_GC_H
#define CEPH_RGW_CLS_FIFO_RGW_GC_H

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/rados/librados.hpp"
#include "include/buffer.h"

#include "common/async/yield_context.h"

#include "cls/rgw/cls_rgw_types.h"
#include "cls/fifo/cls_fifo_types.h"
#include "cls/fifo/cls_fifo_ops.h"

#include "cls_fifo_legacy.h"

namespace rgw::cls::gc::fifo {
namespace fifo = rados::cls::fifo;
namespace lr = librados;

int list_part(lr::IoCtx& ioctx, const std::string& oid,
	      std::optional<std::string_view> tag, std::uint64_t ofs,
	      std::uint64_t max_entries,
        bool expired_only,
	      std::vector<fifo::part_list_entry>* entries,
	      bool* more, bool* full_part, std::string* ptag,
	      std::uint64_t tid, optional_yield y);

class RGW_GC_FIFO : public rgw::cls::fifo::FIFO
{
public:
  RGW_GC_FIFO(const RGW_GC_FIFO&) = delete;
  RGW_GC_FIFO& operator =(const RGW_GC_FIFO&) = delete;
  RGW_GC_FIFO(RGW_GC_FIFO&&) = delete;
  RGW_GC_FIFO& operator =(RGW_GC_FIFO&&) = delete;

  /// Open an existing FIFO.
  static int open(lr::IoCtx ioctx, //< IO Context
		  std::string oid, //< OID for metadata object
		  std::unique_ptr<RGW_GC_FIFO>* gc_fifo, //< OUT: Pointer to GC FIFO object
		  optional_yield y, //< Optional yield context
		  /// Operation will fail if FIFO is not at this version
		  std::optional<fifo::objv> objv = std::nullopt,
		  /// Probing for existence, don't print errors if we
		  /// can't find it.
		  bool probe = false);

  /// Create a new or open an existing GC FIFO.
  static int create(lr::IoCtx ioctx, //< IO Context
		    std::string oid, //< OID for metadata object
		    std::unique_ptr<RGW_GC_FIFO>* gc_fifo, //< OUT: Pointer to GC FIFO object
		    optional_yield y, //< Optional yield context
		    /// Operation will fail if the FIFO exists and is
		    /// not of this version.
		    std::optional<fifo::objv> objv = std::nullopt,
		    /// Prefix for all objects
		    std::optional<std::string_view> oid_prefix = std::nullopt,
		    /// Fail if the FIFO already exists
		    bool exclusive = false,
		    /// Maximum allowed size of parts
		    std::uint64_t max_part_size = rgw::cls::fifo::default_max_part_size,
		    /// Maximum allowed size of entries
		    std::uint64_t max_entry_size = rgw::cls::fifo::default_max_entry_size);

  /// Push an entry to the GC FIFO
  int push(cls_rgw_gc_obj_info& entry, //< Entry to push
					uint32_t expiration_secs, //< Expiration time in seconds
	        optional_yield y //< Optional yield
    );

  /// List entries
  int list(int max_entries, /// Maximum entries to list
	   /// Point after which to begin listing. Start at tail if null
	   std::optional<std::string_view> markstr,
     bool expired_only,
	   std::vector<cls_rgw_gc_obj_info>* out, //< OUT: entries
	   /// OUT: True if more entries in FIFO beyond the last returned
	   bool* more,
     std::string& next_marker,
	   optional_yield y //< Optional yield
    );
};
}

#endif // CEPH_RGW_CLS_FIFO_RGW_GC_H
