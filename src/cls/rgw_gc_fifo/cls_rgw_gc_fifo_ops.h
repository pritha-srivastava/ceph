// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/types.h"

namespace rados::cls::rgw::gc::fifo::op {
inline constexpr auto CLASS = "rgw_gc_fifo";
inline constexpr auto GC_LIST_PART = "rgw_gc_part_list";
} // namespace rados::cls::fifo::op
