// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file
 *
 * This is an OSD class that implements methods for management
 * and use of gc using fifo
 *
 */

#include <cerrno>
#include <optional>
#include <string>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/buffer.h"
#include "include/types.h"

#include "objclass/objclass.h"

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/fifo/cls_fifo.h"
#include "cls/fifo/cls_fifo_ops.h"
#include "cls/fifo/cls_fifo_types.h"
#include "cls/rgw_gc_fifo/cls_rgw_gc_fifo_ops.h"

CLS_VER(1,0)
CLS_NAME(rgw_gc_fifo)

namespace rados::cls::rgw::gc::fifo {

int gc_list_part(cls_method_context_t hctx, ceph::buffer::list* in, ceph::buffer::list* out)
{
  constexpr uint64_t GC_LIST_DEFAULT_MAX = 128;

  auto in_iter = in->cbegin();
  cls_rgw_gc_list_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(5, "ERROR: gc_list_part(): failed to decode input");
    return -EINVAL;
  }

  rados::cls::fifo::op::list_part fifo_op;
  if (! op.max) {
    op.max = GC_LIST_DEFAULT_MAX;
  }
  
  fifo_op.max_entries = op.max;
  char* endptr;
  fifo_op.ofs = ::strtoull(op.marker.c_str(), &endptr, 10);
  if (errno) {
    CLS_LOG(5, "ERROR: gc_list_part(): invalid input marker = %s", op.marker.c_str());
    return -EINVAL;
  }

  rados::cls::fifo::op::list_part_reply reply;
  auto ret = rados::cls::fifo::list_part(hctx, fifo_op, reply);
  if (ret < 0) {
    return ret;
  }

  auto begin_iter = reply.entries.begin();
  for (auto& entry : reply.entries) {
    cls_rgw_gc_obj_info info;
    auto iter = entry.data.cbegin();
    try {
      decode(info, iter);
    } catch (ceph::buffer::error& err) {
      CLS_LOG(5, "ERROR: gc_list_part: failed to decode gc info\n");
      return -EINVAL;
    }
    real_time now = ceph::real_clock::now();
    if (op.expired_only && info.time > now) {
      //Can break out here if info.time > now, since all subsequent entries won't have expired
      reply.more = false;
      reply.entries.erase(begin_iter, reply.entries.end()); // remove remaining elements from the vector
      break;
    }
    begin_iter++;
  }

  out->clear();
  encode(reply, *out);
  return 0;
}

} // namespace rados::cls::rgw::gc::fifo

CLS_INIT(rgw_gc_fifo)
{
  using namespace rados::cls::rgw::gc::fifo;
  CLS_LOG(10, "Loaded rgw gc fifo class!");

  cls_handle_t h_class;
  cls_method_handle_t h_list_part;

  cls_register(op::CLASS, &h_class);

  cls_register_cxx_method(h_class, op::GC_LIST_PART,
                          CLS_METHOD_RD,
                          gc_list_part, &h_list_part);

  return;
}
