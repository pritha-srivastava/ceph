// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/queue/cls_queue_client.h"

#include "common/debug.h"

using namespace librados;

void cls_init_queue(ObjectWriteOperation& op, string& queue_name, uint64_t& size)
{
  bufferlist in;
  cls_init_queue_op call;
  call.has_urgent_data = false;
  call.head.queue_size = size;
  encode(call, in);
  op.exec(QUEUE_CLASS, INIT_QUEUE, in);
}

int cls_get_queue_size(IoCtx& io_ctx, string& oid, uint64_t& size)
{
  bufferlist in, out;
  int r = io_ctx.exec(oid, QUEUE_CLASS, GET_QUEUE_SIZE, in, out);
  if (r < 0)
    return r;

  cls_queue_get_size_ret op_ret;
  auto iter = out.cbegin();
  try {
    decode(op_ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  size = op_ret.queue_size;

  return 0;
}

void cls_gc_enqueue(ObjectWriteOperation& op, uint32_t expiration_secs, vector<bufferlist> bl_data_vec)
{
  bufferlist in;
  cls_enqueue_op call;
  call.bl_data_vec = bl_data_vec;
  encode(call, in);
  op.exec(QUEUE_CLASS, ENQUEUE, in);
}

int cls_list_queue(IoCtx& io_ctx, string& oid, string& marker, uint32_t max,
                    vector<bufferlist>& entries, vector<string>& entry_markers,
                    bool *truncated, string& next_marker)
{
  bufferlist in, out;
  cls_queue_list_op op;
  op.start_marker = marker;
  op.max = max;
  encode(op, in);

  int r = io_ctx.exec(oid, QUEUE_CLASS, QUEUE_LIST_ENTRIES, in, out);
  if (r < 0)
    return r;

  cls_queue_list_ret ret;
  auto iter = out.cbegin();
  try {
    decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  entries = ret.data;
  entry_markers = ret.markers;
  *truncated = ret.is_truncated;

  next_marker = std::move(ret.next_marker);

  return 0;
}

void cls_remove_entries_queue(ObjectWriteOperation& op, string& end_marker)
{
  bufferlist in, out;
  cls_queue_remove_op rem_op;
  rem_op.end_marker = end_marker;
  encode(rem_op, in);
  op.exec(QUEUE_CLASS, QUEUE_REMOVE_ENTRIES, in);
}
