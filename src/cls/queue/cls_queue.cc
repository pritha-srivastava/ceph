// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include <errno.h>

#include "objclass/objclass.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_types.h"
#include "common/Clock.h"
#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "common/Clock.h"
#include "common/strtol.h"
#include "common/escape.h"

#include "include/compat.h"
#include <boost/lexical_cast.hpp>

#define GC_LIST_DEFAULT_MAX 128

CLS_VER(1,0)
CLS_NAME(queue)

static int cls_create_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_create_queue_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_create_queue(): failed to decode entry\n");
    return -EINVAL;
  }

  // create the object
  int ret = cls_cxx_create(hctx, true);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_create returned %d", __func__, ret);
    return ret;
  }
  CLS_LOG(0, "INFO: cls_create_queue create queue of size %lu", op.head.size);

  // write the head
  bufferlist bl_head;
  encode(op.head, bl_head);
  CLS_LOG(0, "INFO: cls_create_queue writing head of size %u, %lu", bl_head.length(), sizeof(cls_queue_head));
  ret = cls_cxx_write2(hctx, 0, bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_write returned %d", __func__, ret);
    return ret;
  }
  return 0;
}

static int cls_get_queue_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // read the head
  cls_queue_head head;
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, sizeof(cls_queue_head), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }

  try {
    decode(head, bl_head);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_get_queue_size: failed to decode entry\n");
    return -EINVAL;
  }

  encode(head.size, *out);

  return 0;
}

static int cls_enqueue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(1, "INFO: cls_enqueue: Read Head of size: %lu\n", sizeof(cls_queue_head));
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, sizeof(cls_queue_head), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }

  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_enqueue: failed to decode head\n");
    return -EINVAL;
  }

  if (head.front == head.tail && ! head.is_empty) {
    // return queue full error
    return -ENOSPC;
  }

  iter = in->cbegin();
  cls_rgw_queue_data data;
  try {
    decode(data, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_enqueue: failed to decode input data \n");
    return -EINVAL;
  }

  uint64_t total_size = sizeof(data.size_data) + data.bl_data.length();
  CLS_LOG(1, "INFO: cls_enqueue(): Total size to be written is %lu and data size is %lu\n", total_size, data.size_data);

  bufferlist bl_size;
  encode(data.size_data, bl_size);

  if (head.tail >= head.front) {
    // check if data can fit in the remaining space in queue
    if ((head.tail + total_size) <= head.size) {
      CLS_LOG(1, "INFO: cls_enqueue: Writing data size: offset: %lu, size: %u\n", head.tail, bl_size.length());
      //write data size at tail offset
      ret = cls_cxx_write(hctx, head.tail, bl_size.length(), &bl_size);
      if (ret < 0)
        return ret;

      head.tail += bl_size.length();

      CLS_LOG(1, "INFO: cls_enqueue: Writing data at offset: %lu\n", head.tail);
      //write data at tail offset
      ret = cls_cxx_write(hctx, head.tail, data.bl_data.length(), &(data.bl_data));
      if (ret < 0)
        return ret;
    } else {
      CLS_LOG(1, "INFO: Wrapping around and checking for free space\n");
      //write 0 as data length to signify wrap around at tail offset
      bufferlist bl_size_zero;
      uint64_t size_zero = 0;
      encode(size_zero, bl_size_zero);
      CLS_LOG(1, "INFO: cls_enqueue: Writing data size 0 at offset: %lu\n", head.tail);
      ret = cls_cxx_write(hctx, head.tail, sizeof(uint64_t), &bl_size_zero);
      //wrap around only if there is space
      uint64_t temp = sizeof(cls_queue_head);
      if ((head.front != temp) && (temp + total_size) <= head.front) {
        head.tail = temp;

        CLS_LOG(1, "INFO: cls_enqueue: Writing data size: offset: %lu, size: %u\n", head.tail, bl_size.length());
        //write data size at tail offset
        ret = cls_cxx_write(hctx, head.tail, bl_size.length(), &bl_size);
        if (ret < 0)
          return ret;

        head.tail += bl_size.length();

        CLS_LOG(1, "INFO: cls_enqueue: Writing data at offset: %lu\n", head.tail);
        //write data at tail offset
        ret = cls_cxx_write(hctx, head.tail, data.bl_data.length(), &(data.bl_data));
        if (ret < 0)
          return ret;
      } else {
        CLS_LOG(1, "ERROR: No space left in queue\n");
        // return queue full error
        return -ENOSPC;
      }
    }
  } else if (head.front > head.tail) {
    if ((head.tail + total_size) < head.front) {
      CLS_LOG(1, "INFO: cls_enqueue: Writing data size: offset: %lu, size: %u\n\n", head.tail, bl_size.length());
      //write data size at tail offset
      ret = cls_cxx_write(hctx, head.tail, bl_size.length(), &bl_size);
      if (ret < 0)
        return ret;

      head.tail += bl_size.length();

      CLS_LOG(1, "INFO: cls_enqueue: Writing data at offset: %lu\n", head.tail);
      //write data at tail offset
      ret = cls_cxx_write(hctx, head.tail, data.bl_data.length(), &(data.bl_data));
      if (ret < 0)
        return ret;
    } else {
      CLS_LOG(1, "ERROR: No space left in queue\n");
      // return queue full error
      return -ENOSPC;
    }
  }

  bl_head.clear();
  head.tail += data.bl_data.length();
  if (head.tail == head.size) {
    head.tail = sizeof(cls_queue_head);
  }
  CLS_LOG(1, "INFO: cls_enqueue: New tail offset: %lu \n", head.tail);
  head.is_empty = false;
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_enqueue: Writing head of size: %u \n", bl_head.length());
  ret = cls_cxx_write2(hctx, 0, bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_enqueue: Writing head returned error: %d \n", ret);
    return ret;
  }

  return 0;
}

static int cls_dequeue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(1, "INFO: cls_dequeue: Reading head of size: %lu\n", sizeof(cls_queue_head));
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, sizeof(cls_queue_head), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_dequeue: failed to decode entry %s\n", bl_head.c_str());
    return -EINVAL;
  }

  if (head.front == head.tail && head.is_empty) {
    CLS_LOG(1, "ERROR: Queue is empty\n");
    return -ENOENT;
  }
  bufferlist bl_size;
  uint64_t data_size = 0;
  //Read size of data first
  ret = cls_cxx_read(hctx, head.front, sizeof(uint64_t), &bl_size);
  if (ret < 0) {
    return ret;
  }
  iter = bl_size.cbegin();
  try {
    decode(data_size, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_dequeue: failed to decode data size \n");
    return -EINVAL;
  }
  CLS_LOG(1, "INFO: cls_dequeue: Data size: %lu, front offset: %lu\n", data_size, head.front);
  if (head.front < head.tail) {
    //Read data based on size obtained above
    head.front += sizeof(uint64_t);
    CLS_LOG(1, "INFO: cls_dequeue: Data is read from from front offset %lu\n", head.front);
    ret = cls_cxx_read(hctx, head.front, data_size, out);
    if (ret < 0) {
      return ret;
    }
    head.front += data_size;
  } else if (head.front >= head.tail) {
    //If remaining space in queue wasn't used, size will be set to 0
    if (data_size == 0) {
      //Read size after wrap around
      head.front = sizeof(cls_queue_head);
      int ret = cls_cxx_read(hctx, head.front, sizeof(uint64_t), &bl_size);
      if (ret < 0) {
        return ret;
      }
      decode(data_size, bl_size);
      CLS_LOG(1, "INFO: cls_dequeue: Data size: %lu, front offset: %lu\n", data_size, head.front);
    }
    //Read data
    head.front += sizeof(uint64_t);
    CLS_LOG(1, "INFO: cls_dequeue: Data is read from from front offset %lu\n", head.front);
    ret = cls_cxx_read(hctx, head.front, data_size, out);
    if (ret < 0) {
      return ret;
    }
    head.front += data_size;
  }

  //front has reached the end, wrap it around
  if (head.front == head.size) {
    head.front = sizeof(cls_queue_head);
  }

  if (head.front == head.tail) {
    head.is_empty = true;
  }
  //Write head back
  bl_head.clear();
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_enqueue: Writing head of size: %u and front offset is: %lu\n", bl_head.length(), head.front);
  ret = cls_cxx_write2(hctx, 0, bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_enqueue: Writing head returned error: %d \n", ret);
    return ret;
  }
  
  return 0;
}

static int cls_queue_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(1, "INFO: cls_queue_list: Reading head at offset %lu\n", sizeof(cls_queue_head));
  uint64_t start_offset = 0;
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, sizeof(cls_queue_head), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_list: failed to decode entry %s\n", bl_head.c_str());
    return -EINVAL;
  }

  cls_queue_list_ret op_ret;
  if ((head.front == head.tail) && head.is_empty) {
    op_ret.is_truncated = false;
    encode(op_ret, *out);
    return 0;
  }

  auto in_iter = in->cbegin();

  cls_queue_list_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_list(): failed to decode input data\n");
    return -EINVAL;
  }
  if (op.start_offset == 0) {
    start_offset = head.front;
  } else {
    start_offset = op.start_offset;
  }

  CLS_LOG(1, "INFO: cls_queue_list(): front is: %lu, tail is %lu,  and start_offset is %lu\n", head.front, head.tail, start_offset);

  op_ret.is_truncated = true;
  for (uint64_t i = 0; i < op.max; i++) {
    // Read the size from the start offset
    bufferlist bl_size;
    uint64_t data_size = 0;
    ret = cls_cxx_read(hctx, start_offset, sizeof(uint64_t), &bl_size);
    if (ret < 0) {
      return ret;
    }
    iter = bl_size.cbegin();
    try {
      decode(data_size, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_queue_list: failed to decode data size \n");
      return -EINVAL;
    }

    // If size of data is zero
    if (data_size == 0) {
      //Read size after wrap around
      start_offset = sizeof(cls_queue_head);
      int ret = cls_cxx_read(hctx, start_offset, sizeof(uint64_t), &bl_size);
      if (ret < 0) {
        return ret;
      }
      iter = bl_size.cbegin();
      try {
        decode(data_size, iter);
      } catch (buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_queue_list: failed to decode data size \n");
        return -EINVAL;
      }
    }

    CLS_LOG(1, "INFO: cls_queue_list: Data size: %lu, start offset: %lu\n", data_size, start_offset);

    //Read data based on size obtained above
    start_offset += sizeof(uint64_t);
    bufferlist bl_data;
    CLS_LOG(1, "INFO: cls_queue_list: Data is read from from start offset %lu\n", start_offset);
    ret = cls_cxx_read(hctx, start_offset, data_size, &bl_data);
    if (ret < 0) {
      return ret;
    }

    op_ret.offsets.emplace_back(start_offset);
    op_ret.data.emplace_back(bl_data);

    // Increment the start offset
    start_offset += data_size;

    // Check if it is the end, then wrap around
    if (start_offset == head.size) {
      start_offset = sizeof(cls_queue_head);
    }
  
    if (start_offset == head.tail) {
      op_ret.is_truncated = false;
      break;
    }
  }
  op_ret.next_offset = start_offset;

  encode(op_ret, *out);

  return 0;
}

static int cls_queue_remove_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, sizeof(cls_queue_head), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode entry %s\n", bl_head.c_str());
    return -EINVAL;
  }

  // If queue is empty, return success
  if ((head.front == head.tail) && head.is_empty) {
    return 0;
  }

  auto in_iter = in->cbegin();

  cls_queue_remove_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode input data\n");
    return -EINVAL;
  }

  // If start offset is not set or set to zero, then we need to shift it to actual front of queue
  if (op.start_offset == 0) {
    op.start_offset = sizeof(cls_queue_head);
  }

  if (op.start_offset != head.front) {
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: invalid start offset\n");
    return -EINVAL;
  }

  for (uint64_t i = 0; i < op.num_entries; i++) {
    // Read the size from the start offset
    bufferlist bl_size;
    uint64_t data_size = 0;
    ret = cls_cxx_read(hctx, head.front, sizeof(uint64_t), &bl_size);
    if (ret < 0) {
      return ret;
    }
    iter = bl_size.cbegin();
    try {
      decode(data_size, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode data size \n");
      return -EINVAL;
    }

    // If size of data is zero
    if (data_size == 0) {
      //Read size after wrap around
      head.front = sizeof(cls_queue_head);
      int ret = cls_cxx_read(hctx, head.front, sizeof(uint64_t), &bl_size);
      if (ret < 0) {
        return ret;
      }
      iter = bl_size.cbegin();
      try {
        decode(data_size, iter);
      } catch (buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode data size \n");
        return -EINVAL;
      }
    }

    CLS_LOG(1, "INFO: cls_queue_remove_entries: Data size: %lu, start offset: %lu\n", data_size, head.front);

    //Increment the start offset by total data size
    head.front += (sizeof(uint64_t) + data_size);

    // Check if it is the end, then wrap around
    if (head.front == head.size) {
      head.front = sizeof(cls_queue_head);
    }
  
    // We've reached the last element
    if (head.front == head.tail) {
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Queue is empty now!\n");
      head.is_empty = true;
      break;
    }
  }

  //Write head back
  bl_head.clear();
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_queue_remove_entries: Writing head of size: %u and front offset is: %lu\n", bl_head.length(), head.front);
  ret = cls_cxx_write2(hctx, 0, bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_queue_remove_entries: Writing head returned error: %d \n", ret);
    return ret;
  }

  return 0;
}

static int cls_queue_update_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_queue_update_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_update_entry: failed to decode input data\n");
    return -EINVAL;
  }

  CLS_LOG(1, "INFO: cls_queue_update_entry: Updating data at offset: %lu\n", op.offset);
  //write data at offset
  int ret = cls_cxx_write(hctx, op.offset, op.bl_data.length(), &(op.bl_data));
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_gc_enqueue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_gc_set_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_set_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  op.info.time = ceph::real_clock::now();
  op.info.time += make_timespan(op.expiration_secs);

  cls_rgw_queue_data data;
  encode(op.info, data.bl_data);
  data.size_data = data.bl_data.length();

  CLS_LOG(1, "INFO: cls_gc_enqueue: Data size is: %lu \n", data.size_data);

  in->clear();
  encode(data, *in);

  return cls_enqueue(hctx, in, out);
}

static int cls_gc_dequeue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = cls_dequeue(hctx, in, out);
  if (r < 0)
    return r;

  cls_rgw_gc_obj_info data;
  auto iter = out->cbegin();
  try {
    decode(data, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_dequeue(): failed to decode entry\n");
    return -EINVAL;
  }

  CLS_LOG(1, "INFO: tag of gc info is %s\n", data.tag.c_str());

  return 0;
}

static int cls_gc_queue_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(1, "INFO: cls_gc_queue_list(): Entered cls_gc_queue_list \n");
  auto in_iter = in->cbegin();

  cls_rgw_gc_list_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode input\n");
    return -EINVAL;
  }

  cls_queue_list_op list_op;
  if (op.marker.empty()) {
    list_op.start_offset = 0;
  } else {
    list_op.start_offset = boost::lexical_cast<uint64_t>(op.marker.c_str());
  }

  if (op.max) {
    list_op.max = op.max;
  } else {
    list_op.max = GC_LIST_DEFAULT_MAX;
  }

  in->clear();
  encode(list_op, *in);

  CLS_LOG(1, "INFO: cls_gc_queue_list(): Entering cls_queue_list \n");
  int ret = cls_queue_list(hctx, in, out);
  if (ret < 0) {
    CLS_LOG(1, "ERROR: cls_queue_list(): returned error %d\n", ret);
    return ret;
  }

  cls_queue_list_ret op_ret;
  auto iter = out->cbegin();
  try {
    decode(op_ret, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode output\n");
    return -EINVAL;
  }

  cls_rgw_gc_list_ret list_ret;
  for (auto it : op_ret.data) {
    cls_rgw_gc_obj_info info;
    decode(info, it);
    CLS_LOG(1, "INFO: cls_gc_queue_list(): entry: %s\n", info.tag.c_str());
    if (op.expired_only) {
      real_time now = ceph::real_clock::now();
      if (info.time <= now) {
        list_ret.entries.emplace_back(info);
      }
    } else {
      list_ret.entries.emplace_back(info);
    }
  }
  list_ret.truncated = op_ret.is_truncated;
  
  if (list_ret.truncated) {
    list_ret.next_marker = boost::lexical_cast<string>(op_ret.next_offset);
  }

  out->clear();
  encode(list_ret, *out);

  return 0;
}

static int cls_gc_queue_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entered cls_gc_queue_list \n");
  auto in_iter = in->cbegin();

  cls_rgw_gc_queue_remove_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_remove(): failed to decode input\n");
    return -EINVAL;
  }

  cls_queue_remove_op rem_op;
  if (op.marker.empty()) {
    rem_op.start_offset = 0;
  } else {
    rem_op.start_offset = boost::lexical_cast<uint64_t>(op.marker.c_str());
  }

  if (op.num_entries) {
    rem_op.num_entries = op.num_entries;
  } else {
    rem_op.num_entries = GC_LIST_DEFAULT_MAX;
  }

  in->clear();
  encode(rem_op, *in);

  CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entering cls_queue_remove_entries \n");
  int ret = cls_queue_remove_entries(hctx, in, out);
  if (ret < 0) {
    CLS_LOG(1, "ERROR: cls_queue_remove_entries(): returned error %d\n", ret);
    return ret;
  }

  return 0;
}

static int cls_gc_queue_update_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int ret = 0;
  CLS_LOG(1, "INFO: cls_gc_queue_update_entry(): Entered cls_gc_queue_list \n");
  auto in_iter = in->cbegin();

  cls_rgw_gc_defer_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_update_entry(): failed to decode input\n");
    return -EINVAL;
  }

  cls_queue_list_op list_op;
  cls_queue_list_ret op_ret;
  list_op.start_offset = 0;
  list_op.max = 100;
  bool entry_found = false;
  uint64_t index = 0;
  cls_queue_update_entry_op update_op;
  cls_rgw_queue_data data;
  update_op.offset = 0;
  do {
    in->clear();
    encode(list_op, *in);

    CLS_LOG(1, "INFO: cls_gc_queue_list(): Entering cls_queue_list \n");
    ret = cls_queue_list(hctx, in, out);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_list(): returned error %d\n", ret);
      return ret;
    }
    
    auto iter = out->cbegin();
    try {
      decode(op_ret, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode output\n");
      return -EINVAL;
    }
    index = 0;
    for (auto it: op_ret.data) {
      cls_rgw_gc_obj_info info;
      decode(info, it);
      CLS_LOG(1, "INFO: cls_gc_queue_list(): entry: %s\n", info.tag.c_str());
      if (info.tag == op.tag) {
        entry_found = true;
        update_op.offset = op_ret.offsets[index];
        encode(info, update_op.bl_data);
        encode(info, data.bl_data);
        data.size_data = data.bl_data.length();
        break;
      }
      index++;
    }//end-for
    if (entry_found) {
        break;
    }
    if (op_ret.is_truncated) {
      list_op.start_offset = op_ret.next_offset;
    }
  } while(op_ret.is_truncated);

  if (entry_found) {
    //Update the entry
    in->clear();
    encode(update_op, *in);
    ret = cls_queue_update_entry(hctx, in, out);
    if (ret < 0)
      return ret;
  
    //Enqueque the entry
    in->clear();
    encode(data, *in);
    ret = cls_enqueue(hctx, in, out);
    if (ret < 0)
      return ret;
  }
  return 0;
}

CLS_INIT(queue)
{
  CLS_LOG(1, "Loaded queue class!");

  cls_handle_t h_class;
  cls_method_handle_t h_create_queue;
  cls_method_handle_t h_get_queue_size;
  cls_method_handle_t h_gc_enqueue;
  cls_method_handle_t h_gc_dequeue;
  cls_method_handle_t h_gc_queue_list;
  cls_method_handle_t h_gc_queue_remove;
  cls_method_handle_t h_gc_queue_update;

  cls_register(QUEUE_CLASS, &h_class);

  /* queue*/
  cls_register_cxx_method(h_class, CREATE_QUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_create_queue, &h_create_queue);
  cls_register_cxx_method(h_class, GET_QUEUE_SIZE, CLS_METHOD_RD | CLS_METHOD_WR, cls_get_queue_size, &h_get_queue_size);
  cls_register_cxx_method(h_class, GC_ENQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_enqueue, &h_gc_enqueue);
  cls_register_cxx_method(h_class, GC_DEQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_dequeue, &h_gc_dequeue);
  cls_register_cxx_method(h_class, GC_QUEUE_LIST, CLS_METHOD_RD, cls_gc_queue_list, &h_gc_queue_list);
  cls_register_cxx_method(h_class, GC_QUEUE_REMOVE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_remove, &h_gc_queue_remove);
  cls_register_cxx_method(h_class, GC_QUEUE_UPDATE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_update_entry, &h_gc_queue_update);

  return;
}

