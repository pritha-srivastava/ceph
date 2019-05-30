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
#include <unordered_map>

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
  CLS_LOG(1, "INFO: cls_create_queue: Is urgent data present: %d\n", op.head.has_urgent_data);

  // write the head
  bufferlist bl_head;
  encode(op.head, bl_head);
  CLS_LOG(0, "INFO: cls_create_queue writing head of size %u", bl_head.length());
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
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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
  CLS_LOG(1, "INFO: cls_enqueue: Read Head of size: %d\n", QUEUE_HEAD_SIZE);
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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
  cls_enqueue_op op;
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_enqueue: failed to decode input data \n");
    return -EINVAL;
  }

  uint64_t total_size = sizeof(op.data.size_data) + op.data.bl_data.length();
  CLS_LOG(1, "INFO: cls_enqueue(): Total size to be written is %lu and data size is %lu\n", total_size, op.data.size_data);

  bufferlist bl_size;
  encode(op.data.size_data, bl_size);

  if (head.tail >= head.front) {
    // check if data can fit in the remaining space in queue
    if ((head.tail + total_size) <= head.size) {
      CLS_LOG(1, "INFO: cls_enqueue: Writing data size: offset: %lu, size: %u\n", head.tail, bl_size.length());
      head.last_entry_offset = head.tail;
      //write data size at tail offset
      ret = cls_cxx_write(hctx, head.tail, bl_size.length(), &bl_size);
      if (ret < 0)
        return ret;

      head.tail += bl_size.length();

      CLS_LOG(1, "INFO: cls_enqueue: Writing data at offset: %lu\n", head.tail);
      //write data at tail offset
      ret = cls_cxx_write2(hctx, head.tail, op.data.bl_data.length(), &(op.data.bl_data), CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
      if (ret < 0)
        return ret;
    } else {
      CLS_LOG(1, "INFO: Wrapping around and checking for free space\n");
      //write 0 as data length to signify wrap around at tail offset
      bufferlist bl_size_zero;
      uint64_t size_zero = 0;
      encode(size_zero, bl_size_zero);
      CLS_LOG(1, "INFO: cls_enqueue: Writing data size 0 at offset: %lu\n", head.tail);
      ret = cls_cxx_write2(hctx, head.tail, sizeof(uint64_t), &bl_size_zero, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
      //wrap around only if there is space
      uint64_t temp = QUEUE_START_OFFSET;
      if ((head.front != temp) && (temp + total_size) <= head.front) {
        head.tail = temp;

        CLS_LOG(1, "INFO: cls_enqueue: Writing data size: offset: %lu, size: %u\n", head.tail, bl_size.length());
        head.last_entry_offset = head.tail;
        //write data size at tail offset
        ret = cls_cxx_write(hctx, head.tail, bl_size.length(), &bl_size);
        if (ret < 0)
          return ret;

        head.tail += bl_size.length();

        CLS_LOG(1, "INFO: cls_enqueue: Writing data at offset: %lu\n", head.tail);
        //write data at tail offset
        ret = cls_cxx_write2(hctx, head.tail, op.data.bl_data.length(), &(op.data.bl_data), CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
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
      head.last_entry_offset = head.tail;
      //write data size at tail offset
      ret = cls_cxx_write(hctx, head.tail, bl_size.length(), &bl_size);
      if (ret < 0)
        return ret;

      head.tail += bl_size.length();

      CLS_LOG(1, "INFO: cls_enqueue: Writing data at offset: %lu\n", head.tail);
      //write data at tail offset
      ret = cls_cxx_write2(hctx, head.tail, op.data.bl_data.length(), &(op.data.bl_data), CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
      if (ret < 0)
        return ret;
    } else {
      CLS_LOG(1, "ERROR: No space left in queue\n");
      // return queue full error
      return -ENOSPC;
    }
  }

  bl_head.clear();
  head.tail += op.data.bl_data.length();
  if (head.tail == head.size) {
    head.tail = QUEUE_START_OFFSET;
  }
  CLS_LOG(1, "INFO: cls_enqueue: New tail offset: %lu \n", head.tail);

  head.is_empty = false;

  //Update urgent data if set
  if (op.has_urgent_data) {
    head.has_urgent_data = true;
    head.bl_urgent_data = op.bl_urgent_data;
  }

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
  CLS_LOG(1, "INFO: cls_dequeue: Reading head of size: %d\n", QUEUE_HEAD_SIZE);
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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
    ret = cls_cxx_read2(hctx, head.front, data_size, out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
    if (ret < 0) {
      return ret;
    }
    head.front += data_size;
  } else if (head.front >= head.tail) {
    //If remaining space in queue wasn't used, size will be set to 0
    if (data_size == 0) {
      //Read size after wrap around
      head.front = QUEUE_START_OFFSET;
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
    ret = cls_cxx_read2(hctx, head.front, data_size, out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
    if (ret < 0) {
      return ret;
    }
    head.front += data_size;
  }

  //front has reached the end, wrap it around
  if (head.front == head.size) {
    head.front = QUEUE_START_OFFSET;
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
  CLS_LOG(1, "INFO: cls_queue_list: Reading head at offset %d\n", QUEUE_HEAD_SIZE);
  uint64_t start_offset = 0;
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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
  CLS_LOG(1, "INFO: cls_queue_list: Is urgent data present: %d\n", head.has_urgent_data);
  //Info related to urgent data
  op_ret.has_urgent_data = head.has_urgent_data;
  op_ret.bl_urgent_data = head.bl_urgent_data;
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
      start_offset = QUEUE_START_OFFSET;
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
    ret = cls_cxx_read2(hctx, start_offset, data_size, &bl_data, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
    if (ret < 0) {
      return ret;
    }

    op_ret.data.emplace_back(bl_data);

    // Increment the start offset
    start_offset += data_size;

    // Check if it is the end, then wrap around
    if (start_offset == head.size) {
      start_offset = QUEUE_START_OFFSET;
    }
  
    if (start_offset == head.tail) {
      CLS_LOG(1, "INFO: Setting truncated to false, reached end of queue");
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
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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
    op.start_offset = QUEUE_START_OFFSET;
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
      head.front = QUEUE_START_OFFSET;
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
      head.front = QUEUE_START_OFFSET;
    }
  
    CLS_LOG(1, "INFO: cls_queue_remove_entries: head offset is: %lu and tail offset is %lu\n", head.front, head.tail);

    // We've reached the last element
    if (head.front == head.tail) {
      CLS_LOG(1, "INFO: cls_queue_remove_entries: Queue is empty now!\n");
      head.is_empty = true;
      break;
    }
  }

  //Update urgent data map
  head.bl_urgent_data = op.bl_urgent_data;
  head.has_urgent_data = op.has_urgent_data;

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

static int cls_queue_get_last_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_update_entry: failed to decode entry %s\n", bl_head.c_str());
    return -EINVAL;
  }

  uint64_t data_size = 0, last_entry_offset = head.last_entry_offset;
  bufferlist bl_size;
  //Read size of data first
  ret = cls_cxx_read(hctx, last_entry_offset, sizeof(uint64_t), &bl_size);
  if (ret < 0) {
    return ret;
  }
  iter = bl_size.cbegin();
  try {
    decode(data_size, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_get_last_entry: failed to decode data size \n");
    return -EINVAL;
  }
  CLS_LOG(1, "INFO: cls_queue_get_last_entry: Data size: %lu, last data offset: %lu\n", data_size, last_entry_offset);
  
  //Read data based on size obtained above
  last_entry_offset += sizeof(uint64_t);
  CLS_LOG(1, "INFO: cls_dequeue: Data is read from from last entry offset %lu\n", last_entry_offset);
  ret = cls_cxx_read2(hctx, last_entry_offset, data_size, out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

static int cls_queue_update_last_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_update_last_entry: failed to decode entry %s\n", bl_head.c_str());
    return -EINVAL;
  }

  auto in_iter = in->cbegin();

  cls_queue_update_last_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_update_last_entry: failed to decode input data\n");
    return -EINVAL;
  }

  uint64_t last_entry_offset = head.last_entry_offset;
  CLS_LOG(1, "INFO: cls_queue_update_last_entry_op: Updating data at last offset: %lu\n", last_entry_offset);
  bufferlist bl_size;
  encode(op.data.size_data, bl_size);

  //write data size at offset
  ret = cls_cxx_write(hctx, last_entry_offset, bl_size.length(), &bl_size);
  if (ret < 0) {
    return ret;
  }
  last_entry_offset += sizeof(uint64_t);
  //write data at offset
  ret = cls_cxx_write(hctx, last_entry_offset, op.data.bl_data.length(), &(op.data.bl_data));
  if (ret < 0) {
    return ret;
  }

  if (op.has_urgent_data) {
    head.has_urgent_data = true;
    head.bl_urgent_data = op.bl_urgent_data;
  }

  bl_head.clear();
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_queue_update_last_entry: Writing head of size: %u \n", bl_head.length());
  ret = cls_cxx_write2(hctx, 0, bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_queue_update_last_entry: Writing head returned error: %d \n", ret);
    return ret;
  }
  return 0;
}

static int cls_queue_read_urgent_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_read_urgent_data: failed to decode entry %s\n", bl_head.c_str());
    return -EINVAL;
  }

  CLS_LOG(1, "INFO: cls_queue_read_urgent_data: tail offset %lu\n", head.tail);

  cls_queue_urgent_data_ret op_ret;
  if(head.has_urgent_data) {
    op_ret.has_urgent_data = true;
    op_ret.bl_urgent_data = head.bl_urgent_data;
  }

  encode(op_ret, *out);

  return 0;
}

static int cls_queue_write_urgent_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_write_urgent_data: failed to decode entry %s\n", bl_head.c_str());
    return -EINVAL;
  }

  CLS_LOG(1, "INFO: cls_queue_write_urgent_data: tail offset %lu\n", head.tail);

  auto in_iter = in->cbegin();

  cls_queue_write_urgent_data_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_write_urgent_data: failed to decode input data\n");
    return -EINVAL;
  }
  //Write urgent data
  head.has_urgent_data = op.has_urgent_data;
  head.bl_urgent_data = op.bl_urgent_data;

  //Write head back
  bl_head.clear();
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_queue_write_urgent_data: Writing head of size: %u\n", bl_head.length());
  ret = cls_cxx_write2(hctx, 0, bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_queue_write_urgent_data: Writing head returned error: %d \n", ret);
    return ret;
  }

  return 0;
}

static int cls_queue_can_urgent_data_fit(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bool can_fit = true;

  // read the head
  bufferlist bl_head;
  int ret = cls_cxx_read2(hctx, 0, QUEUE_HEAD_SIZE, &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_write_urgent_data: failed to decode entry %s\n", bl_head.c_str());
    return -EINVAL;
  }

  head.has_urgent_data = true;
  head.bl_urgent_data = *in;

  bl_head.clear();
  encode(head, bl_head);

  if(bl_head.length() > QUEUE_HEAD_SIZE) {
    can_fit = true;
  }

  encode(can_fit, *out);

  return 0;
}

static int cls_gc_create_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_gc_create_queue_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_create_queue: failed to decode entry\n");
    return -EINVAL;
  }

  cls_create_queue_op create_op;

  if (op.num_urgent_data_entries > 0) {
    std::unordered_map<string,ceph::real_time> urgent_data_map;
    urgent_data_map.reserve(op.num_urgent_data_entries);
    encode(urgent_data_map, create_op.head.bl_urgent_data);
  }

  create_op.head.size = op.size;

  in->clear();
  encode(create_op, *in);

  return cls_create_queue(hctx, in, out);
}

static int cls_gc_enqueue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_gc_set_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_enqueue: failed to decode entry\n");
    return -EINVAL;
  }

  op.info.time = ceph::real_clock::now();
  op.info.time += make_timespan(op.expiration_secs);

  cls_enqueue_op enqueue_op;
  encode(op.info, enqueue_op.data.bl_data);
  enqueue_op.data.size_data = enqueue_op.data.bl_data.length();
  enqueue_op.has_urgent_data = false;

  CLS_LOG(1, "INFO: cls_gc_enqueue: Data size is: %lu \n", enqueue_op.data.size_data);

  in->clear();
  encode(enqueue_op, *in);

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

  if (! op.max) {
    op.max = GC_LIST_DEFAULT_MAX;
  }
  
  list_op.max = op.max;

  cls_queue_list_ret op_ret;
  cls_rgw_gc_list_ret list_ret;
  uint32_t num_entries = 0;
  bool urgent_data_decoded = false;
  std::unordered_map<string,ceph::real_time> urgent_data_map;
  do {
    in->clear();
    encode(list_op, *in);

    CLS_LOG(1, "INFO: cls_gc_queue_list(): Entering cls_queue_list \n");
    int ret = cls_queue_list(hctx, in, out);
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

    if (op_ret.has_urgent_data && ! urgent_data_decoded) {
        auto iter_urgent_data = op_ret.bl_urgent_data.cbegin();
        decode(urgent_data_map, iter_urgent_data);
        urgent_data_decoded = true;
    }
    
    if (op_ret.data.size()) {
      for (auto it : op_ret.data) {
        cls_rgw_gc_obj_info info;
        decode(info, it);
        //Check for info tag in urgent data map
        if (urgent_data_map.size() > 0) {
          auto found = urgent_data_map.find(info.tag);
          if (found != urgent_data_map.end() && found->second > info.time) {
            CLS_LOG(1, "INFO: cls_gc_queue_list(): tag found in urgent data: %s\n", info.tag.c_str());
            continue;
          }
        }
        if (op.expired_only) {
          real_time now = ceph::real_clock::now();
          if (info.time <= now) {
            list_ret.entries.emplace_back(info);
          }
        } else {
          list_ret.entries.emplace_back(info);
        }
        num_entries++;
      }
      CLS_LOG(1, "INFO: cls_gc_queue_list(): num_entries: %lu and op.max: %lu\n", num_entries, op.max);
      if (num_entries < op.max) {
        list_op.max = (op.max - num_entries);
        list_op.start_offset = op_ret.next_offset;
        out->clear();
      } else {
        break;
      }
    } else {
      break;
    }
  } while(op_ret.is_truncated);

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
  CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entered cls_gc_queue_remove \n");

  auto in_iter = in->cbegin();

  cls_rgw_gc_queue_remove_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_remove(): failed to decode input\n");
    return -EINVAL;
  }

  // List entries and calculate total number of entries (including invalid entries)
  cls_queue_list_op list_op;
  if (op.marker.empty()) {
    list_op.start_offset = 0;
  } else {
    list_op.start_offset = boost::lexical_cast<uint64_t>(op.marker.c_str());
  }

  if (! op.num_entries) {
    op.num_entries = GC_LIST_DEFAULT_MAX;
  }
  
  list_op.max = op.num_entries;
 
  cls_queue_list_ret op_ret;
  uint32_t total_num_entries = 0, num_entries = 0;
  std::unordered_map<string,ceph::real_time> urgent_data_map;
  bool urgent_data_decoded = false;
  do {
    in->clear();
    encode(list_op, *in);

    CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entering cls_queue_list \n");
    int ret = cls_queue_list(hctx, in, out);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_gc_queue_remove(): returned error %d\n", ret);
      return ret;
    }

    auto iter = out->cbegin();
    try {
      decode(op_ret, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode output\n");
      return -EINVAL;
    }

    if (op_ret.has_urgent_data && ! urgent_data_decoded) {
      auto iter_urgent_data = op_ret.bl_urgent_data.cbegin();
      decode(urgent_data_map, iter_urgent_data);
      urgent_data_decoded = true;
    }
    // If data is not empty
    if (op_ret.data.size()) {
      for (auto it : op_ret.data) {
        cls_rgw_gc_obj_info info;
        decode(info, it);
        CLS_LOG(1, "INFO: cls_gc_queue_remove(): entry: %s\n", info.tag.c_str());
        total_num_entries++;
        if (urgent_data_map.size() > 0) {
          auto found = urgent_data_map.find(info.tag);
          if (found != urgent_data_map.end()) {
            if (found->second > info.time) {
              CLS_LOG(1, "INFO: cls_gc_queue_remove(): tag found in urgent data: %s\n", info.tag.c_str());
              continue;
            } else if (found->second == info.time) {
              CLS_LOG(1, "INFO: cls_gc_queue_remove(): erasing tag from urgent data: %s\n", info.tag.c_str());
              urgent_data_map.erase(info.tag); //erase entry from map, as it will be removed later
            }
          }//end-if map end
        }//end-if urgent data
          num_entries++;
      }//end-for
      if (num_entries < op.num_entries) {
        list_op.max = (op.num_entries - num_entries);
        list_op.start_offset = op_ret.next_offset;
        out->clear();
      } else {
        break;
      }
    } //end-if
    else {
      break;
    }
  } while(op_ret.is_truncated);
  CLS_LOG(1, "INFO: cls_gc_queue_remove(): Total number of entries to remove: %d\n", total_num_entries);

  cls_queue_remove_op rem_op;
  if (op.marker.empty()) {
    rem_op.start_offset = 0;
  } else {
    rem_op.start_offset = boost::lexical_cast<uint64_t>(op.marker.c_str());
  }

  rem_op.num_entries = total_num_entries;
  if(urgent_data_map.size() == 0) {
    rem_op.has_urgent_data = false;
  }
  encode(urgent_data_map, rem_op.bl_urgent_data);

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
  auto in_iter = in->cbegin();

  cls_gc_defer_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_update_entry(): failed to decode input\n");
    return -EINVAL;
  }

  op.info.time = ceph::real_clock::now();
  op.info.time += make_timespan(op.expiration_secs);

  //Read urgent data
  in->clear();
  out->clear();
  
  ret = cls_queue_read_urgent_data(hctx, in, out);
  
  auto out_iter = out->cbegin();

  cls_queue_urgent_data_ret op_ret;
  try {
    decode(op_ret, out_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_urgent_data_ret(): failed to decode ouput\n");
    return -EINVAL;
  }

  auto bl_iter = op_ret.bl_urgent_data.cbegin();
  std::unordered_map<string,ceph::real_time> urgent_data_map;
  if (op_ret.has_urgent_data) {
    try {
      decode(urgent_data_map, bl_iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_queue_urgent_data_ret(): failed to decode urgent data map\n");
      return -EINVAL;
    }
  }

  bool is_last_entry = false;
  in->clear();
  out->clear();
  ret = cls_queue_get_last_entry(hctx, in, out);
  if (ret < 0) {
    return ret;
  }

  cls_rgw_gc_obj_info info;
  auto iter = out->cbegin();
  try {
    decode(info, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_update_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  CLS_LOG(1, "INFO: tag of gc info is %s\n", info.tag.c_str());
  if (info.tag == op.info.tag) {
    is_last_entry = true;
  }

  cls_enqueue_op enqueue_op;
  encode(op.info, enqueue_op.data.bl_data);
  enqueue_op.data.size_data = enqueue_op.data.bl_data.length();
  CLS_LOG(1, "INFO: cls_gc_update_entry: Data size is: %lu \n", enqueue_op.data.size_data);

  auto it = urgent_data_map.find(op.info.tag);
  if (it != urgent_data_map.end()) {
    it->second = op.info.time;
  } else {
    urgent_data_map.insert({op.info.tag, op.info.time});
    enqueue_op.has_urgent_data = true;
  }

  encode(urgent_data_map, enqueue_op.bl_urgent_data);

  out->clear();
  bool can_fit = false;
  ret = cls_queue_can_urgent_data_fit(hctx, &(enqueue_op.bl_urgent_data), out);
  if (ret < 0) {
     return ret;
  }
  iter = out->cbegin();
  decode(can_fit, iter);
  CLS_LOG(1, "INFO: Can urgent data fit: %d \n", can_fit);

  if (can_fit) {
    in->clear();
    encode(enqueue_op, *in);
    if (! is_last_entry) {
      ret = cls_enqueue(hctx, in, out);
      if (ret < 0) {
        return ret;
      }
    } else {
      ret = cls_queue_update_last_entry(hctx, in, out);
      if (ret < 0) {
        return ret;
      }
    }
  }
  // Else write urgent data to omap as xattrs
  return 0;
}

CLS_INIT(queue)
{
  CLS_LOG(1, "Loaded queue class!");

  cls_handle_t h_class;
  cls_method_handle_t h_gc_create_queue;
  cls_method_handle_t h_get_queue_size;
  cls_method_handle_t h_gc_enqueue;
  cls_method_handle_t h_gc_dequeue;
  cls_method_handle_t h_gc_queue_list;
  cls_method_handle_t h_gc_queue_remove;
  cls_method_handle_t h_gc_queue_update;

  cls_register(QUEUE_CLASS, &h_class);

  /* queue*/
  cls_register_cxx_method(h_class, GC_CREATE_QUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_create_queue, &h_gc_create_queue);
  cls_register_cxx_method(h_class, GET_QUEUE_SIZE, CLS_METHOD_RD | CLS_METHOD_WR, cls_get_queue_size, &h_get_queue_size);
  cls_register_cxx_method(h_class, GC_ENQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_enqueue, &h_gc_enqueue);
  cls_register_cxx_method(h_class, GC_DEQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_dequeue, &h_gc_dequeue);
  cls_register_cxx_method(h_class, GC_QUEUE_LIST, CLS_METHOD_RD, cls_gc_queue_list, &h_gc_queue_list);
  cls_register_cxx_method(h_class, GC_QUEUE_REMOVE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_remove, &h_gc_queue_remove);
  cls_register_cxx_method(h_class, GC_QUEUE_UPDATE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_update_entry, &h_gc_queue_update);

  return;
}

