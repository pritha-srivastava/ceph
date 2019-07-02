// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include <errno.h>

#include "objclass/objclass.h"
#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/queue/cls_queue.h"

static int get_queue_head_size(cls_method_context_t hctx, uint64_t& head_size)
{
  //read head size
  bufferlist bl_head_size;
  int ret = cls_cxx_read(hctx, 0, sizeof(uint64_t), &bl_head_size);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: get_queue_head_size: failed to read head\n");
    return ret;
  }
  //decode head size
  auto iter = bl_head_size.cbegin();
  try {
    decode(head_size, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: get_queue_head_size: failed to decode head size \n");
    return -EINVAL;
  }

  CLS_LOG(10, "INFO: get_queue_head_size: head size is %lu\n", head_size);

  return 0;
}

int cls_create_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
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
  
  CLS_LOG(10, "INFO: cls_create_queue create queue of head size %lu", op.head_size);
  CLS_LOG(10, "INFO: cls_create_queue create queue of size %lu", op.head.size);

  uint64_t head_size = QUEUE_HEAD_SIZE_1K;

  if (op.head.has_urgent_data) {
    op.head.has_urgent_data = true;
    if (op.head_size == 0) {
      head_size = QUEUE_HEAD_SIZE_4K;
      op.head.tail = op.head.front = QUEUE_START_OFFSET_4K;
      op.head.last_entry_offset = QUEUE_START_OFFSET_4K;
    } else {
      head_size = op.head_size;
      op.head.tail = op.head.front = head_size;
      op.head.last_entry_offset = head_size;
    }
  } else {
    head_size = QUEUE_HEAD_SIZE_1K;
    op.head.tail = op.head.front = QUEUE_START_OFFSET_1K;
    op.head.last_entry_offset = QUEUE_START_OFFSET_1K;
  }
  op.head.size += head_size;

  CLS_LOG(10, "INFO: cls_create_queue queue actual size %lu", op.head.size);
  CLS_LOG(10, "INFO: cls_create_queue head size %lu", head_size);
  CLS_LOG(10, "INFO: cls_create_queue queue front offset %lu", op.head.front);


  //encode head size
  bufferlist bl;
  encode(head_size, bl);
  CLS_LOG(0, "INFO: cls_create_queue head size %u", bl.length());

  //encode head
  bufferlist bl_head;
  encode(op.head, bl_head);

  bl.claim_append(bl_head);

  CLS_LOG(0, "INFO: cls_create_queue writing head of size %u", bl.length());
  ret = cls_cxx_write2(hctx, 0, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_write returned %d", __func__, ret);
    return ret;
  }
  return 0;
}

int cls_get_queue_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  // read the head
  cls_queue_head head;
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }

  try {
    decode(head, bl_head);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_get_queue_size: failed to decode entry\n");
    return -EINVAL;
  }

  head.size -= head_size;

  CLS_LOG(10, "INFO: cls_get_queue_size: size of queue is %lu\n", head.size);

  encode(head.size, *out);

  return 0;
}

int cls_enqueue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  CLS_LOG(1, "INFO: cls_enqueue: Read Head of size: %lu\n", head_size);
  // read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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

  for (auto bl_data : op.bl_data_vec) {
    uint64_t total_size = sizeof(uint64_t) + bl_data.length();
    CLS_LOG(1, "INFO: cls_enqueue(): Total size to be written is %lu and data size is %u\n", total_size, bl_data.length());

    bufferlist bl;
    uint64_t data_size = bl_data.length();
    encode(data_size, bl);
    CLS_LOG(1, "INFO: cls_enqueue(): bufferlist length after encoding is %u\n", bl.length());
    bl.claim_append(bl_data);

    if (head.tail >= head.front) {
      // check if data can fit in the remaining space in queue
      if ((head.tail + total_size) <= head.size) {
        CLS_LOG(1, "INFO: cls_enqueue: Writing data size and data: offset: %lu, size: %u\n", head.tail, bl.length());
        head.last_entry_offset = head.tail;
        //write data size and data at tail offset
        ret = cls_cxx_write2(hctx, head.tail, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
          return ret;
        }
        head.tail += total_size;
      } else {
        CLS_LOG(1, "INFO: Wrapping around and checking for free space\n");
        uint64_t free_space_available = (head.size - head.tail) + (head.front - head_size);
        //Split data if there is free space available
        if (total_size <= free_space_available) {
          uint64_t size_before_wrap = head.size - head.tail;
          bufferlist bl_data_before_wrap;
          bl.splice(0, size_before_wrap, &bl_data_before_wrap);
          head.last_entry_offset = head.tail;
          //write spliced (data size and data) at tail offset
          CLS_LOG(1, "INFO: cls_enqueue: Writing spliced data at offset: %lu and data size: %u\n", head.tail, bl_data_before_wrap.length());
          ret = cls_cxx_write2(hctx, head.tail, bl_data_before_wrap.length(), &bl_data_before_wrap, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
          if (ret < 0) {
            return ret;
          }
          head.tail = head_size;
          //write remaining data at tail offset
          CLS_LOG(1, "INFO: cls_enqueue: Writing remaining data at offset: %lu and data size: %u\n", head.tail, bl.length());
          ret = cls_cxx_write2(hctx, head.tail, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
          if (ret < 0) {
            return ret;
          }
          head.tail = bl.length();
        }
        else {
          CLS_LOG(1, "ERROR: No space left in queue\n");
          // return queue full error
          return -ENOSPC;
        }
      }
    } else if (head.front > head.tail) {
      if ((head.tail + total_size) < head.front) {
        CLS_LOG(1, "INFO: cls_enqueue: Writing data size and data: offset: %lu, size: %u\n\n", head.tail, bl.length());
        head.last_entry_offset = head.tail;
        //write data size and data at tail offset
        ret = cls_cxx_write2(hctx, head.tail, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
          return ret;
        }
        head.tail += total_size;
      } else {
        CLS_LOG(1, "ERROR: No space left in queue\n");
        // return queue full error
        return -ENOSPC;
      }
    }

    bl_head.clear();
    if (head.tail == head.size) {
      head.tail = head_size;
    }
    CLS_LOG(1, "INFO: cls_enqueue: New tail offset: %lu \n", head.tail);
  } //end - for

  head.is_empty = false;

  //Update urgent data if set
  if (op.bl_urgent_data.length() > 0) {
    head.bl_urgent_data = op.bl_urgent_data;
  }

  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_enqueue: Writing head of size: %u \n", bl_head.length());
  ret = cls_cxx_write2(hctx, sizeof(uint64_t), bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_enqueue: Writing head returned error: %d \n", ret);
    return ret;
  }

  return 0;
}

int cls_dequeue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  CLS_LOG(1, "INFO: cls_dequeue: Reading head of size: %lu\n", head_size);
  // read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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

  uint64_t data_size = 0;
  bufferlist bl_size;

  if (head.front < head.tail) {
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
    head.front += sizeof(uint64_t);
    //Read data based on size obtained above
    CLS_LOG(1, "INFO: cls_dequeue: Data is read from from front offset %lu\n", head.front);
    ret = cls_cxx_read2(hctx, head.front, data_size, out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
    if (ret < 0) {
      return ret;
    }
    head.front += data_size;
  } else if (head.front >= head.tail) {
    uint64_t actual_data_size = head.size - head.front;
    if (actual_data_size < sizeof(uint64_t)) {
      //Case 1. Data size has been spliced, first reconstruct data size
      CLS_LOG(1, "INFO: cls_dequeue: Spliced data size is read from from front offset %lu\n", head.front);
      ret = cls_cxx_read2(hctx, head.front, actual_data_size, &bl_size, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
      if (ret < 0) {
        return ret;
      }
      head.front = head_size;
      uint64_t remainder_data_size = sizeof(uint64_t) - actual_data_size;
      bufferlist bl_rem_data_size;
      CLS_LOG(1, "INFO: cls_dequeue: Remainder Spliced data size is read from from front offset %lu\n", head.front);
      ret = cls_cxx_read(hctx, head.front, remainder_data_size, &bl_rem_data_size);
      if (ret < 0) {
        return ret;
      }
      bl_size.claim_append(bl_rem_data_size);
      iter = bl_size.cbegin();
      try {
        decode(data_size, iter);
      } catch (buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_dequeue: failed to decode data size \n");
        return -EINVAL;
      }
      head.front += remainder_data_size;
      CLS_LOG(1, "INFO: cls_dequeue: Data is read from from front offset %lu\n", head.front);
      ret = cls_cxx_read2(hctx, head.front, data_size, out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
      if (ret < 0) {
        return ret;
      }
      head.front += data_size;
    } else {
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
      head.front += sizeof(uint64_t);

      actual_data_size = head.size - head.front;
      
      if (actual_data_size < data_size) {
        if (actual_data_size != 0) {
          //Case 2. Data has been spliced
          CLS_LOG(1, "INFO: cls_dequeue: Spliced data is read from from front offset %lu\n", head.front);
          ret = cls_cxx_read2(hctx, head.front, actual_data_size, out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
          if (ret < 0) {
            return ret;
          }
        }
        head.front = head_size;
        bufferlist bl_remainder;
        uint64_t remainder_size = data_size - actual_data_size;
        CLS_LOG(1, "INFO: cls_dequeue: Remaining Data is read from from front offset %lu\n", head.front);
        ret = cls_cxx_read2(hctx, head.front, remainder_size, &bl_remainder, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
          return ret;
        }
        out->claim_append(bl_remainder);
        head.front += remainder_size;
      } else {
        //Case 3. No splicing
        CLS_LOG(1, "INFO: cls_dequeue: Data is read from from front offset %lu\n", head.front);
        ret = cls_cxx_read2(hctx, head.front, data_size, out, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
          return ret;
        }
        head.front += data_size;
      }
    }
  }

  //front has reached the end, wrap it around
  if (head.front == head.size) {
    head.front = head_size;
  }

  if (head.front == head.tail) {
    head.is_empty = true;
  }
  //Write head back
  bl_head.clear();
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_enqueue: Writing head of size: %u and front offset is: %lu\n", bl_head.length(), head.front);
  ret = cls_cxx_write2(hctx, sizeof(uint64_t), bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_enqueue: Writing head returned error: %d \n", ret);
    return ret;
  }
  
  return 0;
}

int cls_queue_list_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  CLS_LOG(1, "INFO: cls_queue_list_entries: Reading head at offset %lu\n", head_size);
  uint64_t start_offset = 0;
  // read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_list_entries: failed to decode entry %s\n", bl_head.c_str());
    return -EINVAL;
  }

  // If queue is empty, return from here
  if (head.is_empty) {
    return -ENOENT;
  }

  cls_queue_list_ret op_ret;
  CLS_LOG(1, "INFO: cls_queue_list_entries: Is urgent data present: %d\n", head.has_urgent_data);
  //Info related to urgent data
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
    CLS_LOG(1, "ERROR: cls_queue_list_entries(): failed to decode input data\n");
    return -EINVAL;
  }
  if (op.start_offset == 0) {
    start_offset = head.front;
  } else {
    start_offset = op.start_offset;
  }

  op_ret.is_truncated = true;
  uint64_t chunk_size = 1024;
  uint64_t contiguous_data_size = 0, size_to_read = 0;
  bool wrap_around = false;

  //Calculate length of contiguous data to be read depending on front, tail and start offset
  if (head.tail > head.front) {
    contiguous_data_size = head.tail - start_offset;
  } else if (head.front >= head.tail) {
    if (start_offset >= head.front) {
      contiguous_data_size = head.size - start_offset;
      wrap_around = true;
    } else if (start_offset <= head.tail) {
      contiguous_data_size = head.tail - start_offset;
    }
  }

  uint64_t num_ops = 0;
  bufferlist bl;
  do
  {
    CLS_LOG(1, "INFO: cls_queue_list_entries(): front is: %lu, tail is %lu,  and start_offset is %lu\n", head.front, head.tail, start_offset);
  
    bufferlist bl_chunk;
    //Read chunk size at a time, if it is less than contiguous data size, else read contiguous data size
    if (contiguous_data_size > chunk_size) {
      size_to_read = chunk_size;
    } else {
      size_to_read = contiguous_data_size;
    }
    CLS_LOG(1, "INFO: cls_queue_list_entries(): size_to_read is %lu\n", size_to_read);
    if (size_to_read == 0) {
      op_ret.is_truncated = false;
      CLS_LOG(1, "INFO: cls_queue_list_entries(): size_to_read is 0, hence breaking out!\n");
      break;
    }

    ret = cls_cxx_read(hctx, start_offset, size_to_read, &bl_chunk);
    if (ret < 0) {
      return ret;
    }

    //If there is leftover data from previous iteration, append new data to leftover data
    bl.claim_append(bl_chunk);
    bl_chunk = bl;
    bl.clear();

    CLS_LOG(1, "INFO: cls_queue_list_entries(): size of chunk %u\n", bl_chunk.length());

    //Process the chunk of data read
    unsigned index = 0;
    auto it = bl_chunk.cbegin();
    uint64_t size_to_process = bl_chunk.length();
    do {
      CLS_LOG(1, "INFO: cls_queue_list_entries(): index: %u, size_to_process: %lu\n", index, size_to_process);
      it.seek(index);
      uint64_t data_size = 0;
      if (size_to_process >= sizeof(uint64_t)) {
        try {
          decode(data_size, it);
        } catch (buffer::error& err) {
          CLS_LOG(1, "ERROR: cls_queue_list_entries: failed to decode data size \n");
          return -EINVAL;
        }
      } else {
        // Copy unprocessed data to bl
        bl_chunk.copy(index, size_to_process, bl);
        CLS_LOG(1, "INFO: cls_queue_list_entries: not enough data to read data size, breaking out!\n");
        break;
      }
      CLS_LOG(1, "INFO: cls_queue_list_entries(): data size: %lu\n", data_size);
      index += sizeof(uint64_t);
      size_to_process -= sizeof(uint64_t);
      bufferlist bl_data;
      if (data_size <= size_to_process) {
        bl_chunk.copy(index, data_size, bl_data);
        //Return data and offset here
        op_ret.data.emplace_back(bl_data);
        uint64_t data_offset = start_offset + (index - sizeof(uint64_t));
        op_ret.offsets.emplace_back(data_offset);
        CLS_LOG(1, "INFO: cls_queue_list_entries(): offset: %lu\n", data_offset);
        index += bl_data.length();
        size_to_process -= bl_data.length();
      } else {
        index -= sizeof(uint64_t);
        size_to_process += sizeof(uint64_t);
        bl_chunk.copy(index, size_to_process, bl);
        CLS_LOG(1, "INFO: cls_queue_list_entries(): not enough data to read data, breaking out!\n");
        break;
      }
      num_ops++;
      if (num_ops == op.max) {
        CLS_LOG(1, "INFO: cls_queue_list_entries(): num_ops is same as op.max, hence breaking out from inner loop!\n");
        break;
      }
      if (index == bl_chunk.length()) {
        break;
      }
    } while(index < bl_chunk.length());

    CLS_LOG(1, "INFO: num_ops: %lu and op.max is %lu\n", num_ops, op.max);

    if (num_ops == op.max) {
      op_ret.next_offset = start_offset + index;
      CLS_LOG(1, "INFO: cls_queue_list_entries(): num_ops is same as op.max, hence breaking out from outer loop with next offset: %lu\n", op_ret.next_offset);
      break;
    }

    //Calculate new start_offset and contiguous data size
    start_offset += size_to_read;
    contiguous_data_size -= size_to_read;
    if (contiguous_data_size == 0) {
      if (wrap_around) {
        start_offset = head_size;
        contiguous_data_size = head.tail - head_size;
        wrap_around = false;
      } else {
        CLS_LOG(1, "INFO: cls_queue_list_entries(): end of queue data is reached, hence breaking out from outer loop!\n");
        op_ret.next_offset = head.front;
        op_ret.is_truncated = false;
        break;
      }
    }
    
  } while(num_ops < op.max);

  //Wrap around next offset if it has reached end of queue
  if (op_ret.next_offset == head.size) {
    op_ret.next_offset = head_size;
  }
  if (op_ret.next_offset == head.tail) {
    op_ret.is_truncated = false;
  }

  encode(op_ret, *out);

  return 0;
}

int cls_queue_remove_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  // read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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

  if ((head.front == head.tail) && head.is_empty) {
    return -ENOENT;
  }

  auto in_iter = in->cbegin();

  cls_queue_remove_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode input data\n");
    return -EINVAL;
  }

  if (op.start_offset == op.end_offset) {
    return -EINVAL;
  }

  // If start offset is not set or set to zero, then we need to shift it to actual front of queue
  if (op.start_offset == 0) {
    op.start_offset = head_size;
  }

  if (op.start_offset != head.front) {
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: invalid start offset\n");
    return -EINVAL;
  }

  // Read the size from the end offset
  bufferlist bl_size;
  uint64_t data_size = 0;
  ret = cls_cxx_read(hctx, op.end_offset, sizeof(uint64_t), &bl_size);
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

  //offset obtained by adding last entry's size
  uint64_t end_offset = op.end_offset + sizeof(uint64_t) + data_size;

  //Zero out the entries that have been removed, to reclaim storage space
  if (end_offset > op.start_offset) {
    ret = cls_cxx_write_zero(hctx, op.start_offset, (end_offset - op.start_offset));
    if (ret < 0) {
      return ret;
    }
  } else { //start offset > end offset
    ret = cls_cxx_write_zero(hctx, op.start_offset, (head.size - op.start_offset));
    if (ret < 0) {
      return ret;
    }
    ret = cls_cxx_write_zero(hctx, head_size, (end_offset - head_size));
    if (ret < 0) {
      return ret;
    }
  }

  head.front = end_offset;

  // Check if it is the end, then wrap around
  if (head.front == head.size) {
    head.front = head_size;
  }

  CLS_LOG(1, "INFO: cls_queue_remove_entries: front offset is: %lu and tail offset is %lu\n", head.front, head.tail);

  // We've reached the last element
  if (head.front == head.tail) {
    CLS_LOG(1, "INFO: cls_queue_remove_entries: Queue is empty now!\n");
    head.is_empty = true;
  }

  //Update urgent data map
  if (op.bl_urgent_data.length() > 0) {
    head.bl_urgent_data = op.bl_urgent_data;
  }

  //Write head back
  bl_head.clear();
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_queue_remove_entries: Writing head of size: %u and front offset is: %lu\n", bl_head.length(), head.front);
  ret = cls_cxx_write2(hctx, sizeof(uint64_t), bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_queue_remove_entries: Writing head returned error: %d \n", ret);
    return ret;
  }

  return 0;
}

int cls_queue_get_last_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  // read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    return ret;
  }
  cls_queue_head head;
  auto iter = bl_head.cbegin();
  try {
    decode(head, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_get_last_entry: failed to decode entry %s\n", bl_head.c_str());
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

int cls_queue_update_last_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  // read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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

  bufferlist bl;
  uint64_t data_size = op.bl_data.length();
  encode(data_size, bl);
  bl.claim_append(op.bl_data);

  CLS_LOG(1, "INFO: cls_queue_update_last_entry_op: Updating data at last offset: %lu and total data size is %u\n", head.last_entry_offset, bl.length());

  //write data size + data at offset
  ret = cls_cxx_write(hctx, head.last_entry_offset, bl.length(), &bl);
  if (ret < 0) {
    return ret;
  }

  if (op.bl_urgent_data.length() > 0) {
    head.bl_urgent_data = op.bl_urgent_data;
  }

  bl_head.clear();
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_queue_update_last_entry: Writing head of size: %u \n", bl_head.length());
  ret = cls_cxx_write2(hctx, sizeof(uint64_t), bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_queue_update_last_entry: Writing head returned error: %d \n", ret);
    return ret;
  }
  return 0;
}

int cls_queue_read_urgent_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  // read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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
  
  op_ret.bl_urgent_data = head.bl_urgent_data;
  
  encode(op_ret, *out);

  return 0;
}

int cls_queue_write_urgent_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  // read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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
  head.bl_urgent_data = op.bl_urgent_data;

  //Write head back
  bl_head.clear();
  encode(head, bl_head);
  CLS_LOG(1, "INFO: cls_queue_write_urgent_data: Writing head of size: %u\n", bl_head.length());
  ret = cls_cxx_write2(hctx, sizeof(uint64_t), bl_head.length(), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (ret < 0) {
    CLS_LOG(1, "INFO: cls_queue_write_urgent_data: Writing head returned error: %d \n", ret);
    return ret;
  }

  return 0;
}

int cls_queue_can_urgent_data_fit(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bool can_fit = true;

  //get head size
  uint64_t head_size = 0;
  int ret = get_queue_head_size(hctx, head_size);
  if (ret < 0) {
    return ret;
  }

  // read the head
  bufferlist bl_head;
  ret = cls_cxx_read2(hctx, sizeof(uint64_t), (head_size - sizeof(uint64_t)), &bl_head, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
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

  head.bl_urgent_data = *in;

  bl_head.clear();
  encode(head, bl_head);

  if(bl_head.length() > head_size) {
    can_fit = false;
  }

  encode(can_fit, *out);

  return 0;
}

