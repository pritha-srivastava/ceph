#ifndef CEPH_CLS_QUEUE_TYPES_H
#define CEPH_CLS_QUEUE_TYPES_H

#include "common/ceph_time.h"
#include "common/Formatter.h"

#include "rgw/rgw_basic_types.h"

#define QUEUE_HEAD_SIZE 1024
//Actual start offset of queue data
#define QUEUE_START_OFFSET QUEUE_HEAD_SIZE

struct cls_queue_head
{
  uint64_t front = QUEUE_START_OFFSET;
  uint64_t tail = QUEUE_START_OFFSET;
  uint64_t size{0};
  uint64_t last_entry_offset = QUEUE_START_OFFSET;
  bool is_empty{true};
  bool has_urgent_data{false};
  bufferlist bl_urgent_data;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(front, bl);
    encode(tail, bl);
    encode(size, bl);
    encode(last_entry_offset, bl);
    encode(is_empty, bl);
    encode(has_urgent_data, bl);
    encode(bl_urgent_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(front, bl);
    decode(tail, bl);
    decode(size, bl);
    decode(last_entry_offset, bl);
    decode(is_empty, bl);
    decode(has_urgent_data, bl);
    decode(bl_urgent_data, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_queue_head*>& o);
};
WRITE_CLASS_ENCODER(cls_queue_head)

struct cls_rgw_queue_data
{
  bufferlist bl_data;

  cls_rgw_queue_data() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bl_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(bl_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_queue_data)


#endif