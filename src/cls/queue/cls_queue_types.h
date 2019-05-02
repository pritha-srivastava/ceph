#ifndef CEPH_CLS_QUEUE_TYPES_H
#define CEPH_CLS_QUEUE_TYPES_H

#include "common/ceph_time.h"
#include "common/Formatter.h"

#include "rgw/rgw_basic_types.h"

struct cls_queue_head
{
  uint64_t front{0};
  uint64_t tail{0};
  uint64_t size{0};
  bool is_empty{true};

  cls_queue_head() {
    front += sizeof(cls_queue_head);
    tail = front;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(front, bl);
    encode(tail, bl);
    encode(size, bl);
    encode(is_empty, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(front, bl);
    decode(tail, bl);
    decode(size, bl);
    decode(is_empty, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_queue_head*>& o);
};
WRITE_CLASS_ENCODER(cls_queue_head)

struct cls_rgw_queue_data
{
  uint64_t size_data{0};
  bufferlist bl_data;

  cls_rgw_queue_data() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(size_data, bl);
    encode(bl_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(size_data, bl);
    decode(bl_data, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_rgw_queue_data*>& o);
};
WRITE_CLASS_ENCODER(cls_rgw_queue_data)


#endif