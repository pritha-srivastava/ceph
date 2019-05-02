#ifndef CEPH_CLS_QUEUE_OPS_H
#define CEPH_CLS_QUEUE_OPS_H

#include "cls/queue/cls_queue_types.h"

struct cls_create_queue_op {
  cls_queue_head head;

  cls_create_queue_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(head, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(head, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cls_create_queue_op*>& ls);
};
WRITE_CLASS_ENCODER(cls_create_queue_op)

struct cls_enqueue_op {
  bufferlist bl;

  cls_enqueue_op() {}

  static void generate_test_instances(list<cls_enqueue_op*>& ls);
};

struct cls_dequeue_op {
  bufferlist bl;

  cls_dequeue_op() {}

  static void generate_test_instances(list<cls_dequeue_op*>& ls);
};

struct cls_queue_list_op {
  uint64_t max;
  uint64_t start_offset;

  cls_queue_list_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max, bl);
    encode(start_offset, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max, bl);
    decode(start_offset, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_list_op)

struct cls_queue_list_ret {
  bool is_truncated;
  uint64_t next_offset;
  vector<uint64_t> offsets;
  vector<bufferlist> data;

  cls_queue_list_ret() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(is_truncated, bl);
    encode(next_offset, bl);
    encode(offsets, bl);
    encode(data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(is_truncated, bl);
    decode(next_offset, bl);
    decode(offsets, bl);
    decode(data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_list_ret)

struct cls_queue_remove_op {
  uint64_t num_entries;
  uint64_t start_offset;

  cls_queue_remove_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(num_entries, bl);
    encode(start_offset, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(num_entries, bl);
    decode(start_offset, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_remove_op)

struct cls_queue_update_entry_op {
  uint64_t offset;
  bufferlist bl_data;

  cls_queue_update_entry_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(offset, bl);
    encode(bl_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(offset, bl);
    decode(bl_data, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_queue_update_entry_op)

struct cls_rgw_gc_queue_remove_op {
  uint64_t num_entries;
  string marker;

  cls_rgw_gc_queue_remove_op() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(num_entries, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(num_entries, bl);
    decode(marker, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_queue_remove_op)

#endif /* CEPH_CLS_QUEUE_OPS_H */