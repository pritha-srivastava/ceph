// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/queue/cls_queue_types.h"
#include "common/ceph_json.h"
#include "include/utime.h"

void cls_queue_head::dump(Formatter *f) const
{
  f->dump_bool("is_empty", is_empty);
  f->dump_unsigned("front", front);
  f->dump_unsigned("tail", tail);
  f->dump_unsigned("size", size);
  f->dump_unsigned("has_urgent_data", front);
  f->dump_unsigned("bl_urgent_data", size);
}

void cls_queue_head::generate_test_instances(list<cls_queue_head*>& ls)
{
  ls.push_back(new cls_queue_head);
  ls.push_back(new cls_queue_head);
}

void cls_rgw_queue_data::dump(Formatter *f) const
{
  f->dump_unsigned("size_data", size_data);
}

void cls_rgw_queue_data::generate_test_instances(list<cls_rgw_queue_data*>& ls)
{
  ls.push_back(new cls_rgw_queue_data);
  ls.push_back(new cls_rgw_queue_data);
}