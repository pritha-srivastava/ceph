#ifndef CEPH_CLS_QUEUE_CLIENT_H
#define CEPH_CLS_QUEUE_CLIENT_H

#include "include/str_list.h"
#include "include/rados/librados.hpp"
#include "cls/queue/cls_queue_types.h"
#include "cls_queue_ops.h"
#include "common/RefCountedObj.h"
#include "include/compat.h"
#include "common/ceph_time.h"
#include "common/Mutex.h"
#include "common/Cond.h"

void cls_init_queue(librados::ObjectWriteOperation& op, string& queue_name, uint64_t& size);
int cls_get_queue_size(librados::IoCtx& io_ctx, string& oid, uint64_t& size);
void cls_gc_enqueue(librados::ObjectWriteOperation& op, uint32_t expiration_secs, vector<bufferlist> bl_data_vec);
int cls_list_queue(librados::IoCtx& io_ctx, string& oid, string& marker, uint32_t max,
                    vector<bufferlist>& entries, vector<string>& entry_markers,
                    bool *truncated, string& next_marker);
void cls_remove_entries_queue(librados::ObjectWriteOperation& op, string& end_marker);

#endif