// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include <errno.h>

#include "objclass/objclass.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/queue/cls_queue.h"

#include <boost/lexical_cast.hpp>
#include <unordered_map>

#include "common/ceph_context.h"
#include "global/global_context.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define GC_LIST_DEFAULT_MAX 128

CLS_VER(1,0)
CLS_NAME(rgw_queue)

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

  CLS_LOG(10, "INFO: cls_gc_create_queue: queue size is %lu\n", op.size);
  create_op.head.size = op.size;
  create_op.head.num_urgent_data_entries = op.num_urgent_data_entries;
  create_op.head_size = g_ceph_context->_conf->rgw_gc_queue_head_size;

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
  bufferlist bl_data;
  encode(op.info, bl_data);
  enqueue_op.bl_data_vec.emplace_back(bl_data);
  enqueue_op.has_urgent_data = false;

  CLS_LOG(1, "INFO: cls_gc_enqueue: Data size is: %u \n", bl_data.length());

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

    CLS_LOG(1, "INFO: cls_gc_queue_list(): Entering cls_queue_list_entries \n");
    int ret = cls_queue_list_entries(hctx, in, out);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_list_entries(): returned error %d\n", ret);
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
        try {
          decode(info, it);
        } catch (buffer::error& err) {
          CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode gc info\n");
          return -EINVAL;
        }
        //Check for info tag in urgent data map
        if (urgent_data_map.size() > 0) {
          auto found = urgent_data_map.find(info.tag);
          if (found != urgent_data_map.end()) {
            if (found->second > info.time) {
              CLS_LOG(1, "INFO: cls_gc_queue_list(): tag found in urgent data: %s\n", info.tag.c_str());
              continue;
            }
          }
        } else {
          //Search in xattrs
          bufferlist bl_xattrs;
          int ret = cls_cxx_getxattr(hctx, "cls_queue_urgent_data", &bl_xattrs);
          if (ret < 0 && (ret != -ENOENT && ret != -ENODATA)) {
            CLS_LOG(0, "ERROR: %s(): cls_cxx_getxattrs() returned %d", __func__, ret);
            return ret;
          }
          if (ret != -ENOENT && ret != -ENODATA) {
            std::unordered_map<string,ceph::real_time> xattr_urgent_data_map;
            auto iter = bl_xattrs.cbegin();
            try {
              decode(xattr_urgent_data_map, iter);
            } catch (buffer::error& err) {
              CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode xattrs urgent data map\n");
              return -EINVAL;
            } //end - catch
            if (xattr_urgent_data_map.size() > 0) {
              auto found = xattr_urgent_data_map.find(info.tag);
              if (found != xattr_urgent_data_map.end()) {
                if (found->second > info.time) {
                  CLS_LOG(1, "INFO: cls_gc_queue_list(): tag found in xattrs urgent data map: %s\n", info.tag.c_str());
                  continue;
                }
              }
            } // end - if xattrs size ...
          } // end - ret != ENOENT && ENODATA
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
      CLS_LOG(1, "INFO: cls_gc_queue_list(): num_entries: %u and op.max: %u\n", num_entries, op.max);
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
  bool is_truncated = true;
  uint32_t total_num_entries = 0, num_entries = 0;
  std::unordered_map<string,ceph::real_time> urgent_data_map;
  bool urgent_data_decoded = false;
  uint64_t end_offset = 0;
  do {
    in->clear();
    encode(list_op, *in);

    CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entering cls_queue_list_entries \n");
    int ret = cls_queue_list_entries(hctx, in, out);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_gc_queue_remove(): returned error %d\n", ret);
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
    is_truncated = op_ret.is_truncated;
    unsigned int index = 0;
    if (op_ret.has_urgent_data && ! urgent_data_decoded) {
      auto iter_urgent_data = op_ret.bl_urgent_data.cbegin();
      try {
        decode(urgent_data_map, iter_urgent_data);
      } catch (buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode urgent data map\n");
        return -EINVAL;
      }
      urgent_data_decoded = true;
    }
    // If data is not empty
    if (op_ret.data.size()) {
      for (auto it : op_ret.data) {
        cls_rgw_gc_obj_info info;
        try {
          decode(info, it);
        } catch (buffer::error& err) {
          CLS_LOG(1, "ERROR: cls_gc_queue_remove(): failed to decode gc info\n");
          return -EINVAL;
        }
        CLS_LOG(1, "INFO: cls_gc_queue_remove(): entry: %s\n", info.tag.c_str());
        total_num_entries++;
        index++;
        //Search for tag in urgent data map
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
        else {
          //Search in xattrs
          bufferlist bl_xattrs;
          int ret = cls_cxx_getxattr(hctx, "cls_queue_urgent_data", &bl_xattrs);
          if (ret < 0 && (ret != -ENOENT && ret != -ENODATA)) {
            CLS_LOG(0, "ERROR: %s(): cls_cxx_getxattrs() returned %d", __func__, ret);
            return ret;
          }
          if (ret != -ENOENT && ret != -ENODATA) {
            std::unordered_map<string,ceph::real_time> xattr_urgent_data_map;
            auto iter = bl_xattrs.cbegin();
            try {
              decode(xattr_urgent_data_map, iter);
            } catch (buffer::error& err) {
              CLS_LOG(1, "ERROR: cls_gc_queue_remove(): failed to decode xattrs urgent data map\n");
              return -EINVAL;
            } //end - catch
            if (xattr_urgent_data_map.size() > 0) {
              auto found = xattr_urgent_data_map.find(info.tag);
              if (found != xattr_urgent_data_map.end()) {
                if (found->second > info.time) {
                  CLS_LOG(1, "INFO: cls_gc_queue_remove(): tag found in xattrs urgent data map: %s\n", info.tag.c_str());
                  continue;
                } else if (found->second == info.time) {
                  CLS_LOG(1, "INFO: cls_gc_queue_remove(): erasing tag from xattrs urgent data: %s\n", info.tag.c_str());
                  xattr_urgent_data_map.erase(info.tag); //erase entry from map, as it will be removed later
                }
              }
            } // end - if xattrs size ...
            if (xattr_urgent_data_map.size() == 0) {
              //remove from xattrs ???
            }
          } // end - ret != ENOENT && ENODATA
        }// search in xattrs
        num_entries++;
      }//end-for
      
      if (num_entries < op.num_entries) {
        list_op.max = (op.num_entries - num_entries);
        list_op.start_offset = op_ret.next_offset;
        out->clear();
      } else {
        end_offset = op_ret.offsets[index - 1];
        CLS_LOG(1, "INFO: cls_gc_queue_remove(): index is %u and end_offset is: %lu\n", index, end_offset);
        break;
      }
    } //end-if
    else {
      break;
    }
  } while(is_truncated);
  CLS_LOG(1, "INFO: cls_gc_queue_remove(): Total number of entries to remove: %d\n", total_num_entries);

  cls_queue_remove_op rem_op;
  if (op.marker.empty()) {
    rem_op.start_offset = 0;
  } else {
    rem_op.start_offset = boost::lexical_cast<uint64_t>(op.marker.c_str());
  }

  rem_op.end_offset = end_offset;
  CLS_LOG(1, "INFO: cls_gc_queue_remove(): start offset: %lu and end offset: %lu\n", rem_op.start_offset, rem_op.end_offset);
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

  bool has_urgent_data = false;
  auto it = urgent_data_map.find(op.info.tag);
  if (it != urgent_data_map.end()) {
    it->second = op.info.time;
  } else {
    urgent_data_map.insert({op.info.tag, op.info.time});
    has_urgent_data = true;
  }

  out->clear();
  bool can_fit = false;
  bufferlist bl_urgent_data;
  encode(urgent_data_map, bl_urgent_data);
  ret = cls_queue_can_urgent_data_fit(hctx, &bl_urgent_data, out);
  if (ret < 0) {
     return ret;
  }
  iter = out->cbegin();
  decode(can_fit, iter);
  CLS_LOG(1, "INFO: Can urgent data fit: %d \n", can_fit);

  if (can_fit) {
    in->clear();
    if (! is_last_entry) {
      cls_enqueue_op enqueue_op;
      bufferlist bl_data;
      encode(op.info, bl_data);
      enqueue_op.bl_data_vec.emplace_back(bl_data);
      CLS_LOG(1, "INFO: cls_gc_update_entry: Data size is: %u \n", bl_data.length());
      enqueue_op.bl_urgent_data = bl_urgent_data;
      enqueue_op.has_urgent_data = has_urgent_data;
      encode(enqueue_op, *in);
      ret = cls_enqueue(hctx, in, out);
      if (ret < 0) {
        return ret;
      }
    } else {
      cls_queue_update_last_entry_op update_op;
      encode(op.info, update_op.bl_data);
      CLS_LOG(1, "INFO: cls_gc_update_entry: Data size is: %u \n", update_op.bl_data.length());
      update_op.bl_urgent_data = bl_urgent_data;
      update_op.has_urgent_data = has_urgent_data;
      encode(update_op, *in);
      ret = cls_queue_update_last_entry(hctx, in, out);
      if (ret < 0) {
        return ret;
      }
    }
  }
  // Else write urgent data as xattrs
  else {
    std::unordered_map<string,ceph::real_time> xattr_urgent_data_map;
    xattr_urgent_data_map.insert({op.info.tag, op.info.time});
    bufferlist bl_map;
    encode(xattr_urgent_data_map, bl_map);
    ret = cls_cxx_setxattr(hctx, "cls_queue_urgent_data", &bl_map);
    CLS_LOG(20, "%s(): setting attr: %s", __func__, "cls_queue_urgent_data");
    if (ret < 0) {
      CLS_LOG(0, "ERROR: %s(): cls_cxx_setxattr (attr=%s) returned %d", __func__, "cls_queue_urgent_data", ret);
      return ret;
    }
  }
  return 0;
}

CLS_INIT(rgw_queue)
{
  CLS_LOG(1, "Loaded rgw queue class!");

  cls_handle_t h_class;
  cls_method_handle_t h_gc_create_queue;
  cls_method_handle_t h_gc_enqueue;
  cls_method_handle_t h_gc_dequeue;
  cls_method_handle_t h_gc_queue_list_entries;
  cls_method_handle_t h_gc_queue_remove_entries;
  cls_method_handle_t h_gc_queue_update_entry;

  cls_register(RGW_QUEUE_CLASS, &h_class);

  /* gc */
  cls_register_cxx_method(h_class, GC_CREATE_QUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_create_queue, &h_gc_create_queue);
  cls_register_cxx_method(h_class, GC_ENQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_enqueue, &h_gc_enqueue);
  cls_register_cxx_method(h_class, GC_DEQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_dequeue, &h_gc_dequeue);
  cls_register_cxx_method(h_class, GC_QUEUE_LIST_ENTRIES, CLS_METHOD_RD, cls_gc_queue_list, &h_gc_queue_list_entries);
  cls_register_cxx_method(h_class, GC_QUEUE_REMOVE_ENTRIES, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_remove, &h_gc_queue_remove_entries);
  cls_register_cxx_method(h_class, GC_QUEUE_UPDATE_ENTRY, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_update_entry, &h_gc_queue_update_entry);

  return;
}

