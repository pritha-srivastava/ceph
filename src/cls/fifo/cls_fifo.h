#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "objclass/objclass.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/types.h"

#include "cls/fifo/cls_fifo_ops.h"

namespace rados::cls::fifo {
  static constexpr auto CLS_FIFO_MAX_PART_HEADER_SIZE = 512;

  struct entry_header_pre {
    ceph_le64 magic;
    ceph_le64 pre_size;
    ceph_le64 header_size;
    ceph_le64 data_size;
    ceph_le64 index;
    ceph_le32 reserved;
  } __attribute__ ((packed));

  struct entry_header {
    ceph::real_time mtime;

    void encode(ceph::buffer::list& bl) const {
      ENCODE_START(1, 1, bl);
      encode(mtime, bl);
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(mtime, bl);
      DECODE_FINISH(bl);
    }
  };
  WRITE_CLASS_ENCODER(entry_header)

  class EntryReader {
  static constexpr std::uint64_t prefetch_len = (128 * 1024);

  cls_method_context_t hctx;

  const rados::cls::fifo::part_header& part_header;

  std::uint64_t ofs;
  ceph::buffer::list data;

  int fetch(std::uint64_t num_bytes);
  int read(std::uint64_t num_bytes, ceph::buffer::list* pbl);
  int peek(std::uint64_t num_bytes, char *dest);
  int seek(std::uint64_t num_bytes);

  public:
    EntryReader(cls_method_context_t hctx,
                const rados::cls::fifo::part_header& part_header,
                uint64_t ofs) : hctx(hctx),
              part_header(part_header),
              ofs(ofs < part_header.min_ofs ?
            part_header.min_ofs :
            ofs) {}

    std::uint64_t get_ofs() const {
      return ofs;
    }

    bool end() const {
      return (ofs >= part_header.next_ofs);
    }

    int peek_pre_header(entry_header_pre* pre_header);
    int get_next_entry(ceph::buffer::list* pbl,
                      std::uint64_t* pofs,
                      ceph::real_time* pmtime);
  };

  bool full_part(const rados::cls::fifo::part_header& part_header);
  int read_part_header(cls_method_context_t hctx,
        rados::cls::fifo::part_header* part_header);
  int list_part(cls_method_context_t hctx, op::list_part& op, op::list_part_reply& reply);
}