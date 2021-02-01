// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file
 *
 * This is an OSD class that implements methods for management
 * and use of fifo
 *
 */

#include <cerrno>
#include <optional>
#include <string>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/buffer.h"
#include "include/types.h"

#include "objclass/objclass.h"

#include "cls/fifo/cls_fifo.h"

namespace rados::cls::fifo {

int read_part_header(cls_method_context_t hctx,
		     part_header* part_header)
{
  ceph::buffer::list bl;
  int r = cls_cxx_read2(hctx, 0, CLS_FIFO_MAX_PART_HEADER_SIZE, &bl,
			CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
  if (r < 0) {
    CLS_ERR("ERROR: %s: cls_cxx_read2() on obj returned %d", __PRETTY_FUNCTION__, r);
    return r;
  }

  auto iter = bl.cbegin();
  try {
    decode(*part_header, iter);
  } catch (const ceph::buffer::error& err) {
    CLS_ERR("ERROR: %s: failed decoding part header", __PRETTY_FUNCTION__);
    return -EIO;
  }

  using ceph::operator <<;
  std::ostringstream ss;
  ss << part_header->max_time;
  CLS_LOG(5, "%s:%d read part_header:\n"
	  "\ttag=%s\n"
	  "\tmagic=0x%" PRIx64 "\n"
	  "\tmin_ofs=%" PRId64 "\n"
	  "\tlast_ofs=%" PRId64 "\n"
	  "\tnext_ofs=%" PRId64 "\n"
	  "\tmin_index=%" PRId64 "\n"
	  "\tmax_index=%" PRId64 "\n"
	  "\tmax_time=%s\n",
	  __PRETTY_FUNCTION__, __LINE__,
	  part_header->tag.c_str(),
	  part_header->magic,
	  part_header->min_ofs,
	  part_header->last_ofs,
	  part_header->next_ofs,
	  part_header->min_index,
	  part_header->max_index,
	  ss.str().c_str());

  return 0;
}

bool full_part(const part_header& part_header)
{
  return (part_header.next_ofs > part_header.params.full_size_threshold);
}

int list_part(cls_method_context_t hctx, op::list_part& op, op::list_part_reply& reply)
{
  part_header part_header;
  int r = read_part_header(hctx, &part_header);
  if (r < 0) {
    CLS_ERR("%s: failed to read part header", __PRETTY_FUNCTION__);
    return r;
  }

  if (op.tag &&
      !(part_header.tag == *op.tag)) {
    CLS_ERR("%s: bad tag", __PRETTY_FUNCTION__);
    return -EINVAL;
  }

  EntryReader reader(hctx, part_header, op.ofs);

  if (op.ofs >= part_header.min_ofs &&
      !reader.end()) {
    r = reader.get_next_entry(nullptr, nullptr, nullptr);
    if (r < 0) {
      CLS_ERR("ERROR: %s: unexpected failure at get_next_entry: r=%d", __PRETTY_FUNCTION__, r);
      return r;
    }
  }

  reply.tag = part_header.tag;

  auto max_entries = std::min(op.max_entries, op::MAX_LIST_ENTRIES);

  for (int i = 0; i < max_entries && !reader.end(); ++i) {
    ceph::buffer::list data;
    ceph::real_time mtime;
    std::uint64_t ofs;

    r = reader.get_next_entry(&data, &ofs, &mtime);
    if (r < 0) {
      CLS_ERR("ERROR: %s: unexpected failure at get_next_entry: r=%d",
	      __PRETTY_FUNCTION__, r);
      return r;
    }

    reply.entries.emplace_back(std::move(data), ofs, mtime);
  }

  reply.more = !reader.end();
  reply.full_part = full_part(part_header);

  return 0;
}

int EntryReader::fetch(std::uint64_t num_bytes)
{
  CLS_LOG(5, "%s: fetch %d bytes, ofs=%d data.length()=%d", __PRETTY_FUNCTION__, (int)num_bytes, (int)ofs, (int)data.length());
  if (data.length() < num_bytes) {
    ceph::buffer::list bl;
    CLS_LOG(5, "%s: reading % " PRId64 " bytes at ofs=%" PRId64, __PRETTY_FUNCTION__,
	    prefetch_len, ofs + data.length());
    int r = cls_cxx_read2(hctx, ofs + data.length(), prefetch_len, &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (r < 0) {
      CLS_ERR("ERROR: %s: cls_cxx_read2() on obj returned %d", __PRETTY_FUNCTION__, r);
      return r;
    }
    data.claim_append(bl);
  }

  if (static_cast<unsigned>(num_bytes) > data.length()) {
    CLS_ERR("%s: requested %" PRId64 " bytes, but only "
	    "%u were available", __PRETTY_FUNCTION__, num_bytes, data.length());
    return -ERANGE;
  }

  return 0;
}

int EntryReader::read(std::uint64_t num_bytes, ceph::buffer::list* pbl)
{
  int r = fetch(num_bytes);
  if (r < 0) {
    return r;
  }
  data.splice(0, num_bytes, pbl);

  ofs += num_bytes;

  return 0;
}

int EntryReader::peek(std::uint64_t num_bytes, char* dest)
{
  int r = fetch(num_bytes);
  if (r < 0) {
    return r;
  }

  data.begin().copy(num_bytes, dest);

  return 0;
}

int EntryReader::seek(std::uint64_t num_bytes)
{
  ceph::buffer::list bl;

  CLS_LOG(5, "%s:%d: num_bytes=%" PRIu64, __PRETTY_FUNCTION__, __LINE__, num_bytes);
  return read(num_bytes, &bl);
}

int EntryReader::peek_pre_header(entry_header_pre* pre_header)
{
  if (end()) {
    return -ENOENT;
  }

  int r = peek(sizeof(*pre_header),
	       reinterpret_cast<char*>(pre_header));
  if (r < 0) {
    CLS_ERR("ERROR: %s: peek() size=%zu failed: r=%d", __PRETTY_FUNCTION__,
	    sizeof(pre_header), r);
    return r;
  }

  if (pre_header->magic != part_header.magic) {
    CLS_ERR("ERROR: %s: unexpected pre_header magic", __PRETTY_FUNCTION__);
    return -ERANGE;
  }

  return 0;
}


int EntryReader::get_next_entry(ceph::buffer::list* pbl,
                                std::uint64_t* pofs,
                                ceph::real_time* pmtime)
{
  entry_header_pre pre_header;
  int r = peek_pre_header(&pre_header);
  if (r < 0) {
    CLS_ERR("ERROR: %s: peek_pre_header() failed: r=%d", __PRETTY_FUNCTION__, r);
    return r;
  }

  if (pofs) {
    *pofs = ofs;
  }

  CLS_LOG(5, "%s:%d: pre_header.pre_size=%" PRIu64, __PRETTY_FUNCTION__, __LINE__,
	  uint64_t(pre_header.pre_size));
  r = seek(pre_header.pre_size);
  if (r < 0) {
    CLS_ERR("ERROR: %s: failed to seek: r=%d", __PRETTY_FUNCTION__, r);
    return r;
  }

  ceph::buffer::list header;
  CLS_LOG(5, "%s:%d: pre_header.header_size=%d", __PRETTY_FUNCTION__, __LINE__, (int)pre_header.header_size);
  r = read(pre_header.header_size, &header);
  if (r < 0) {
    CLS_ERR("ERROR: %s: failed to read entry header: r=%d", __PRETTY_FUNCTION__, r);
    return r;
  }

  entry_header entry_header;
  auto iter = header.cbegin();
  try {
    decode(entry_header, iter);
  } catch (ceph::buffer::error& err) {
    CLS_ERR("%s: failed decoding entry header", __PRETTY_FUNCTION__);
    return -EIO;
  }

  if (pmtime) {
    *pmtime = entry_header.mtime;
  }

  if (pbl) {
    r = read(pre_header.data_size, pbl);
    if (r < 0) {
      CLS_ERR("%s: failed reading data: r=%d", __PRETTY_FUNCTION__, r);
      return r;
    }
  } else {
    r = seek(pre_header.data_size);
    if (r < 0) {
      CLS_ERR("ERROR: %s: failed to seek: r=%d", __PRETTY_FUNCTION__, r);
      return r;
    }
  }

  return 0;
}

} // namespace rados::cls::fifo