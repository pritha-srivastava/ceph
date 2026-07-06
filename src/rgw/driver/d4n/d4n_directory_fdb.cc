#include <algorithm>
#include <type_traits>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory_fdb.h"
#include <iomanip>
#include <sstream>

namespace rgw::d4n {

using fdb_conn = lfdb::database;

using std::map;
using std::string;

static std::string encode_score(double score)
{
  std::ostringstream ss;
  ss << std::setw(20) << std::setfill('0') << std::fixed << std::setprecision(6) << score;
  return ss.str();
}

int FDBDirectory::get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val)
{
  std::map<std::string, std::string> kvs;
  if (!lfdb::get(FDBconn, key, kvs)) {
    ldpp_dout(dpp, 0) << "FDBDirectory::" << __func__
                      << "() ERROR: get returned false" << dendl;
    return -EIO;
  }
  auto it = kvs.find(field);
  if (it != kvs.end()) {
    out_val = it->second;
  }
  return 0;
}

int FDBDirectory::set_kv(const DoutPrefixProvider* dpp, optional_yield y,
                    const std::string& key,
                    const std::string& field,
                    const std::string& val)
{
  lfdb::set(FDBconn, key, std::map<std::string, std::string>{{field, val}});
  return 0;
}

int FDBDirectory::get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                      const std::string& key,
                      const std::vector<std::string>& fields,
                      std::map<std::string, std::string>& out_vals)
{
  std::map<std::string, std::string> kvs;
  if (!lfdb::get(FDBconn, key, kvs)) {
    ldpp_dout(dpp, 0) << "FDBDirectory::" << __func__
                      << "() ERROR: get returned false" << dendl;
    return -EIO;
  }
  for (const auto& field : fields) {
    auto it = kvs.find(field);
    if (it != kvs.end()) {
      out_vals[field] = it->second;
    }
  }
  return 0;
}

int FDBDirectory::set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                        const std::string& key,
                        const std::map<std::string, std::string>& vals)
{
  lfdb::set(FDBconn, key, vals);
  return 0;
}

int FDBDirectory::set_kv_multi_init_field(const DoutPrefixProvider* dpp, optional_yield y,
                                    const std::string& key,
                                    const std::map<std::string, std::string>& always_set,
                                    const std::string& init_field,
                                    const std::string& init_val)
{
  std::map<std::string, std::string> existing;
  if (!lfdb::get(FDBconn, key, existing)) {
    ldpp_dout(dpp, 0) << "FDBDirectory::" << __func__
                      << "() ERROR: get returned false" << dendl;
  }
  std::map<std::string, std::string> to_write(always_set);
  if (existing.find(init_field) == existing.end()) {
    to_write[init_field] = init_val;
  }
  lfdb::set(FDBconn, key, to_write);
  return 0;
}

int FDBBucketDirectory::exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, optional_yield y) 
{
  std::string key = bucket_id;
  return lfdb::key_exists(lfdb::make_transaction(FDBconn), key, lfdb::commit_after_op::commit);
}

//FIXME: this is a dummy function and should be updated.
int FDBBucketDirectory::del(const DoutPrefixProvider* dpp, const std::string& bucket_id, optional_yield y)
{
  return 0;
}

int FDBBucketDirectory::add_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, optional_yield y, Pipeline* pipeline)
{
  return fdb_add(dpp, bucket_id, 0, object_name, params, y);
}

int FDBBucketDirectory::remove_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, optional_yield y)
{
  return fdb_rem(dpp, bucket_id, object_name, y);
}

int FDBBucketDirectory::list_objects(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, optional_yield y)
{
  //TODO - check if the two paths can be combined into one
  if (!prefix.empty()) {
    // SCAN_OBJECTS path (with prefix)
    std::string continuation_token;

    auto ret = scan_objects(
      dpp,
      bucket_id,
      start_token,
      prefix,
      marker,
      marker_inclusive,
      count,
      objs_info,
      continuation_token,
      y);

    if (ret < 0 ) {
      ldpp_dout(dpp, 0) << "FDBBucketDirectory::" << __func__ << " scan_objects failed: " << ret << dendl;
      return ret;
    }
  } else {
    // GET_RANGE path (no prefix)
    std::string continuation_token;
    auto ret = get_range(
      dpp,
      bucket_id,
      marker,
      count,
      marker_inclusive,
      objs_info,
      continuation_token,
      y);

    if (ret < 0) {
      ldpp_dout(dpp, 0) << "FDBBucketDirectory::" << __func__ << " get_range failed: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

int FDBBucketDirectory::scan_objects(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, optional_yield y)
{
  return fdb_scan(dpp, bucket_id, marker, prefix, count, marker_inclusive, objs_info, continuation_token, y);
}

int FDBBucketDirectory::get_range(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, uint64_t count, bool start_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, optional_yield y)
{
  return fdb_range(dpp, bucket_id, start, count, objs_info, continuation_token, start_inclusive, y);
}

template<SeqContainer Container>
int FDBBucketDirectory::set_values(const DoutPrefixProvider* dpp, CacheObject& obj_info, Container& fdbValues, optional_yield y)
{
  auto add_value = [&](const std::string& key, const auto& value) {
    using ValueType = typename Container::value_type;

    std::string str_value;

    if constexpr (std::is_convertible_v<decltype(value), std::string>) {
      str_value = value;
    } else {
      str_value = std::to_string(value);
    }

    if constexpr (requires(Container c, ValueType v) {
                  c.push_back(v);
                }) {
      fdbValues.push_back(ValueType{key, str_value});
    } else {
      fdbValues.insert(ValueType{key, str_value});
    }
  };

  add_value("objName", obj_info.objName);
  add_value("bucketId", obj_info.bucketId);
  add_value("etag", obj_info.etag);
  add_value("size", obj_info.size);
  add_value("creationTime", obj_info.creationTime);

  return 0;
}

int FDBBucketDirectory::fdb_add(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                double score,
                                const std::string& member,
                                std::optional<CacheObject> params,
                                optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " :member " << member << dendl;
    std::string encoded_score = encode_score(score);
    std::string member_key = bucket_id + "/member/" + member;

    std::string existing;
    bool found =  lfdb::get(tr, member_key, existing);

    if (found) {
      lfdb::erase(tr, bucket_id + "/ordered/" + existing + "/" + member);
    }

    if (params) {
      std::map<std::string, std::string> values;
      set_values(dpp, *params, values, y);
      lfdb::set(tr, bucket_id + "/ordered/" + encoded_score + "/" + member, values);
    } else {
      lfdb::set(tr, bucket_id + "/ordered/" + encoded_score + "/" + member, "1");
    }
    lfdb::set(tr, member_key, encoded_score);

    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " :member_key " << member_key << dendl;
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " " << bucket_id + "/ordered/" + encoded_score + "/" + member << dendl;
    lfdb::commit(tr);

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBBucketDirectory::fdb_rem(const DoutPrefixProvider* dpp,
                                   const std::string& bucket_id,
                                   const std::string& member,
                                   optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string member_key = bucket_id + "/member/" + member;
    std::string existing_key;
    bool found = lfdb::get(tr, member_key, existing_key);

    if (!found) {
      ldpp_dout(dpp, 10)
          << "FDBBucketDirectory::" << __func__
          << "() key: " << member_key 
		  << " does not exist"
          << dendl;
      return -ENOENT;
    }
    lfdb::erase(tr, bucket_id + "/ordered/" + existing_key + "/" + member);
    lfdb::erase(tr, member_key);

    lfdb::commit(tr);

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBBucketDirectory::fdb_range(const DoutPrefixProvider* dpp,
                              const std::string& bucket_id,
                              const std::string& start,
                              uint64_t count,
                              std::vector<CacheObject>& objs_info,
                              std::string& continuation_token,
                              bool start_inclusive,
                              optional_yield y)
{
  continuation_token.clear();
  try {
    auto tr = lfdb::make_transaction(FDBconn);
    std::string encoded_score = encode_score(0);
    std::string base = bucket_id + "/ordered/" + encoded_score + "/";

    std::string begin_key;
    if (start.empty()) {
        begin_key = base;
    } else if (start_inclusive) {
        begin_key = base + start; // include this key
    } else {
        begin_key = base + start + std::string(1, '\x00'); // exclusive resume
    }
    std::string end_key = base + "\xff";
    std::vector<std::pair<std::string, std::string>> kvs;

    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " begin_key: " << begin_key << dendl;
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " end_key: " << end_key << dendl;
    bool ok = lfdb::get(
        tr,
        lfdb::select{begin_key, end_key},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      ldpp_dout(dpp, 10)
          << "FDBBucketDirectory::" << __func__
          << "() Empty response"
          << dendl;
      return -ENOENT;
    }

    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " kvs.size(): " << kvs.size() << dendl;
    uint64_t actual_size = count ? std::min<uint64_t>(count, kvs.size()) : kvs.size();
    for (uint64_t i = 0; i < actual_size; ++i) {
      const std::string& key = kvs[i].first;
      // key layout: "<bucket_id>/ordered/<encoded_score>/<member>"
      // member itself may contain '/', so we can't use rfind("/") here.
      objs_info.push_back(CacheObject{});
      objs_info.back().objName = key.substr(base.size());
      if (objs_info.size() == count) {
        break;
      }
    }
    if(kvs.size() > count) {
      continuation_token = objs_info.back().objName;
    }
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " continuation_token: " << continuation_token << dendl;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBBucketDirectory::fdb_scan(const DoutPrefixProvider* dpp,
                             const std::string& bucket_id,
                             const std::string& start_token,
                             const std::string& prefix,
                             uint64_t count,
                             bool marker_inclusive, 
                             std::vector<CacheObject>& objs_info,
                             std::string& continuation_token,
                             optional_yield y)
{
  continuation_token.clear();
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string encoded_score = encode_score(0);
    std::string base = bucket_id + "/ordered/" + encoded_score + "/";

    std::string prefix_lo = base + prefix;
    std::string prefix_hi = base + prefix + "\xff";

    // start_token may come from an S3-level marker the caller chose
    // freely -- it is NOT guaranteed to fall within [prefix_lo, prefix_hi).
    // Clamp it into that range rather than trusting it blindly.
    std::string candidate_begin;
    if (start_token.empty()) {
      candidate_begin = prefix_lo;
    } else {
      if (marker_inclusive) {
        candidate_begin = base + start_token;
      } else {
        candidate_begin = base + start_token + std::string(1, '\x00');
      }
    }

    std::string range_begin = std::max(candidate_begin, prefix_lo);
    std::string range_end   = prefix_hi;
    if (range_begin >= range_end) {
      // The marker is already past the end of this prefix's range --
      // nothing to return.
      return -ENOENT;
    }
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << "() range_begin: " << range_begin << dendl;
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << "() range_end: " << range_end << dendl;

    std::vector<std::pair<std::string, std::string>> kvs;
    bool ok = lfdb::get(
        tr,
        lfdb::select{range_begin, range_end},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }

    for (uint64_t i = 0; i < kvs.size(); ++i) {
      const std::string& key = kvs[i].first;
      // key layout: "<bucket_id>/ordered/<encoded_score>/<member>"
      // member itself may contain '/', so we can't use rfind("/") here.
      std::string member = key.substr(base.size());
      objs_info.push_back(CacheObject{});
      objs_info.back().objName = member;
      if (count && objs_info.size() == count) {
        // More results exist only if there's at least one more raw
        // key left in what we fetched.
        if (i + 1 < kvs.size()) {
          continuation_token = member;
        }
        break;
      }
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}


int FDBObjectDirectory::exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, optional_yield y) 
{
  std::string key = build_index(bucket_id, obj_name);
  return lfdb::key_exists(lfdb::make_transaction(FDBconn), key, lfdb::commit_after_op::commit);
}

int FDBObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  std::string key = build_index(object->bucketName, object->objName);
  lfdb::erase(FDBconn, key);
  return 0; 
}

int FDBObjectDirectory::fdb_add(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                double score,
                                const std::string& version,
                                optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :bucket_id " << bucket_id << dendl;
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :obj_name " << obj_name << dendl;

    std::string index = build_index(bucket_id, obj_name);
    std::string encoded_score = encode_score(score);

    std::string member_key = index + "/member/" + version;

    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :index " << index << dendl;
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " member_key " << member_key << dendl;
    std::string existing;
    if (lfdb::get(tr, member_key, existing)){
      lfdb::erase(tr, index + "/ordered/" + existing + "/" + version);
    }

    lfdb::set(tr, index + "/ordered/" + encoded_score + "/" + version, "1");
    lfdb::set(tr, member_key, encoded_score);
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " ordered_key " << index + "/ordered/" + encoded_score + "/" + version << dendl;
    lfdb::commit(tr);

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBObjectDirectory::fdb_range(const DoutPrefixProvider* dpp,
                                  const std::string& bucket_id,
                                  const std::string& obj_name,
                                  int start,
                                  int stop,
                                  std::vector<std::string>& members,
                                  optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(bucket_id, obj_name);
    std::string prefix = index + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }

    int end = std::min(stop + 1, (int)kvs.size());

    for (int i = start; i < end; ++i) {
      const std::string& key = kvs[i].first;

      auto pos = key.find('/', prefix.size());

      if (pos != std::string::npos) {
        members.push_back(key.substr(pos + 1));
      }
    }

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBObjectDirectory::fdb_revrange(const DoutPrefixProvider* dpp,
                                    const std::string& bucket_id,
                                    const std::string& obj_name,
                                    const std::string& marker_version,
                                    uint64_t count,
                                    std::vector<std::string>& members,
                                    optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(bucket_id, obj_name);
    std::string prefix = index + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    std::string range_begin = prefix;
    std::string range_end = prefix + "\xff";  // default: everything, if no marker

    if (!marker_version.empty()) {
      // Point lookup: resolve the marker's own encoded_score via the
      // reverse index (index + "/member/" + member -> encoded_score),
      // rather than scanning to find it.
      std::string marker_score;
      std::string member_key = index + "/member/" + marker_version;
      bool found = lfdb::get(tr, member_key, marker_score);
      if (!found) {
          ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__
                              << "() marker version not found: " << marker_version << dendl;
          return -ENOENT;
      }

      // Bound strictly before the marker's own key (score + "/" + version),
      // so the marker itself is excluded and only genuinely older
      // versions (lower scores) are returned.
      range_end = prefix + marker_score + "/" + marker_version;
    }
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() range_begin: " << range_begin << dendl;
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() range_end: " << range_end << dendl;
    bool ok = lfdb::get(
        tr,
        lfdb::select{range_begin, range_end},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }

    std::reverse(kvs.begin(), kvs.end());

    uint64_t end = count ? std::min(count, (uint64_t)kvs.size()) : kvs.size();
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "count: " << count << dendl;

    for (uint64_t i = 0; i < end; ++i) {
      const std::string& key = kvs[i].first;
      // key layout: prefix + encoded_score + "/" + member.
      // member may itself contain '/', so don't use rfind("/") -- instead
      // skip forward past the fixed prefix, then past the score segment
      // (delimited by the first '/' after the prefix), and take everything
      // that remains as the member.
      size_t score_start = prefix.size();
      size_t score_end = key.find('/', score_start);
      if (score_end == std::string::npos) {
          ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__
                            << "() malformed key (no member segment): " << key << dendl;
          continue;
      }
      members.push_back(key.substr(score_end + 1));
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() members: " << members[i] << dendl;
    }

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBObjectDirectory::fdb_rem(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                const std::string& version,
                                optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(bucket_id, obj_name);
    std::string member_key = index + "/member/" + version;

    std::string existing;
    bool found = lfdb::get(tr, member_key, existing);

    if (!found) {
      return -ENOENT;
    }

    lfdb::erase(tr, index + "/ordered/" + existing + "/" + version);
    lfdb::erase(tr, member_key);

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }
  return 0;
}

int FDBObjectDirectory::fdb_remrangebyscore(const DoutPrefixProvider* dpp,
                                            const std::string& bucket_id,
                                            const std::string& obj_name,
                                            double min,
                                            double max,
                                            optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(bucket_id, obj_name);
    std::string prefix = index + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok)
      return -ENOENT;

    std::string min_s = encode_score(min);
    std::string max_s = encode_score(max);

    for (const auto& kv : kvs) {
      auto pos = kv.first.find('/', prefix.size());

      if (pos == std::string::npos)
        continue;

      std::string score =
          kv.first.substr(prefix.size(), pos - prefix.size());

      if (score >= min_s && score <= max_s) {
        lfdb::erase(tr, kv.first);
      }
    }

    lfdb::commit(tr);

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBObjectDirectory::fdb_rank(const DoutPrefixProvider* dpp,
                                 const std::string& bucket_id,
                                 const std::string& obj_name,
                                 const std::string& member,
                                 std::string& index,
                                 optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string prefix = build_index(bucket_id, obj_name) + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok)
      return -ENOENT;

    for (size_t i = 0; i < kvs.size(); ++i) {
      auto pos = kvs[i].first.find('/', prefix.size());

      if (pos != std::string::npos &&
          kvs[i].first.substr(pos + 1) == member) {
        index = std::to_string(i);
        return 0;
      }
    }

    return -ENOENT;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }
  return 0;
}

int FDBObjectDirectory::add_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, optional_yield y, Pipeline* pipeline)
{
  auto score = ceph::real_clock::to_double(creation_time);
  ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "(): Score of object name: "<< obj_name << " version: " << version << " is: "  << score << dendl;
  return fdb_add(dpp, bucket_id, obj_name, score, version, y);
}

int FDBObjectDirectory::remove_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, optional_yield y)
{
  return fdb_rem(dpp, bucket_id, obj_name, version, y);
}

int FDBObjectDirectory::remove_version_by_creation_time(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const double& creation_time,optional_yield y)
{
  return fdb_remrangebyscore(dpp, bucket_id, obj_name, creation_time, creation_time, y);;
}

int FDBObjectDirectory::list_versions(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, optional_yield y)
{
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " obj_name: " << obj_name << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " marker_version: " << marker_version << dendl;
  std::vector<std::string> members;
  auto ret = fdb_revrange(dpp, bucket_id, obj_name, marker_version, (count + 1), members, y);
  if (ret < 0 ) {
    return ret;
  }
  if (members.empty()) {
    return -ENOENT;
  }
  uint64_t actual_size = count ? std::min<uint64_t>(count, members.size()) : members.size();
  obj_versions.reserve(actual_size);
  for (uint64_t i = 0; i < actual_size; i++) {
    auto& obj_version = obj_versions.emplace_back();
    obj_version.bucketId = bucket_id;
    obj_version.objName = obj_name;
    obj_version.version = members[i];
  }
  if(members.size() > count) {
    continuation_token = members[count - 1];
  }

  return ret;
}

int FDBBlockDirectory::exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  return lfdb::key_exists(lfdb::make_transaction(FDBconn), key, lfdb::commit_after_op::commit);
}

template<SeqContainer Container>
int FDBBlockDirectory::set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& fdbValues, optional_yield y)
{
  std::string endpoint;

  for (const auto& host : block.cacheObj.hostsList) {
    if (endpoint.empty())
      endpoint = host;
    else
      endpoint += "_" + host;
  }

  auto add_value = [&](const std::string& key, const auto& value) {
    using ValueType = typename Container::value_type;

    std::string str_value;

    if constexpr (std::is_convertible_v<decltype(value), std::string>) {
      str_value = value;
    } else {
      str_value = std::to_string(value);
    }

    if constexpr (requires(Container c, ValueType v) {
                  c.push_back(v);
                }) {
      fdbValues.push_back(ValueType{key, str_value});
    } else {
      fdbValues.insert(ValueType{key, str_value});
    }
  };


  add_value("blockID", std::to_string(block.blockID));
  add_value("version", block.version);
  add_value("deleteMarker", block.deleteMarker ? "1" : "0");
  add_value("size", std::to_string(block.size));
  add_value("globalWeight", std::to_string(block.globalWeight));
  add_value("objName", block.cacheObj.objName);
  add_value("bucketName", block.cacheObj.bucketName);
  add_value("creationTime", block.cacheObj.creationTime);
  add_value("dirty", block.cacheObj.dirty ? "1" : "0");
  add_value("hosts", endpoint);
  add_value("etag", block.cacheObj.etag);
  add_value("objSize", std::to_string(block.cacheObj.size));
  add_value("userId", block.cacheObj.user_id);
  add_value("displayName", block.cacheObj.display_name);

  return 0;
}

int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline)
{
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  map<string, string> fdbValues;

  auto ret = set_values(dpp, *block, fdbValues, y);
  if (ret < 0) {
    return ret;
  }

  lfdb::set(FDBconn, key, fdbValues);
  return 0;
}


int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{
  auto txn = lfdb::make_transaction(FDBconn);

  for (auto block : blocks) {
    std::string key = build_index(&block);
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    map<string, string> fdbValues;

    auto ret = set_values(dpp, block, fdbValues, y);
    if (ret < 0) {
      return ret;
    }

    lfdb::set(txn, key, fdbValues);
  }

  if (!lfdb::commit(txn)) {
    ldpp_dout(dpp, 0)
      << "FDB commit failed in " << __func__ << dendl;
    return -1;
  }

  return 0;
}

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  std::map<std::string, std::string> out_kvs;

  if (lfdb::get(FDBconn, key, out_kvs) != true){
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << "get function returned false! " << dendl;
	return -ENOENT;
  }

  block->blockID = std::stoull(out_kvs.at("blockID"));
  block->version = out_kvs.at("version");
  block->deleteMarker = (out_kvs.at("deleteMarker") == "1");
  block->size = std::stoull(out_kvs.at("size"));
  block->globalWeight = std::stoull(out_kvs.at("globalWeight"));
  block->cacheObj.objName      = out_kvs.at("objName");
  block->cacheObj.bucketName   = out_kvs.at("bucketName");
  block->cacheObj.creationTime = out_kvs.at("creationTime");
  block->cacheObj.dirty        = (out_kvs.at("dirty") == "1");
  boost::split(
    block->cacheObj.hostsList,
    out_kvs.at("hosts"),
    boost::is_any_of("_")
  );
  block->cacheObj.etag         = out_kvs.at("etag");
  block->cacheObj.size         = std::stoull(out_kvs.at("objSize"));
  block->cacheObj.user_id      = out_kvs.at("userId");
  block->cacheObj.display_name = out_kvs.at("displayName");

  return 0;
}


int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{
  std::vector<std::map<std::string, std::string>> out_kvs(blocks.size());

  auto txn = lfdb::make_transaction(FDBconn);

  // -------- FETCH PHASE --------
  for (size_t i = 0; i < blocks.size(); i++) {
    auto& block = blocks[i];

    std::string key = build_index(&block);
    ldpp_dout(dpp, 20)
          << "FDBBlockDirectory::" << __func__
          << "() key: " << key
          << dendl;

    if (!lfdb::get(txn, key, out_kvs[i])) {
      ldpp_dout(dpp, 0)
          << "FDBBlockDirectory::" << __func__
          << "() ERROR: get function returned false!"
          << dendl;
      return -ENOENT;
    }
  }

  if (!lfdb::commit(txn)) {
    ldpp_dout(dpp, 0)
      << "FDB commit failed in " << __func__ << dendl;
    return -ENOENT;
  }


  // -------- POPULATE PHASE --------
  for (size_t i = 0; i < blocks.size(); i++) {
    auto& block = blocks[i];
    auto& kvs = out_kvs[i];

    block.blockID = std::stoull(kvs.at("blockID"));
    block.version = kvs.at("version");
    block.deleteMarker = (kvs.at("deleteMarker") == "1");
    block.size = std::stoull(kvs.at("size"));
    block.globalWeight = std::stoull(kvs.at("globalWeight"));

    block.cacheObj.objName      = kvs.at("objName");
    block.cacheObj.bucketName   = kvs.at("bucketName");
    block.cacheObj.creationTime = kvs.at("creationTime");
    block.cacheObj.dirty        = (kvs.at("dirty") == "1");

    boost::split(
        block.cacheObj.hostsList,
        kvs.at("hosts"),
        boost::is_any_of("_")
    );

    block.cacheObj.etag         = kvs.at("etag");
    block.cacheObj.size         = std::stoull(kvs.at("objSize"));
    block.cacheObj.user_id      = kvs.at("userId");
    block.cacheObj.display_name = kvs.at("displayName");
  }

  return 0;
}


//FIXME: shouldn't copyName reflect block's name instead of object name?
//the same for redis class.
int FDBBlockDirectory::copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{
  // Retrieve the block from the directory in case it has been updated by a remote cache.
  if (this->get(dpp, block, y) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -ENOENT;
  }

  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };
  std::string copyKey = build_index(&copyBlock);

  copyBlock.version = block->version;
  copyBlock.deleteMarker = block->deleteMarker;
  copyBlock.size = block->size;
  copyBlock.globalWeight = block->globalWeight;

  copyBlock.cacheObj.dirty = block->cacheObj.dirty;
  copyBlock.cacheObj.creationTime = block->cacheObj.creationTime;
  copyBlock.cacheObj.hostsList = block->cacheObj.hostsList;
  copyBlock.cacheObj.etag = block->cacheObj.etag;
  copyBlock.cacheObj.size = block->cacheObj.size;
  copyBlock.cacheObj.user_id = block->cacheObj.user_id;
  copyBlock.cacheObj.display_name = block->cacheObj.display_name;

  //FIXME: return value should be checked. 
  this->set(dpp, &copyBlock, y);

  return 0;
}

int FDBBlockDirectory::del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y)
{
  std::string key = build_index(block);
  lfdb::erase(FDBconn, key);
  return 0; 
}

int FDBBlockDirectory::update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y)
{
  int ret = -1;

  if (!(ret = exist_key(dpp, block, y))) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
	return -ENOENT;
  }

  if (this->get(dpp, block, y) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -ENOENT;
  }

  if (field == "blockID") {
    block->blockID = std::stoull(value);
  }
  else if (field == "version") {
    block->version = value;
  }
  else if (field == "deleteMarker") {
    block->deleteMarker = (value == "1");
  }
  else if (field == "size") {
    block->size = std::stoull(value);
  }
  else if (field == "globalWeight") {
    block->globalWeight = std::stoull(value);
  }
  else if (field == "objName") {
    block->cacheObj.objName = value;
  }
  else if (field == "bucketName") {
    block->cacheObj.bucketName = value;
  }
  else if (field == "dirty") {
    block->cacheObj.dirty = (value == "1");
  }
  else if (field == "creationTime") {
    block->cacheObj.creationTime = value;
  }
  else if (field == "hosts") {
    block->cacheObj.hostsList.insert(value);
  }
  else if (field == "etag") {
    block->cacheObj.etag = value;
  }
  else if (field == "objSize") {
    block->cacheObj.size = std::stoull(value);
  }
  else if (field == "userId") {
    block->cacheObj.user_id = value;
  }
  else if (field == "displayName") {
    block->cacheObj.display_name = value;
  }

  //FIXME: return value should be checked. 
  this->set(dpp, block, y);

  return 0;
}

int FDBBlockDirectory::remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y)
{
  int ret = -1;

  if (!(ret = exist_key(dpp, block, y))) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
	return -ENOENT;
  }

  if (this->get(dpp, block, y) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -ENOENT;
  }

  block->cacheObj.hostsList.erase(value);

  //FIXME: return value should be checked. 
  this->set(dpp, block, y);

  return 0;
}

#if 0
int FDBBlockDirectory::fdb_add(const DoutPrefixProvider* dpp,
                            CacheBlock* block,
                            double score,
                            const std::string& member,
                            optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(block);
    std::string encoded_score = encode_score(score);

    std::string member_key = index + "/member/" + member;

    std::string existing;
    bool found = lfdb::get(tr, member_key, existing);

    if (found)
      lfdb::erase(tr, index + "/ordered/" + existing + "/" + member);

    lfdb::set(tr, index + "/ordered/" + encoded_score + "/" + member, "");
    lfdb::set(tr, member_key, encoded_score);

    lfdb::commit(tr);

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBlockDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBBlockDirectory::fdb_range(const DoutPrefixProvider* dpp,
                              CacheBlock* block,
                              int start,
                              int stop,
                              std::vector<std::string>& members,
                              optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(block);
    std::string prefix = index + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      ldpp_dout(dpp, 10)
          << "FDBBlockDirectory::" << __func__
          << "() Empty response"
          << dendl;
      return -ENOENT;
    }

    int end = std::min(stop + 1, (int)kvs.size());

    for (int i = start; i < end; ++i) {
      const std::string& key = kvs[i].first;
      members.push_back(key.substr(key.rfind("/") + 1));
    }

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBlockDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}


int FDBBlockDirectory::fdb_revrange(const DoutPrefixProvider* dpp,
                                 CacheBlock* block,
                                 int start,
                                 int stop,
                                 std::vector<std::string>& members,
                                 optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(block);
    std::string prefix = index + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      ldpp_dout(dpp, 10)
          << "FDBBlockDirectory::" << __func__
          << "() Empty response"
          << dendl;
      return -ENOENT;
    }

    std::reverse(kvs.begin(), kvs.end());

    int end = std::min(stop + 1, (int)kvs.size());

    for (int i = start; i < end; ++i) {
      const std::string& key = kvs[i].first;
      members.push_back(key.substr(key.rfind("/") + 1));
    }

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBlockDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}


int FDBBlockDirectory::fdb_rem(const DoutPrefixProvider* dpp,
                            CacheBlock* block,
                            const std::string& member,
                            optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(block);
    std::string member_key = index + "/member/" + member;

    std::string existing;
    bool found = lfdb::get(tr, member_key, existing);

    if (!found) {
      ldpp_dout(dpp, 10)
          << "FDBBlockDirectory::" << __func__
          << "() Member does not exist"
          << dendl;
      return -ENOENT;
    }

    lfdb::erase(tr, index + "/ordered/" + existing + "/" + member);
    lfdb::erase(tr, member_key);

    lfdb::commit(tr);

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBlockDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}
#endif

} // namespace rgw::d4n
