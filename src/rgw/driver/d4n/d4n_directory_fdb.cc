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

int FDBBucketDirectory::add_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, optional_yield y, Pipeline* pipeline)
{
  return 0;
}

int FDBBucketDirectory::zadd(const DoutPrefixProvider* dpp,
                            const std::string& bucket_id,
                            double score,
                            const std::string& member,
                            optional_yield y,
                            Pipeline* pipeline)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string encoded_score = encode_score(score);
    std::string member_key = bucket_id + "/member/" + member;

    std::string existing;
    bool found =  lfdb::get(tr, member_key, existing);

    if (found) {
      lfdb::erase(tr, bucket_id + "/ordered/" + existing + "/" + member);
    }

    lfdb::set(tr, bucket_id + "/ordered/" + encoded_score + "/" + member, "");
    lfdb::set(tr, member_key, encoded_score);

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

int FDBBucketDirectory::remove_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string member_key = bucket_id + "/member/" + member;

    std::string existing;
    bool found = lfdb::get(tr, member_key, existing);

    if (!found) {
      ldpp_dout(dpp, 10)
          << "FDBBucketDirectory::" << __func__
          << "() Member does not exist"
          << dendl;
      return -ENOENT;
    }

    lfdb::erase(tr, bucket_id + "/ordered/" + existing + "/" + member);
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

//Performs an incremental scan of objects within the specified bucket, returning a subset of results based on the provided cursor position and count.
int FDBBucketDirectory::scan_objects(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t start_pos, const std::string& pattern, uint64_t count, std::vector<std::string>& objects, std::optional<CacheObject>& params, uint64_t& next_pos, optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string prefix = bucket_id + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix + start, prefix + stop + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      ldpp_dout(dpp, 10)
          << "FDBBucketDirectory::" << __func__
          << "() Empty response"
          << dendl;
      return -ENOENT;
    }

    uint64_t begin = offset;
    uint64_t end = count ? std::min(begin + count, (uint64_t)kvs.size())
                          : kvs.size();

    for (uint64_t i = begin; i < end; ++i) {
      const std::string& key = kvs[i].first;
      members.push_back(key.substr(key.rfind("/") + 1));
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

int FDBBucketDirectory::get_range(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& objects, std::optional<CacheObject>& params, optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string prefix = bucket_id + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }

    uint64_t start = cursor;
    uint64_t end = std::min(start + count, (uint64_t)kvs.size());

    next_cursor = (end >= kvs.size()) ? 0 : end;

    for (uint64_t i = start; i < end; ++i) {
      const std::string& key = kvs[i].first;
      std::string member = key.substr(key.rfind("/") + 1);

      if (!pattern.empty()) {
        if (member.find(pattern) == std::string::npos)
          continue;
      }

      members.push_back(member);
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

#if 0
int FDBObjectDirectory::set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  int ret = -1;
  if ((ret = check_bool(std::to_string(object->dirty))) != -EINVAL) {
    object->dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
    return -EINVAL;
  }

  std::string key = build_index(object);

  std::string endpoint;
  if (!object->hostsList.empty()){
    for (auto const& host : object->hostsList) {
      if (endpoint.empty())
        endpoint = host + "_";
      else
        endpoint = endpoint + host + "_";
    }

    endpoint.pop_back();
  }

  map<string, string> object_entries = {
    { "objName", object->objName },
    { "bucketName", object->bucketName },
    {"creationTime", object->creationTime},
    {"dirty", object->dirty ? "1" : "0"},
    { "hosts", endpoint },
    { "etag", object->etag },
    { "objSize", std::to_string(object->size) },
    { "userId", object->user_id },
    { "displayName", object->display_name }
  };
 
  lfdb::set(FDBconn, key, object_entries);

  return 0;
}

int FDBObjectDirectory::get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);
  std::map<std::string, std::string> out_kvs;

  if (lfdb::get(lfdb::make_transaction(FDBconn), key, out_kvs, lfdb::commit_after_op::commit) != true){
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: " << "get function returned false! " << dendl;
	return -1;
  }

  object->objName      = out_kvs.at("objName");
  object->bucketName   = out_kvs.at("bucketName");
  object->creationTime = out_kvs.at("creationTime");
  object->dirty        = (out_kvs.at("dirty") == "1");
  boost::split(
    object->hostsList,
    out_kvs.at("hosts"),
    boost::is_any_of("_")
  );
  object->etag         = out_kvs.at("etag");
  object->size         = std::stoull(out_kvs.at("objSize"));
  object->user_id      = out_kvs.at("userId");
  object->display_name = out_kvs.at("displayName");

  return 0;
}

int FDBObjectDirectory::copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string copyName, const std::string copyBucketName, optional_yield y)
{
  if (this->get(dpp, object, y) < 0){
    ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -1;
  }

  auto copyObj = CacheObj{ .objName = copyName, .bucketName = copyBucketName };
  std::string copyKey = build_index(&copyObj);

  copyObj.dirty = object->dirty;
  copyObj.creationTime = object->creationTime;
  copyObj.hostsList = object->hostsList;
  copyObj.etag = object->etag;
  copyObj.size = object->size;
  copyObj.user_id = object->user_id;
  copyObj.display_name = object->display_name;

  this->set(dpp, &copyObj, y);

  return 0;
}

int FDBObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);
  //FIXME: JWF: how can we make sure erase has worked?
  lfdb::erase(lfdb::make_transaction(FDBconn), key, lfdb::commit_after_op::commit);
  return 0; 
}

int FDBObjectDirectory::update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y)
{
  int ret = -1;

  if (!(ret = exist_key(dpp, object, y))) {
    ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "(): Object does not exist." << dendl;
	return -1;
  }

  if (this->get(dpp, object, y) < 0){
    ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -1;
  }


  if (field == "objName") {
    object->objName = value;
  }
  else if (field == "bucketName") {
    object->bucketName = value;
  }
  else if (field == "dirty") {
    object->dirty = (value == "1");
  }
  else if (field == "creationTime") {
    object->creationTime = value;
  }
  else if (field == "hosts") {
    object->hostsList.insert(value);
  }
  else if (field == "etag") {
    object->etag = value;
  }
  else if (field == "size") {
    object->size = std::stoull(value);
  }
  else if (field == "userId") {
    object->user_id = value;
  }
  else if (field == "displayName") {
    object->display_name = value;
  }

  this->set(dpp, object, y);

  return 0;
}

int FDBObjectDirectory::zadd(const DoutPrefixProvider* dpp,
                            CacheObj* object,
                            double score,
                            const std::string& member,
                            optional_yield y,
                            Pipeline* pipeline)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(object);
    std::string encoded_score = encode_score(score);

    std::string member_key = index + "/member/" + member;

    std::string existing;
    if (lfdb::get(tr, member_key, existing)){
      lfdb::erase(tr, index + "/ordered/" + existing + "/" + member);
    }

    lfdb::set(tr, index + "/ordered/" + encoded_score + "/" + member, "");
    lfdb::set(tr, member_key, encoded_score);

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

int FDBObjectDirectory::zrange(const DoutPrefixProvider* dpp,
                              CacheObj* object,
                              int start,
                              int stop,
                              std::vector<std::string>& members,
                              optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(object);
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
      members.push_back(key.substr(key.rfind("/") + 1));
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

int FDBObjectDirectory::zrevrange(const DoutPrefixProvider* dpp,
                                 CacheObj* object,
                                 const std::string& start,
                                 const std::string& stop,
                                 std::vector<std::string>& members,
                                 optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(object);
    std::string prefix = index + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix + start, prefix + stop + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }

    std::reverse(kvs.begin(), kvs.end());

    for (const auto& kv : kvs) {
      const std::string& key = kv.first;
      members.push_back(key.substr(key.rfind("/") + 1));
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

int FDBObjectDirectory::zrem(const DoutPrefixProvider* dpp,
                            CacheObj* object,
                            const std::string& member,
                            optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(object);
    std::string member_key = index + "/member/" + member;

    std::string existing;
    bool found = lfdb::get(tr, member_key, existing);

    if (!found) {
      return -ENOENT;
    }

    lfdb::erase(tr, index + "/ordered/" + existing + "/" + member);
    lfdb::erase(tr, member_key);

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


int FDBObjectDirectory::zremrangebyscore(const DoutPrefixProvider* dpp,
                                         CacheObj* object,
                                         double min,
                                         double max,
                                         optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string index = build_index(object);
    std::string prefix = index + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }

    std::string min_s = encode_score(min);
    std::string max_s = encode_score(max);

    for (const auto& kv : kvs) {
      const std::string& key = kv.first;

      // key = index/ordered/<score>/<member>
      auto pos = key.find("/ordered/");
      if (pos == std::string::npos) continue;

      std::string score = key.substr(pos + 9);
      score = score.substr(0, score.find('/'));

      if (score >= min_s && score <= max_s) {
        lfdb::erase(tr, key);
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

int FDBObjectDirectory::zrank(const DoutPrefixProvider* dpp,
                              CacheObj* object,
                              const std::string& member,
                              std::string& index,
                              optional_yield y)
{
  try {
    auto tr = lfdb::make_transaction(FDBconn);

    std::string prefix = build_index(object) + "/ordered/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        tr,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }

    for (size_t i = 0; i < kvs.size(); ++i) {
      const std::string& key = kvs[i].first;

      std::string m = key.substr(key.rfind("/") + 1);

      if (m == member) {
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
}

int FDBObjectDirectory::incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  std::string key = build_index(object);
  key = key + "_versioned_epoch";

  std::string out_value;

  if (lfdb::get(FDBconn, key, out_value) != true){
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: " << "get function returned false! " << dendl;
	return -1;
  }

  auto value = std::stoull(out_value);
  value++;

  lfdb::set(FDBconn, key, value);

  return value;
}
#endif

int FDBObjectDirectory::add_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, optional_yield y, Pipeline* pipeline)
{
  return 0;
}

int FDBObjectDirectory::remove_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::remove_version_by_creation_time(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const double& creation_time,optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::list_versions(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& start, const std::string& stop, std::vector<CacheObjectVersion>& versions, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::get_version_index(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::string& index, optional_yield y)
{
  return 0;
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
	return -1;
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

    if (!lfdb::get(txn, key, out_kvs[i])) {
      ldpp_dout(dpp, 0)
          << "FDBBlockDirectory::" << __func__
          << "() ERROR: get function returned false!"
          << dendl;
      return -1;
    }
  }

  if (!lfdb::commit(txn)) {
    ldpp_dout(dpp, 0)
      << "FDB commit failed in " << __func__ << dendl;
    return -1;
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
	return -1;
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
	return -1;
  }

  if (this->get(dpp, block, y) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -1;
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

  this->set(dpp, block, y);

  return 0;
}

int FDBBlockDirectory::remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y)
{
  int ret = -1;

  if (!(ret = exist_key(dpp, block, y))) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
	return -1;
  }

  if (this->get(dpp, block, y) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -1;
  }

  block->cacheObj.hostsList.erase(value);

  this->set(dpp, block, y);

  return 0;
}

} // namespace rgw::d4n
