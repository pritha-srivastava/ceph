#include <algorithm>
#include <type_traits>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory_fdb.h"

namespace rgw { namespace d4n {

using fdb_conn = lfdb::database;

using std::map;
using std::string;

/*
struct initiate_exec {
  std::shared_ptr<fdb_conn> conn;
};
*/

int FDBBucketDirectory::zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, Pipeline* pipeline)
{
  return 0;
}

int FDBBucketDirectory::zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y)
{
  return 0;
}

int FDBBucketDirectory::zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBBucketDirectory::zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y)
{
  return 0;
}

int FDBBucketDirectory::zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);
  return lfdb::key_exists(lfdb::make_transaction(FDBconn), key, lfdb::commit_after_op::commit) ? 1 : 0;
}

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
  for (auto const& host : object->hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  if (!endpoint.empty())
    endpoint.pop_back();

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
 
  //FIXME: return value should be checked. 
  lfdb::set(FDBconn, key, object_entries);

  return 0;
}

int FDBObjectDirectory::get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);
  std::map<std::string, std::string> out_kvs;

  //FIXME: return value should be checked. 
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

int FDBObjectDirectory::copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
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

  //FIXME: return value should be checked. 
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

  //FIXME: return value should be checked. 
  this->set(dpp, object, y);

  return 0;
}

int FDBObjectDirectory::zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, Pipeline* pipeline)
{
  return 0;
}

int FDBObjectDirectory::zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y)
{
  return 0;
}

//FIXME
int FDBObjectDirectory::incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  return lfdb::key_exists(lfdb::make_transaction(FDBconn), key, lfdb::commit_after_op::commit) ? 1 : 0;
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

  //FIXME: return value should be checked. 
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
  //we should get block from directory in case of it is updated by a remote cache
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

  //FIXME: return value should be checked. 
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

  //FIXME: return value should be checked. 
  this->set(dpp, block, y);

  return 0;
}

int FDBBlockDirectory::zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y)
{
  return 0;
}


} } // namespace rgw::d4n
