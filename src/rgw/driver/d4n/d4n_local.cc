#include "d4n_local.h"
#include "d4n_policy.h"
#include "common/dout.h"

namespace rgw { namespace d4n {

int LocalStrategy::initialize(CephContext *cct, const DoutPrefixProvider* dpp)
{
  return 0;
}

int LocalStrategy::get(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, optional_yield y) 
{
  return 0;
}

int LocalStrategy::update_head(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterBlock* block, const std::string& key, optional_yield y)
{
  //maintain an entry to get the latest version of the object
  std::string key_in_map = block->object->get_bucket()->get_name() + "_" + block->object->get_name();
  HeadEntry entry(block->version, block->dirty);
  head_entry_map.emplace(key_in_map, entry);

  return 0;
}

int LocalStrategy::update_obj(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& key, bool dirty, const std::string& version, optional_yield y)
{
  time_t creationTime = object->get_creation_time(dpp);
  if (dirty) {
    policyDriver->get_cache_policy()->updateObj(dpp, key, version, true, object->get_obj_size(), creationTime, std::get<rgw_user>(object->get_bucket()->get_owner()), object->get_etag(dpp), object->get_bucket()->get_name(), object->get_key(), y);
  }
  //add all objects to an in-memory data structure
  object_set.emplace(key);

  return 0;
}

int LocalStrategy::put(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterBlock* block, const std::string& key, optional_yield y)
{
  int ret;
  std::string key_in_cache;
  uint64_t total_size = block->len + block->attrs.size();
  if (block->dirty) { //data is being written to the write-back cache
    if (policyDriver->get_cache_policy()->is_write_space_available(dpp, total_size)) {
      key_in_cache = "D_" + key;
      ret = 0;
    } else {
      ret = -ENOSPC;
    }
  } else { //data is being written to the read cache
    ret = policyDriver->get_cache_policy()->eviction(dpp, total_size, y);
    key_in_cache = key;
  }
  if (ret == 0) {
    ret = cacheDriver->put(dpp, key_in_cache, block->bl, block->len, block->attrs, y);
  } else {
    ldpp_dout(dpp, 0) << "LocalStrategy::" << __func__ << "(): Failed to evict data/no write-back cache space, ret=" << ret << dendl;
    return ret;
  }

  policyDriver->get_cache_policy()->update(dpp, key, block->offset, block->len, block->version, block->dirty, total_size, y);

  if (block->is_head) {
    if (block->is_latest_version) {
      ret = update_head(dpp, block, key, y);
      if (ret < 0) {
        return ret;
      }
    }
    ret = update_obj(dpp, block->object, key, block->dirty, block->version, y);
    if (ret < 0) {
      return ret;
    }
  }
  return 0;
}

int LocalStrategy::del(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& key, optional_yield y)
{
  return 0;
}

int LocalStrategy::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
  return 0;
}

int LocalStrategy::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
  return 0;
}

int LocalStrategy::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
  return 0;
}

std::string LocalStrategy::get_version(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  return std::string("");
}

bool LocalStrategy::is_dirty(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  return false;
}
}} //rgw::d4n