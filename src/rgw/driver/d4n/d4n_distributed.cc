#include "d4n_distributed.h"
#include "d4n_policy.h"
#include "common/dout.h"

namespace rgw { namespace d4n {

int DistributedStrategy::initialize(CephContext *cct, const DoutPrefixProvider* dpp)
{
  return 0;
}

int DistributedStrategy::get(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, optional_yield y) 
{
  return 0;
}

int DistributedStrategy::update_head(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterBlock* block, const std::string& key, optional_yield y)
{
  rgw::sal::D4NFilterObject* object = block->object;
  time_t creationTime = object->get_creation_time(dpp);

  // this entry is based off object name (and not oid), so that for 'get' requests containing no version, this entry can be used to determine the latest version(for versioned and non versioned objects)
  rgw::d4n::CacheObj cache_obj = rgw::d4n::CacheObj{
    .objName = object->get_name(),
    .bucketName = object->get_bucket()->get_name(),
    .creationTime = std::to_string(creationTime),
    .dirty = block->dirty,
    .hostsList = { dpp->get_cct()->_conf->rgw_local_cache_address },
    };

  rgw::d4n::CacheBlock blk = rgw::d4n::CacheBlock{
  .cacheObj = cache_obj,
  .blockID = 0,
  .version = block->version,
  .dirty = block->dirty,
  .size = 0,
  .hostsList = { dpp->get_cct()->_conf->rgw_local_cache_address },
  };

  auto ret = blockDir->set(dpp, &blk, y);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "DistributedStrategy::" << __func__ << "(): BlockDirectory set method failed for object with ret: " << ret << dendl;
    return ret;
  }

  return 0;
}

int DistributedStrategy::update_obj(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& key, bool dirty, const std::string& version, optional_yield y)
{
  time_t creationTime = object->get_creation_time(dpp);
  if (dirty) {
    policyDriver->get_cache_policy()->updateObj(dpp, key, version, true, object->get_obj_size(), creationTime, std::get<rgw_user>(object->get_bucket()->get_owner()), object->get_etag(dpp), object->get_bucket()->get_name(), object->get_key(), y);
  }
  //write object to directory - is this needed? we might need an entry for object listing, which can be done using the head block entry also, can be removed if not needed
  rgw::d4n::CacheObj cache_object = rgw::d4n::CacheObj{
      .objName = object->get_oid(),
      .bucketName = object->get_bucket()->get_name(),
      .creationTime = std::to_string(creationTime),
      .dirty = dirty,
      .hostsList = { dpp->get_cct()->_conf->rgw_local_cache_address }
  };
  auto ret = objDir->set(dpp, &cache_object, y);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "DistributedStrategy::" << __func__ << "(): ObjectDirectory set method failed with err: " << ret << dendl;
    return ret;
  }
  return 0;
}

int DistributedStrategy::put(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterBlock* block, const std::string& key, optional_yield y)
{
  int ret;
  std::string key_in_cache;
  uint64_t total_size = block->len + block->attrs.size();
  rgw::sal::D4NFilterObject* object = block->object;
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
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): cacheDriver->put failed with ret=" << ret << dendl;
    }
  } else {
    ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): Failed to evict data/no write-back cache space, ret=" << ret << dendl;
    return ret;
  }

  policyDriver->get_cache_policy()->update(dpp, key, block->offset, block->len, block->version, block->dirty, total_size, y);
  
  //if the incoming block is a head block we store a directory entry for every instance, or if the incoming block is a data block we store a directory entry, irrespective of it being versioned
  if ((object->have_instance() && block->is_head) || (!block->is_head)) {
    rgw::d4n::CacheObj cache_obj = rgw::d4n::CacheObj{
      .objName = object->get_oid(),
      .bucketName = object->get_bucket()->get_name(),
      .creationTime = std::to_string(object->get_creation_time(dpp)),
      .dirty = block->dirty,
      .hostsList = { dpp->get_cct()->_conf->rgw_local_cache_address },
    };

    rgw::d4n::CacheBlock blk = rgw::d4n::CacheBlock{
      .cacheObj = cache_obj,
      .blockID = block->offset,
      .version = block->version,
      .dirty = block->dirty,
      .size = block->len,
      .hostsList = { dpp->get_cct()->_conf->rgw_local_cache_address },
    };

    if (!block->dirty) { //if block is not dirty, check if it is already cached in a cache other than the local cache
      rgw::d4n::CacheBlock existing_block;
      existing_block.cacheObj.objName = blk.cacheObj.objName;
      existing_block.cacheObj.bucketName = blk.cacheObj.bucketName;
      existing_block.blockID = blk.blockID;
      existing_block.size = blk.size;
      if ((ret = blockDir->get(dpp, &existing_block, y)) == 0 || ret == -ENOENT) {
        if (ret == 0) {
		      blk = existing_block;
          blk.version = block->version;
        }
        blk.hostsList.insert(dpp->get_cct()->_conf->rgw_local_cache_address);
        if ((ret = blockDir->set(dpp, &blk, y)) < 0) {
          ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): BlockDirectory set() method failed, ret=" << ret << dendl;
        }
      } else {
        ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): BlockDirectory get() method failed, ret=" << ret << dendl;
      }
    } else { // if the block is dirty, add directory entry with latest version, hostList etc
      ret = blockDir->set(dpp, &blk, y);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "D4NFilterObject::" << __func__ << "(): BlockDirectory set method failed for object with ret: " << ret << dendl;
        return ret;
      }
    }
  }

  if (block->is_head) {
    if (block->is_latest_version) {
      ret = update_head(dpp, block, key, y);
      if (ret < 0) {
        return ret;
      }
    } //update_head
    ret = update_obj(dpp, object, key, block->dirty, block->version, y);
    if (ret < 0) {
      return ret;
    }
  } //is_head
  return 0;
}

int DistributedStrategy::del(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& key, optional_yield y)
{
  return 0;
}

int DistributedStrategy::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
  return 0;
}

int DistributedStrategy::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
  return 0;
}

int DistributedStrategy::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
  return 0;
}

std::string DistributedStrategy::get_version(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  return std::string("");
}

bool DistributedStrategy::is_dirty(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  return false;
}
}} //rgw::d4n