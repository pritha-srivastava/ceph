#include "d4n_distributed.h"
#include "d4n_policy.h"
#include "common/dout.h"

namespace rgw { namespace d4n {

int DistributedStrategy::initialize(CephContext *cct, const DoutPrefixProvider* dpp)
{
  return 0;
}

void DistributedStrategy::cancel(rgw::Aio* aio) {
  aio->drain();
}

int DistributedStrategy::drain(const DoutPrefixProvider* dpp, rgw::Aio* aio, rgw::sal::D4NFilterObject* object, RGWGetDataCB* cb, optional_yield y) {
  auto c = aio->drain();
  int r = flush(dpp, std::move(c), object, cb, y);
  if (r < 0) {
    cancel(aio);
    return r;
  }
  return 0;
}

int DistributedStrategy::flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results, rgw::sal::D4NFilterObject* object, RGWGetDataCB* cb, optional_yield y) {
  int r = rgw::check_for_errors(results);

  if (r < 0) {
    return r;
  }

  std::list<bufferlist> bl_list;

  auto cmp = [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; };
  results.sort(cmp); // merge() requires results to be sorted first
  completed.merge(results, cmp); // merge results in sorted order

  ldpp_dout(dpp, 20) << "D4NFilterObject::In flush:: " << dendl;

  while (!completed.empty() && completed.front().id == offset) {
    auto bl = std::move(completed.front().data);

    ldpp_dout(dpp, 20) << "D4NFilterObject::flush:: calling handle_data for offset: " << offset << " bufferlist length: " << bl.length() << dendl;

    bl_list.push_back(bl);
    if (cb) {
      int r = cb->handle_data(bl, 0, bl.length());
      if (r < 0) {
        return r;
      }
    }
    auto it = blocks_info.find(offset);
    if (it != blocks_info.end()) {
      std::string version = object->get_object_version();
      std::string prefix = object->get_prefix();
      std::pair<uint64_t, uint64_t> ofs_len_pair = it->second;
      uint64_t ofs = ofs_len_pair.first;
      uint64_t len = ofs_len_pair.second;
      bool dirty = false;

      rgw::d4n::CacheBlock block;
      block.cacheObj.objName = object->get_key().get_oid();
      block.cacheObj.bucketName = object->get_bucket()->get_name();
      block.blockID = ofs;
      block.size = len;

      std::string oid_in_cache = prefix + "_" + std::to_string(ofs) + "_" + std::to_string(len);

      if (blockDir->get(dpp, &block, y) == 0){
        if (block.dirty == true){ 
          dirty = true;
        }
      }

      ldpp_dout(dpp, 20) << "DistributedStrategy::" << __func__ << " calling update for offset: " << offset << " adjusted offset: " << ofs  << " length: " << len << " oid_in_cache: " << oid_in_cache << dendl;
      ldpp_dout(dpp, 20) << "DistributedStrategy::" << __func__ << " version stored in update method is: " << version << " " << object->get_object_version() << dendl;
      policyDriver->get_cache_policy()->update(dpp, oid_in_cache, ofs, len, version, dirty, len, y);
      rgw::sal::D4NFilterObject* dest_object = object->get_destination_object(dpp);
      if (dest_object) {
        std::string key = dest_object->get_name() + "_" + dest_object->get_object_version() + "_" + dest_object->get_name() +
                                        "_" + std::to_string(ofs) + "_" + std::to_string(len);
        rgw::sal::Attrs attrs;
        rgw::sal::D4NFilterBlock blk = rgw::sal::D4NFilterBlock {
          .object = dest_object,
          .version = dest_object->get_object_version(),
          .dirty = true,
          .bl = bl,
          .len = bl.length(),
          .offset = ofs,
          .attrs = attrs,
          .is_head = false,
          .is_latest_version = false
        };
        auto ret = put(dpp, &blk, key, y);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): put for block failed, ret=" << ret << dendl;
          return ret;
        }
      }
      blocks_info.erase(it);
    } else {
      ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << " offset not found: " << offset << dendl;
    }
  
    offset += bl.length();
    completed.pop_front_and_dispose(std::default_delete<rgw::AioResultEntry>{});
  }

  ldpp_dout(dpp, 20) << "DistributedStrategy::returning from flush:: " << dendl;
  return 0;
}

int DistributedStrategy::get(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterBlock* block, rgw::Aio* aio, RGWGetDataCB* cb, uint64_t read_offset, uint64_t read_len, optional_yield y) 
{
  rgw::sal::D4NFilterObject* object = block->object;
  rgw::d4n::CacheObj cache_obj = rgw::d4n::CacheObj{
        .objName = object->get_oid(), //version-enabled buckets will not have version for latest version, so this will work even when versio is not provided in input
        .bucketName = object->get_bucket()->get_name(),
        };

  rgw::d4n::CacheBlock blk = rgw::d4n::CacheBlock{
          .cacheObj = cache_obj,
          .blockID = block->offset,
          .size = block->len,
          };

  std::string key_in_cache, key;
  int ret;
  //if the block corresponding to head object does not exist in directory, implies it is not cached
  if ((ret = blockDir->get(dpp, &blk, y) == 0)) {
    if(block->is_head) {
      block->version = blk.version;
      block->dirty = blk.dirty;
      key = object->get_bucket()->get_name() + "_" + block->version + "_" + object->get_name();
    } else {
      block->dirty = blk.dirty;
      key = object->get_bucket()->get_name() + "_" + block->version + "_" + object->get_name() + "_" + std::to_string(block->offset) + "_" + std::to_string(block->len);
    }

    ldpp_dout(dpp, 10) << "DistributedStrategy::" << __func__ << "(): Is block dirty: " << block->dirty << dendl;
    if (block->dirty) {
      key_in_cache = "D_" + key;
    } else {
      key_in_cache = key;
    }
    if (block->is_head) {
      //for distributed cache-the blockHostsList can be used to determine if the head block resides on the localhost, then get the block from localhost, whether or not the block is dirty
      //can be determined using the block entry.
      ldpp_dout(dpp, 10) << "DistributedStrategy::" << __func__ << "(): Fetching attrs from cache for key: " << key_in_cache << dendl;
      auto ret = cacheDriver->get_attrs(dpp, key_in_cache, block->attrs, y);
      if (ret < 0) {
        ldpp_dout(dpp, 10) << "DistributedStrategy::" << __func__ << "(): CacheDriver get_attrs method failed." << dendl;
        return -ENOENT;
      }
    } else { // data blocks
      auto it = find(blk.hostsList.begin(), blk.hostsList.end(), dpp->get_cct()->_conf->rgw_local_cache_address);
      if (it != blk.hostsList.end()) { /* Local copy */
	      ldpp_dout(dpp, 20) << "DistributedStrategy::" << __func__ << "(): Block found in directory: " << key_in_cache << dendl;
	      if (blk.version == block->version) {
          ldpp_dout(dpp, 20) << "DistributedStrategy::" << __func__ << "(): READ FROM CACHE: key_in_cache = " << key_in_cache << dendl;
	        if (policyDriver->get_cache_policy()->exist_key(key) > 0) {
            // Read From Cache
            uint64_t cost = read_len;
            uint64_t id = block->offset;
            if (read_offset != 0) {
              id += read_offset;
            }
            if(!is_offset_set) {
              this->offset = id;
              is_offset_set = true;
            }

            auto completed = cacheDriver->get_async(dpp, y, aio, key_in_cache, read_offset, read_len, cost, id); 

	          this->blocks_info.insert(std::make_pair(id, std::make_pair(block->offset, block->len)));

	          ldpp_dout(dpp, 20) << "DistributedStrategy::" << __func__ << "(): Info: flushing data for key: " << key_in_cache << dendl;
	          auto r = flush(dpp, std::move(completed), object, cb, y);
            if (r < 0) {
              drain(dpp, aio, object, cb, y);
              ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
              return r;
            }
          // end-if (source->driver->get_policy_driver()->get_cache_policy()->exist_key(oid_in_cache) > 0) 
	        } else {
            int r = -1;
            if ((r = blockDir->remove_host(dpp, &blk, dpp->get_cct()->_conf->rgw_local_cache_address, y)) < 0) {
              ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): Error: failed to remove incorrect host from block with key=" << key_in_cache <<", ret=" << r << dendl;
            }
            if ((blk.hostsList.size() - 1) > 0 && r == 0) { /* Remote copy */
              ldpp_dout(dpp, 20) << "DistributedStrategy::" << __func__ << "(): Block with key=" << key_in_cache << " found in remote cache." << dendl;
              // TODO: Retrieve remotely
              // Policy decision: should we cache remote blocks locally?
            } else {
              ldpp_dout(dpp, 20) << "DistributedStrategy::" << __func__ << "(): Info: draining data for key: " << key_in_cache << dendl;
              auto r = drain(dpp, aio, object, cb, y);
              if (r < 0) {
                ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
                return r;
              }
            }
	        }
        // end-if (block.version == version)
        } else {
          // TODO: If data has already been returned for any older versioned block, then return ‘retry’ error, else
          ldpp_dout(dpp, 20) << "DistributedStrategy::" << __func__ << "(): Info: draining data for key: " << key_in_cache << dendl;
          auto r = drain(dpp, aio, object, cb, y);
          if (r < 0) {
            ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
            return r;
          }
        }
        // end-if (it != block.hostsList.end())
      }
    }
  } else {
    ldpp_dout(dpp, 10) << "DistributedStrategy::" << __func__ << "(): Block not found in BlockDirectory." << dendl;
    if (!block->is_head) {
      auto r = drain(dpp, aio, object, cb, y);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "DistributedStrategy::" << __func__ << "(): Error: failed to drain, ret=" << r << dendl;
        return r;
      }
    }
    return -ENOENT;
  }

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