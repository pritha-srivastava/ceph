#include "d4n_policy.h"
#include "rgw_sal_d4n.h"
#include "d4n_directory.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "common/split.h"
#include "rgw_perf_counters.h"

namespace rgw { namespace d4n {

/* Changes state to INVALID for dirty objects. An INVALID state indicates that a delete request has been
 issued on an object and it must be deleted rather than written to the backend. This lazy deletion occurs
 in the Cleaning method and prevents data races during concurrent requests. The method below returns "false"
 if the state has not been set to INVALID, and "true" if it has. The state is not set to INVALID when
 cleaning is in progress, a process which writes the object to the backend store. */
bool LFUDAPolicy::invalidate_dirty_object(const DoutPrefixProvider* dpp, const std::string& key) {
  std::unique_lock<std::mutex> l(lfuda_cleaning_lock);

  if (o_entries_map.empty())
    return false;

  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): key=" << key << " not found" << dendl;
    return false;
  }

  if (p->second.second == State::INIT) {
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Setting State::INVALID for key=" << key << dendl;
    p->second.second = State::INVALID;
    int ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_INVALID, "1", y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Failed to set xattr, ret=" << ret << dendl;
      return false;
    }
    return true;
  } else if (p->second.second == State::IN_PROGRESS) {
    state_cond.wait(l, [this, &key]{ return (o_entries_map.find(key) == o_entries_map.end()); });
  }

  return false;
}

CacheBlock* LFUDAPolicy::get_victim_block(const DoutPrefixProvider* dpp, optional_yield y) {
  if (entries_heap.empty()) {
    return nullptr;
  }

  // Get victim cache block 
  LFUDAEntry* entry = entries_heap.top();
  std::string key = entry->key;
  CacheBlock* victim = new CacheBlock();

  auto parts = split(key, "#");
  std::vector<std::string> block_info;
  block_info.assign(parts.begin(), parts.end());
  
  if (block_info.size() != 5) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Key of the top entry in the min heap has not been constructed correctly." << dendl;
    return nullptr;
  }

  victim->cacheObj.bucketName = block_info[0]; 
  victim->version = block_info[1]; 
  victim->cacheObj.objName = block_info[2]; 
  victim->blockID = std::stoull(block_info[3]); 
  victim->size = std::stoull(block_info[4]); 

  /* check dirty flag of entry to be evicted, if the flag is dirty, all entries on the local node are dirty
    check refcount also, if refcount > 0 then no entries are available for eviction */
  if (entry->dirty || entry->refcount > 0) {
    ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Top entry in min heap is dirty or with positive refcount, no entry is available for eviction!" << dendl;
    return nullptr;
  }

  return victim;
}

int LFUDAPolicy::exist_key(const std::string& key) {
  const std::lock_guard l(lfuda_lock);
  if (entries_map.count(key) != 0) {
    return true;
  }

  return false;
}

int LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  int ret = -1;
  uint64_t freeSpace = cacheDriver->get_free_space(dpp, y);

  while (freeSpace < size) { // TODO: Think about parallel reads and writes; can this turn into an infinite loop?
    std::unique_lock<std::mutex> l(lfuda_lock);
    CacheBlock* victim = get_victim_block(dpp, y);
  
    if (victim == nullptr) {
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): Could not retrieve victim block." << dendl;
      delete victim;
      l.unlock();
      return -ENOSPC;
    }

    std::string key = entries_heap.top()->key;
    auto it = entries_map.find(key);
    if (it == entries_map.end()) {
      delete victim;
      l.unlock();
      return -ENOENT;
    }

    int avgWeight = weightSum / entries_map.size();
    /* the following part takes care of updating the weight (globalWeight) of the block if this is the last copy in a remote setup
       and is pushed out to a remote cache where space is available */
#if 0
    if (victim->cacheObj.hostsList.size() == 1 && *(victim->cacheObj.hostsList.begin()) == dpp->get_cct()->_conf->rgw_d4n_local_rgw_address) { // Last copy 
      if (victim->globalWeight) {
	it->second->localWeight += victim->globalWeight;
        (*it->second->handle)->localWeight = it->second->localWeight;
	entries_heap.decrease(it->second->handle); // larger value means node must be decreased to maintain min heap 
	if ((ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_LOCAL_WEIGHT, std::to_string(it->second->localWeight), y)) < 0) { 
	  delete victim;
	  return ret;
        }

	victim->globalWeight = 0;
      }

      if (it->second->localWeight > avgWeight) {
	// TODO: push victim block to remote cache
	// add remote cache host to host list
      }
    }

    victim->globalWeight += it->second->localWeight;
    if ((ret = blockDir->update_field(dpp, victim, "globalWeight", std::to_string(victim->globalWeight), y)) < 0) {
      delete victim;
      return ret;
    }
#endif
    //erase also updates weightSum, is the following needed?
    weightSum = (avgWeight * entries_map.size()) - it->second->localWeight;

    age = std::max(it->second->localWeight, age);
    _erase(dpp, key, y);

    l.unlock();

    //Need to get and then update the host atomically in a remote setup
    if ((ret = blockDir->remove_host(dpp, victim, dpp->get_cct()->_conf->rgw_d4n_local_rgw_address, y)) < 0) {
      delete victim;
      return ret;
    }

    delete victim;

    if ((ret = cacheDriver->delete_data(dpp, key, y)) < 0) {
      return ret;
    }

    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Block " << key << " has been evicted." << dendl;

    if (perfcounter) {
      perfcounter->inc(l_rgw_d4n_cache_evictions);
    }
    freeSpace = cacheDriver->get_free_space(dpp, y);
  }
  
  return 0;
}

bool LFUDAPolicy::update_refcount_if_key_exists(const DoutPrefixProvider* dpp, const std::string& key, uint8_t op, optional_yield y)
{
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updating refcount for entry: " << key << dendl;
  const std::lock_guard l(lfuda_lock);
  auto entry = find_entry(key);
  uint64_t refcount = 0;
  if (entry == nullptr) {
    return false;
  }
  refcount = entry->refcount;
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): old refcount is: " << refcount << dendl;
  if (op == RefCount::INCR) {
    refcount += 1;
  } else if (op == RefCount::DECR) {
    if (refcount > 0) {
      refcount -= 1;
    }
  }
  (*entry->handle)->refcount = refcount;
  entries_heap.update(entry->handle);
  ldpp_dout(dpp, 20) << "LFUDAPolicy::" << __func__ << "(): updated refcount is: " << (*entry->handle)->refcount << dendl;

  return true;
}

void LFUDAPolicy::update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, std::optional<bool> dirty, uint8_t op, optional_yield y, std::string& restore_val)
{
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): updating entry: " << key << dendl;
  using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
  const std::lock_guard l(lfuda_lock);
  int localWeight = age;
  auto entry = find_entry(key);
  bool updateLocalWeight = true;
  uint64_t refcount = 0;

  if (!restore_val.empty()) {
    updateLocalWeight = false;
    localWeight = std::stoull(restore_val);
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): restored localWeight is: " << localWeight << dendl;
  }

  /* check the dirty flag in the existing entry for the key and the incoming dirty flag. If the
     incoming dirty flag is false, that means update() is invoked as part of cleaning process,
     so we must not update its localWeight. */
  if (entry) {
    refcount = entry->refcount;
    if (entry->dirty && dirty.has_value()) {
      bool is_dirty = dirty.value();
      if (!is_dirty) {
        localWeight = entry->localWeight;
        updateLocalWeight = false;
      }
    }
    if (updateLocalWeight) {
      localWeight = entry->localWeight + age;
    }
    if (op == RefCount::INCR) {
      refcount += 1;
    }
    if (op == RefCount::DECR) {
      if (refcount > 0) {
        refcount -= 1;
      }
    }
  }
  //pick the existing value of dirty, if no value has been passed in
  bool is_dirty = false;
  if (dirty.has_value()) {
    is_dirty = dirty.value();
  } else if (entry) {
    is_dirty = entry->dirty;
  }
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): updated refcount is: " << refcount << dendl;

  if (entry) {
    entry->key = key;
    entry->offset = offset;
    entry->len = len;
    entry->version = version;
    entry->dirty = is_dirty;
    entry->refcount = refcount;
    entry->localWeight = localWeight;
    entries_heap.update(entry->handle, entry);
  } else {
    LFUDAEntry* e = new LFUDAEntry(key, offset, len, version, is_dirty, refcount, localWeight);
    handle_type handle = entries_heap.push(e);
    e->set_handle(handle);
    entries_map.emplace(key, e);
  }

  if (updateLocalWeight) {
    int ret = -1;
    if ((ret = cacheDriver->set_attr(dpp, key, RGW_CACHE_ATTR_LOCAL_WEIGHT, std::to_string(localWeight), y)) < 0) 
      ldpp_dout(dpp, 0) << "LFUDAPolicy::" << __func__ << "(): CacheDriver set_attr method failed, ret=" << ret << dendl;
  }

  weightSum += ((localWeight < 0) ? 0 : localWeight);
}

void LFUDAPolicy::update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id, const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val)
{
  using handle_type = boost::heap::fibonacci_heap<LFUDAObjEntry*, boost::heap::compare<ObjectComparator<LFUDAObjEntry>>>::handle_type;
  State state{State::INIT};
  ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): Before acquiring lock, adding entry: " << key << dendl;

  if (!restore_val.empty() && restore_val == "1") { // No need to set the xattr because this case only occurs when the state has
    state = State::INVALID;                         // been retrieved from the xattr itself.
    ldpp_dout(dpp, 10) << "LFUDAPolicy::" << __func__ << "(): State restored to INVALID." << dendl;
  } else {
    state = State::INIT;
  }

  const std::lock_guard l(lfuda_cleaning_lock);
  LFUDAObjEntry* e = new LFUDAObjEntry{key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key};
  handle_type handle = object_heap.push(e);
  e->set_handle(handle);
  o_entries_map.emplace(key, std::make_pair(e, state));
  cond.notify_one();
}

bool LFUDAPolicy::_erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }

  weightSum -= ((p->second->localWeight < 0) ? 0 : p->second->localWeight);

  entries_heap.erase(p->second->handle);
  delete p->second;
  p->second = nullptr;
  entries_map.erase(p);
  
  return true;
}

bool LFUDAPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_lock);
  return _erase(dpp, key, y);
}

bool LFUDAPolicy::erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lfuda_cleaning_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }

  object_heap.erase(p->second.first->handle);
  delete p->second.first;
  p->second.first = nullptr;
  o_entries_map.erase(p);
  state_cond.notify_one();

  return true;
}

int LFUDAPolicy::delete_data_blocks(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, optional_yield y) {
  off_t lst = e->size, fst = 0;

  do {
    if (fst >= lst) {
      break;
    }
    off_t cur_size = std::min<off_t>(fst + dpp->get_cct()->_conf->rgw_max_chunk_size, lst);
    off_t cur_len = cur_size - fst;
    std::string oid_in_cache = rgw::sal::get_key_in_cache(e->key, std::to_string(fst), std::to_string(cur_len));

    int ret = -1;
    std::unique_lock<std::mutex> ll(lfuda_lock);
    auto it = entries_map.find(oid_in_cache);
    if (it != entries_map.end()) {
      if (it->second->refcount > 0) {
        return -EBUSY;//better error code?
      }
    }
    ll.unlock();
    if ((ret = cacheDriver->delete_data(dpp, oid_in_cache, y)) == 0) {
      if (!(ret = erase(dpp, oid_in_cache, y))) {
	ldpp_dout(dpp, 0) << "Failed to delete policy entry for: " << oid_in_cache << ", ret=" << ret << dendl;
        return -EINVAL;
      }
    } else {
      ldpp_dout(dpp, 0) << "Failed to delete data block " << oid_in_cache << ", ret=" << ret << dendl;
      return -EINVAL;
    }

    fst += cur_len;
  } while (fst < lst);

  return 0;
}

int LRUPolicy::exist_key(const std::string& key)
{
  const std::lock_guard l(lru_lock);
  if (entries_map.count(key) != 0) {
      return true;
    }
    return false;
}

int LRUPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  uint64_t freeSpace = cacheDriver->get_free_space(dpp, y);

  while (freeSpace < size) {
    auto p = entries_lru_list.front();
    entries_map.erase(entries_map.find(p.key));
    entries_lru_list.pop_front_and_dispose(Entry_delete_disposer());
    auto ret = cacheDriver->delete_data(dpp, p.key, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << "(): Failed to delete data from the cache backend, ret=" << ret << dendl;
      return ret;
    }

    freeSpace = cacheDriver->get_free_space(dpp, y);
  }

  return 0;
}

void LRUPolicy::update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, std::optional<bool> dirty, uint8_t op, optional_yield y, std::string& restore_val)
{
  const std::lock_guard l(lru_lock);
  _erase(dpp, key, y);
  bool is_dirty = false;
  if (dirty.has_value()) {
    is_dirty = dirty.value();
  }
  Entry* e = new Entry(key, offset, len, version, is_dirty, 0);
  entries_lru_list.push_back(*e);
  entries_map.emplace(key, e);
}

void LRUPolicy::update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val)
{
  const std::lock_guard l(lru_lock);
  ObjEntry* e = new ObjEntry(key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key);
  o_entries_map.emplace(key, e);
  return;
}


bool LRUPolicy::erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  return _erase(dpp, key, y);
}

bool LRUPolicy::erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  const std::lock_guard l(lru_lock);
  auto p = o_entries_map.find(key);
  if (p == o_entries_map.end()) {
    return false;
  }
  o_entries_map.erase(p);
  return true;
}

bool LRUPolicy::_erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  auto p = entries_map.find(key);
  if (p == entries_map.end()) {
    return false;
  }
  entries_map.erase(p);
  entries_lru_list.erase_and_dispose(entries_lru_list.iterator_to(*(p->second)), Entry_delete_disposer());
  return true;
}


} } // namespace rgw::d4n
