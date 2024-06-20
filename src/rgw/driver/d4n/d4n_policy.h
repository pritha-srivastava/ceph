#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/system/detail/errc.hpp>

#include "d4n_directory.h"
#include "rgw_sal_d4n.h"
#include "rgw_cache_driver.h"

namespace rgw::sal {
  class D4NFilterObject;
}

namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;

class CachePolicy {
  protected:
    BlockDirectory* blockDir;
    ObjectDirectory* objDir;
    rgw::sal::Driver *driver;
    rgw::cache::CacheDriver* cacheDriver;
    optional_yield y = null_yield;
    std::mutex cleaning_lock;
    std::condition_variable cond;
    bool quit{false};
    std::thread tc;
    uint64_t cur_read_cache_size{0};
    uint64_t cur_write_cache_size{0};
    uint64_t total_cache_size{0};
    uint64_t max_read_cache_size{0};
    uint64_t max_write_cache_size{0};
    struct Entry : public boost::intrusive::list_base_hook<> {
      std::string key;
      uint64_t offset;
      uint64_t len;
      std::string version;
      bool dirty;
      uint64_t total_size; //this includes size of attrs or attrs + data len, len can be used later for this also if there is no need for this extra variable
      Entry(std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, uint64_t total_size) : key(key), offset(offset), 
											        len(len), version(version), dirty(dirty), total_size(total_size) {}
      };

    template<typename T>
    struct ObjectComparator {
      bool operator()(T* const e1, T* const e2) const {
        // order the min heap using creationTime
        return e1->creationTime > e2->creationTime;
      }
    };

    struct ObjEntry {
      using handle_type = boost::heap::fibonacci_heap<ObjEntry*, boost::heap::compare<ObjectComparator<ObjEntry>>>::handle_type;
      handle_type handle;
      std::string key;
      std::string version;
      bool dirty;
      uint64_t size;
      time_t creationTime;
      rgw_user user;
      std::string etag;
      std::string bucket_name;
      rgw_obj_key obj_key;
      ObjEntry() = default;
      ObjEntry(std::string& key, std::string version, bool dirty, uint64_t size, 
        time_t creationTime, rgw_user user, std::string& etag, 
        const std::string& bucket_name, const rgw_obj_key& obj_key) : key(key), version(version), dirty(dirty), size(size), 
									      creationTime(creationTime), user(user), etag(etag), 
									      bucket_name(bucket_name), obj_key(obj_key) {}
      void set_handle(handle_type handle_) { handle = handle_; }
    };
  
    using Object_Heap = boost::heap::fibonacci_heap<ObjEntry*, boost::heap::compare<ObjectComparator<ObjEntry>>>;
    Object_Heap object_heap; //This heap contains dirty objects ordered by their creation time, used for cleaning method
    std::unordered_map<std::string, ObjEntry*> o_entries_map; //Contains only dirty objects, used for look-up
  
    virtual int evict_for_clean_block(const DoutPrefixProvider* dpp, std::string key, uint64_t total_size) = 0;

  public:
    CachePolicy(rgw::cache::CacheDriver* cacheDriver) : cacheDriver(cacheDriver) {}
    virtual ~CachePolicy() {
      std::lock_guard l(cleaning_lock);
      quit = true;
      cond.notify_all();
    }

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver);
    virtual int exist_key(std::string key) = 0;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) = 0;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, uint64_t total_size, optional_yield y) = 0;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, 
			    time_t creationTime, const rgw_user user, std::string& etag, const std::string& bucket_name, 
			    const rgw_obj_key& obj_key, optional_yield y) = 0;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual void cleaning(const DoutPrefixProvider* dpp);
    virtual bool is_write_space_available(const DoutPrefixProvider* dpp, uint64_t size);
    virtual bool is_read_space_available(const DoutPrefixProvider* dpp, uint64_t size);
};

class LFUDAPolicy : public CachePolicy {
  private:
    template<typename T>
    struct EntryComparator {
      bool operator()(T* const e1, T* const e2) const {
        // order the min heap using localWeight and dirty flag so that dirty blocks are at the bottom
        if ((e1->dirty && e2->dirty) || (!e1->dirty && !e2->dirty)) {
	        return e1->localWeight > e2->localWeight;
        } else if (e1->dirty && !e2->dirty){
          return true;
        } else if (!e1->dirty && e2->dirty) {
          return false;
        } else {
          return e1->localWeight > e2->localWeight;
        }
      }
    }; 

    struct LFUDAEntry : public Entry {
      int localWeight;
      using handle_type = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>::handle_type;
      handle_type handle;

      LFUDAEntry(std::string& key, uint64_t offset, uint64_t len, std::string& version, bool dirty, int localWeight, uint64_t total_size) : Entry(key, offset, len, version, dirty, total_size), 
														       localWeight(localWeight) {}
      
      void set_handle(handle_type handle_) { handle = handle_; }
    };

    using Heap = boost::heap::fibonacci_heap<LFUDAEntry*, boost::heap::compare<EntryComparator<LFUDAEntry>>>;
    Heap entries_heap;
    std::unordered_map<std::string, LFUDAEntry*> entries_map;
    std::mutex lfuda_lock;

    int age = 1, weightSum = 0, postedSum = 0;
    optional_yield y = null_yield;
    std::shared_ptr<connection> conn;
    std::optional<asio::steady_timer> rthread_timer;

    CacheBlock* get_victim_block(const DoutPrefixProvider* dpp, optional_yield y);
    int age_sync(const DoutPrefixProvider* dpp, optional_yield y); 
    int local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y); 
    asio::awaitable<void> redis_sync(const DoutPrefixProvider* dpp, optional_yield y);
    void rthread_stop() {
      std::lock_guard l{lfuda_lock};

      if (rthread_timer) {
	rthread_timer->cancel();
      }
    }
    LFUDAEntry* find_entry(std::string key) { 
      auto it = entries_map.find(key); 
      if (it == entries_map.end())
        return nullptr;
      return it->second;
    }
    virtual int evict_for_clean_block(const DoutPrefixProvider* dpp, std::string key, uint64_t total_size) override { return 0; }
  public:
    LFUDAPolicy(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver) : CachePolicy(cacheDriver), 
											   conn(conn)
    {
      blockDir = new BlockDirectory{conn};
      objDir = new ObjectDirectory{conn};
    }
    ~LFUDAPolicy() {
      rthread_stop();
      delete blockDir;
      delete objDir;
    } 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver);
    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, uint64_t total_size, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    void save_y(optional_yield y) { this->y = y; }
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, 
			    time_t creationTime, const rgw_user user, std::string& etag, const std::string& bucket_name, 
			    const rgw_obj_key& obj_key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y);
    //virtual void cleaning(const DoutPrefixProvider* dpp) override;
    ObjEntry* find_obj_entry(std::string key) {
      auto it = o_entries_map.find(key);
      if (it == o_entries_map.end()) {
        return nullptr;
      }
      return it->second;
    }
};

class LRUPolicy : public CachePolicy {
  private:
    template<typename T>
    struct EntryComparator {
      bool operator()(T* const e1, T* const e2) const {
        // order the min heap using access time and dirty flag so that dirty blocks are at the bottom
        if ((e1->dirty && e2->dirty) || (!e1->dirty && !e2->dirty)) {
	        return e1->access_time > e2->access_time;
        } else if (e1->dirty && !e2->dirty){
          return true;
        } else if (!e1->dirty && e2->dirty) {
          return false;
        } else {
          return e1->access_time > e2->access_time;
        }
      }
    };

    struct LRUEntry : public Entry {
      uint64_t access_time;
      using handle_type = boost::heap::fibonacci_heap<LRUEntry*, boost::heap::compare<EntryComparator<LRUEntry>>>::handle_type;
      handle_type handle;

      LRUEntry(std::string& key, uint64_t offset, uint64_t len, std::string& version, bool dirty, uint64_t total_size) : Entry(key, offset, len, version, dirty, total_size)
														      { access_time = get_current_timestamp(); }

      void set_handle(handle_type handle_) { handle = handle_; }
    };

    using Heap = boost::heap::fibonacci_heap<LRUEntry*, boost::heap::compare<EntryComparator<LRUEntry>>>;
    std::unordered_map<std::string, LRUEntry*> entries_map;
    std::mutex lru_lock;
    Heap entries_heap;
    std::shared_ptr<connection> conn;

    bool _erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y);
    static inline uint64_t get_current_timestamp() { return std::chrono::system_clock::now().time_since_epoch().count(); }
    LRUEntry* find_entry(std::string key) { 
      auto it = entries_map.find(key); 
      if (it == entries_map.end())
        return nullptr;
      return it->second;
    }
    int evict_top_element(const DoutPrefixProvider* dpp);
    virtual int evict_for_clean_block(const DoutPrefixProvider* dpp, std::string key, uint64_t total_size) override;

  public:
    LRUPolicy(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver) : CachePolicy(cacheDriver), conn(conn)
    {
      blockDir = new BlockDirectory{conn};
      objDir = new ObjectDirectory{conn};
    }

    ~LRUPolicy()
    {
      delete blockDir;
      delete objDir;
    }
    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver); 
    virtual int exist_key(std::string key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, std::string& key, uint64_t offset, uint64_t len, std::string version, bool dirty, uint64_t total_size, optional_yield y) override;
    virtual void updateObj(const DoutPrefixProvider* dpp, std::string& key, std::string version, bool dirty, uint64_t size, 
			    time_t creationTime, const rgw_user user, std::string& etag, const std::string& bucket_name, 
			    const rgw_obj_key& obj_key, optional_yield y) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool eraseObj(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
};

class PolicyDriver {
  private:
    std::string policyName;
    CachePolicy* cachePolicy;

  public:
    PolicyDriver(std::shared_ptr<connection>& conn, rgw::cache::CacheDriver* cacheDriver, std::string _policyName) : policyName(_policyName) 
    {
      if (policyName == "lfuda") {
	cachePolicy = new LFUDAPolicy(conn, cacheDriver);
      } else if (policyName == "lru") {
	cachePolicy = new LRUPolicy(conn, cacheDriver);
      }
    }
    ~PolicyDriver() {
      delete cachePolicy;
    }

    CachePolicy* get_cache_policy() { return cachePolicy; }
    std::string get_policy_name() { return policyName; }
};

} } // namespace rgw::d4n
