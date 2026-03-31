#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/system/detail/errc.hpp>

#include "d4n_directory.h"
#include "d4n_connection.h"
#include "rgw_sal_d4n.h"

#include "driver/cache/rgw_cache_driver.h"

namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;

static std::string empty = std::string();

class FDBLFUDAPolicy : public LFUDAPolicy {
  private:
    std::shared_ptr<FDBConnection> conn;
    FDBBlockDirectory* blockDir;
    FDBObjectDirectory* objDir;
    FDBBucketDirectory* bucketDir;

    virtual CacheBlock* get_victim_block(const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual int age_sync(const DoutPrefixProvider* dpp, optional_yield y) override; 
    virtual int local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) override; 
    virtual int delete_data_blocks(const DoutPrefixProvider* dpp, LFUDAObjEntry* e, optional_yield y) override;

  public:
    FDBLFUDAPolicy(std::shared_ptr<D4NConnection>& conn, rgw::cache::CacheDriver* cacheDriver, optional_yield y) : CachePolicy(), 
                                                                                                             y(y),
													     conn(conn), 
													     cacheDriver(cacheDriver)
    {
      blockDir = new FDBBlockDirectory{conn};
      objDir = new FDBObjectDirectory{conn};
      bucketDir = new FDBBucketDirectory{conn};
    }
    ~FDBLFUDAPolicy() {
      delete bucketDir;
      delete blockDir;
      delete objDir;
    } 

	/* FIXME: these functions should be implemnted in this class
    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver);
    virtual int exist_key(const std::string& key) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual bool update_refcount_if_key_exists(const DoutPrefixProvider* dpp, const std::string& key, uint8_t op, optional_yield y) override;
    virtual void update(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, std::optional<bool> dirty, uint8_t op, optional_yield y, std::string& restore_val=empty) override;
    virtual bool erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool _erase(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y);
    virtual void update_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, 
			    double creationTime, const rgw_user& user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
			    const rgw_obj_key& obj_key, uint8_t op, optional_yield y, std::string& restore_val=empty) override;
    virtual bool erase_dirty_object(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
    virtual bool invalidate_dirty_object(const DoutPrefixProvider* dpp, const std::string& key) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override;
	*/
};

} } // namespace rgw::d4n
