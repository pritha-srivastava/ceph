#pragma once

#include "d4n_policy.h"
#include "d4n_directory_redis.h"
#include "d4n_connection.h"


namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;


class RedisLFUDAPolicy : public LFUDAPolicy {
  private:
    std::shared_ptr<RedisConnection> conn;
    RedisBlockDirectory* blockDir;
    RedisObjectDirectory* objDir;
    RedisBucketDirectory* bucketDir;

    //virtual CacheBlock* get_victim_block(const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual int age_sync(const DoutPrefixProvider* dpp, optional_yield y) override; 
    virtual int local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) override ; 
    asio::awaitable<void> redis_sync(const DoutPrefixProvider* dpp, optional_yield y);

  public:
    RedisLFUDAPolicy(std::shared_ptr<D4NConnection>& conn, rgw::cache::CacheDriver* cacheDriver, optional_yield y) : LFUDAPolicy(conn, cacheDriver, y), 
													     conn(std::dynamic_pointer_cast<RedisConnection>(conn))
    {
	  blockDir = new RedisBlockDirectory(this->conn);
	  objDir = new RedisObjectDirectory(this->conn);
	  bucketDir = new RedisBucketDirectory(this->conn);
    }
    ~RedisLFUDAPolicy() {
      rthread_stop();
      delete bucketDir;
      delete blockDir;
      delete objDir;
      quit = true;
      cond.notify_all();
      if (tc.joinable()) { tc.join(); }
    } 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override;
};

} } // namespace rgw::d4n
