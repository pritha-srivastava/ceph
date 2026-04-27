#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/system/detail/errc.hpp>

#include "d4n_policy_redis.h"
#include "d4n_policy_fdb.h"

namespace rgw { namespace d4n {

class D4NConnection;

class PolicyDriver {
  private:
    std::string policyName;
	std::unique_ptr<CachePolicy> cachePolicy;

  public:
    PolicyDriver(std::shared_ptr<D4NConnection>& conn, std::string directory_type,  rgw::cache::CacheDriver* cacheDriver, const std::string& _policyName, optional_yield y) : policyName(_policyName) 
    {
      if (policyName == "lfuda") {
		if (directory_type == "redis"){
		  cachePolicy = std::make_unique<RedisLFUDAPolicy>(conn, cacheDriver, y);
		}
		else if (directory_type == "fdb"){
		  cachePolicy = std::make_unique<FDBLFUDAPolicy>(conn, cacheDriver, y);
		}
      } else if (policyName == "lru") {
		cachePolicy = std::make_unique<LRUPolicy>(cacheDriver);
      }
    }

	~PolicyDriver() = default;

    CachePolicy* get_cache_policy() { return cachePolicy.get(); }
    std::string get_policy_name() { return policyName; }
};



} } // namespace rgw::d4n
