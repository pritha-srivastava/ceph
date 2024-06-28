#pragma once

#include "d4n_cache_strategy.h"
#include "d4n_distributed.h"
#include "d4n_local.h"

namespace rgw::d4n {
  class PolicyDriver;
}

namespace rgw { namespace d4n {
class CacheStrategyManager {
  private:
    std::string strategy;
    CacheStrategy* cachestrategy;

  public:
    CacheStrategyManager(rgw::cache::CacheDriver* cacheDriver, rgw::d4n::ObjectDirectory* objDir, rgw::d4n::BlockDirectory* blockDir, rgw::d4n::PolicyDriver* policyDriver, std::string strategy) : strategy(strategy) 
    {
      if (strategy == "local") {
        cachestrategy = new LocalStrategy(cacheDriver, policyDriver);
      } else if (strategy == "d4n") {
	      cachestrategy = new DistributedStrategy(cacheDriver, objDir, blockDir, policyDriver);
      }
    }
    ~CacheStrategyManager() {
      delete cachestrategy;
    }

    CacheStrategy* get_cache_strategy() { return cachestrategy; }
    std::string get_strategy_name() { return strategy; }
};

}}//rgw::d4n