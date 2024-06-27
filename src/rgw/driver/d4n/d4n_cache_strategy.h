#pragma once

#include "d4n_directory.h"
#include "rgw_sal_d4n.h"
#include "rgw_cache_driver.h"

namespace rgw { namespace d4n {

class CacheStrategy {
  protected:
    rgw::cache::CacheDriver* cacheDriver;
    rgw::d4n::PolicyDriver* policyDriver;

  public:
    CacheStrategy(rgw::cache::CacheDriver* cacheDriver, rgw::d4n::PolicyDriver* policyDriver): cacheDriver(cacheDriver), policyDriver(policyDriver) {}
    virtual ~CacheStrategy() {}
    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp) = 0;
    virtual int get(const DoutPrefixProvider* dpp, std::string key, uint64_t offset, uint64_t len,  RGWGetDataCB* cb, optional_yield y) = 0;
    virtual int put(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, std::string key, bufferlist bl, uint64_t len, rgw::sal::Attrs& attrs, const std::string& version, bool is_dirty, std::string& etag, optional_yield y) = 0;
    virtual int del(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, std::string key, optional_yield y) = 0;
    virtual int get_attrs(const DoutPrefixProvider* dpp, std::string key, rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int set_attrs(const DoutPrefixProvider* dpp, std::string key, const rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, std::string key, const rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual std::string get_version(const DoutPrefixProvider* dpp, std::string key, optional_yield y) = 0;
    virtual bool is_dirty(const DoutPrefixProvider* dpp, std::string key, optional_yield y) = 0;
    virtual void update_obj(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& version, bool is_dirty, std::string& etag) = 0;
};

class CacheStrategyManager {
  private:
    std::string strategy;
    CacheStrategy* cachestrategy;

  public:
    CacheStrategyManager(rgw::cache::CacheDriver* cacheDriver, rgw::d4n::PolicyDriver* policyDriver, std::string strategy) : strategy(strategy) 
    {
      if (strategy == "local") {
        //cachestrategy = new LFUDAPolicy(conn, cacheDriver);
      } else if (strategy == "d4n") {
	      //cachePolicy = new LRUPolicy(conn, cacheDriver);
      }
    }
    ~CacheStrategyManager() {
      //delete cachestrategy;
    }

    CacheStrategy* get_cache_strategy() { return cachestrategy; }
    std::string get_strategy_name() { return strategy; }
};
}}//rgw::d4n