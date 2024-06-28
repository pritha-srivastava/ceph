#pragma once

#include "rgw_cache_driver.h"
#include "d4n_cache_strategy.h"

namespace rgw::sal {
  class D4NFilterObject;
  struct D4NFilterBlock;
}

namespace rgw::d4n {
  class PolicyDriver;
}

namespace rgw { namespace d4n {

  class DistributedStrategy : public CacheStrategy {
    private:
      int update_head(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterBlock* block, const std::string& key, optional_yield y);
      int update_obj(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& key, bool dirty, const std::string& version, optional_yield y);
    protected:
      rgw::cache::CacheDriver* cacheDriver;
      rgw::d4n::ObjectDirectory* objDir;
      rgw::d4n::BlockDirectory* blockDir;
      rgw::d4n::PolicyDriver* policyDriver;
    public:
      DistributedStrategy(rgw::cache::CacheDriver* cacheDriver, rgw::d4n::ObjectDirectory* objDir, rgw::d4n::BlockDirectory* blockDir, rgw::d4n::PolicyDriver* policyDriver) : cacheDriver(cacheDriver), objDir(objDir), blockDir(blockDir), policyDriver(policyDriver) {}
      virtual int initialize(CephContext *cct, const DoutPrefixProvider* dpp) override;
      virtual int get(const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, optional_yield y) override;
      virtual int put(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterBlock* block, const std::string& key, optional_yield y) override;
      virtual int del(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& key, optional_yield y) override;
      virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
      virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
      virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
      virtual std::string get_version(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
      virtual bool is_dirty(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) override;
  };
}} //rgw::d4n