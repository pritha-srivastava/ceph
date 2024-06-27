#pragma once

#include "d4n_cache_strategy.h"
#include "rgw_sal_d4n.h"
#include "rgw_cache_driver.h"

namespace rgw { namespace d4n {

  class LocalStrategy : public CacheStrategy {
    public:
      LocalStrategy(rgw::cache::CacheDriver* cacheDriver, rgw::d4n::PolicyDriver* policyDriver): CacheStrategy(cacheDriver, policyDriver) {}
      virtual int init(CephContext *cct, const DoutPrefixProvider* dpp) override { return 0; }
      virtual int get(const DoutPrefixProvider* dpp, std::string key, uint64_t offset, uint64_t len,  RGWGetDataCB* cb, optional_yield y) override;
      virtual int put(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, std::string key, bufferlist bl, uint64_t len, rgw::sal::Attrs& attrs, const std::string& version, bool is_dirty, std::string& etag, optional_yield y) override;
      virtual int del(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, std::string key, optional_yield y) override;
      virtual int get_attrs(const DoutPrefixProvider* dpp, std::string key, rgw::sal::Attrs& attrs, optional_yield y) override;
      virtual int set_attrs(const DoutPrefixProvider* dpp, std::string key, const rgw::sal::Attrs& attrs, optional_yield y) override;
      virtual int delete_attrs(const DoutPrefixProvider* dpp, std::string key, const rgw::sal::Attrs& attrs, optional_yield y) override;
      virtual std::string get_version(const DoutPrefixProvider* dpp, std::string key, optional_yield y) override;
      virtual bool is_dirty(const DoutPrefixProvider* dpp, std::string key, optional_yield y) override;
      virtual void update_obj(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& version, bool is_dirty, std::string& etag) override;
  };
}} //rgw::d4n

    