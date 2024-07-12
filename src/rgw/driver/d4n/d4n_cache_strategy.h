#pragma once

#include "d4n_directory.h"
#include "rgw_cache_driver.h"

class RGWGetDataCB;

namespace rgw::sal {
  struct D4NFilterBlock;
  class D4NFilterObject;
}

namespace rgw::d4n {
  class PolicyDriver;
}

namespace rgw { namespace d4n {

class CacheStrategy {
  public:
    CacheStrategy() = default;
    virtual ~CacheStrategy() {}
    virtual int initialize(CephContext *cct, const DoutPrefixProvider* dpp) = 0;
    virtual int get(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterBlock* block, rgw::Aio* aio, RGWGetDataCB* cb, uint64_t read_offset, uint64_t read_len, optional_yield y) = 0;
    virtual int put(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterBlock* block, const std::string& key, optional_yield y) = 0;
    virtual int del(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& key, optional_yield y) = 0;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual std::string get_version(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual bool is_dirty(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
};

}}//rgw::d4n