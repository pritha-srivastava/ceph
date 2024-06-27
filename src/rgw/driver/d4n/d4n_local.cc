#include "d4n_local.h"
#include "common/dout.h"

namespace rgw { namespace d4n {

int LocalStrategy::get(const DoutPrefixProvider* dpp, std::string key, uint64_t offset, uint64_t len,  RGWGetDataCB* cb, optional_yield y)
{
  return 0;
}

int LocalStrategy::put(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, std::string key, bufferlist bl, uint64_t len, rgw::sal::Attrs& attrs, const std::string& version, bool is_dirty, std::string& etag, optional_yield y)
{
  int ret;
  std::string key_in_cache;
  uint64_t size = len + attrs.size();
  if (is_dirty) { //data is being written to the write-back cache
    if (policyDriver->get_cache_policy()->is_write_space_available(dpp, size)) {
      key_in_cache = "D_" + key;
      ret = 0;
    } else {
      ret = -ENOSPC;
    }
  } else { //data is being written to the read cache
    ret = policyDriver->get_cache_policy()->eviction(dpp, size, y);
    key_in_cache = key;
  }
  if (ret == 0) {
    bufferlist bl;
    ret = cacheDriver->put(dpp, key_in_cache, bl, 0, attrs, y);
  } else {
    ldpp_dout(dpp, 0) << "LocalStrategy::" << __func__ << "(): Failed to evict data/no write-back cache space, ret=" << ret << dendl;
    return ret;
  }

  policyDriver->get_cache_policy()->update(dpp, key, 0, bl.length(), version, is_dirty, size, y);

  return 0;
}

int LocalStrategy::del(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, std::string key, optional_yield y)
{
  return 0;
}

int LocalStrategy::get_attrs(const DoutPrefixProvider* dpp, std::string key, rgw::sal::Attrs& attrs, optional_yield y)
{
  return 0;
}

int LocalStrategy::set_attrs(const DoutPrefixProvider* dpp, std::string key, const rgw::sal::Attrs& attrs, optional_yield y)
{
  return 0;
}

int LocalStrategy::delete_attrs(const DoutPrefixProvider* dpp, std::string key, const rgw::sal::Attrs& attrs, optional_yield y)
{
  return 0;
}

std::string LocalStrategy::get_version(const DoutPrefixProvider* dpp, std::string key, optional_yield y)
{
  return std::string("");
}

bool LocalStrategy::is_dirty(const DoutPrefixProvider* dpp, std::string key, optional_yield y)
{
  return false;
}

void LocalStrategy::update_obj(const DoutPrefixProvider* dpp, rgw::sal::D4NFilterObject* object, const std::string& version, bool is_dirty, std::string& etag)
{

}

}} //rgw::d4n