#include "d4n_remote_cache_manager.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"

namespace rgw { namespace d4n {

//placeholder for any initialization that is needed
int RemoteCachePut::init(CephContext* cct, const DoutPrefixProvider* dpp)
{
  return 0;
}

int RemoteCachePut::send_request(const DoutPrefixProvider* dpp, bufferlist& bl, optional_yield& y)
{
  in_bl.clear();
  cb = std::make_unique<RemoteGetCB>(&in_bl);

  RGWAccessKey accessKey;
  std::string findKey;

  std::unique_ptr<rgw::sal::User> c_user = driver->get_user(op.bucket_owner);
  int ret = c_user->load_user(dpp, y);
  if (ret < 0) {
    return -EPERM;
  }

  if (c_user->get_info().access_keys.empty()) {
    return -EINVAL;
  }

  accessKey.id = c_user->get_info().access_keys.begin()->second.id;
  accessKey.key = c_user->get_info().access_keys.begin()->second.key;

  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;
  extra_headers["x-rgw-remote-cache-request"] = "true";
  extra_headers["x-rgw-cache-object-version"] = op.version;
  extra_headers["x-rgw-cache-blk-offset"] = std::to_string(op.offset);
  extra_headers["x-rgw-cache-blk-len"] = std::to_string(op.len);
  extra_headers["x-rgw-cache-obj-size"] = std::to_string(op.obj_size);

  auto resource = get_resource(op.bucket_name, op.oid);
  sender = std::make_unique<RGWRESTStreamRWRequest>(dpp->get_cct(), "PUT", op.remote_addr, cb.get(), nullptr, nullptr, "", host_style);

  ret = sender->send_request(dpp, &accessKey, extra_headers, resource, nullptr, &bl);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RemoteCachePut::complete_request(const DoutPrefixProvider* dpp, optional_yield& y)
{
  if (!sender) {
    return -EINVAL;
  }

  int ret = sender->complete_request(dpp, y);
  sender.reset();
  return ret;
}

} } // namespace rgw::d4n
