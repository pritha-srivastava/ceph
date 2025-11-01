#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/system/detail/errc.hpp>

#include <aio.h>
#include "rgw_common.h"
#include "rgw_sal_d4n.h"

namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;

inline std::string get_resource(std::string& bucket_name, std::string& oid) {
  return fmt::format("{}{}{}", bucket_name, "/", oid);
}

class RemoteGetCB : public RGWHTTPStreamRWRequest::ReceiveCB {
public:
  bufferlist *in_bl;
  RemoteGetCB(bufferlist* _bl): in_bl(_bl) {}
  int handle_data(bufferlist& bl, bool *pause) override {
    this->in_bl->append(bl);
    return 0;
  }
};

class RemoteCachePut {
  public:
    struct RemoteCachePutOp {
      std::string bucket_name;
      std::string oid;
      uint64_t offset;
      uint64_t len;
      std::string version;
      rgw_user bucket_owner;
      std::string remote_addr;
      uint64_t obj_size;
    };
    RemoteCachePut(rgw::sal::Driver* driver, RemoteCachePutOp& op) : driver(driver), op(op) {}
    virtual ~RemoteCachePut() = default; 

    int init(CephContext* cct, const DoutPrefixProvider* dpp);
    int send_request(const DoutPrefixProvider* dpp, bufferlist& bl, optional_yield& y);

  private:
    rgw::sal::Driver* driver;
    RemoteCachePutOp op;
};

} } // namespace rgw::d4n
