#pragma once

#include "d4n_policy.h"
#include "d4n_directory_fdb.h"
#include "d4n_connection.h"

namespace rgw { namespace d4n {

namespace asio = boost::asio;
namespace sys = boost::system;

class FDBLFUDAPolicy : public LFUDAPolicy {
  private:
    std::shared_ptr<FDBConnection> conn;
    FDBBlockDirectory* blockDir;
    FDBObjectDirectory* objDir;
    FDBBucketDirectory* bucketDir;

    virtual int age_sync(const DoutPrefixProvider* dpp, optional_yield y) override; 
    virtual int local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) override; 

  public:
    FDBLFUDAPolicy(std::shared_ptr<D4NConnection>& conn, rgw::cache::CacheDriver* cacheDriver, optional_yield y) : LFUDAPolicy(conn, cacheDriver, y), 
                                                         conn(std::dynamic_pointer_cast<FDBConnection>(conn)) 
    {
	  blockDir = new FDBBlockDirectory(this->conn);
	  objDir = new FDBObjectDirectory(this->conn);
	  bucketDir = new FDBBucketDirectory(this->conn);
    }
    ~FDBLFUDAPolicy() {
      delete bucketDir;
      delete blockDir;
      delete objDir;
    } 

    virtual int init(CephContext *cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver *_driver) override;
    virtual int eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) override;
    virtual void cleaning(const DoutPrefixProvider* dpp) override;
};

} } // namespace rgw::d4n
