#pragma once

#include "rgw_common.h"
#include "rgw_asio_thread.h"
#include "d4n_connection.h"

#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>
#include <condition_variable>
#include <deque>
#include <memory>
#include <concepts>

namespace rgw { namespace d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

inline int check_bool(std::string str) {
  if (str == "true" || str == "1") {
    return 1;
  } else if (str == "false" || str == "0") {
    return 0;
  } else {
    return -EINVAL;
  }
}


//FIXME: AMIN: should be moved to redis directory
class RedisPool {
public:
    RedisPool(boost::asio::io_context* ioc, const boost::redis::config& cfg, std::size_t size)
        :  m_ioc(ioc),m_cfg(cfg) {
        for (std::size_t i = 0; i < size; ++i) {
            // Each connection gets its own strand
            auto strand = boost::asio::make_strand(*m_ioc);
            auto conn = std::make_shared<boost::redis::connection>(strand);
            m_pool.push_back(conn);
        }
    }

    ~RedisPool() {
      cancel_all();
    }

    std::shared_ptr<boost::redis::connection> acquire(const DoutPrefixProvider* dpp = nullptr) {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);

	if (!m_is_pool_connected) {
		for(auto& it:m_pool) {
	    		auto conn = it;
	    		conn->async_run(m_cfg, {}, boost::asio::consign(boost::asio::detached, conn));
		}
	    m_is_pool_connected = true;
	}

        if (m_pool.empty()) {
		if (dpp) {
			maybe_warn_about_blocking(dpp);
		}
		//wait until m_pool is not empty
		m_cond_var.wait(lock, [this] { return !m_pool.empty(); });
        }
        auto conn = m_pool.front();
        m_pool.pop_front();
        return conn;
    }

    void release(std::shared_ptr<boost::redis::connection> conn) {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        m_pool.push_back(conn);
	// Notify one waiting thread that a connection is available
	m_cond_var.notify_one();
    }

    int current_pool_size() const {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        return m_pool.size();
    }

    void cancel_all() {
        std::unique_lock<std::mutex> lock(m_aquire_release_mtx);
        if(m_is_pool_connected) {
	for(auto& conn : m_pool) {
		conn->cancel();
        }
      }
    }

private:
    boost::asio::io_context* m_ioc;
    boost::redis::config m_cfg;
    std::deque<std::shared_ptr<boost::redis::connection>> m_pool;
    mutable std::mutex m_aquire_release_mtx;
    std::condition_variable m_cond_var;
    bool m_is_pool_connected{false};
};

class Pipeline {
  public:
    Pipeline(std::shared_ptr<boost::redis::connection>& conn, std::shared_ptr<RedisPool> redis_pool) : REDISconn(conn), redis_pool(redis_pool) {}
    void start() { pipeline_mode = true; }
    //executes all commands and sets pipeline mode to false
    int execute(const DoutPrefixProvider* dpp, optional_yield y);
    bool is_pipeline() { return pipeline_mode; }
    request& get_request() { return req; }

  private:
    std::shared_ptr<boost::redis::connection> REDISconn;
    std::shared_ptr<RedisPool> redis_pool{nullptr};
    request req;
    bool pipeline_mode{false};
};

//END FIXME

template<typename T>
  concept SeqContainer = requires(T& t, typename T::value_type v) {
      t.push_back(v);
  };

enum class ObjectFields { // Fields stored in object directory 
  ObjName,
  BucketName,
  CreationTime,
  Dirty,
  Hosts,
  Etag,
  ObjSize,
  UserID,
  DisplayName
};

enum class BlockFields { // Fields stored in block directory 
  BlockID,
  Version, 
  DeleteMarker,
  Size,
  GlobalWeight,
  ObjName,
  BucketName,
  CreationTime,
  Dirty,
  Hosts,
  Etag,
  ObjSize,
  UserID,
  DisplayName
};

struct CacheObj {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  std::string creationTime; /* Creation time of the S3 Object */
  bool dirty{false};
  std::unordered_set<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
  std::string etag; //etag needed for list objects
  uint64_t size; //total object size (and not block size), needed for list objects
  std::string user_id; // id of user, needed for list object versions
  std::string display_name; // display name of owner, needed for list object versions
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t blockID;
  std::string version;
  bool deleteMarker{false};
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0; /* LFUDA policy variable */
  /* Blocks use the cacheObj's dirty and hostsList metadata to store their dirty flag values and locations in the block directory. */
};

class Directory {
public:
    Directory() {}
	virtual ~Directory() = default;
};

class BucketDirectory: public Directory {
  public:
    BucketDirectory() {}
    virtual ~BucketDirectory() = default;
	
    virtual int zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr) = 0;
    virtual int zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y) = 0;
    virtual int zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y) = 0;
    virtual int zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y) = 0;
    virtual int zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y) = 0;

  private:
};

class ObjectDirectory: public Directory {
  public:
    ObjectDirectory(){}
    virtual ~ObjectDirectory() = default;
	
    virtual int exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) = 0;

    virtual int set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) = 0;
    virtual int get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) = 0;
    virtual int copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y) = 0;
    virtual int del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) = 0;
    virtual int update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y) = 0;
    virtual int zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, Pipeline* pipeline=nullptr) = 0;
    virtual int zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y) = 0;
    virtual int zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y) = 0;
    virtual int zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y) = 0;
    virtual int zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y) = 0;
    virtual int zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y) = 0;
    //Return value is the incremented value, else return error
    virtual int incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) = 0;

  private:

  protected:
    std::string build_index(CacheObj* object);
};

class BlockDirectory: public Directory {
  public:
    BlockDirectory(){}
    virtual ~BlockDirectory() = default;
    
	
    virtual int exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) = 0;

    //Pipelined version of set
    virtual int set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y) = 0;
    virtual int set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline=nullptr) = 0;
    virtual int get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) = 0;
    //FIXME: AMIN: we need to come up with a version for FDB
	//Pipelined version of get using boost::redis::response for list bucket
	/*
    template <size_t N = 100>
    int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);
	*/
    //Pipelined version of get using boost::redis::generic_response
    virtual int get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y) = 0;
    virtual int copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y) = 0;
    virtual int del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) = 0;
    virtual int update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y) = 0;
	
    virtual int remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y) = 0;
	
    virtual int zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y) = 0;
    virtual int zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y) = 0;
    virtual int zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y) = 0;
    virtual int zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y) = 0;

  private:

  protected:
    std::string build_index(CacheBlock* block);

};


} } // namespace rgw::d4n
