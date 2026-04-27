#include <algorithm>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory_fdb.h"

namespace rgw { namespace d4n {

using fdb_conn = lfdb::database;

/*
struct initiate_exec {
  std::shared_ptr<fdb_conn> conn;
};
*/

int FDBBucketDirectory::zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, Pipeline* pipeline)
{
  return 0;
}

int FDBBucketDirectory::zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y)
{
  return 0;
}

int FDBBucketDirectory::zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBBucketDirectory::zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y)
{
  return 0;
}

int FDBBucketDirectory::zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);
  return 0;
}

int FDBObjectDirectory::set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  return 0;
}

int FDBObjectDirectory::copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  return 0; 
}

int FDBObjectDirectory::update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, Pipeline* pipeline)
{
  return 0;
}

int FDBObjectDirectory::zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y)
{
  return 0;
}

int FDBObjectDirectory::incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  return 0;
}

template<SeqContainer Container>
int FDBBlockDirectory::set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& fdbValues, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{
  return 0;
}


int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline)
{
  return 0;
}

//explicit instantiation for 100 elements
/*
template int FDBBlockDirectory::get<100>(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y);

template <size_t N>
int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{
  return 0;
}
*/

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  return 0;
}


int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{
  return 0;
}


int FDBBlockDirectory::copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{
	return 0;
}

int FDBBlockDirectory::del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y)
{
  return 0; 
}

int FDBBlockDirectory::update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  return 0;
}

int FDBBlockDirectory::zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y)
{
  return 0;
}


} } // namespace rgw::d4n
