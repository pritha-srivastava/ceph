#include "d4n_policy_redis.h"
#include "rgw_sal_d4n.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "common/split.h"
#include "rgw_perf_counters.h"

namespace rgw::d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, const boost::redis::request& req, Response& resp)
  {
    auto h = asio::consign(std::move(handler), conn);
    return asio::dispatch(get_executor(),
        [c=conn, &req, &resp, h=std::move(h)] () mutable {
          c->async_exec(req, resp, std::move(h));
        });
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn)}, token, req, resp);
}

template <typename... Types>
static inline void redis_exec(std::shared_ptr<connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<Types...>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

int RedisLFUDAPolicy::init(CephContext* cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) {
  response<int, int, int, int> resp;
  static auto obj_callback = [this](
          const DoutPrefixProvider* dpp, const std::string& key, const std::string& version, bool deleteMarker, uint64_t size, 
			    double creationTime, const rgw_user user, const std::string& etag, const std::string& bucket_name, const std::string& bucket_id,
			    const rgw_obj_key& obj_key, optional_yield y, std::string& restore_val) {
    update_dirty_object(dpp, key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key, RefCount::NOOP, y, restore_val);
  };

  static auto block_callback = [this](
          const DoutPrefixProvider* dpp, const std::string& key, uint64_t offset, uint64_t len, const std::string& version, bool dirty, optional_yield y, std::string& restore_val) {
    update(dpp, key, offset, len, version, dirty, RefCount::NOOP, y, restore_val);
  };

  cacheDriver->restore_blocks_objects(dpp, obj_callback, block_callback);

  driver = _driver;
  if (dpp->get_cct()->_conf->d4n_writecache_enabled) {
    quit = false;
    tc = std::thread(&CachePolicy::cleaning, this, dpp);
  }

  lwthread = std::thread(&LFUDAPolicy::localweight_writer, this, dpp);
  lw_quit = false;

  try {
    boost::system::error_code ec;
    response<
      ignore_t,
      ignore_t,
      ignore_t,
      response<std::optional<int>, std::optional<int>>
    > resp;
    request req;
    req.push("MULTI");
    req.push("HSET", "lfuda", "minLocalWeights_sum", std::to_string(weightSum), /* New cache node will always have the minimum average weight */
              "minLocalWeights_size", std::to_string(entries_map.size()), 
              "minLocalWeights_address", dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
    req.push("HSETNX", "lfuda", "age", age); /* Only set maximum age if it doesn't exist */
    req.push("EXEC");
  
    redis_exec(conn->get_redis_conn(), ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisLFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisLFUDAPolicy::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  asio::co_spawn(io_context.get_executor(),
		   directory_sync(dpp, y), asio::detached);

  return 0;
}

int RedisLFUDAPolicy::age_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  response< std::optional<std::string> > resp;

  try { 
    boost::system::error_code ec;
    request req;
    req.push("HGET", "lfuda", "age");
      
    redis_exec(conn->get_redis_conn(), ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisLFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    return -EINVAL;
  }

  if (std::get<0>(resp).value().value().empty() || age > std::stoi(std::get<0>(resp).value().value())) { /* Set new maximum age */
    try { 
      boost::system::error_code ec;
      response<ignore_t> ret;
      request req;
      req.push("HSET", "lfuda", "age", age);

      redis_exec(conn->get_redis_conn(), ec, req, ret, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "RedisLFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    } catch (std::exception &e) {
      return -EINVAL;
    }
  } else {
    age = std::stoi(std::get<0>(resp).value().value());
  }

  return 0;
}

int RedisLFUDAPolicy::local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  if (fabs(weightSum - postedSum) > (postedSum * 0.1)) {
    response<std::vector<std::string>> resp;

    try { 
      boost::system::error_code ec;
      request req;
      req.push("HMGET", "lfuda", "minLocalWeights_sum", "minLocalWeights_size");
	
      redis_exec(conn->get_redis_conn(), ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "RedisLFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    } catch (std::exception &e) {
      return -EINVAL;
    }
  
    float minAvgWeight = std::stof(std::get<0>(resp).value()[0]) / std::stof(std::get<0>(resp).value()[1]);
    float localAvgWeight = 0;
    if (entries_map.size())
      localAvgWeight = static_cast<float>(weightSum) / static_cast<float>(entries_map.size());

    if (localAvgWeight < minAvgWeight) { /* Set new minimum weight */
      try { 
	boost::system::error_code ec;
	response<ignore_t> resp;
	request req;
	req.push("HSET", "lfuda", "minLocalWeights_sum", std::to_string(weightSum), 
                  "minLocalWeights_size", std::to_string(entries_map.size()), 
                  "minLocalWeights_address", dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);

	redis_exec(conn->get_redis_conn(), ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "RedisLFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      } catch (std::exception &e) {
	return -EINVAL;
      }
    } else {
      weightSum = std::stoi(std::get<0>(resp).value()[0]);
      postedSum = std::stoi(std::get<0>(resp).value()[0]);
    }
  }

  try { /* Post update for local cache */
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push("HSET", dpp->get_cct()->_conf->rgw_d4n_local_rgw_address, "avgLocalWeight_sum", std::to_string(weightSum), 
              "avgLocalWeight_size", std::to_string(entries_map.size()));

    redis_exec(conn->get_redis_conn(), ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisLFUDAPolicy::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    return 0;
  } catch (std::exception &e) {
    return -EINVAL;
  }
}

} // namespace rgw::d4n
