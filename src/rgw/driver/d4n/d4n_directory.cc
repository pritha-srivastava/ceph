#include <boost/asio/consign.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory.h"

namespace rgw { namespace d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, const boost::redis::request& req, Response& resp)
  {
    auto h = boost::asio::consign(std::move(handler), conn);
    return boost::asio::dispatch(get_executor(),
        [c = conn, &req, &resp, h = std::move(h)] () mutable {
            return c->async_exec(req, resp, std::move(h));
    });
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn)}, token, req, resp);
}

template <typename T>
void redis_exec(std::shared_ptr<connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<T>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

std::string ObjectDirectory::build_index(CacheObj* object) 
{
  return object->bucketName + "_" + object->objName;
}

int ObjectDirectory::exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", key);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return false;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
}

int ObjectDirectory::set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);
    
  /* Every set will be treated as new */
  std::string endpoint;
  std::list<std::string> redisValues;
    
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("objName");
  redisValues.push_back(object->objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(object->bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(object->creationTime); 
  redisValues.push_back("dirty");
  redisValues.push_back(std::to_string(object->dirty));
  redisValues.push_back("objHosts");

  for (auto const& host : object->hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  if (!endpoint.empty())
    endpoint.pop_back();

  redisValues.push_back(endpoint); 

  try {
    boost::system::error_code ec;
    request req;
    req.push_range("HMSET", key, redisValues);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);

  if (exist_key(dpp, object, y)) {
    std::vector<std::string> fields;

    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("creationTime");
    fields.push_back("dirty");
    fields.push_back("objHosts");

    try {
      boost::system::error_code ec;
      request req;
      req.push_range("HMGET", key, fields);
      response< std::vector<std::string> > resp;

      redis_exec(conn, ec, req, resp, y);

      if (std::get<0>(resp).value().empty()) {
	ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "(): No values returned." << dendl;
	return -ENOENT;
      } else if (ec) {
	ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      object->objName = std::get<0>(resp).value()[0];
      object->bucketName = std::get<0>(resp).value()[1];
      object->creationTime = std::get<0>(resp).value()[2];
      object->dirty = boost::lexical_cast<bool>(std::get<0>(resp).value()[3]);
      boost::split(object->hostsList, std::get<0>(resp).value()[4], boost::is_any_of("_"));
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }

  return 0;
}

/* Note: This method is not compatible for use on Ubuntu systems. */
int ObjectDirectory::copy(const DoutPrefixProvider* dpp, CacheObj* object, std::string copyName, std::string copyBucketName, optional_yield y) 
{
  std::string key = build_index(object);
  auto copyObj = CacheObj{ .objName = copyName, .bucketName = copyBucketName };
  std::string copyKey = build_index(&copyObj);

  if (exist_key(dpp, object, y)) {
    try {
      response<int> resp;
     
      {
	boost::system::error_code ec;
	request req;
	req.push("COPY", key, copyKey);

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      }

      {
	boost::system::error_code ec;
	request req;
	req.push("HMSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
	response<std::string> res;

	redis_exec(conn, ec, req, res, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      }

      return std::get<0>(resp).value() - 1; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }
}

int ObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);

  if (exist_key(dpp, object, y)) {
    try {
      boost::system::error_code ec;
      request req;
      req.push("DEL", key);
      response<int> resp;

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      return std::get<0>(resp).value() - 1; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return 0; /* No delete was necessary */
  }
}

int ObjectDirectory::update_field(const DoutPrefixProvider* dpp, CacheObj* object, std::string field, std::string value, optional_yield y) 
{
  std::string key = build_index(object);

  if (exist_key(dpp, object, y)) {
    try {
      /* Ensure field exists */
      {
	boost::system::error_code ec;
	request req;
	req.push("HEXISTS", key, field);
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if (!std::get<0>(resp).value()) {
	  return -ENOENT;
	} else if (ec) {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      }

      if (field == "objHosts") {
	/* Append rather than overwrite */
	ldpp_dout(dpp, 20) << "ObjectDirectory::" << __func__ << "() Appending to hosts list." << dendl;

	boost::system::error_code ec;
	request req;
	req.push("HGET", key, field);
	response<std::string> resp;

	redis_exec(conn, ec, req, resp, y);

	if (std::get<0>(resp).value().empty()) {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "(): No values returned." << dendl;
	  return -ENOENT;
	} else if (ec) {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	std::get<0>(resp).value() += "_";
	std::get<0>(resp).value() += value;
	value = std::get<0>(resp).value();
      }

      {
	boost::system::error_code ec;
	request req;
	req.push_range("HSET", key, std::map<std::string, std::string>{{field, value}});
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	return std::get<0>(resp).value(); 
      }
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }
}

std::string BlockDirectory::build_index(CacheBlock* block) 
{
  return block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);
}

int BlockDirectory::exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  response<int> resp;
  ldpp_dout(dpp, 10) << __func__ << "(): index is: " << key << dendl;
  try {
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", key);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return false;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
}

int BlockDirectory::set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
    
  /* Every set will be treated as new */
  std::string endpoint;
  std::list<std::string> redisValues;
    
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("blockID");
  redisValues.push_back(std::to_string(block->blockID));
  redisValues.push_back("version");
  redisValues.push_back(block->version);
  redisValues.push_back("size");
  redisValues.push_back(std::to_string(block->size));
  redisValues.push_back("globalWeight");
  redisValues.push_back(std::to_string(block->globalWeight));
  redisValues.push_back("blockHosts");
  
  for (auto const& host : block->hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  if (!endpoint.empty())
    endpoint.pop_back();

  redisValues.push_back(endpoint);

  redisValues.push_back("objName");
  redisValues.push_back(block->cacheObj.objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(block->cacheObj.bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(block->cacheObj.creationTime); 
  redisValues.push_back("dirty");
  if (block->cacheObj.dirty == true || block->cacheObj.dirty == 1) {
    block->cacheObj.dirty = 1;
  }
  if (block->cacheObj.dirty == false || block->cacheObj.dirty == 0) {
    block->cacheObj.dirty = 0;
  }
  redisValues.push_back(std::to_string(block->cacheObj.dirty));
  redisValues.push_back("objHosts");
  
  endpoint.clear();
  for (auto const& host : block->cacheObj.hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  if (!endpoint.empty())
    endpoint.pop_back();

  redisValues.push_back(endpoint);

  try {
    boost::system::error_code ec;
    request req;
    req.push_range("HMSET", key, redisValues);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);

  ldpp_dout(dpp, 10) << __func__ << "(): index is: " << key << dendl;

  if (exist_key(dpp, block, y)) {
    std::vector<std::string> fields;

    fields.push_back("blockID");
    fields.push_back("version");
    fields.push_back("size");
    fields.push_back("globalWeight");
    fields.push_back("blockHosts");

    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("creationTime");
    fields.push_back("dirty");
    fields.push_back("objHosts");

    try {
      boost::system::error_code ec;
      request req;
      req.push_range("HMGET", key, fields);
      response< std::vector<std::string> > resp;

      redis_exec(conn, ec, req, resp, y);

      if (std::get<0>(resp).value().empty()) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): No values returned." << dendl;
	return -ENOENT;
      } else if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      block->blockID = boost::lexical_cast<uint64_t>(std::get<0>(resp).value()[0]);
      block->version = std::get<0>(resp).value()[1];
      block->size = boost::lexical_cast<uint64_t>(std::get<0>(resp).value()[2]);
      block->globalWeight = boost::lexical_cast<int>(std::get<0>(resp).value()[3]);
      boost::split(block->hostsList, std::get<0>(resp).value()[4], boost::is_any_of("_"));
      block->cacheObj.objName = std::get<0>(resp).value()[5];
      block->cacheObj.bucketName = std::get<0>(resp).value()[6];
      block->cacheObj.creationTime = std::get<0>(resp).value()[7];
      block->cacheObj.dirty = boost::lexical_cast<bool>(std::get<0>(resp).value()[8]);
      boost::split(block->cacheObj.hostsList, std::get<0>(resp).value()[9], boost::is_any_of("_"));
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }

  return 0;
}

/* Note: This method is not compatible for use on Ubuntu systems. */
int BlockDirectory::copy(const DoutPrefixProvider* dpp, CacheBlock* block, std::string copyName, std::string copyBucketName, optional_yield y) 
{
  std::string key = build_index(block);
  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };
  std::string copyKey = build_index(&copyBlock);

  if (exist_key(dpp, block, y)) {
    try {
      response<int> resp;
     
      {
	boost::system::error_code ec;
	request req;
	req.push("COPY", key, copyKey);

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      }

      {
	boost::system::error_code ec;
	request req;
	req.push("HMSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
	response<std::string> res;

	redis_exec(conn, ec, req, res, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      }

      return std::get<0>(resp).value() - 1; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }
}

int BlockDirectory::del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);

  if (exist_key(dpp, block, y)) {
    try {
      boost::system::error_code ec;
      request req;
      req.push("DEL", key);
      response<int> resp;

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      return std::get<0>(resp).value() - 1; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return 0; /* No delete was necessary */
  }
}

int BlockDirectory::update_field(const DoutPrefixProvider* dpp, CacheBlock* block, std::string field, std::string value, optional_yield y) 
{
  std::string key = build_index(block);

  if (exist_key(dpp, block, y)) {
    try {
      /* Ensure field exists */
      {
	boost::system::error_code ec;
	request req;
	req.push("HEXISTS", key, field);
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if (!std::get<0>(resp).value()) {
	  return -ENOENT;
	} else if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      }

      if (field == "blockHosts") { 
	/* Append rather than overwrite */
	ldpp_dout(dpp, 20) << "BlockDirectory::" << __func__ << "() Appending to hosts list." << dendl;

	boost::system::error_code ec;
	request req;
	req.push("HGET", key, field);
	response<std::string> resp;

	redis_exec(conn, ec, req, resp, y);

	if (std::get<0>(resp).value().empty()) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): No values returned." << dendl;
	  return -ENOENT;
	} else if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	std::get<0>(resp).value() += "_";
	std::get<0>(resp).value() += value;
	value = std::get<0>(resp).value();
      }
  if (field == "dirty") { 
    if (value == "true" || value == "1") {
      value = "1";
    }
    if (value == "false" || value == "0") {
      value = "0";
    }
  }
      {
	boost::system::error_code ec;
	request req;
	req.push_range("HSET", key, std::map<std::string, std::string>{{field, value}});
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	return std::get<0>(resp).value(); 
      }
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }
}

int BlockDirectory::remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string delValue, optional_yield y) 
{
  std::string key = build_index(block);

  if (exist_key(dpp, block, y)) {
    try {
      /* Ensure field exists */
      {
	boost::system::error_code ec;
	request req;
	req.push("HEXISTS", key, "blockHosts");
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if (!std::get<0>(resp).value()) {
	  return -ENOENT;
	} else if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}
      }

      {
	boost::system::error_code ec;
	request req;
	req.push("HGET", key, "blockHosts");
	response<std::string> resp;

	redis_exec(conn, ec, req, resp, y);

	if (std::get<0>(resp).value().empty()) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): No values returned." << dendl;
	  return -ENOENT;
	} else if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	if (std::get<0>(resp).value().find("_") == std::string::npos) /* Last host, delete entirely */
          return del(dpp, block, y);

        std::string result = std::get<0>(resp).value();
        auto it = result.find(delValue);
        if (it != std::string::npos) 
          result.erase(result.begin() + it, result.begin() + it + delValue.size());
        else
          return -ENOENT;

        if (result[0] == '_')
          result.erase(0, 1);

	delValue = result;
      }

      {
	boost::system::error_code ec;
	request req;
	req.push_range("HSET", key, std::map<std::string, std::string>{{"blockHosts", delValue}});
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	return std::get<0>(resp).value();
      }
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }
}

} } // namespace rgw::d4n
