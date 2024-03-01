#include <boost/algorithm/string.hpp>
#include "rgw_redis_driver.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace cache {

/* Base metadata and data fields should remain consistent */
std::vector<std::string> baseFields {
  "mtime",
  "object_size",
  "accounted_size",
  "epoch",
  "version_id",
  "source_zone_short_id",
  "bucket_count",
  "bucket_size",
  "user_quota.max_size",
  "user_quota.max_objects",
  "max_buckets",
  "data"};

std::vector< std::pair<std::string, std::string> > build_attrs(rgw::sal::Attrs* binary) {
  std::vector< std::pair<std::string, std::string> > values;
  rgw::sal::Attrs::iterator attrs;

  /* Convert to vector */
  if (binary != NULL) {
    for (attrs = binary->begin(); attrs != binary->end(); ++attrs) {
      values.push_back(std::make_pair(attrs->first, attrs->second.to_str()));
    }
  }

  return values;
}

int RedisDriver::insert_entry(const DoutPrefixProvider* dpp, std::string key, off_t offset, uint64_t len) {
  auto ret = entries.emplace(key, Entry(key, offset, len));
  return ret.second;
}

int RedisDriver::remove_entry(const DoutPrefixProvider* dpp, std::string key) {
  return entries.erase(key);
}

std::optional<Entry> RedisDriver::get_entry(const DoutPrefixProvider* dpp, std::string key) {
  auto iter = entries.find(key);

  if (iter != entries.end()) {
    return iter->second;
  }

  return std::nullopt;
}

int RedisDriver::initialize(CephContext* cct, const DoutPrefixProvider* dpp) {
  if (client.is_connected())
    return 0;

  if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client.connect("127.0.0.1", 6379, nullptr);

  if (!client.is_connected()) 
    return ECONNREFUSED;

  return 0;
}

bool RedisDriver::key_exists(const DoutPrefixProvider* dpp, const std::string& key) {
  int result = -1;
  std::string entryName = "rgw-object:" + key + ":cache";
  std::vector<std::string> keys;
  keys.push_back(entryName);

  if (!client.is_connected()) 
    return ECONNREFUSED;

  try {
    client.exists(keys, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {}

  return result;
}

std::vector<Entry> RedisDriver::list_entries(const DoutPrefixProvider* dpp) {
  std::vector<std::string> keys;
  std::vector<Entry> entries;

  if (!client.is_connected()) 
    return {};

  try {
    size_t cursor = 0;
    const std::string pattern = "*:cache";

    do {
      auto reply = client.scan(cursor, pattern); 
      client.sync_commit(std::chrono::milliseconds(1000));

      auto arr = reply.get().as_array();
      cursor = std::stoi(arr[0].as_string());
      auto result = arr[1].as_array();
  
      for (auto it = result.begin(); it != result.end(); ++it) {
        int i = std::distance(result.begin(), it);
	std::string entryName = result[i].as_string();
	keys.push_back(entryName.substr(11, entryName.length() - 17));
      }
    } while (cursor != 0);
  } catch(std::exception &e) {
    return {};
  }

  /* Construct list of entries */
  for (auto it = keys.begin(); it != keys.end(); ++it) {
    Entry entry;

    if (key_exists(dpp, *it)) {
      try {
	std::vector<std::string> fields;
        std::string entryName = "rgw-object:" + *it + ":cache";

	entry.key = *it;
	fields.push_back("offset");
	fields.push_back("len");
	fields.push_back("localWeight");

	client.hmget(entryName, fields, [&entry](cpp_redis::reply &reply) {
	  if (reply.is_array()) {
	    auto arr = reply.as_array();
      
	    if (!arr[0].is_null()) {
	      entry.offset = std::stol(arr[0].as_string().c_str());
	      entry.len = std::stoi(arr[1].as_string());
	      entry.localWeight = std::stoi(arr[2].as_string());
	    }
	  }
	});

	client.sync_commit(std::chrono::milliseconds(1000));
      } catch(std::exception &e) {
	return {}; // return failure or skip entry? -Sam
      }
    } else { // if one entry isn't found, shoud entire operation return a failure? -Sam
      dout(20) << "RGW Redis Cache: Entry " << *it << " was not retrievable." << dendl;
    }

    entries.push_back(entry);
  } 

  return entries;
}

size_t RedisDriver::get_num_entries(const DoutPrefixProvider* dpp) {
  int result = -1;

  if (!client.is_connected()) 
    return ECONNREFUSED;

  try {
    client.keys(":cache", [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result < 0) {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}

Partition RedisDriver::get_current_partition_info(const DoutPrefixProvider* dpp) {
  Partition part;
  return part; // Implement -Sam
}

uint64_t RedisDriver::get_free_space(const DoutPrefixProvider* dpp) {
  int result = -1;

  if (!client.is_connected()) 
    return ECONNREFUSED;

  try {
    client.info([&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        int usedMem = -1;
	int maxMem = -1;

        std::istringstream iss(reply.as_string());
	std::string line;    
        while (std::getline(iss, line)) {
	  size_t pos = line.find_first_of(":");
	  if (pos != std::string::npos) {
	    if (line.substr(0, pos) == "used_memory") {
	      usedMem = std::stoi(line.substr(pos + 1, line.length() - pos - 2));
	    } else if (line.substr(0, line.find_first_of(":")) == "maxmemory") {
	      maxMem = std::stoi(line.substr(pos + 1, line.length() - pos - 2));
	    } 
	  }
        }

	if (usedMem > -1 && maxMem > -1)
	  result = maxMem - usedMem;
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}

int RedisDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) {
  std::string entryName = "rgw-object:" + key + ":cache";

  if (!client.is_connected()) 
    return ECONNREFUSED;

  /* Every set will be treated as new */
  try {
    /* Set data field */
    int result; 

    client.hset(entryName, "data", bl.to_str(), [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != 0) {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  try {
    /* Set attribute fields */
    std::string result; 
    std::vector< std::pair<std::string, std::string> > redisAttrs = build_attrs(&attrs);

    client.hmset(entryName, redisAttrs, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
	result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != "OK") {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int RedisDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs) { // for whole objects? -Sam
  std::string result;
  std::string entryName = "rgw-object:" + key + ":cache";
  
  if (!client.is_connected()) 
    return ECONNREFUSED;
    
  if (key_exists(dpp, key)) {
    rgw::sal::Attrs::iterator it;
    std::vector< std::pair<std::string, std::string> > redisAttrs;
    std::vector<std::string> getFields;

    /* Retrieve existing values from cache */
    try {
      client.hgetall(entryName, [&bl, &attrs](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
    	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      if (arr[i].as_string() == "data")
                bl.append(arr[i + 1].as_string());
	      else {
	        buffer::list temp;
		temp.append(arr[i + 1].as_string());
                attrs.insert({arr[i].as_string(), temp});
		temp.clear();
	      }
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    dout(20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data) {
  std::string result;
  std::string value = "";
  std::string entryName = "rgw-object:" + key + ":cache";

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, key)) {
    try {
      client.hget(entryName, "data", [&value](cpp_redis::reply &reply) {
        if (!reply.is_null()) {
          value = reply.as_string();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -2;
    }
  }

  try {
    /* Append to existing value or set as new value */
    std::string temp = value + bl_data.to_str();
    std::vector< std::pair<std::string, std::string> > field;
    field.push_back({"data", temp});

    client.hmset(entryName, field, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != "OK") {
      return -1;
    }
  } catch(std::exception &e) {
    return -2;
  }

  return 0;
}

int RedisDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key) {
  int result = 0;
  std::string entryName = "rgw-object:" + key + ":cache";
  std::vector<std::string> deleteField;
  deleteField.push_back("data");

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, key)) {
    try {
    client.hdel(entryName, deleteField, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
    return -2;
    }
  } else {
    return 0; /* No delete was necessary */
  }

  return result - 1;
}

int RedisDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) {
  int exists = -2;
  std::string result;
  std::string entryName = "rgw-object:" + key + ":cache";

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, key)) {
    rgw::sal::Attrs::iterator it;
    std::vector< std::pair<std::string, std::string> > redisAttrs;
    std::vector<std::string> getFields;

    /* Retrieve existing values from cache */
    try {
      client.hgetall(entryName, [&getFields](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      getFields.push_back(arr[i].as_string());
	    }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

    /* Ensure all metadata, attributes, and data has been set */
    for (const auto& field : baseFields) {
      auto it = std::find_if(getFields.begin(), getFields.end(),
	[&](const auto& comp) { return comp == field; });

      if (it == getFields.end()) {
	return -1;
      }
    }

    getFields.erase(std::find(getFields.begin(), getFields.end(), "data")); /* Do not query for data field */
    
    /* Get attributes from cache */
    try {
      client.hmget(entryName, getFields, [&exists, &attrs, &getFields](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    exists = 0;

	    for (long unsigned int i = 0; i < getFields.size(); ++i) {
	      std::string tmp = arr[i].as_string();
	      buffer::list bl;
	      bl.append(tmp);
	      attrs.insert({getFields[i], bl});
	    }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      exit(-1);
    }

    if (exists < 0) {
      dout(20) << "RGW Redis Cache: Object was not retrievable." << dendl;
      return -2;
    }
  }

  return 0;
}

int RedisDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) {
  /* Creating the index based on oid */
  std::string entryName = "rgw-object:" + key + ":cache";
  std::string result;

  if (!client.is_connected()) 
    return ECONNREFUSED;

  /* Every set will be treated as new */
  try {
    std::vector< std::pair<std::string, std::string> > redisAttrs = build_attrs(&attrs);
      
    if (redisAttrs.empty()) {
      return -1;
    } 
      
    client.hmset(entryName, redisAttrs, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != "OK") {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int RedisDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) {
  std::string result;
  std::string entryName = "rgw-object:" + key + ":cache";

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, key)) {
    try {
      std::vector< std::pair<std::string, std::string> > redisAttrs;
      for (const auto& it : attrs) {
        redisAttrs.push_back({it.first, it.second.to_str()});
      }

      client.hmset(entryName, redisAttrs, [&result](cpp_redis::reply &reply) {
        if (!reply.is_null()) {
          result = reply.as_string();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (result != "OK") {
        return -1;
      }
    } catch(std::exception &e) {
      return -2;
    }
  } else {
    return -2;
  }

  return 0;
}

int RedisDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs) {
  int result = 0;
  std::string entryName = "rgw-object:" + key + ":cache";

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, key)) {
    std::vector<std::string> getFields;

    /* Retrieve existing values from cache */
    try {
      client.hgetall(entryName, [&getFields](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      getFields.push_back(arr[i].as_string());
	    }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

    std::vector< std::pair<std::string, std::string> > redisAttrs = build_attrs(&del_attrs);
    std::vector<std::string> redisFields;

    std::transform(begin(redisAttrs), end(redisAttrs), std::back_inserter(redisFields),
                          [](auto const& pair) { return pair.first; });

    /* Only delete attributes that have been stored */
    for (const auto& it : redisFields) {
      if (std::find(getFields.begin(), getFields.end(), it) == getFields.end()) {
        redisFields.erase(std::find(redisFields.begin(), redisFields.end(), it));
      }
    }

    try {
      client.hdel(entryName, redisFields, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  }

  dout(20) << "RGW Redis Cache: Object is not in cache." << dendl;
  return -2;
}

std::string RedisDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name) {
  int exists = -2;
  std::string result;
  std::string entryName = "rgw-object:" + key + ":cache";
  std::string attrValue;

  if (!client.is_connected()) 
    return {};

  if (key_exists(dpp, key)) {
    std::string getValue;

    /* Ensure field was set */
    try {
      client.hexists(entryName, attr_name, [&exists](cpp_redis::reply& reply) {
	if (!reply.is_null()) {
	  exists = reply.as_integer();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return {};
    }
    
    if (!exists) {
      dout(20) << "RGW Redis Cache: Attribute was not set." << dendl;
      return {};
    }

    /* Retrieve existing value from cache */
    try {
      client.hget(entryName, attr_name, [&exists, &attrValue](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  exists = 0;
	  attrValue = reply.as_string();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return {};
    }

    if (exists < 0) {
      dout(20) << "RGW Redis Cache: Object was not retrievable." << dendl;
      return {};
    }
  }

  return attrValue;
}

int RedisDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attrVal) {
  /* Creating the index based on key */
  std::string entryName = "rgw-object:" + key + ":cache";
  int result = -1;
    
  if (!client.is_connected()) 
    return ECONNREFUSED;
    
  /* Every set will be treated as new */
  try {
    client.hset(entryName, attr_name, attrVal, [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}

std::unique_ptr<CacheAioRequest> RedisDriver::get_cache_aio_request_ptr(const DoutPrefixProvider* dpp)
{
    return std::make_unique<RedisCacheAioRequest>(this);
}

rgw::AioResultList RedisDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id)
{
    rgw_raw_obj r_obj;
    r_obj.oid = key;
    return aio->get(r_obj, rgw::Aio::cache_read_op(dpp, y, this, ofs, len, key), cost, id);
}

void RedisCacheAioRequest::cache_aio_read(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, off_t ofs, uint64_t len, rgw::Aio* aio, rgw::AioResult& r) {}
void RedisCacheAioRequest::cache_aio_write(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, bufferlist& bl, uint64_t len, rgw::Aio* aio, rgw::AioResult& r) {}

} } // namespace rgw::cal
