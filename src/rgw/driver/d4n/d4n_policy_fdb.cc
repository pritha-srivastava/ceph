#include "d4n_policy_fdb.h"
#include "rgw_sal_d4n.h"
#include "rgw/ceph_fdb.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "common/split.h"
#include "rgw_perf_counters.h"

namespace rgw::d4n {

int FDBLFUDAPolicy::init(CephContext* cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) {
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

  auto fdb_conn = std::static_pointer_cast<fdbase>(conn->get_fdb_conn());
  std::map<std::string, std::string> out_kvs;
  if (lfdb::get(fdb_conn, "lfuda", out_kvs) != true){
    ldpp_dout(dpp, 0) << "FDBLFUDAPolicy::" << __func__ << "() ERROR: " << "get function returned false! " << dendl;
	return -1;
  }

  int dir_age = std::stoull(out_kvs.at("age"));


  std::map<std::string, std::string> fdbValues;
  fdbValues.emplace("minLocalWeights_sum", std::to_string(weightSum));
  fdbValues.emplace("minLocalWeights_size", std::to_string(entries_map.size()));
  fdbValues.emplace("minLocalWeights_address", dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);

  if (dir_age < age)
    fdbValues.emplace("age", std::to_string(age));
  else
    fdbValues.emplace("age", std::to_string(dir_age));

  lfdb::set(fdb_conn, "lfuda", fdbValues);


  asio::co_spawn(io_context.get_executor(),
		   directory_sync(dpp, y), asio::detached);

  return 0;
}

int FDBLFUDAPolicy::age_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  auto fdb_conn = std::static_pointer_cast<fdbase>(conn->get_fdb_conn());
  std::map<std::string, std::string> out_kvs;
  if (lfdb::get(fdb_conn, "lfuda", out_kvs) != true){
    ldpp_dout(dpp, 0) << "FDBLFUDAPolicy::" << __func__ << "() ERROR: " << "get function returned false! " << dendl;
	return -1;
  }

  int dir_age = std::stoull(out_kvs.at("age"));

  std::map<std::string, std::string> fdbValues;

  if (dir_age < age)
    fdbValues.emplace("age", std::to_string(age));
  else
    fdbValues.emplace("age", std::to_string(dir_age));

  lfdb::set(fdb_conn, "lfuda", fdbValues);

  return 0;
}

int FDBLFUDAPolicy::local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  auto fdb_conn = std::static_pointer_cast<fdbase>(conn->get_fdb_conn());

  if (fabs(weightSum - postedSum) > (postedSum * 0.1)) {

    std::map<std::string, std::string> out_kvs;
    if (lfdb::get(fdb_conn, "lfuda", out_kvs) != true){
      ldpp_dout(dpp, 0) << "FDBLFUDAPolicy::" << __func__ << "() ERROR: " << "get function returned false! " << dendl;
  	  return -1;
    }

    auto dir_minLocalWeights_sum = std::stof(out_kvs.at("minLocalWeights_sum"));
    auto dir_minLocalWeights_size = std::stof(out_kvs.at("minLocalWeights_size"));

	float minAvgWeight = dir_minLocalWeights_sum/dir_minLocalWeights_size;
	float localAvgWeight = 0;

    if (entries_map.size())
      localAvgWeight = static_cast<float>(weightSum) / static_cast<float>(entries_map.size());

    if (localAvgWeight < minAvgWeight) { /* Set new minimum weight */
 	  std::map<std::string, std::string> fdbValues;
	  fdbValues.emplace("minLocalWeights_sum", std::to_string(weightSum));
  	  fdbValues.emplace("minLocalWeights_size", std::to_string(entries_map.size()));
  	  fdbValues.emplace("minLocalWeights_address", dpp->get_cct()->_conf->rgw_d4n_local_rgw_address);
	
  	  lfdb::set(fdb_conn, "lfuda", fdbValues);

	} else {
      weightSum = (int)dir_minLocalWeights_sum;
      postedSum = (int)dir_minLocalWeights_size;
    }

  }

  std::map<std::string, std::string> fdbValues;
  fdbValues.emplace("avgLocalWeight_sum", std::to_string(weightSum));
  fdbValues.emplace("avgLocalWeight_size", std::to_string(entries_map.size()));

  lfdb::set(fdb_conn, dpp->get_cct()->_conf->rgw_d4n_local_rgw_address, fdbValues);
  
  return 0;
}

} // namespace rgw::d4n
=======
