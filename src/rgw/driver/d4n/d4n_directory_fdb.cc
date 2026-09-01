#include <algorithm>
#include <limits>
#include <type_traits>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory_fdb.h"

namespace rgw::d4n {

using std::map;
using std::string;
namespace fdbc = lfdb::layer::content;
namespace q    = lfdb::query;

static std::string encode_score(int64_t score)
{
  return fmt::format("{:019d}", score);
}

// Returns count+1 so callers can detect whether a continuation token is needed:
// if FDB returns count+1 rows, there are more; if fewer, the range is exhausted.
// Returns 0 when count==0 (unbounded).
static int fdb_page_read_limit(uint64_t count)
{
  if (count == 0) return 0;
  return count < static_cast<uint64_t>(std::numeric_limits<int>::max())
       ? static_cast<int>(count + 1)
       : std::numeric_limits<int>::max();
}

int FDBTransaction::commit(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (executed_) {
    return -EINVAL;
  }

  try {
    if (lfdb::commit(txn_)) {
      executed_ = true;
      return 0;
    }

    executed_ = true;
    ldpp_dout(dpp, 10) << "FDBTransaction::" << __func__ << "() transaction conflict, replay required" << dendl;
    return -EAGAIN;
  } catch (const lfdb::libfdb_exception& e) {
    executed_ = true;
    ldpp_dout(dpp, 0) << "FDBTransaction::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBTransaction::abort(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (executed_) {
    return -EINVAL;
  }

  executed_ = true;
  txn_.reset();
  return 0;
}

template <typename Func>
int FDBDirectory::with_fdb_transaction(Transaction* txn, Func&& func)
{
  if (txn) {
    auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
    if (!fdb_txn) {
      return -EINVAL;
    }

    auto& tr = fdb_txn->get_transaction();
    return func(tr);
  }

  return lfdb::make_transactor(FDBdb)([&](auto& tr) {
    return func(tr);
  });
}


int FDBDirectory::get_kv(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& field, std::string& out_val, Transaction* txn)
{
  try {
    auto get = [&](auto& tr) {
      std::map<std::string, std::string> kvs;
      if (!lfdb::get(tr, key, kvs)) {
        return -ENOENT;
      }
      auto it = kvs.find(field);
      if (it == kvs.end()) {
        return -ENOENT;
      }
      out_val = it->second;
      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return get(tr);
    }

    return lfdb::make_transactor(FDBdb)(get);
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}


int FDBDirectory::set_kv(const DoutPrefixProvider* dpp, optional_yield y,
    const std::string& key, const std::string& field,
    const std::string& val, Transaction* txn)
{
  try {
    auto set = [&](auto& tr) {
      std::map<std::string, std::string> existing;
      lfdb::get(tr, key, existing);
      existing[field] = val;
      lfdb::set(tr, key, existing);
      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return set(tr);
    }

    return lfdb::make_transactor(FDBdb)(set);
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBDirectory::" << __func__
      << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBDirectory::get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
    const std::string& key, const std::vector<std::string>& fields,
    std::map<std::string, std::string>& out_vals, Transaction* txn)
{
  try {
    auto get = [&](auto& tr) {
      std::map<std::string, std::string> kvs;
      if (!lfdb::get(tr, key, kvs)) {
        return -ENOENT;
      }

      out_vals.clear();
      for (const auto& field : fields) {
        auto it = kvs.find(field);
        if (it == kvs.end()) {
          return -ENOENT;
        }
        out_vals[field] = it->second;
      }
      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return get(tr);
    }

    return lfdb::make_transactor(FDBdb)(get);
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBDirectory::" << __func__
      << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBDirectory::set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
    const std::string& key,
    const std::map<std::string, std::string>& vals,
    Transaction* txn)
{
  try {
    auto set = [&](auto& tr) {
      std::map<std::string, std::string> existing;
      lfdb::get(tr, key, existing);
      for (const auto& [field, value] : vals) {
        existing[field] = value;
      }
      lfdb::set(tr, key, existing);
      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return set(tr);
    }

    return lfdb::make_transactor(FDBdb)(set);
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBDirectory::" << __func__
      << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}


int FDBDirectory::set_kv_if_not_exists(const DoutPrefixProvider* dpp, optional_yield y,
                                        const std::string& key,
                                        const std::string& field,
                                        const std::string& val,
					Transaction* txn)
{
  try {
    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }

      auto& tr = fdb_txn->get_transaction();
      std::map<std::string, std::string> existing;

      lfdb::get(tr, key, existing);
      if (existing.find(field) == existing.end()) {
        existing[field] = val;
        lfdb::set(tr, key, existing);
      }
      return 0;
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      std::map<std::string, std::string> existing;

      lfdb::get(tr, key, existing);
      if (existing.find(field) == existing.end()) {
        existing[field] = val;
        lfdb::set(tr, key, existing);
      }
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBDirectory::" << __func__
      << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBBucketDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, Transaction* txn)
{
  try {
    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return lfdb::key_exists(tr, bucket_id);
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      return lfdb::key_exists(tr, bucket_id);
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBBucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBBucketDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, Transaction* txn)
{
  try {
    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      lfdb::erase(tr, bucket_id);
      return 0;
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      lfdb::erase(tr, bucket_id);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBBucketDirectory::add_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, Transaction* txn)
{
  return fdb_add(dpp, y, bucket_id, 0, object_name, std::move(params), txn);
}

int FDBBucketDirectory::remove_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, Transaction* txn)
{
  return fdb_rem(dpp, y, bucket_id, object_name, txn);
}

int FDBBucketDirectory::list_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, Transaction* txn)
{
  return fdb_scan(dpp, y, bucket_id, marker, prefix, count, marker_inclusive, objs_info, continuation_token, txn);
}

//Key form is <bucket-id>/objects/<object-name>
std::string FDBBucketDirectory::build_object_index(const std::string& bucket_id, const std::string& obj_name)
{
  return std::string(libfdb_key_view(fdbc::keyspace(bucket_id) / "objects")) + obj_name;
}

int FDBBucketDirectory::collect_range(const DoutPrefixProvider* dpp, 
		    const FDBRange& range,
		    const std::string& base,
		    uint64_t count,
		    std::vector<CacheObject>& objs_info,
		    std::string& continuation_token,
		    Transaction* txn)
{
  objs_info.clear();
  continuation_token.clear();

  auto collect_transaction = [&](auto& tr) -> int {
    auto selector = lfdb::select{range.begin, range.end};
    selector.options.result_limit = fdb_page_read_limit(count);

    bool have_more = false;
    std::size_t fetched = 0;

    for (auto&& [key, value] :
         lfdb::scan<CacheObject>(tr, selector)) {

      if (key.size() < base.size()) {
        ldpp_dout(dpp, 0)
            << "FDBBucketDirectory::" << __func__
            << "() malformed key: " << key << dendl;
        continue;
      }

      // The scan asks FDB for count + 1 rows. The extra row
      // tells us that another page exists.
      if (count && fetched >= count) {
        have_more = true;
        break;
      }

      const std::string member = key.substr(base.size());

      objs_info.push_back(std::move(value));
      objs_info.back().objName = member;

      ++fetched;
    }

    if (have_more && !objs_info.empty()) {
      continuation_token = objs_info.back().objName;
    }

    return 0;
  };

  try {
    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }

      auto& tr = fdb_txn->get_transaction();
      return collect_transaction(tr);
    }

    return lfdb::make_transactor(FDBdb)(
        [&](auto& tr) -> int {
          return collect_transaction(tr);
        });

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what() << dendl;

    return -EIO;
  }
}

FDBRange FDBBucketDirectory::build_range(const std::string& base, const std::string& start, bool inclusive)
{
  FDBRange range;

  if (start.empty()) {
    range.begin = base;
  } else if (inclusive) {
    range.begin = base + start;
  } else {
    range.begin = base + start + '\0';
  }

  range.end = base + "\xff";

  return range;
}

int FDBBucketDirectory::fdb_add(const DoutPrefixProvider* dpp, optional_yield y,
                                const std::string& bucket_id,
                                double score,
                                const std::string& member,
                                std::optional<CacheObject> params,
                                Transaction* txn)
{
  if (!params) {
    return -EINVAL;
  }

  try {
    std::string member_key = build_object_index(bucket_id, member);
    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();

      lfdb::set(tr, member_key, *params);

      return 0;
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      lfdb::set(tr, member_key, *params);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBBucketDirectory::" << __func__
      << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBBucketDirectory::fdb_rem(const DoutPrefixProvider* dpp, optional_yield y,
                                const std::string& bucket_id,
                                const std::string& member,
                                Transaction* txn)
{
  try {
    std::string member_key = build_object_index(bucket_id, member);
    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      lfdb::erase(tr, member_key);
      return 0;
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      lfdb::erase(tr, member_key);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBBucketDirectory::" << __func__
      << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}


int FDBBucketDirectory::fdb_scan(const DoutPrefixProvider* dpp, optional_yield y,
                                 const std::string& bucket_id,
                                 const std::string& start_token,
                                 const std::string& prefix,
                                 uint64_t count,
                                 bool marker_inclusive,
                                 std::vector<CacheObject>& objs_info,
                                 std::string& continuation_token,
                                 Transaction* txn)
{
  continuation_token.clear();

  try {
    const std::string base =
        std::string(libfdb_key_view(fdbc::keyspace(bucket_id) / "objects"));
    const std::string prefix_begin = base + prefix;
    const std::string marker_key = base + start_token;

    const auto object_query = start_token.empty()
        ? q::prefix(prefix_begin)
        : marker_inclusive
          ? q::prefix_starting_at(prefix_begin, marker_key)
          : q::prefix_starting_after(prefix_begin, marker_key);

    if (q::is_empty(object_query)) {
      return -ENOENT;
    }

    ldpp_dout(dpp, 20)
        << "FDBBucketDirectory::" << __func__
        << "() prefix_begin: " << prefix_begin << dendl;

    const auto page_query = q::with_options(
        object_query,
        q::query_options{
            .result_limit = fdb_page_read_limit(count)});

    auto scan_transaction = [&](auto& tr) -> int {
      auto gen = lfdb::scan<CacheObject>(tr, page_query);
      auto it = std::ranges::begin(gen);
      auto end = std::ranges::end(gen);

      std::vector<std::pair<std::string, CacheObject>> rows;
      const int limit = fdb_page_read_limit(count);

      for (int n = 0;
           (limit == 0 || n < limit) && it != end;
           ++n, ++it) {
        rows.push_back(*it);
      }

      if (rows.empty()) {
        return -ENOENT;
      }

      const auto returned = (count == 0)
          ? std::size(rows)
          : std::min(std::size(rows),
                     static_cast<std::size_t>(count));

      objs_info.reserve(returned);

      for (std::size_t i = 0; i < returned; ++i) {
        objs_info.push_back(std::move(rows[i].second));
      }

      if (returned < std::size(rows) && !objs_info.empty()) {
        continuation_token = objs_info.back().objName;
      }

      return 0;
    };

    // Use the caller's transaction when one was supplied.
    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }

      auto& tr = fdb_txn->get_transaction();
      return scan_transaction(tr);
    }

    // Otherwise create and commit our own transaction.
    return lfdb::make_transactor(FDBdb)(
        [&](auto& tr) -> int {
          return scan_transaction(tr);
        });

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what() << dendl;

    return -EIO;
  }
}

/*
  Key formats:
  <bucket-id>#<object-name>/versions/<score>/<version> --> stores versions in order
  <bucket-id>#<object-name>/score/<version> --> for reverse lookup of a version key using its score
*/
std::string FDBObjectDirectory::get_versions_subspace(const DoutPrefixProvider* dpp,
                                                      const std::string& bucket_id,
                                                      const std::string& obj_name)
{
  const std::string index = build_index(bucket_id, obj_name);
  ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :index " << index << dendl;
  return std::string(libfdb_key_view(fdbc::keyspace(index) / "versions"));
}

std::string FDBObjectDirectory::get_score_subspace(const DoutPrefixProvider* dpp,
                                                    const std::string& bucket_id,
                                                    const std::string& obj_name)
{
  const std::string index = build_index(bucket_id, obj_name);
  ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :index " << index << dendl;
  return std::string(libfdb_key_view(fdbc::keyspace(index) / "score"));
}

std::string FDBObjectDirectory::build_versions_index(const DoutPrefixProvider* dpp,
                                                     const std::string& bucket_id,
                                                     const std::string& obj_name,
                                                     const std::string& score,
                                                     const std::string& version)
{
  const std::string subspace = get_versions_subspace(dpp, bucket_id, obj_name);
  return subspace + std::string(libfdb_key_view(fdbc::key(score, version)));
}

std::string FDBObjectDirectory::build_version_score_index(const DoutPrefixProvider* dpp,
                                                          const std::string& bucket_id,
                                                          const std::string& obj_name,
                                                          const std::string& version)
{
  const std::string subspace = get_score_subspace(dpp, bucket_id, obj_name);
  return subspace + std::string(libfdb_key_view(fdbc::key(version)));
}

int FDBObjectDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, Transaction* txn)
{
  try {
    std::string key = build_index(bucket_id, obj_name);

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return lfdb::key_exists(tr, key);
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      return lfdb::key_exists(tr, key);
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}


int FDBObjectDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, CacheObj* object, Transaction* txn)
{
  try {
    std::string key = build_index(object->bucketName, object->objName);

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      lfdb::erase(tr, key);
      return 0;
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      lfdb::erase(tr, key);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

std::string FDBObjectDirectory::get_versions_range_end(const std::string& versions_subspace) const
{
    return versions_subspace + "\xff";
}

bool FDBObjectDirectory::scan_versions(const DoutPrefixProvider* dpp, optional_yield y, const std::string& begin, const std::string& end, bool reverse, std::vector<std::pair<std::string, CacheObjectVersion>>& kvs, Transaction* txn)
{
    try {
        auto range = lfdb::select{begin, end};
        range.options.reverse_order = reverse;

        auto scan_transaction = [&](auto& tr) -> bool {
            for (auto&& [key, value] : lfdb::scan<CacheObjectVersion>(tr, range)) {
                kvs.emplace_back(std::string(key), std::move(value));
            }
            return !kvs.empty();
        };

        if (txn) {
            auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
            if (!fdb_txn) {
                return false;
            }

            auto& tr = fdb_txn->get_transaction();
            return scan_transaction(tr);
        }

        return lfdb::make_transactor(FDBdb)([&](auto& tr) -> bool {
            return scan_transaction(tr);
        });

    } catch (const lfdb::libfdb_exception& e) {
        ldpp_dout(dpp, 0) << "FDBObjectDirectory::scan_versions ERROR: " << e.what() << dendl;
        return false;
    }
}

bool FDBObjectDirectory::parse_version_key(
    const std::string& versions_subspace,
    const std::string& key,
    std::string& score,
    std::string& member) const
{
    size_t score_start = versions_subspace.size();

    size_t score_end =
        key.find('/', score_start);


    if (score_end == std::string::npos) {
        return false;
    }


    score = key.substr(
        score_start,
        score_end - score_start);


    member =
        key.substr(score_end + 1);


    return true;
}

int FDBObjectDirectory::fdb_add(const DoutPrefixProvider* dpp, optional_yield y,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                int64_t score,
                                const std::string& version,
                                std::optional<CacheObjectVersion> params,
                                Transaction* txn)
{
  if (!params) {
    return -EINVAL;
  }
  try {
    auto add = [&](auto& tr) {
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :bucket_id " << bucket_id << dendl;
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :obj_name " << obj_name << dendl;

      std::string encoded_score = encode_score(score);
      std::string score_key = build_version_score_index(dpp, bucket_id, obj_name, version);

      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " score_key " << score_key << dendl;

      std::string existing;
      if (lfdb::get(tr, score_key, existing)) {
        std::string existing_versions_key = build_versions_index(dpp, bucket_id, obj_name, existing, version);
        lfdb::erase(tr, existing_versions_key);
      }

      std::string versions_key = build_versions_index(dpp, bucket_id, obj_name, encoded_score, version);
      lfdb::set(tr, versions_key, *params);
      lfdb::set(tr, score_key, encoded_score);
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " versions_key: " << versions_key << dendl;
      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return add(tr);
    }

    return lfdb::make_transactor(FDBdb)(add);
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBObjectDirectory::fdb_revrange(const DoutPrefixProvider* dpp, optional_yield y,
		    const std::string& bucket_id,
		    const std::string& obj_name,
		    const std::string& marker_version,
		    uint64_t count,
		    std::vector<CacheObjectVersion>& obj_versions,
		    std::string& continuation_token,
		    Transaction* txn)
{
  continuation_token.clear();
  obj_versions.clear();

  try {
    const std::string versions_subspace =
        get_versions_subspace(dpp, bucket_id, obj_name);

    ldpp_dout(dpp, 20)
        << "FDBObjectDirectory::" << __func__
        << "() versions_subspace: " << versions_subspace
        << dendl;

    // Build the eligible key range using query algebra.  Both branches produce
    // q::interval so the variable can hold either without type erasure.
    auto revrange_transaction = [&](auto& tr) -> int {
      q::interval versions_query = q::prefix(versions_subspace);

      if (!marker_version.empty()) {
        // Resolve the marker's score using the same transaction.
        std::string marker_score;

        // Point lookup: resolve the marker's encoded score via the reverse index
        // rather than scanning for it.
        const std::string score_key =
            build_version_score_index(dpp, bucket_id, obj_name, marker_version);

        if (!lfdb::get(tr, score_key, marker_score)) {
          ldpp_dout(dpp, 10)
              << "FDBObjectDirectory::" << __func__
              << "() marker version not found: "
              << marker_version << dendl;
          return -ENOENT;
        }

        const std::string marker_key = build_versions_index(dpp, bucket_id, obj_name, marker_score, marker_version);

        // ending_before gives:
        // [versions_subspace, marker_key)
        // which excludes the marker itself.
        versions_query =
            q::ending_before(
                q::prefix(versions_subspace),
                marker_key);
      }

      if (q::is_empty(versions_query)) {
        return -ENOENT;
      }

      const auto page_query =
          q::with_options(
              versions_query,
              q::query_options{
                  .result_limit = fdb_page_read_limit(count),
                  .reverse_order = true
              });

      auto gen = lfdb::scan<CacheObjectVersion>(tr, page_query);

      auto it = std::ranges::begin(gen);
      auto end = std::ranges::end(gen);

      const int limit = fdb_page_read_limit(count);

      std::vector<std::pair<std::string, CacheObjectVersion>> rows;

      for (int n = 0; (limit == 0 || n < limit) && it != end; ++n, ++it) {
        rows.push_back(*it);
      }

      if (rows.empty()) {
        return -ENOENT;
      }

      ldpp_dout(dpp, 20)
          << "FDBObjectDirectory::" << __func__
          << "() count: " << count << dendl;

      for (const auto& [key, value] : rows) {
        obj_versions.push_back(value);

        ldpp_dout(dpp, 20)
            << "FDBObjectDirectory::" << __func__
            << "() version: "
            << obj_versions.back().version
            << dendl;

        ldpp_dout(dpp, 20)
            << "FDBObjectDirectory::" << __func__
            << "() user_id: "
            << obj_versions.back().user_id
            << dendl;

        ldpp_dout(dpp, 20)
            << "FDBObjectDirectory::" << __func__
            << "() display_name: "
            << obj_versions.back().display_name
            << dendl;

        if (count && obj_versions.size() == count) {
          if (rows.size() > count) {
            continuation_token = obj_versions.back().version;
          }
          break;
        }
      }

      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }

      auto& tr = fdb_txn->get_transaction();
      return revrange_transaction(tr);
    }

    return lfdb::make_transactor(FDBdb)(
        [&](auto& tr) -> int {
          return revrange_transaction(tr);
        });

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBObjectDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;

    return -EIO;
  }
}

int FDBObjectDirectory::fdb_rem(const DoutPrefixProvider* dpp, optional_yield y,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                const std::string& version,
                                Transaction* txn)
{
  try {
    auto rem = [&](auto& tr) {
      std::string score_key = build_version_score_index(dpp, bucket_id, obj_name, version);
      std::string existing_score;
      bool found = lfdb::get(tr, score_key, existing_score);

      if (!found) {
        return -ENOENT;
      }

      std::string version_key = build_versions_index(dpp, bucket_id, obj_name, existing_score, version);
      lfdb::erase(tr, version_key);
      lfdb::erase(tr, score_key);
      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return rem(tr);
    }

    return lfdb::make_transactor(FDBdb)(rem);
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBObjectDirectory::fdb_remrangebyscore(const DoutPrefixProvider* dpp, optional_yield y,
                                            const std::string& bucket_id,
                                            const std::string& obj_name,
                                            double min,
                                            double max,
                                            Transaction* txn)
{
  try {
    const std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);
    const std::string min_s = encode_score(min);
    const std::string max_s = encode_score(max);
    const auto score_range = q::intersection(q::prefix(versions_subspace), q::between(versions_subspace + min_s, versions_subspace + max_s + "\xff"));

    if (q::is_empty(score_range)) {
      return -ENOENT;
    }

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      lfdb::erase(tr, score_range);
      return 0;
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) -> int {
      lfdb::erase(tr, score_range);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int FDBObjectDirectory::fdb_rank(const DoutPrefixProvider* dpp, optional_yield y,
                                 const std::string& bucket_id,
                                 const std::string& obj_name,
                                 const std::string& member,
                                 std::string& index,
                                 Transaction* txn)
{
  try {
    const std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }

      auto& tr = fdb_txn->get_transaction();
      const auto kvs = lfdb::collect<CacheObjectVersion>(tr, q::prefix(versions_subspace));

      if (kvs.empty()) {
        return -ENOENT;
      }

      for (size_t i = 0; i < kvs.size(); ++i) {
        if (kvs[i].second.version == member) {
          index = std::to_string(i);
          return 0;
        }
      }

      return -ENOENT;
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) -> int {
      const auto kvs = lfdb::collect<CacheObjectVersion>(tr, q::prefix(versions_subspace));

      if (kvs.empty()) {
        return -ENOENT;
      }

      for (size_t i = 0; i < kvs.size(); ++i) {
        if (kvs[i].second.version == member) {
          index = std::to_string(i);
          return 0;
        }
      }

      return -ENOENT;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int FDBObjectDirectory::add_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, Transaction* txn)
{
  auto score = std::chrono::duration_cast<std::chrono::nanoseconds>(
      creation_time.time_since_epoch()).count();
  ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "(): Score of object name: "<< obj_name << " version: " << version << " is: "  << score << dendl;
  return fdb_add(dpp, y, bucket_id, obj_name, score, version, params, txn);
}

int FDBObjectDirectory::remove_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, Transaction* txn)
{
  return fdb_rem(dpp, y, bucket_id, obj_name, version, txn);
}

int FDBObjectDirectory::remove_version_by_creation_time(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, ceph::real_time creation_time, Transaction* txn)
{
  auto score = std::chrono::duration_cast<std::chrono::nanoseconds>(
      creation_time.time_since_epoch()).count();
  return fdb_remrangebyscore(dpp, y, bucket_id, obj_name, score, score, txn);
}

int FDBObjectDirectory::list_versions(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, Transaction* txn)
{
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " obj_name: " << obj_name << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " marker_version: " << marker_version << dendl;
  std::vector<std::string> members;
  auto ret = fdb_revrange(dpp, y, bucket_id, obj_name, marker_version, count, obj_versions, continuation_token, txn);
  if (ret < 0 ) {
    return ret;
  }
  return 0;
}

int FDBBlockDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, Transaction* txn)
{
  try {
    std::string key = build_index(block);

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return lfdb::key_exists(tr, key);
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      return lfdb::key_exists(tr, key);
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}


template<AssociativeContainer Container>
int FDBBlockDirectory::set_values(const DoutPrefixProvider* dpp,
                                  CacheBlock& block,
                                  Container& fdbValues,
                                  optional_yield y)
{
  std::string hosts;

  auto add_value = [&](const std::string& key, const auto& value) {
    using ValueType = typename Container::value_type;

    std::string str_value;

    if constexpr (std::is_convertible_v<decltype(value), std::string>) {
      str_value = value;
    } else {
      str_value = std::to_string(value);
    }

    if constexpr (requires(Container c, ValueType v) {
                    c.push_back(v);
                  }) {
      fdbValues.push_back(ValueType{key, str_value});
    } else {
      fdbValues.insert(ValueType{key, str_value});
    }
  };

  int ret = -1;

  add_value("blockID", block.blockID);
  add_value("version", block.version);

  if ((ret = check_bool(std::to_string(block.deleteMarker))) != -EINVAL) {
    block.deleteMarker = (ret != 0);
  } else {
    ldpp_dout(dpp, 0)
      << "BlockDirectory::" << __func__
      << "() ERROR: Invalid bool value for delete marker"
      << dendl;
    return -EINVAL;
  }

  add_value("deleteMarker", block.deleteMarker);
  add_value("size", block.size);
  add_value("globalWeight", block.globalWeight);
  add_value("objName", block.cacheObj.objName);
  add_value("bucketName", block.cacheObj.bucketName);
  add_value("creationTime", block.cacheObj.creationTime);

  if ((ret = check_bool(std::to_string(block.cacheObj.dirty))) != -EINVAL) {
    block.cacheObj.dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0)
      << "BlockDirectory::" << __func__
      << "() ERROR: Invalid bool value"
      << dendl;
    return -EINVAL;
  }

  add_value("dirty", block.cacheObj.dirty);

  hosts.clear();
  for (const auto& host : block.cacheObj.hostsList) {
    if (hosts.empty())
      hosts = host + "_";
    else
      hosts += host + "_";
  }

  if (!hosts.empty())
    hosts.pop_back();

  add_value("hosts", hosts);
  add_value("etag", block.cacheObj.etag);
  add_value("objSize", block.cacheObj.size);
  add_value("userId", block.cacheObj.user_id);
  add_value("displayName", block.cacheObj.display_name);
  add_value("acl", block.cacheObj.acl);

  add_value("attrsCount", block.cacheObj.attrs.size());

  for (const auto& [key, bl] : block.cacheObj.attrs) {
    add_value("attr_" + key, bl.to_str());
  }

  return 0;
}

int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, Transaction* txn)
{
  if (!block) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "() ERROR: null block pointer" << dendl;
    return -EINVAL;
  }

  try {
    std::string key = build_index(block);
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    std::map<std::string, std::string> fdbValues;
    auto ret = set_values(dpp, *block, fdbValues, y);
    if (ret < 0) {
      return ret;
    }

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      lfdb::set(tr, key, fdbValues);
      return 0;
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      lfdb::set(tr, key, fdbValues);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

/* In this function, if a transaction is supplied, all the blocks will be set in the same transaction */
int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, Transaction* txn)
{
  try {
    auto set_blocks = [&](auto& tr) {
      for (auto& block : blocks) {
        std::string key = build_index(&block);
        ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

        std::map<std::string, std::string> fdbValues;
        auto ret = set_values(dpp, block, fdbValues, y);
        if (ret < 0) {
          return ret;
        }

        lfdb::set(tr, key, fdbValues);
      }

      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return set_blocks(tr);
    }

    return lfdb::make_transactor(FDBdb)(set_blocks);
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, Transaction* txn)
{
  if (!block) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "() ERROR: null block pointer" << dendl;
    return -EINVAL;
  }

  try {
    std::string key = build_index(block);
    std::map<std::string, std::string> out_kvs;

    auto get_block = [&](auto& tr) {
      if (!lfdb::get(tr, key, out_kvs)) {
        ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: get function returned false!" << dendl;
        return -ENOENT;
      }

      block->blockID = std::stoull(out_kvs.at("blockID"));
      block->version = out_kvs.at("version");
      block->deleteMarker = (out_kvs.at("deleteMarker") == "1");
      block->size = std::stoull(out_kvs.at("size"));
      block->globalWeight = std::stoull(out_kvs.at("globalWeight"));
      block->cacheObj.objName = out_kvs.at("objName");
      block->cacheObj.bucketName = out_kvs.at("bucketName");
      block->cacheObj.creationTime = out_kvs.at("creationTime");
      block->cacheObj.dirty = (out_kvs.at("dirty") == "1");
      boost::split(block->cacheObj.hostsList, out_kvs.at("hosts"), boost::is_any_of("_"));
      block->cacheObj.etag = out_kvs.at("etag");
      block->cacheObj.size = std::stoull(out_kvs.at("objSize"));
      block->cacheObj.user_id = out_kvs.at("userId");
      block->cacheObj.display_name = out_kvs.at("displayName");
      block->cacheObj.acl = out_kvs.at("acl");

      size_t attrs_count = std::stoull(out_kvs.at("attrsCount"));
      size_t found_attrs = 0;

      for (const auto& [k, v] : out_kvs) {
        if (!k.starts_with("attr_"))
          continue;

        ceph::buffer::list bl;
        bl.append(v);
        block->cacheObj.attrs[k.substr(5)] = std::move(bl);
        found_attrs++;
      }

      if (found_attrs != attrs_count) {
        ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "() ERROR: expected " << attrs_count << " attrs but found " << found_attrs << dendl;
        return -EINVAL;
      }

      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return get_block(tr);
    }

    return lfdb::make_transactor(FDBdb)(get_block);
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, Transaction* txn)
{
  try {
    auto get_blocks = [&](auto& tr) {
      std::vector<std::map<std::string, std::string>> out_kvs(blocks.size());

      // -------- FETCH PHASE --------
      for (size_t i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];
        std::string key = build_index(&block);

        ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

        if (!lfdb::get(tr, key, out_kvs[i])) {
          ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: get function returned false!" << dendl;
          return -ENOENT;
        }
      }

      // -------- POPULATE PHASE --------
      for (size_t i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];
        auto& kvs = out_kvs[i];

        block.blockID = std::stoull(kvs.at("blockID"));
        block.version = kvs.at("version");
        block.deleteMarker = (std::stoi(kvs.at("deleteMarker")) != 0);
        block.size = std::stoull(kvs.at("size"));
        block.globalWeight = std::stoull(kvs.at("globalWeight"));
        block.cacheObj.objName = kvs.at("objName");
        block.cacheObj.bucketName = kvs.at("bucketName");
        block.cacheObj.creationTime = kvs.at("creationTime");
        block.cacheObj.dirty = (std::stoi(kvs.at("dirty")) != 0);
        block.cacheObj.hostsList.clear();
        boost::split(block.cacheObj.hostsList, kvs.at("hosts"), boost::is_any_of("_"));
        block.cacheObj.etag = kvs.at("etag");
        block.cacheObj.size = std::stoull(kvs.at("objSize"));
        block.cacheObj.user_id = kvs.at("userId");
        block.cacheObj.display_name = kvs.at("displayName");
        block.cacheObj.acl = kvs.at("acl");

        if (auto it = kvs.find("attrsCount"); it != kvs.end()) {
          [[maybe_unused]] size_t attrsCount = std::stoul(it->second);
        }

        block.cacheObj.attrs.clear();

        for (const auto& [field, value] : kvs) {
          if (field.rfind("attr_", 0) == 0) {
            ceph::buffer::list bl;
            bl.append(value);
            block.cacheObj.attrs[field.substr(5)] = std::move(bl);
          }
        }
      }

      return 0;
    };

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      return get_blocks(tr);
    }

    return lfdb::make_transactor(FDBdb)(get_blocks);
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int FDBBlockDirectory::copy(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, Transaction* txn)
{
  if (block == nullptr) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "(): null block pointer" << dendl;
    return -EINVAL;
  }

  if (this->get(dpp, y, block, txn) < 0) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
    return -ENOENT;
  }

  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };
  std::string copyKey = build_index(&copyBlock);

  copyBlock.version = block->version;
  copyBlock.deleteMarker = block->deleteMarker;
  copyBlock.size = block->size;
  copyBlock.globalWeight = block->globalWeight;
  copyBlock.cacheObj.dirty = block->cacheObj.dirty;
  copyBlock.cacheObj.creationTime = block->cacheObj.creationTime;
  copyBlock.cacheObj.hostsList = block->cacheObj.hostsList;
  copyBlock.cacheObj.etag = block->cacheObj.etag;
  copyBlock.cacheObj.size = block->cacheObj.size;
  copyBlock.cacheObj.user_id = block->cacheObj.user_id;
  copyBlock.cacheObj.display_name = block->cacheObj.display_name;
  copyBlock.cacheObj.acl = block->cacheObj.acl;
  copyBlock.cacheObj.attrs = block->cacheObj.attrs;

  return this->set(dpp, y, &copyBlock, txn);
}

int FDBBlockDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, Transaction* txn)
{
  if (block == nullptr) {
    return -EINVAL;
  }

  try {
    std::string key = build_index(block);

    if (txn) {
      auto* fdb_txn = dynamic_cast<FDBTransaction*>(txn);
      if (!fdb_txn) {
        return -EINVAL;
      }
      auto& tr = fdb_txn->get_transaction();
      lfdb::erase(tr, key);
      return 0;
    }

    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      lfdb::erase(tr, key);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}


int FDBBlockDirectory::update_field(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& field, std::string& value, Transaction* txn)
{
  int ret = -1;
  if (block == nullptr) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "(): null block pointer" << dendl;
    return -EINVAL;
  }

  if (!(ret = exist_key(dpp, y, block, txn))) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
	return -ENOENT;
  }

  if (this->get(dpp, y, block, txn) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -ENOENT;
  }

  if (field == "blockID") {
    block->blockID = std::stoull(value);
  }
  else if (field == "version") {
    block->version = value;
  }
  else if (field == "deleteMarker") {
    block->deleteMarker = (value == "1");
  }
  else if (field == "size") {
    block->size = std::stoull(value);
  }
  else if (field == "globalWeight") {
    block->globalWeight = std::stoull(value);
  }
  else if (field == "objName") {
    block->cacheObj.objName = value;
  }
  else if (field == "bucketName") {
    block->cacheObj.bucketName = value;
  }
  else if (field == "dirty") {
    block->cacheObj.dirty = (value == "1");
  }
  else if (field == "creationTime") {
    block->cacheObj.creationTime = value;
  }
  else if (field == "hosts") {
    block->cacheObj.hostsList.insert(value);
  }
  else if (field == "etag") {
    block->cacheObj.etag = value;
  }
  else if (field == "objSize") {
    block->cacheObj.size = std::stoull(value);
  }
  else if (field == "userId") {
    block->cacheObj.user_id = value;
  }
  else if (field == "displayName") {
    block->cacheObj.display_name = value;
  }

  return this->set(dpp, y, block, txn);

}

int FDBBlockDirectory::remove_host(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& value, Transaction* txn)
{
  int ret = -1;

  if (!(ret = exist_key(dpp, y, block, txn))) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
	return -ENOENT;
  }

  if (this->get(dpp, y, block, txn) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -ENOENT;
  }

  block->cacheObj.hostsList.erase(value);

  this->set(dpp, y, block, txn);

  return 0;
}

} // namespace rgw::d4n
