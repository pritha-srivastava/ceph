#include "common/async/completion.h"
#include "common/errno.h"
#include "common/async/blocked_completion.h"
#include "rgw_ssd_driver.h"
#if defined(__linux__)
#include <features.h>
#include <sys/xattr.h>
#endif

#include <filesystem>
#include <errno.h>
namespace efs = std::filesystem;

namespace rgw { namespace cache {

static std::atomic<uint64_t> index{0};
static std::atomic<uint64_t> dir_index{0};

static inline std::string get_key(const CacheKey& key) {
  if (key.len == 0 && key.offset == 0) {
    return fmt::format("{}{}{}{}{}", url_encode(key.bucket_id, true), CACHE_DELIM, url_encode(key.version, true), CACHE_DELIM, url_encode(key.obj_name, true));
  } else {
    return fmt::format("{}{}{}{}{}{}{}{}{}", url_encode(key.bucket_id, true), CACHE_DELIM, url_encode(key.version, true), CACHE_DELIM, url_encode(key.obj_name, true), CACHE_DELIM, std::to_string(key.offset), CACHE_DELIM, std::to_string(key.len));
  }
}

#if 0
static std::vector<std::string> tokenize_key(std::string_view key)
{
    std::vector<std::string> tokens;
    size_t start = 0, end = 0;
    while ((end = key.find(CACHE_DELIM, start)) != std::string_view::npos) {
        tokens.emplace_back(key.substr(start, end - start));
        start = end + 1;
    }
    // Add the last token
    if (start < key.length()) {
        tokens.emplace_back(key.substr(start));
    }
    return tokens;
}
#endif
/*
* Parses key to return directory path and file name
*/
static void parse_key(const DoutPrefixProvider* dpp, const std::string& location, const CacheKey& key, std::string& dir_path, std::string& file_name, bool temp = false) {
    ldpp_dout(dpp, 10) << __func__ << "() key is: " << key << dendl;
    dir_path = location + "/" + url_encode(key.bucket_id, true) + "/" + url_encode(key.obj_name, true);
    if (key.offset == 0 && key.len == 0) {
        file_name = key.version;
    } else {
        file_name = key.version + CACHE_DELIM + std::to_string(key.offset) + CACHE_DELIM + std::to_string(key.len);
    }

    if (temp) {
        file_name += "_" + std::to_string(index++);
    }
    ldpp_dout(dpp, 10) <<  __func__ << "() dir_path is " << dir_path << dendl;
    ldpp_dout(dpp, 10) <<  __func__ << "() file_name is " << file_name << dendl;
    return;
#if 0
    version += CACHE_DELIM + parts[3] + CACHE_DELIM + parts[4];
    std::string bucket_id, object, version;
    std::vector<std::string> parts = tokenize_key(key);

    ldpp_dout(dpp, 10) << __func__ << "() parts.size() is " << parts.size() << dendl;

    if (parts.size() == 3 || parts.size() == 5) {
        bucket_id = parts[0];
        ldpp_dout(dpp, 10) <<  __func__ << "() bucket_id is " << bucket_id << dendl;
        object = parts[2];
        ldpp_dout(dpp, 10) <<  __func__ << "() object is " << object << dendl;
        version = parts[1];
        if (parts.size() == 5) { //has offset and length
            version += CACHE_DELIM + parts[3] + CACHE_DELIM + parts[4];
        }
        if (temp) {
            version += "_" + std::to_string(index++);
        }
        ldpp_dout(dpp, 10) <<  __func__ << "() version is " << version << dendl;
        dir_path = location + "/" + bucket_id + "/" + object;
        file_name = version;
        ldpp_dout(dpp, 10) <<  __func__ << "() dir_path is " << dir_path << dendl;
    }
    return;
#endif
}

static void create_directories(const DoutPrefixProvider* dpp, const std::string& dir_path, SSDDriver* cache_driver)
{
    std::error_code ec;
    std::string temp_dir_path = dir_path + "_" + std::to_string(dir_index++);
    if (!efs::exists(dir_path, ec)) {
        if (!efs::create_directories(temp_dir_path, ec)) {
            ldpp_dout(dpp, 0) << "create_directories::: ERROR creating directory: '" << temp_dir_path <<
                            "' : " << ec.value() << dendl;
        } else {
            efs::rename(temp_dir_path, dir_path, ec);
            if (ec) {
                ldpp_dout(dpp, 0) << "create_directories::: ERROR renaming directory: '" << temp_dir_path <<
                            "' : " << ec.value() << dendl;
                efs::remove(temp_dir_path, ec);
            } else {
                uid_t uid = dpp->get_cct()->get_set_uid();
                gid_t gid = dpp->get_cct()->get_set_gid();

                ldpp_dout(dpp, 5) << "create_directories:: uid is " << uid << " and gid is " << gid << dendl;
                ldpp_dout(dpp, 5) << "create_directories:: changing permissions for directory: " << dendl;

                if (uid) {
                    if (chown(dir_path.c_str(), uid, gid) == -1) {
                        ldpp_dout(dpp, 5) << "create_directories: chown return error: " << strerror(errno) << dendl;
                    }

                    if (chmod(dir_path.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
                        ldpp_dout(dpp, 5) << "create_directories: chmod return error: " << strerror(errno) << dendl;
                    }
                }
                if (cache_driver && cache_driver->get_dir_fd() == 0) {
                    int dir_fd = open(cache_driver->get_current_partition_info(dpp).location.c_str(), O_RDONLY | O_DIRECTORY);
                    if (dir_fd < 0) {
                        ldpp_dout(dpp, 5) << "open directory returned error: " << dir_fd << dendl;
                    }
                    cache_driver->set_dir_fd(dir_fd);
                }
            }
        }
    }
}

static inline std::string get_file_path(const DoutPrefixProvider* dpp, const std::string& dir_path, const std::string& file_name)
{
    return dir_path + "/" + file_name;
}

static std::string create_dirs_get_filepath_from_key(const DoutPrefixProvider* dpp, const std::string& location, const CacheKey& key, SSDDriver* cache_driver=nullptr, bool temp=false)
{
    std::string dir_path, file_name;
    parse_key(dpp, location, key, dir_path, file_name, temp);
    create_directories(dpp, dir_path, cache_driver);
    return get_file_path(dpp, dir_path, file_name);

}

static int open_file_for_writing(const DoutPrefixProvider* dpp, const std::string& location, const CacheKey& key, int dir_fd)
{
    std::string dir_path, file_name;
    parse_key(dpp, location, key, dir_path, file_name);
    std::string file_path_name1 = url_encode(key.bucket_id, true) + "/" + url_encode(key.obj_name, true) + "/" + file_name;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    int fd = ::openat(dir_fd, file_path_name1.c_str(), O_WRONLY | O_CREAT | O_TRUNC | dpp->get_cct()->_conf->rgw_d4n_l1_write_open_flags, mode);
    if (fd < 0) {
        ldpp_dout(dpp, 5) << "file_path_name1: " << file_path_name1 << dendl;
        ldpp_dout(dpp, 5) << "open file failed: " << strerror(errno) << dendl;
        return -errno;
    }
    return fd;
}

static int open_file_for_reading(const DoutPrefixProvider* dpp, const std::string& location, const CacheKey& key, int dir_fd)
{
    std::string dir_path, file_name;
    parse_key(dpp, location, key, dir_path, file_name);
    std::string file_path_name1 = url_encode(key.bucket_id, true) + "/" + url_encode(key.obj_name, true) + "/" + file_name;
    int fd = TEMP_FAILURE_RETRY(::openat(dir_fd, file_path_name1.c_str(), O_RDONLY|O_CLOEXEC|O_BINARY));
    if (fd < 0) {
        ldpp_dout(dpp, 5) << "file_path_name1: " << file_path_name1 << dendl;
        ldpp_dout(dpp, 5) << "open file failed: " << strerror(errno) << dendl;
        return -errno;
    }
    return fd;
}


int SSDDriver::initialize(const DoutPrefixProvider* dpp)
{
    if(partition_info.location.back() != '/') {
      partition_info.location += "/";
    }

    try {
        if (efs::exists(partition_info.location)) {
            if (dpp->get_cct()->_conf->rgw_d4n_l1_evict_cache_on_start) {
                ldpp_dout(dpp, 5) << "initialize: evicting the persistent storage directory on start" << dendl;

		uid_t uid = dpp->get_cct()->get_set_uid();
		gid_t gid = dpp->get_cct()->get_set_gid();

		ldpp_dout(dpp, 5) << "initialize:: uid is " << uid << " and gid is " << gid << dendl;
		ldpp_dout(dpp, 5) << "initialize:: changing permissions for datacache directory." << dendl;

		if (uid) { 
                  if (chown(partition_info.location.c_str(), uid, gid) == -1) {
		    ldpp_dout(dpp, 5) << "initialize: chown return error: " << strerror(errno) << dendl;
                  }

                  if (chmod(partition_info.location.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
		    ldpp_dout(dpp, 5) << "initialize: chmod return error: " << strerror(errno) << dendl;
                  }
		}

                for (auto& p : efs::directory_iterator(partition_info.location)) {
                    efs::remove_all(p.path());
                }
            }
        } else {
            ldpp_dout(dpp, 5) << "initialize:: creating the persistent storage directory on start: " << partition_info.location << dendl;
            std::error_code ec;
            if (!efs::create_directories(partition_info.location, ec)) {
                ldpp_dout(dpp, 0) << "initialize::: ERROR initializing the cache storage directory: '" << partition_info.location <<
                                "' : " << ec.value() << dendl;
            } else {
		uid_t uid = dpp->get_cct()->get_set_uid();
		gid_t gid = dpp->get_cct()->get_set_gid();

		ldpp_dout(dpp, 5) << "initialize:: uid is " << uid << " and gid is " << gid << dendl;
		ldpp_dout(dpp, 5) << "initialize:: changing permissions for datacache directory." << dendl;
		
		if (uid) { 
                  if (chown(partition_info.location.c_str(), uid, gid) == -1) {
		    ldpp_dout(dpp, 5) << "initialize: chown return error: " << strerror(errno) << dendl;
                  }

                  if (chmod(partition_info.location.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
		    ldpp_dout(dpp, 5) << "initialize: chmod return error: " << strerror(errno) << dendl;
                  }
		}
            }
        }
    } catch (const efs::filesystem_error& e) {
        ldpp_dout(dpp, 0) << "initialize::: ERROR initializing the cache storage directory '" << partition_info.location <<
                                "' : " << e.what() << dendl;
        //return -EINVAL; Should return error from here?
    }

    #if defined(HAVE_LIBAIO) && defined(__GLIBC__)
    // libaio setup
    struct aioinit ainit{0};
    ainit.aio_threads = dpp->get_cct()->_conf.get_val<int64_t>("rgw_d4n_libaio_aio_threads");
    ainit.aio_num = dpp->get_cct()->_conf.get_val<int64_t>("rgw_d4n_libaio_aio_num");
    ainit.aio_idle_time = 120;
    aio_init(&ainit);
    #endif

    efs::space_info space = efs::space(partition_info.location);
    //currently partition_info.size is unused
    this->free_space = space.available;

    return 0;
}

int SSDDriver::restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func)
{
    if (dpp->get_cct()->_conf->rgw_d4n_l1_evict_cache_on_start) {
        return 0; //don't do anything as the cache directory must have been evicted during start-up
    }
    std::string cache_location = partition_info.location;
    if (cache_location.back() == '/') {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): cache_location: " << cache_location << dendl;
        cache_location.pop_back();
    }
    for (auto const& dir_entry : efs::directory_iterator{partition_info.location}) {
        std::string bucket_id, object_name;
        if (dir_entry.is_directory()) {
            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Is directory, path: " << dir_entry.path() << dendl;
            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): File Name: " << dir_entry.path().filename() << dendl;
            bucket_id = dir_entry.path().filename();
            for (auto const& sub_dir_entry : efs::directory_iterator{dir_entry.path()}) {
                if (sub_dir_entry.is_directory()) {
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Is directory, path: " << sub_dir_entry.path() << dendl;
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): File Name: " << sub_dir_entry.path().filename() << dendl;
                    object_name = sub_dir_entry.path().filename();
                    for (auto const& file_entry : efs::directory_iterator{sub_dir_entry.path()}) {
                        try {
                            if (file_entry.is_regular_file()) {
                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): filename: " << file_entry.path().filename() << dendl;
                                std::string file_name = file_entry.path().filename();
                                bool parsed = false;
                                std::vector<std::string> parts;
                                std::string part;
                                std::stringstream ss(file_name);
                                while (std::getline(ss, part, CACHE_DELIM)) {
                                    parts.push_back(part);
                                }
                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): parts.size(): " << parts.size() << dendl;
  
				std::string dirtyStr;
				bool dirty;
                if (parts.size() == 1 || parts.size() == 3) {
				    std::string version = parts[0];
				    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): version: " << version << dendl;

				    std::string key = bucket_id + CACHE_DELIM + version + CACHE_DELIM + object_name;
				    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key: " << key << dendl;

                    CacheKey cache_key{url_decode(bucket_id), url_decode(object_name), url_decode(version), 0, 0};
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): cache_key: " << cache_key << dendl;

				    uint64_t len = 0, offset = 0;
				    if (parts.size() == 1) {
                        auto ret = get_attr(dpp, cache_key, RGW_CACHE_ATTR_DIRTY, dirtyStr, null_yield);
                        if (ret == 0 && dirtyStr == "1") {
                            ldpp_dout(dpp, 10) << "SSDCache: " << __func__ << "(): Dirty xattr retrieved" << dendl;
                                dirty = true;
                        } else if (ret < 0) {
                            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << RGW_CACHE_ATTR_DIRTY << ", ret=" << ret << dendl;
                            dirty = false;
                        } else {
                            dirty = false;
                        }

					if (dirtyStr == "0") {
					    //non-dirty or clean blocks - version in head block and offset, len in data blocks
					    std::string localWeightStr;
					    ret = get_attr(dpp, cache_key, RGW_CACHE_ATTR_LOCAL_WEIGHT, localWeightStr, null_yield);
					    if (ret < 0) {
						ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << RGW_CACHE_ATTR_LOCAL_WEIGHT << dendl;
					    } else {
						ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): localWeightStr: " << localWeightStr << dendl;
					    }
					    block_func(dpp, key, offset, len, version, false, bucket_id, object_name, null_yield, localWeightStr);
					    parsed = true;
				        } else if (dirtyStr == "1") {
                                            //dirty blocks - version in head block and offset, len in data blocks
					    std::string localWeightStr;
					    std::string invalidStr;
					    rgw::sal::Attrs attrs;
					    get_attrs(dpp, cache_key, attrs, null_yield);
					    std::string etag, bucket_name;
					    uint64_t size = 0;
					    time_t creationTime = time_t(nullptr);
					    rgw_user user;
					    rgw_obj_key obj_key;
					    bool deleteMarker = false;
					    if (attrs.find(RGW_ATTR_ETAG) != attrs.end()) {
						etag = attrs[RGW_ATTR_ETAG].to_str();
						ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): etag: " << etag << dendl;
					    }
					    if (attrs.find(RGW_CACHE_ATTR_OBJECT_SIZE) != attrs.end()) {
						size = std::stoull(attrs[RGW_CACHE_ATTR_OBJECT_SIZE].to_str());
						ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): size: " << size << dendl;
					    }
					    if (attrs.find(RGW_CACHE_ATTR_MTIME) != attrs.end()) {
						creationTime = ceph::real_clock::to_time_t(ceph::real_clock::from_double(std::stod(attrs[RGW_CACHE_ATTR_MTIME].to_str())));
						ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): creationTime: " << creationTime << dendl;
					    }
					    if (attrs.find(RGW_ATTR_ACL) != attrs.end()) {
						bufferlist bl_acl = attrs[RGW_ATTR_ACL];
						RGWAccessControlPolicy policy;
						auto iter = bl_acl.cbegin();
						try {
						    policy.decode(iter);
						} catch (buffer::error& err) {
						    ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
						    continue;
						}
						user = std::get<rgw_user>(policy.get_owner().id);
						ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): rgw_user: " << user.to_str() << dendl;
					    }
					    obj_key.name = object_name;
					    if (attrs.find(RGW_CACHE_ATTR_VERSION_ID) != attrs.end()) {
						std::string instance = attrs[RGW_CACHE_ATTR_VERSION_ID].to_str();
						if (instance != "null") {
						    obj_key.instance = instance;
						}
					    }
					    if (attrs.find(RGW_CACHE_ATTR_OBJECT_NS) != attrs.end()) {
						obj_key.ns = attrs[RGW_CACHE_ATTR_OBJECT_NS].to_str();
					    }
					    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): rgw_obj_key: " << obj_key.get_oid() << dendl;
					    if (attrs.find(RGW_CACHE_ATTR_BUCKET_NAME) != attrs.end()) {
						bucket_name = attrs[RGW_CACHE_ATTR_BUCKET_NAME].to_str();
						ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): bucket_name: " << bucket_name << dendl;
					    }

					    if (attrs.find(RGW_CACHE_ATTR_LOCAL_WEIGHT) != attrs.end()) {
						localWeightStr = attrs[RGW_CACHE_ATTR_LOCAL_WEIGHT].to_str();
						ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): localWeightStr: " << localWeightStr << dendl;
					    }

					    if (attrs.find(RGW_CACHE_ATTR_DELETE_MARKER) != attrs.end()) {
						std::string deleteMarkerStr = attrs[RGW_CACHE_ATTR_LOCAL_WEIGHT].to_str();
						deleteMarker = (deleteMarkerStr == "1") ? true : false;
						ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): deleteMarker: " << deleteMarker << dendl;
					    }

					    if (attrs.find(RGW_CACHE_ATTR_INVALID) != attrs.end()) {
						invalidStr = attrs[RGW_CACHE_ATTR_INVALID].to_str();
						ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): invalidStr: " << invalidStr << dendl;
					    }

					    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): calling func for: " << key << dendl;
					    obj_func(dpp, key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key, null_yield, invalidStr);
					    block_func(dpp, key, offset, len, version, dirty, bucket_id, object_name, null_yield, localWeightStr);
					    parsed = true;
                                        } // end-if dirtyStr == "1"
				    } else if (parts.size() == 3) { //end-if parts.size() == 1
					offset = std::stoull(parts[1]);
					ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): offset: " << offset << dendl;

					len = std::stoull(parts[2]);
					ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): len: " << len << dendl;

					key = key + CACHE_DELIM + std::to_string(offset) + CACHE_DELIM + std::to_string(len);
					ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key: " << key << dendl;

                    cache_key.offset = offset;
                    cache_key.len = len;

                    auto ret = get_attr(dpp, cache_key, RGW_CACHE_ATTR_DIRTY, dirtyStr, null_yield);
                    if (ret == 0 && dirtyStr == "1") {
                        ldpp_dout(dpp, 10) << "SSDCache: " << __func__ << "(): Dirty xattr retrieved" << dendl;
                        dirty = true;
                    } else if (ret < 0) {
                        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << RGW_CACHE_ATTR_DIRTY << ", ret=" << ret << dendl;
                        dirty = false;
                    } else {
                        dirty = false;
                    }
					std::string localWeightStr;
					ret = get_attr(dpp, cache_key, RGW_CACHE_ATTR_LOCAL_WEIGHT, localWeightStr, null_yield);
					if (ret < 0) {
					    ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << RGW_CACHE_ATTR_LOCAL_WEIGHT << dendl;
					} else {
					    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): localWeightStr: " << localWeightStr << dendl;
					}
					block_func(dpp, key, offset, len, version, dirty, bucket_id, object_name, null_yield, localWeightStr);
					parsed = true;
				    } 
				    if (!parsed) {
					ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Unable to parse file_name: " << file_name << dendl;
					continue;
				    }
			        }
                            }
                        }//end - try
                        catch(...) {
                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Execption while parsing entry: " << file_entry.path() << dendl;
                            continue;
                        }
                    }
                }
            }
        }
    }

    return 0;
}

uint64_t SSDDriver::get_free_space(const DoutPrefixProvider* dpp)
{
    efs::space_info space = efs::space(partition_info.location);
    return space.available;
}

void SSDDriver::set_free_space(const DoutPrefixProvider* dpp, uint64_t free_space)
{
    std::lock_guard l(cache_lock);
    this->free_space = free_space;
}

int SSDDriver::put(const DoutPrefixProvider* dpp, const CacheKey& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    boost::system::error_code ec;
    if (y) {
        using namespace boost::asio;
        yield_context yield = y.get_yield_context();
        auto ex = yield.get_executor();
        this->put_async(dpp, ex, key, bl, len, attrs, yield[ec]);
    } else {
      auto ex = boost::asio::system_executor{};
      this->put_async(dpp, ex, key, bl, len, attrs, ceph::async::use_blocked[ec]);
    }
    if (ec) {
        return ec.value();
    }
    return 0;
}

int SSDDriver::get(const DoutPrefixProvider* dpp, const CacheKey& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y)
{
    char buffer[len];
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << __func__ << "(): location=" << location << dendl;
    FILE *cache_file = nullptr;
    int r = 0;
    size_t nbytes = 0;

    cache_file = fopen(location.c_str(), "r+");
    if (cache_file == nullptr) {
        ldpp_dout(dpp, 0) << "ERROR: get::fopen file has return error, errno=" << errno << dendl;
        return -errno;
    }

    fseek(cache_file, offset, SEEK_SET);

    nbytes = fread(buffer, 1, len, cache_file);
    if (nbytes != len) {
        fclose(cache_file);
        ldpp_dout(dpp, 0) << "ERROR: get::io_read: fread has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << len << dendl;
        return -EIO;
    }

    r = fclose(cache_file);
    if (r != 0) {
        ldpp_dout(dpp, 0) << "ERROR: get::fclose file has return error, errno=" << errno << dendl;
        return -errno;
    }

    bl.append(buffer, len);

    r = get_attrs(dpp, key, attrs, y);
    if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: get::get_attrs: failed to get attrs, r = " << r << dendl;
        return r;
    }

    return 0;
}

int SSDDriver::append_data(const DoutPrefixProvider* dpp, const CacheKey& key, const bufferlist& bl_data, optional_yield y)
{
    bufferlist src = bl_data;
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);

    ldpp_dout(dpp, 20) << __func__ << "(): location=" << location << dendl;
    FILE *cache_file = nullptr;
    int r = 0;
    size_t nbytes = 0;

    cache_file = fopen(location.c_str(), "a+");
    if (cache_file == nullptr) {
        ldpp_dout(dpp, 0) << "ERROR: put::fopen file has return error, errno=" << errno << dendl;
        return -errno;
    }

    nbytes = fwrite(src.c_str(), 1, src.length(), cache_file);
    if (nbytes != src.length()) {
        ldpp_dout(dpp, 0) << "ERROR: append_data: fwrite has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << bl_data.length() << dendl;
        return -EIO;
    }

    r = fclose(cache_file);
    if (r != 0) {
        ldpp_dout(dpp, 0) << "ERROR: append_data::fclose file has return error, errno=" << errno << dendl;
        return -errno;
    }
    std::lock_guard l(cache_lock);
    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

template <typename Executor1, typename CompletionHandler>
auto SSDDriver::AsyncReadOp::create(const Executor1& ex1, CompletionHandler&& handler)
{
    auto p = Completion::create(ex1, std::move(handler));
    return p;
}

template <typename Executor1, typename CompletionHandler>
auto SSDDriver::AsyncWriteRequest::create(const Executor1& ex1, CompletionHandler&& handler)
{
    auto p = Completion::create(ex1, std::move(handler));
    return p;
}

template <typename Executor, typename CompletionToken>
auto SSDDriver::get_async(const DoutPrefixProvider *dpp, const Executor& ex, const CacheKey& key,
                off_t read_ofs, off_t read_len, CompletionToken&& token)
{
  using Op = AsyncReadOp;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this] (auto handler, const DoutPrefixProvider *dpp,
              const Executor& ex, const CacheKey& key,
              off_t read_ofs, off_t read_len) {
    auto p = Op::create(ex, handler);
    auto& op = p->user_data;

    op.priv_data = this;
    int ret = op.prepare_libaio_read_op(dpp, key, read_ofs, read_len, p.get());
    if(0 == ret) {
        ret = ::aio_read(op.aio_cb.get());
    }
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::aio_read(), ret=" << ret << dendl;
    if(ret < 0) {
        auto ec = boost::system::error_code{-ret, boost::system::system_category()};
        ceph::async::post(std::move(p), ec, bufferlist{});
    } else {
        (void)p.release();
    }
  }, token, dpp, ex, key, read_ofs, read_len);
}

template <typename Executor, typename CompletionToken>
void SSDDriver::put_async(const DoutPrefixProvider *dpp, const Executor& ex, const CacheKey& key,
                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token)
{
  using Op = AsyncWriteRequest;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this] (auto handler, const DoutPrefixProvider *dpp,
              const Executor& ex, const CacheKey& key, const bufferlist& bl,
              uint64_t len, const rgw::sal::Attrs& attrs) {
    auto p = Op::create(ex, handler);
    auto& op = p->user_data;

    std::string dir_path;
    parse_key(dpp, this->get_current_partition_info(dpp).location, key, dir_path, op.file_path);
    parse_key(dpp, this->get_current_partition_info(dpp).location, key, dir_path, op.temp_file_path, true);
    create_directories(dpp, dir_path, this);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): op.file_path=" << op.file_path << dendl;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): op.temp_file_path=" << op.temp_file_path << dendl;

    int r = 0;
    bufferlist src = bl;
    op.priv_data = this;
    op.key = key;
    r = op.prepare_libaio_write_op(dpp, src, len, op.temp_file_path);
    op.cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    op.cb->aio_sigevent.sigev_notify_function = SSDDriver::AsyncWriteRequest::libaio_write_cb;
    op.cb->aio_sigevent.sigev_notify_attributes = nullptr;
    op.cb->aio_sigevent.sigev_value.sival_ptr = (void*)p.get();
    op.dpp = dpp;
    op.attrs = std::move(attrs);
    if (r >= 0) {
        r = ::aio_write(op.cb.get());
    } else {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::prepare_libaio_write_op(), r=" << r << dendl;
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::aio_write(), r=" << r << dendl;
    if(r < 0) {
        auto ec = boost::system::error_code{-r, boost::system::system_category()};
        ceph::async::dispatch(std::move(p), ec);
    } else {
        (void)p.release();
    }
  }, token, dpp, ex, key, bl, len, attrs);
}

rgw::Aio::OpFunc SSDDriver::ssd_cache_read_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                off_t read_ofs, off_t read_len, const CacheKey& key) {
  return [this, dpp, y, read_ofs, read_len, key] (Aio* aio, AioResult& r) mutable {
    ceph_assert(y);
    ldpp_dout(dpp, 20) << "SSDCache: cache_read_op(): Read From Cache, oid=" << r.obj.oid << dendl;

    using namespace boost::asio;
    yield_context yield = y.get_yield_context();
    auto ex = yield.get_executor();

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    this->get_async(dpp, ex, key, read_ofs, read_len, bind_executor(ex, SSDDriver::libaio_read_handler{aio, r}));
  };
}

rgw::Aio::OpFunc SSDDriver::ssd_cache_write_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, const CacheKey& key) {
  return [this, dpp, y, bl, len, attrs, key] (Aio* aio, AioResult& r) mutable {
    ceph_assert(y);
    ldpp_dout(dpp, 20) << "SSDCache: cache_write_op(): Write to Cache, oid=" << r.obj.oid << dendl;

    using namespace boost::asio;
    yield_context yield = y.get_yield_context();
    auto ex = yield.get_executor();

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    this->put_async(dpp, ex, key, bl, len, attrs, bind_executor(ex, SSDDriver::libaio_write_handler{aio, r}));
  };
}

rgw::AioResultList SSDDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const CacheKey& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id)
{
    rgw_raw_obj r_obj;
    std::string k = get_key(key);
    r_obj.oid = k;
    return aio->get(r_obj, ssd_cache_read_op(dpp, y, this, ofs, len, key), cost, id);
}

rgw::AioResultList SSDDriver::put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const CacheKey& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id)
{
    rgw_raw_obj r_obj;
    std::string k = get_key(key);
    r_obj.oid = k;
    return aio->get(r_obj, ssd_cache_write_op(dpp, y, this, bl, len, attrs, key), cost, id);
}

int SSDDriver::delete_data(const DoutPrefixProvider* dpp, const CacheKey& key, optional_yield y)
{
    std::string dir_path, file_name;
    parse_key(dpp, partition_info.location, key, dir_path, file_name);
    std::string location = get_file_path(dpp, dir_path, file_name);
    ldpp_dout(dpp, 20) << "INFO: delete_data::file to remove: " << location << dendl;
    std::error_code ec;

    //Remove file
    if (!efs::remove(location, ec)) {
        ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the file: " << location << dendl;
        return -ec.value();
    }

    //Remove directory if empty, removes object directory
    if (efs::is_empty(dir_path, ec)) {
        ldpp_dout(dpp, 20) << "INFO: delete_data::object directory to remove: " << dir_path << " :" << ec.value() << dendl;
        if (!efs::remove(dir_path, ec)) {
            //another version could have been written between the check and removal, hence not returning error from here
            ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the directory: " << dir_path  << " :" << ec.value() << dendl;
        }
    }
    auto pos = dir_path.find_last_of('/');
    if (pos != std::string::npos) {
        dir_path.erase(pos, (dir_path.length() - pos));

        //Remove bucket directory
        if (efs::is_empty(dir_path, ec)) {
            ldpp_dout(dpp, 20) << "INFO: delete_data::bucket directory to remove: " << dir_path << " :" << ec.value() << dendl;
            if (!efs::remove(dir_path, ec)) {
                //another object could have been written between the check and removal, hence not returning error from here
                ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the directory: " << dir_path << " :" << ec.value() << dendl;
            }
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::rename(const DoutPrefixProvider* dpp, const CacheKey& oldKey, const CacheKey& newKey, optional_yield y)
{ 
    std::string old_file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, oldKey);
    std::string new_file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, newKey);
    int ret = std::rename(old_file_path.c_str(), new_file_path.c_str());
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDDriver: ERROR: failed to rename the file: " << old_file_path << dendl;
        return ret;
    }

    return 0;
}


int SSDDriver::AsyncWriteRequest::prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path)
{
    int r = 0;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Write To Cache, location=" << file_path << dendl;
    cb.reset(new struct aiocb);
    memset(cb.get(), 0, sizeof(struct aiocb));
    std::string file_path_name1 = url_encode(key.bucket_id, true) + "/" + url_encode(key.obj_name, true) + "/" + file_path;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    r = fd = ::openat(priv_data->dir_fd, file_path_name1.c_str(), O_WRONLY | O_CREAT | O_TRUNC | dpp->get_cct()->_conf->rgw_d4n_l1_write_open_flags, mode);
    if (fd < 0) {
        //directories might have been deleted by a parallel delete of the last version of an object
        if (errno == ENOENT) {
            //retry after creating directories
            std::string dir_path = priv_data->get_current_partition_info(dpp).location + "/" + file_path_name1;
            ldpp_dout(dpp, 20) << "INFO: AsyncWriteRequest::prepare_libaio_write_op: dir_path for creating directories=" << dir_path << dendl;
            create_directories(dpp, dir_path, priv_data);
            r = fd = ::openat(priv_data->dir_fd, file_path_name1.c_str(), O_WRONLY | O_CREAT | O_TRUNC | dpp->get_cct()->_conf->rgw_d4n_l1_write_open_flags, mode);
            if (r < 0) {
                ldpp_dout(dpp, 0) << "ERROR: AsyncWriteRequest::prepare_libaio_write_op: open file failed, errno=" << errno << ", location='" << file_path.c_str() << "'" << dendl;
                return r;
            }
        } else {
            ldpp_dout(dpp, 0) << "ERROR: AsyncWriteRequest::prepare_libaio_write_op: open file failed, errno=" << errno << ", location='" << file_path.c_str() << "'" << dendl;
            return r;
        }
    }
    if (dpp->get_cct()->_conf->rgw_d4n_l1_fadvise != POSIX_FADV_NORMAL)
        posix_fadvise(fd, 0, 0, dpp->get_cct()->_conf->rgw_d4n_l1_fadvise);
    cb->aio_fildes = fd;

    data = malloc(len);
    if (!data) {
        ldpp_dout(dpp, 0) << "ERROR: AsyncWriteRequest::prepare_libaio_write_op: memory allocation failed" << dendl;
        ::close(fd);
        return r;
    }
    cb->aio_buf = data;
    memcpy((void*)data, bl.c_str(), len);
    cb->aio_nbytes = len;
    return r;
}

void SSDDriver::AsyncWriteRequest::libaio_write_cb(sigval sigval) {
    auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
    auto op = std::move(p->user_data);
    ldpp_dout(op.dpp, 20) << "INFO: AsyncWriteRequest::libaio_write_cb: key: " << op.file_path << dendl;
    int ret = -aio_error(op.cb.get());
    boost::system::error_code ec;
    if (ret < 0) {
        ec.assign(-ret, boost::system::system_category());
        ceph::async::dispatch(std::move(p), ec);
        return;
    }
    int attr_ret = 0;
    if (op.attrs.size() > 0) {
        //TODO - fix yield_context
        optional_yield y{null_yield};
        attr_ret = op.priv_data->set_attrs(op.dpp, op.fd, op.attrs, y);
        if (attr_ret < 0) {
            ldpp_dout(op.dpp, 0) << "ERROR: AsyncWriteRequest::libaio_write_yield_cb::set_attrs: failed to set attrs, ret = " << attr_ret << dendl;
            ec.assign(-ret, boost::system::system_category());
            ceph::async::dispatch(std::move(p), ec);
            return;
        }
    }

    Partition partition_info = op.priv_data->get_current_partition_info(op.dpp);
    efs::space_info space = efs::space(partition_info.location);
    op.priv_data->set_free_space(op.dpp, space.available);

    ldpp_dout(op.dpp, 20) << "INFO: AsyncWriteRequest::libaio_write_yield_cb: new_path: " << op.file_path << dendl;
    ldpp_dout(op.dpp, 20) << "INFO: AsyncWriteRequest::libaio_write_yield_cb: old_path: " << op.temp_file_path << dendl;

    std::string temp_file_name = url_encode(op.key.bucket_id, true) + "/" + url_encode(op.key.obj_name, true) + "/" + op.temp_file_path;
    std::string file_name = url_encode(op.key.bucket_id, true) + "/" + url_encode(op.key.obj_name, true) + "/" + op.file_path;
    ret = ::renameat(op.priv_data->get_dir_fd(), temp_file_name.c_str(), op.priv_data->get_dir_fd(), file_name.c_str());
    if (ret < 0) {
        ret = errno;
        ldpp_dout(op.dpp, 0) << "ERROR: put::rename: failed to rename file: " << ret << dendl;
        ec.assign(-ret, boost::system::system_category());
    }
    ceph::async::dispatch(std::move(p), ec);
}

int SSDDriver::AsyncReadOp::prepare_libaio_read_op(const DoutPrefixProvider *dpp, const CacheKey& cache_key, off_t read_ofs, off_t read_len, void* arg)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << cache_key << dendl;
    aio_cb.reset(new struct aiocb);
    memset(aio_cb.get(), 0, sizeof(struct aiocb));
    aio_cb->aio_fildes = open_file_for_reading(dpp, priv_data->partition_info.location, cache_key, priv_data->get_dir_fd());
    if(aio_cb->aio_fildes < 0) {
        int err = errno;
        ldpp_dout(dpp, 1) << "ERROR: SSDCache: " << __func__ << "(): can't open " << cache_key << " : " << " error: " << err << dendl;
        return -err;
    }
    if (dpp->get_cct()->_conf->rgw_d4n_l1_fadvise != POSIX_FADV_NORMAL) {
        posix_fadvise(aio_cb->aio_fildes, 0, 0, g_conf()->rgw_d4n_l1_fadvise);
    }

    bufferptr bp(read_len);
    aio_cb->aio_buf = bp.c_str();
    result.append(std::move(bp));

    aio_cb->aio_nbytes = read_len;
    aio_cb->aio_offset = read_ofs;
    aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    aio_cb->aio_sigevent.sigev_notify_function = libaio_cb_aio_dispatch;
    aio_cb->aio_sigevent.sigev_notify_attributes = nullptr;
    aio_cb->aio_sigevent.sigev_value.sival_ptr = arg;

    return 0;
}

void SSDDriver::AsyncReadOp::libaio_cb_aio_dispatch(sigval sigval)
{
    auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
    auto op = std::move(p->user_data);
    const int ret = -aio_error(op.aio_cb.get());
    boost::system::error_code ec;
    if (ret < 0) {
        ec.assign(-ret, boost::system::system_category());
    }

    ceph::async::dispatch(std::move(p), ec, std::move(op.result));
}

int SSDDriver::update_attrs(const DoutPrefixProvider* dpp, const CacheKey& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : attrs) {
        std::string attr_name = it.first;
        std::string attr_val = it.second.to_str();
        auto ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), XATTR_REPLACE);
        if (ret < 0 && errno == ENODATA) {
            ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), XATTR_CREATE);
        }
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not modify attr value for attr name: " << attr_name << " key: " << key << " ERROR: " << cpp_strerror(errno) <<dendl;
            return ret;
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;
    return 0;
}

int SSDDriver::delete_attrs(const DoutPrefixProvider* dpp, const CacheKey& key, rgw::sal::Attrs& del_attrs, optional_yield y)
{
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : del_attrs) {
        auto ret = delete_attr(dpp, key, it.first);
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not remove attr value for attr name: " << it.first << " key: " << key << cpp_strerror(errno) << dendl;
            return ret;
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::get_attrs(const DoutPrefixProvider* dpp, int fd, rgw::sal::Attrs& attrs, optional_yield y)
{
    char namebuf[64 * 1024];
    int ret;
    ssize_t buflen = flistxattr(fd, namebuf, sizeof(namebuf));
    if (buflen < 0) {
        ret = errno;
        return -ret;
    }
    char *keyptr = namebuf;
    while (buflen > 0) {
        ssize_t keylen;

        keylen = strlen(keyptr) + 1;
        std::string attr_name(keyptr);
        std::string::size_type prefixloc = attr_name.find(RGW_ATTR_PREFIX);
        buflen -= keylen;
        keyptr += keylen;
        if (prefixloc == std::string::npos) {
            continue;
        }
        std::string attr_value;
        get_attr(dpp, fd, attr_name, attr_value, y);
        bufferlist bl_value;
        bl_value.append(attr_value);
        attrs.emplace(std::move(attr_name), std::move(bl_value));
    }
    return 0;
}

int SSDDriver::get_attrs(const DoutPrefixProvider* dpp, const CacheKey& key, rgw::sal::Attrs& attrs, optional_yield y)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    int fd = open_file_for_reading(dpp, partition_info.location, key, dir_fd);
    if (fd < 0) {
        return fd;
    }
    auto ret = get_attrs(dpp, fd, attrs, y);
    //close fd, if fd cache is not enabled
    ::close(fd);
    return ret;
}

int SSDDriver::get_attr(const DoutPrefixProvider* dpp, int fd, const std::string& attr_name, std::string& attr_val, optional_yield y)
{
    size_t buffer_size = 256;
    while (true) {
        attr_val.resize(buffer_size);
        ssize_t attr_size = fgetxattr(fd, attr_name.c_str(), attr_val.data(), attr_val.size());
        if (attr_size < 0) {
            if (errno == ERANGE) {
                // Buffer too small, get actual size needed
                attr_size = fgetxattr(fd, attr_name.c_str(), nullptr, 0);
                if (attr_size < 0) {
                    ldpp_dout(dpp, 0) << "ERROR: could not get attribute " << attr_name << ": " << cpp_strerror(errno) << dendl;
                    attr_val = "";
                    return errno;
                }
                if (attr_size == 0) {
                    ldpp_dout(dpp, 0) << "ERROR: no attribute value found for attr_name: " << attr_name << dendl;
                    attr_val = "";
                    return 0;
                }
                // Resize and try again
                buffer_size = static_cast<size_t>(attr_size);
                continue;
            }
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not get attribute " << attr_name << ": " << cpp_strerror(errno) << dendl;
            attr_val = "";
            return errno;
        } //end-if result < 0
        if (attr_size == 0) {
            ldpp_dout(dpp, 0) << "ERROR: no attribute value found for attr_name: " << attr_name << dendl;
            attr_val = "";
            return 0;
        } //end-if result == 0
        // Success - resize buffer to actual data size and return
        ldpp_dout(dpp, 20) << "INFO: attr_size is: " << attr_size << dendl;
        attr_val.resize(static_cast<size_t>(attr_size));
        return 0;
    }
}

int SSDDriver::get_attr(const DoutPrefixProvider* dpp, const CacheKey& key, const std::string& attr_name, std::string& attr_val, optional_yield y)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): get_attr: attr_name: " << attr_name << dendl;
    int fd = open_file_for_reading(dpp, partition_info.location, key, dir_fd);
    if (fd < 0) {
        return fd;
    }
    auto ret = get_attr(dpp, fd, attr_name, attr_val, y);
    //close fd, if fd cache is not enabled
    ::close(fd);
    return ret;
}

int SSDDriver::set_attrs(const DoutPrefixProvider* dpp, int fd, const rgw::sal::Attrs& attrs, optional_yield y)
{
    for (auto& [attr_name, attr_val_bl] : attrs) {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): attr_name = " << attr_name << " attr_val_bl length: " << attr_val_bl.length() << dendl;
        if (attr_val_bl.length() != 0) {
            auto ret = set_attr(dpp, fd, attr_name, attr_val_bl.to_str(), y);
            if (ret < 0) {
                ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not set attr value for attr name: " << attr_name << cpp_strerror(errno) << dendl;
                return ret;
            }
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::set_attrs(const DoutPrefixProvider* dpp, const CacheKey& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    int fd = open_file_for_writing(dpp, partition_info.location, key, dir_fd);
    auto ret = set_attrs(dpp, fd, attrs, y);
    if (ret < 0) {
        ldpp_dout(dpp, 10) << "SSDCache: " << __func__ << "(): ret=" << ret << dendl;
    }
    ::close(fd);
    return ret;
}

int SSDDriver::set_attr(const DoutPrefixProvider* dpp, int fd, const std::string& attr_name, const std::string& attr_val, optional_yield y)
{
    if (attr_name == RGW_ATTR_ACL) {
      if (dpp->get_cct()->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
        std::string policy_json;
        RGWAccessControlPolicy policy;
        bufferlist bl;
        bl.append(attr_val);
        auto bliter = bl.cbegin();
        try {
          policy.decode(bliter);
          Formatter *f = Formatter::create("json");
          policy.dump(f);
          std::stringstream ss;
          f->flush(ss);
          policy_json = ss.str();
          delete f;
        } catch (buffer::error& err) {
          ldpp_dout(dpp, 0) << "ERROR: decode policy failed" << err.what() << dendl;
          policy_json = "ERROR: decode policy failed";
        }
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): set_attr: key: " << attr_name << " val: " << policy_json << dendl;
      }
    } else {
      ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): set_attr: key: " << attr_name << " val: " << attr_val << dendl;
    }

    auto ret = fsetxattr(fd, attr_name.c_str(), attr_val.c_str(), attr_val.size(), 0);
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not set attr value for attr name: " << attr_name << cpp_strerror(errno) << dendl;
        return ret;
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::set_attr(const DoutPrefixProvider* dpp, const CacheKey& key, const std::string& attr_name, const std::string& attr_val, optional_yield y)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    int fd = open_file_for_writing(dpp, partition_info.location, key, dir_fd);
    auto ret = set_attr(dpp, fd, attr_name, attr_val, y);
    if (ret < 0) {
        ldpp_dout(dpp, 10) << "SSDCache: " << __func__ << "(): ret=" << ret << dendl;
    }
    ::close(fd);
    return ret;
}

int SSDDriver::delete_attr(const DoutPrefixProvider* dpp, const CacheKey& key, const std::string& attr_name)
{
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    auto ret = removexattr(location.c_str(), attr_name.c_str());
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not remove attr value for attr name: " << attr_name << " key: " << key << cpp_strerror(errno) << dendl;
        return ret;
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

} } // namespace rgw::cache
