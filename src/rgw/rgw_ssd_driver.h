#pragma once

#include <aio.h>
#include <boost/intrusive/set.hpp>
#include "rgw_common.h"
#include "rgw_cache_driver.h"

namespace rgw { namespace cache {

struct FileDescriptorEntry : public boost::intrusive::list_base_hook<>,
                              public boost::intrusive::set_base_hook<> {
    std::string file_path;
    int fd;

    FileDescriptorEntry(const std::string& file_path, int fd) 
        : file_path(file_path), fd(fd) {}

    std::string get_key() const { return file_path; }

    bool operator<(const FileDescriptorEntry& other) {
        return file_path < other.file_path;
    }

    bool operator>(const FileDescriptorEntry& other) {
        return file_path > other.file_path;
    }

    bool operator==(const FileDescriptorEntry& other) {
        return file_path == other.file_path;
    }
};

template<typename Entry, typename Key = std::string>
class LRUCache {
private:
    struct Entry_delete_disposer {
      void operator()(Entry* e) {
        delete e;
      }
    };
    struct key_compare {
        bool operator()(const Entry& a, const Entry& b) const {
            return a.get_key() < b.get_key();
        }
    };
    std::mutex lru_lock;
    typedef boost::intrusive::list<Entry> List;
    typedef boost::intrusive::set<Entry, boost::intrusive::compare<key_compare>> Set;
    List entries_lru_list;
    Set entries_set;
    const uint64_t max_entries;
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> cache_evictions{0};
    std::atomic<uint64_t> fd_lseek{0};

    std::function<void(Entry*)> cleanup_func;

    Entry* evict(const DoutPrefixProvider* dpp);

public:
    LRUCache(const uint64_t max_entries, 
              std::function<void(Entry*)> cleanup = nullptr) : max_entries(max_entries), cleanup_func(cleanup) {}
    ~LRUCache() {
        const std::lock_guard l(lru_lock);
        entries_set.clear();
        entries_lru_list.clear_and_dispose(Entry_delete_disposer());
    }
    Entry* get(const DoutPrefixProvider* dpp, const Key& key);
    Entry* put(const DoutPrefixProvider* dpp, Entry* entry);
    int put_with_cleanup(const DoutPrefixProvider* dpp, Entry* entry);
    int erase(const DoutPrefixProvider* dpp, const Key& key);
};

using FileDescriptorCache = LRUCache<FileDescriptorEntry>;

class SSDDriver : public CacheDriver {
public:
  SSDDriver(Partition& partition_info) : partition_info(partition_info) {}
  virtual ~SSDDriver() { ::close(dir_fd); }

  virtual int initialize(const DoutPrefixProvider* dpp) override;
  virtual int put(const DoutPrefixProvider* dpp, const CacheKey& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int get(const DoutPrefixProvider* dpp, const CacheKey& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual rgw::AioResultList get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const CacheKey& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;
  virtual rgw::AioResultList put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const CacheKey& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id) override;
  virtual int append_data(const DoutPrefixProvider* dpp, const CacheKey& key, const bufferlist& bl_data, optional_yield y) override;
  virtual int delete_data(const DoutPrefixProvider* dpp, const CacheKey& key, optional_yield y) override;
  virtual int rename(const DoutPrefixProvider* dpp, const CacheKey& oldKey, const CacheKey& newKey, optional_yield y) override;
  virtual int get_attrs(const DoutPrefixProvider* dpp, const CacheKey& key, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int set_attrs(const DoutPrefixProvider* dpp, const CacheKey& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int update_attrs(const DoutPrefixProvider* dpp, const CacheKey& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int delete_attrs(const DoutPrefixProvider* dpp, const CacheKey& key, rgw::sal::Attrs& del_attrs, optional_yield y) override;
  virtual int get_attr(const DoutPrefixProvider* dpp, const CacheKey& key, const std::string& attr_name, std::string& attr_val, optional_yield y) override;
  virtual int set_attr(const DoutPrefixProvider* dpp, const CacheKey& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) override;
  int delete_attr(const DoutPrefixProvider* dpp, const CacheKey& key, const std::string& attr_name);

  /* Partition */
  virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
  virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override;
  void set_free_space(const DoutPrefixProvider* dpp, uint64_t free_space);

  virtual int restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func) override;
  int get_dir_fd() { return dir_fd; }
  void set_dir_fd(int dir_fd) { this->dir_fd = dir_fd; }

private:
  Partition partition_info;
  uint64_t free_space;
  CephContext* cct;
  std::mutex cache_lock;
  int dir_fd{0};
  std::unique_ptr<FileDescriptorCache> fd_cache = nullptr;

  struct libaio_read_handler {
    rgw::Aio* throttle = nullptr;
    rgw::AioResult& r;
    // read callback
    void operator()(boost::system::error_code ec, bufferlist bl) const {
      r.result = -ec.value();
      r.data = std::move(bl);
      throttle->put(r);
    }
  };

  struct libaio_write_handler {
    rgw::Aio* throttle = nullptr;
    rgw::AioResult& r;
    // write callback
    void operator()(boost::system::error_code ec) const {
      r.result = -ec.value();
      throttle->put(r);
    }
  };

  // unique_ptr with custom deleter for struct aiocb
  struct libaio_write_aiocb_deleter {
    void operator()(struct aiocb* c) {
      if(c->aio_fildes > 0) {
	      TEMP_FAILURE_RETRY(::close(c->aio_fildes));
      }
      c->aio_buf = nullptr;
      delete c;
    }
  };

  struct libaio_read_aiocb_deleter {
    void operator()(struct aiocb* c) {
      c->aio_buf = nullptr;
      delete c;
    }
  };

  template <typename Executor, typename CompletionToken>
    auto get_async(const DoutPrefixProvider *dpp, const Executor& ex, const CacheKey& key,
		    off_t read_ofs, off_t read_len, CompletionToken&& token);
  
  template <typename Executor, typename CompletionToken>
  void put_async(const DoutPrefixProvider *dpp, const Executor& ex, const CacheKey& key,
                  const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token);
  
  rgw::Aio::OpFunc ssd_cache_read_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
				  off_t read_ofs, off_t read_len, const CacheKey& key);

  rgw::Aio::OpFunc ssd_cache_write_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, const CacheKey& key);

  using unique_aio_read_cb_ptr = std::unique_ptr<struct aiocb, libaio_read_aiocb_deleter>;
  using unique_aio_write_cb_ptr = std::unique_ptr<struct aiocb, libaio_write_aiocb_deleter>;

  struct AsyncReadOp {
    bufferlist result;
    unique_aio_read_cb_ptr aio_cb;
    SSDDriver *priv_data;
    bool close_fd;
    using Signature = void(boost::system::error_code, bufferlist);
    using Completion = ceph::async::Completion<Signature, AsyncReadOp>;

    int prepare_libaio_read_op(const DoutPrefixProvider *dpp, const CacheKey& cache_key, off_t read_ofs, off_t read_len, void* arg);
    static void libaio_cb_aio_dispatch(sigval sigval);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

  struct AsyncWriteRequest {
    const DoutPrefixProvider* dpp;
	  std::string file_path;
    std::string temp_file_path;
	  void *data;
	  int fd;
    CacheKey key;
	  unique_aio_write_cb_ptr cb;
    SSDDriver *priv_data;
    rgw::sal::Attrs attrs;

    using Signature = void(boost::system::error_code);
    using Completion = ceph::async::Completion<Signature, AsyncWriteRequest>;

	  int prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path);
    static void libaio_write_cb(sigval sigval);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };
  int get_attrs(const DoutPrefixProvider* dpp, int fd, rgw::sal::Attrs& attrs, optional_yield y);
  int get_attr(const DoutPrefixProvider* dpp, int fd, const std::string& attr_name, std::string& attr_val, optional_yield y);
  int set_attrs(const DoutPrefixProvider* dpp, int fd, const rgw::sal::Attrs& attrs, optional_yield y);
  int set_attr(const DoutPrefixProvider* dpp, int fd, const std::string& attr_name, const std::string& attr_val, optional_yield y);
};

} } // namespace rgw::cache

