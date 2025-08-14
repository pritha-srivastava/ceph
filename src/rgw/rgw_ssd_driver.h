#pragma once

#include <aio.h>
#include <boost/intrusive/set.hpp>
#include <xxhash.h>
#include "common/cohort_lru.h"
#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_cache_driver.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace cache {

namespace bi = boost::intrusive;

struct FileDescriptorEntry : public cohort::lru::Object {
  std::string file_path;
  int fd;
  uint64_t hk;
  std::mutex mtx;
  uint32_t flags;
  static constexpr uint64_t seed = 8675309;
  static constexpr uint32_t FLAG_NONE = 0x0000;
  static constexpr uint32_t FLAG_LOCKED = 0x0001;//do we need LOCK?
  static constexpr uint32_t FLAG_LOCK =   0x0002;//do we need LOCKED?
  static constexpr uint32_t FLAG_DELETED = 0x0004;

  bool reclaim(const cohort::lru::ObjectFactory* newobj_fac) override;

  struct FdLT
  {
    bool operator()(const FileDescriptorEntry& lhs, const FileDescriptorEntry& rhs) const
    { return (lhs.file_path < rhs.file_path); }

    bool operator()(const std::string& k, const FileDescriptorEntry& fd) const
    { return k < fd.file_path; }

    bool operator()(const FileDescriptorEntry& fd, const std::string& k) const
    { return fd.file_path < k; }
  };

  struct FdEQ
  {
    bool operator()(const FileDescriptorEntry& lhs, const FileDescriptorEntry& rhs) const
    { return (lhs.file_path == rhs.file_path); }

    bool operator()(const std::string& k, const FileDescriptorEntry& fd) const
    { return k == fd.file_path; }

    bool operator()(const FileDescriptorEntry& fd, const std::string& k) const
    { return fd.file_path == k; }
  };

  typedef bi::link_mode<bi::safe_link> link_mode;
  typedef bi::set_member_hook<link_mode> tree_hook_type;
  tree_hook_type fd_hook;

  typedef cohort::lru::LRU<std::mutex> FDLRU;
  typedef bi::member_hook<FileDescriptorEntry, tree_hook_type, &FileDescriptorEntry::fd_hook> FdHook;
  typedef bi::rbtree<FileDescriptorEntry, bi::compare<FdLT>, FdHook> FdTree;
  typedef cohort::lru::TreeX<FileDescriptorEntry, FdTree, FdLT, FdEQ, std::string, std::mutex> FDCache;

  class Factory : public cohort::lru::ObjectFactory {
  public:
    std::string path;
    uint64_t hk;
    FDCache* fd_cache;
    int fd;

    Factory(const std::string& path, FDCache* fd_cache) : path(path), fd_cache(fd_cache)
    {
      hk = XXH64(path.c_str(), path.length(), FileDescriptorEntry::seed);
    }

    Factory() = delete;

    void recycle (cohort::lru::Object* o) override;

    cohort::lru::Object* alloc() override {
        return new FileDescriptorEntry(path, hk, fd_cache);
    }
  };

  FDCache* fd_cache;
  FileDescriptorEntry(const std::string& path, uint64_t hk, FDCache* fd_cache);

  ~FileDescriptorEntry() override {
    if (fd_hook.is_linked()) {
      fd_cache->remove(this->hk, this, FDCache::FLAG_LOCK);
    }
    if (fd > 0) {
        ::close(fd);
    }
  }
};

struct FileHandleEntry : public cohort::lru::Object {
  std::string file_path;
  struct file_handle* handle;
  int mount_id;
  uint64_t hk;
  std::mutex mtx;
  uint32_t flags;
  int fd;
  static constexpr uint64_t seed = 8675309;
  static constexpr uint32_t FLAG_NONE = 0x0000;
  static constexpr uint32_t FLAG_LOCKED = 0x0001;//do we need LOCK?
  static constexpr uint32_t FLAG_LOCK =   0x0002;//do we need LOCKED?
  static constexpr uint32_t FLAG_DELETED = 0x0004;

  bool reclaim(const cohort::lru::ObjectFactory* newobj_fac) override;

  struct FhLT
  {
    bool operator()(const FileHandleEntry& lhs, const FileHandleEntry& rhs) const
    { return (lhs.file_path < rhs.file_path); }

    bool operator()(const std::string& k, const FileHandleEntry& fd) const
    { return k < fd.file_path; }

    bool operator()(const FileHandleEntry& fd, const std::string& k) const
    { return fd.file_path < k; }
  };

  struct FhEQ
  {
    bool operator()(const FileHandleEntry& lhs, const FileHandleEntry& rhs) const
    { return (lhs.file_path == rhs.file_path); }

    bool operator()(const std::string& k, const FileHandleEntry& fd) const
    { return k == fd.file_path; }

    bool operator()(const FileHandleEntry& fd, const std::string& k) const
    { return fd.file_path == k; }
  };

  typedef bi::link_mode<bi::safe_link> link_mode;
  typedef bi::set_member_hook<link_mode> tree_hook_type;
  tree_hook_type fh_hook;

  typedef cohort::lru::LRU<std::mutex> FHLRU;
  typedef bi::member_hook<FileHandleEntry, tree_hook_type, &FileHandleEntry::fh_hook> FhHook;
  typedef bi::rbtree<FileHandleEntry, bi::compare<FhLT>, FhHook> FhTree;
  typedef cohort::lru::TreeX<FileHandleEntry, FhTree, FhLT, FhEQ, std::string, std::mutex> FHCache;

  class Factory : public cohort::lru::ObjectFactory {
  public:
    std::string path;
    uint64_t hk;
    FHCache* fh_cache;
    int fd;

    Factory(const std::string& path, FHCache* fh_cache) : path(path), fh_cache(fh_cache)
    {
      hk = XXH64(path.c_str(), path.length(), FileHandleEntry::seed);
    }

    Factory() = delete;

    void recycle (cohort::lru::Object* o) override;

    cohort::lru::Object* alloc() override {
        return new FileHandleEntry(path, hk, fh_cache);
    }
  };

  FHCache* fh_cache;
  FileHandleEntry(const std::string& path, uint64_t hk, FHCache* fh_cache);

  ~FileHandleEntry() override {
    if (fh_hook.is_linked()) {
      fh_cache->remove(this->hk, this, FHCache::FLAG_LOCK);
    }
    free(handle);
  }
};

class SSDDriver : public CacheDriver {
public:
  static inline int dir_fd{0};
  std::pair<int, int> mount_id_fd;
  // Cache statistics
  std::atomic<uint64_t> l1_hits{0};
  std::atomic<uint64_t> l1_misses{0};
  std::atomic<uint64_t> l2_hits{0};
  std::atomic<uint64_t> l2_misses{0};

  SSDDriver(Partition& partition_info, bool admin, uint32_t max_files=g_ceph_context->_conf->rgw_d4n_file_descriptor_cache_size, uint8_t max_lanes=5,
	      uint8_t max_partitions=5) : partition_info(partition_info), admin(admin),
            fd_cache(max_lanes, max_files/max_partitions),
            fd_lru(max_lanes, max_files/max_lanes),
            fh_cache(max_lanes, (4*max_files)/max_partitions),
            fh_lru(max_lanes, (4*max_files)/max_lanes) {}

  virtual ~SSDDriver() {
    dout(20) << "l1_hits: " << l1_hits << dendl;
    dout(20) << "l1_misses: " << l1_misses << dendl;
    dout(20) << "l2_hits: " << l2_hits << dendl;
    dout(20) << "l2_misses: " << l2_misses << dendl;
    ::close(dir_fd);
  }

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

  FileDescriptorEntry* get_fde(const DoutPrefixProvider* dpp, const std::string& path, uint32_t flags);
  FileDescriptorEntry* lookup_fde(const DoutPrefixProvider* dpp, const std::string& path, uint32_t flags);
  FileDescriptorEntry* insert_fde(const DoutPrefixProvider* dpp, const std::string& path, uint32_t flags);
  FileHandleEntry* get_fhe(const DoutPrefixProvider* dpp, const std::string& path, uint32_t flags);
  FileHandleEntry* lookup_fhe(const DoutPrefixProvider* dpp, const std::string& path, uint32_t flags);
  FileHandleEntry* insert_fhe(const DoutPrefixProvider* dpp, const std::string& path, uint32_t flags);

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
  bool admin;
  FileDescriptorEntry::FDCache fd_cache;
  FileDescriptorEntry::FDLRU fd_lru;
  FileHandleEntry::FHCache fh_cache;
  FileHandleEntry::FHLRU fh_lru;

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
  struct libaio_aiocb_deleter {
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

  using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;
  using unique_aio_read_cb_ptr = std::unique_ptr<struct aiocb, libaio_read_aiocb_deleter>;

  struct AsyncReadOp {
    bufferlist result;
    unique_aio_read_cb_ptr aio_cb;
    SSDDriver *priv_data;
    FileDescriptorEntry* fde{nullptr};
    FileHandleEntry* fhe{nullptr};
    int fd{0};
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
	  unique_aio_cb_ptr cb;
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

