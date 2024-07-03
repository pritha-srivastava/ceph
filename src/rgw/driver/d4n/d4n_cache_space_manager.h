#pragma once

#include <cstdint>
#include "common/ceph_context.h"

namespace rgw { namespace d4n {
  class CacheSpaceManager {
    protected:
      uint64_t total_cache_size{0};
      uint64_t cur_read_cache_size{0};
      uint64_t cur_write_cache_size{0};
      uint64_t max_read_cache_size{0};
      uint64_t max_write_cache_size{0};
    public:
      CacheSpaceManager() = default;

      int initialize(CephContext *cct, const DoutPrefixProvider* dpp, uint64_t total_cache_size) {
        this->total_cache_size = total_cache_size;
        float readcache_to_cachesize_ratio = dpp->get_cct()->_conf->rgw_d4n_readcache_to_cachesize_ratio;
        this->max_read_cache_size = readcache_to_cachesize_ratio*this->total_cache_size;
        this->max_write_cache_size = this->total_cache_size - this->max_read_cache_size;

        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "(): total_cache_size: " << total_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "(): max_read_cache_size: " << max_read_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "(): max_write_cache_size: " << max_write_cache_size << dendl;

        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "(): cur_read_cache_size: " << cur_read_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "(): cur_write_cache_size: " << cur_write_cache_size << dendl;
        return 0;
      }
      bool is_write_space_available(const DoutPrefixProvider* dpp, uint64_t size) {
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() cur_write_cache_size: " << cur_write_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() max_write_cache_size: " << max_write_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() size: " << size << dendl;
        return (cur_write_cache_size + size) <= max_write_cache_size;
      }

      bool is_read_space_available(const DoutPrefixProvider* dpp, uint64_t size) {
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() cur_read_cache_size: " << cur_read_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() max_read_cache_size: " << max_read_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() size: " << size << dendl;
        return (cur_read_cache_size + size) <= max_read_cache_size;
      }

      void increase_write_cache_size(const DoutPrefixProvider* dpp, uint64_t size=1) {
        cur_write_cache_size += size;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() cur_write_cache_size: " << cur_write_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() max_write_cache_size: " << max_write_cache_size << dendl;
      }

      void increase_read_cache_size(const DoutPrefixProvider* dpp, uint64_t size=1) {
        cur_read_cache_size += size;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() cur_read_cache_size: " << cur_read_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() max_read_cache_size: " << max_read_cache_size << dendl;
      }

      void decrease_write_cache_size(const DoutPrefixProvider* dpp, uint64_t size=1) {
        cur_write_cache_size -= size;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() cur_write_cache_size: " << cur_write_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() max_write_cache_size: " << max_write_cache_size << dendl;
      }

      void decrease_read_cache_size(const DoutPrefixProvider* dpp, uint64_t size=1) {
        cur_read_cache_size -= size;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() cur_read_cache_size: " << cur_read_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() max_read_cache_size: " << max_read_cache_size << dendl;
      }

      uint64_t get_cur_write_cache_size(const DoutPrefixProvider* dpp) {
        return cur_write_cache_size;
      }

      uint64_t get_cur_read_cache_size(const DoutPrefixProvider* dpp) {
        return cur_read_cache_size;
      }

      uint64_t get_max_write_cache_size(const DoutPrefixProvider* dpp) {
        return max_write_cache_size;
      }

      uint64_t get_max_read_cache_size(const DoutPrefixProvider* dpp) {
        return max_read_cache_size;
      }

      uint64_t get_total_cache_size(const DoutPrefixProvider* dpp) {
        return total_cache_size;
      }

      uint64_t get_free_write_cache_size(const DoutPrefixProvider* dpp) {
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() max_write_cache_size: " << max_write_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() cur_write_cache_size: " << cur_write_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() free_write_cache_size: " << (max_write_cache_size - cur_write_cache_size) << dendl;
        return max_write_cache_size - cur_write_cache_size;
      }

      uint64_t get_free_read_cache_size(const DoutPrefixProvider* dpp) {
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() max_read_cache_size: " << max_read_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() cur_read_cache_size: " << cur_read_cache_size << dendl;
        ldpp_dout(dpp, 20) << "CacheSpaceManager::" << __func__ << "() free_read_cache_size: " << (max_read_cache_size - cur_read_cache_size) << dendl;
        return max_read_cache_size - cur_read_cache_size;
      }
  };
}}//rgw::d4n