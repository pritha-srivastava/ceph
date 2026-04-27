#include "d4n_policy_fdb.h"
#include "rgw_sal_d4n.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"
#include "common/split.h"
#include "rgw_perf_counters.h"

namespace rgw { namespace d4n {

int FDBLFUDAPolicy::init(CephContext* cct, const DoutPrefixProvider* dpp, asio::io_context& io_context, rgw::sal::Driver* _driver) {
  return 0;
}

int FDBLFUDAPolicy::age_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  return 0;
}

int FDBLFUDAPolicy::local_weight_sync(const DoutPrefixProvider* dpp, optional_yield y) {
  return 0;
}

int FDBLFUDAPolicy::eviction(const DoutPrefixProvider* dpp, uint64_t size, optional_yield y) {
  return 0;
}

void FDBLFUDAPolicy::cleaning(const DoutPrefixProvider* dpp){
}


} } // namespace rgw::d4n
