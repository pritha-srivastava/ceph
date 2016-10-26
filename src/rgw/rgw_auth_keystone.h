// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_RGW_AUTH_KEYSTONE_H
#define CEPH_RGW_AUTH_KEYSTONE_H

#include "rgw_auth.h"
#include "rgw_common.h"
#include "rgw_keystone.h"

namespace rgw {
namespace auth {
namespace keystone {

/* Dedicated namespace for Keystone-related auth engines. We need it because
 * Keystone offers three different authentication mechanisms (token, EC2 and
 * regular user/pass). RadosGW actually does support the first two. */

class TokenEngine : public rgw::auth::Engine {
  CephContext* const cct;

  using acl_strategy_t = rgw::auth::RemoteApplier::acl_strategy_t;
  using auth_info_t = rgw::auth::RemoteApplier::AuthInfo;
  using result_t = rgw::auth::Engine::result_t;
  using token_envelope_t = rgw::keystone::TokenEnvelope;

  const rgw::auth::TokenExtractor* const extractor;
  const rgw::auth::RemoteApplier::Factory* const apl_factory;
  rgw::keystone::Config& config;

  /* Helper methods. */
  bool is_applicable() const noexcept;
  token_envelope_t decode_pki_token(const std::string& token) const;
  token_envelope_t get_from_keystone(const std::string& token) const;
  acl_strategy_t get_acl_strategy(const token_envelope_t& token) const;
  auth_info_t get_creds_info(const token_envelope_t& token,
                             const std::vector<std::string>& admin_roles
                            ) const noexcept;
  result_t authenticate(const std::string& token) const;

public:
  TokenEngine(CephContext* const cct,
              const rgw::auth::TokenExtractor* const extractor,
              const rgw::auth::RemoteApplier::Factory* const apl_factory,
              rgw::keystone::Config& config)
    : cct(cct),
      extractor(extractor),
      apl_factory(apl_factory),
      config(config) {
  }

  const char* get_name() const noexcept override {
    return "rgw::auth::keystone::TokenEngine";
  }

  result_t authenticate(const req_state* const s) const override {
    return authenticate(extractor->get_token(s));
  }
}; /* class TokenEngine */

}; /* namespace keystone */
}; /* namespace auth */
}; /* namespace rgw */

#endif /* CEPH_RGW_AUTH_KEYSTONE_H */
