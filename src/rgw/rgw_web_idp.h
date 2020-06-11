// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_WEB_IDP_H
#define CEPH_RGW_WEB_IDP_H

namespace rgw {
namespace web_idp {

//WebToken contains some claims from the decoded token which are of interest to us.
struct WebTokenClaims {
  //Subject of the token
  std::string sub;
  //Intended audience for this token
  std::string aud;
  //Issuer of this token
  std::string iss;
  //Human-readable id for the resource owner
  std::string user_name;
  //Client Id
  std::string client_id;
  //AMR
  std::string amr;
  //app_id
  std::string app_id;
  //user_id
  std::string user_id;
  //id
  std::string id;
};

}; /* namespace web_idp */
}; /* namespace rgw */

#endif /* CEPH_RGW_WEB_IDP_H */
