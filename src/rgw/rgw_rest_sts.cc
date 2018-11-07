#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>
#include <boost/tokenizer.hpp>

#include "include/assert.h"
#include "ceph_ver.h"

#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"

#include "rgw_rest.h"
#include "rgw_auth.h"
#include "rgw_auth_s3.h"
#include "rgw_rest_sts.h"
#include "rgw_formats.h"
#include "rgw_client_io.h"

#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_iam_policy.h"
#include "rgw_iam_policy_keywords.h"

#include "rgw_rest_sts.h"

#include <array>
#include <sstream>
#include <memory>

#include <boost/utility/string_ref.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int RGWREST_STS::verify_permission()
{
  STS::STSService _sts(s->cct, store, s->user->user_id, s->auth.identity.get());
  sts = std::move(_sts);

  string rArn = s->info.args.get("RoleArn");
  int ret; RGWRole role;
  //const auto& [ret, role] = sts.getRoleInfo(rArn);
  std::tie(ret, role) = sts.getRoleInfo(rArn);;
  if (ret < 0) {
    return ret;
  }
  string policy = role.get_assume_role_policy();
  buffer::list bl = buffer::list::static_from_string(policy);

  //Parse the policy
  //TODO - This step should be part of Role Creation
  try {
    const rgw::IAM::Policy p(s->cct, s->user->user_id.tenant, bl);
    //Check if the input role arn is there as one of the Principals in the policy,
  // If yes, then return 0, else -EPERM
    auto res = p.eval_principal(s->env, *s->auth.identity);
    if (res == rgw::IAM::Effect::Deny) {
      return -EPERM;
    }
  } catch (rgw::IAM::PolicyParseException& e) {
    ldout(s->cct, 20) << "failed to parse policy: " << e.what() << dendl;
    return -EPERM;
  }

  return 0;
}

void RGWREST_STS::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWSTSGetSessionToken::verify_permission()
{
  return 0;
}

int RGWSTSGetSessionToken::get_params()
{
  duration = s->info.args.get("DurationSeconds");
  serialNumber = s->info.args.get("SerialNumber");
  tokenCode = s->info.args.get("TokenCode");

  if (! duration.empty()) {
    uint64_t duration_in_secs = stoull(duration);
    if (duration_in_secs < STS::GetSessionTokenRequest::getMinDuration() ||
            duration_in_secs > s->cct->_conf->rgw_sts_max_session_duration)
      return -EINVAL;
  }

  return 0;
}

void RGWSTSGetSessionToken::execute()
{
  op_ret = get_params();;
  if (op_ret < 0) {
    return;
  }

  STS::STSService sts(s->cct, store, s->user->user_id, s->auth.identity.get());
  STS::GetSessionTokenRequest req(duration, serialNumber, tokenCode);
  int ret;
  STS::Credentials creds;
  //const auto& [ret, creds] = sts.getSessionToken(req);
  std::tie(ret, creds) = sts.getSessionToken(req);
  op_ret = std::move(ret);
  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section("GetSessionTokenResponse");
    s->formatter->open_object_section("GetSessionTokenResult");
    s->formatter->open_object_section("Credentials");
    creds.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWSTSAssumeRole::get_params()
{
  duration = s->info.args.get("DurationSeconds");
  externalId = s->info.args.get("ExternalId");
  policy = s->info.args.get("Policy");
  roleArn = s->info.args.get("RoleArn");
  roleSessionName = s->info.args.get("RoleSessionName");
  serialNumber = s->info.args.get("SerialNumber");
  tokenCode = s->info.args.get("TokenCode");

  if (roleArn.empty() || roleSessionName.empty()) {
    ldout(s->cct, 20) << "ERROR: one of role arn or role session name is empty" << dendl;
    return -EINVAL;
  }

  if (! policy.empty()) {
    bufferlist bl = bufferlist::static_from_string(policy);
    try {
      const rgw::IAM::Policy p(s->cct, s->user->user_id.tenant, bl);
    }
    catch (rgw::IAM::PolicyParseException& e) {
      ldout(s->cct, 20) << "failed to parse policy: " << e.what() << "policy" << policy << dendl;
      return -ERR_MALFORMED_DOC;
    }
  }

  return 0;
}

void RGWSTSAssumeRole::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  STS::AssumeRoleRequest req(duration, externalId, policy, roleArn,
                        roleSessionName, serialNumber, tokenCode);
  int ret;
  STS::AssumedRoleUser assumedRoleUser;
  STS::Credentials creds;
  uint64_t packedPolicySize;
  //const auto& [ret, assumedRoleUser, creds, packedPolicySize] =
  //sts.assumeRole(req);
  std::tie(ret, assumedRoleUser, creds, packedPolicySize) = sts.assumeRole(req);
  op_ret = std::move(ret);
  //Dump the output
  if (op_ret == 0) {
    s->formatter->open_object_section("AssumeRoleResponse");
    s->formatter->open_object_section("AssumeRoleResult");
    s->formatter->open_object_section("Credentials");
    creds.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->open_object_section("AssumedRoleUser");
    assumedRoleUser.dump(s->formatter);
    s->formatter->close_section();
    encode_json("PackedPolicySize", packedPolicySize , s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

RGWOp *RGWHandler_REST_STS::op_post()
{
  int len = 0;
  char *data = nullptr;
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  auto ret = rgw_rest_read_all_input(s, &data, &len, max_size, false);
  if (ret < 0) {
    return nullptr;
  }
  ldout(s->cct, 0) << "Content of POST: " << data << dendl;
  string post_body = data;

  if (post_body.find("Action") != string::npos) {
    boost::char_separator<char> sep("&");
    boost::tokenizer<boost::char_separator<char>> tokens(post_body, sep);
    for (const auto& t : tokens) {
      auto pos = t.find("=");
      if (pos != string::npos) {
         std::string key = t.substr(0, pos);
         std::string value = t.substr(pos + 1, t.size() - 1);
         if (key == "RoleArn") {
           value = url_decode(value);
         }
         ldout(s->cct, 0) << "Key: " << key << "Value: " << value << dendl;
         s->info.args.append(key, value);
       }
     }
  }

  free(data);

  if (s->info.args.exists("Action"))    {
    string action = s->info.args.get("Action");
    if (action == "AssumeRole") {
      return new RGWSTSAssumeRole;
    } else if (action == "GetSessionToken") {
      return new RGWSTSGetSessionToken;
    }
  }

  return nullptr;
}

int RGWHandler_REST_STS::init(RGWRados *store,
                              struct req_state *s,
                              rgw::io::BasicClient *cio)
{
  s->dialect = "sts";

  int ret = RGWHandler_REST_STS::init_from_header(s, RGW_FORMAT_XML, true);
  if (ret < 0) {
    ldout(s->cct, 10) << "init_from_header returned err=" << ret <<  dendl;
    return ret;
  }

  return RGWHandler_REST::init(store, s, cio);
}

int RGWHandler_REST_STS::init_from_header(struct req_state* s,
                                          int default_formatter,
                                          bool configurable_format)
{
  string req;
  string first;

  s->prot_flags |= RGW_REST_STS;

  const char *p, *req_name = s->relative_uri.c_str();
  if (*req_name == '?') {
    p = req_name;
  } else {
    p = s->info.request_params.c_str();
  }

  s->info.args.set(p);
  s->info.args.parse();

  /* must be called after the args parsing */
  int ret = allocate_formatter(s, default_formatter, configurable_format);
  if (ret < 0)
    return ret;

  if (*req_name != '/')
    return 0;

  req_name++;

  if (!*req_name)
    return 0;

  req = req_name;
  int pos = req.find('/');
  if (pos >= 0) {
    first = req.substr(0, pos);
  } else {
    first = req;
  }

  return 0;
}

RGWHandler_REST*
RGWRESTMgr_STS::get_handler(struct req_state* const s,
                              const rgw::auth::StrategyRegistry& auth_registry,
                              const std::string& frontend_prefix)
{
  return new RGWHandler_REST_STS(auth_registry);
}
