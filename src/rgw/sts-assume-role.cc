#include <errno.h>
#include <ctime>
#include <regex>
#include <boost/format.hpp>
#include <boost/algorithm/string/replace.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"
#include "auth/Crypto.h"
#include "include/ceph_fs.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_role.h"
#include "sts-assume-role.h"

#define dout_subsys ceph_subsys_rgw

namespace STS {
  
int Credentials::generateCredentials(CephContext* cct)
{
  uuid_d accessKey, secretKey;
  char accessKeyId_str[MAX_ACCESS_KEY_LEN], secretAccessKey_str[MAX_ACCESS_KEY_LEN];

  //AccessKeyId
  accessKey.generate_random();
  accessKey.print(accessKeyId_str);
  accessKeyId = accessKeyId_str;

  //SecretAccessKey
  secretKey.generate_random();
  secretKey.print(secretAccessKey_str);
  secretAccessKey = secretAccessKey_str;

  //Expiration
  real_clock::time_point t = real_clock::now();

  struct timeval tv;
  real_clock::to_timeval(t, tv);
  tv.tv_sec += EXPIRATION_TIME_IN_SECS;

  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  int usec = (int)tv.tv_usec/1000;
  expiration = boost::str(boost::format("%s-%s-%sT%s:%s:%s.%sZ")
                                         % (result.tm_year + 1900)
                                         % (result.tm_mon + 1)
                                         % result.tm_mday
                                         % result.tm_hour
                                         % result.tm_min
                                         % result.tm_sec
                                         % usec);

  //Session Token - Encrypt using AES & base64 encode the result
  auto* cryptohandler = cct->get_crypto_handler(CEPH_CRYPTO_AES);
  if (! cryptohandler) {
    return -EINVAL;
  }
  char secret_s[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  };
  bufferptr secret(secret_s, sizeof(secret_s));
  int ret = 0;
  if (ret = cryptohandler->validate_secret(secret); ret < 0) {
    ldout(cct, 0) << "ERROR: Invalid secret key" << dendl;
    return ret;
  }
  string error;
  auto* keyhandler = cryptohandler->get_key_handler(secret, error);
  if (! keyhandler) {
    return -EINVAL;
  }
  error.clear();
  string encrypted_str, input_str = "acess_key_id=" + accessKeyId + "&" +
                     "secret_access_key=" + secretAccessKey + "&" +
                     "expiration=" + expiration;
  bufferlist input, enc_output;
  input.append(input_str);
  if (ret = keyhandler->encrypt(input, enc_output, &error); ret < 0) {
    return ret;
  }

  enc_output.append('\0');
  encrypted_str = enc_output.c_str();

  std::string decoded_str;
  bufferlist enc_bp, encoded_op;
  enc_bp.append(encrypted_str);
  encoded_op.encode_base64(enc_bp);
  encoded_op.append('\0');
  sessionToken = encoded_op.c_str();

  return ret;
}

int AssumedRoleUser::generateAssumedRoleUser(CephContext* cct,
                                              RGWRados *store,
                                              const string& roleArn,
                                              const string& roleSessionName)
{
  auto r_arn = rgw::IAM::ARN::parse(roleArn);
  string resource = r_arn->resource;
  boost::replace_first(resource, "role", "assumed-role");
  resource.append("/");
  resource.append(roleSessionName);
  
  rgw::IAM::ARN assumed_role_arn(rgw::IAM::Partition::aws,
                                  rgw::IAM::Service::sts,
                                  "", r_arn->account, resource);
  arn = assumed_role_arn.to_string();

  //Assumeroleid = roleid:rolesessionname
  auto pos = r_arn->resource.find_last_of('/');
  string roleName = r_arn->resource.substr(pos + 1);
  RGWRole role(cct, store, roleName, r_arn->account);
  if (int ret = role.get(); ret < 0) {
    return ret;
  }
  assumeRoleId = role.get_id() + ":" + roleSessionName;

  return 0;
}

AssumeRoleResponse STSService::assumeRole(const AssumeRoleRequest& req)
{
  AssumedRoleUser user;
  user.generateAssumedRoleUser(cct, store, req.getRoleARN(), req.getRoleSessionName());

  Credentials cred;
  cred.generateCredentials(cct);

  string policy = req.getPolicy();
  uint64_t packedPolicySize = (policy.size() / req.getMaxPolicySize()) * 100;
  return make_tuple(user, cred, packedPolicySize);
}

}