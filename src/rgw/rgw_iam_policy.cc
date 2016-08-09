// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#include <cstring>
#include <regex>
#include <stack>
#include <utility>

#include "rapidjson/reader.h"

#include "rgw_auth.h"
#include "rgw_iam_policy.h"

namespace {
constexpr int dout_subsys = ceph_subsys_rgw;
}

using std::bitset;
using std::find;
using std::int64_t;
using std::move;
using std::pair;
using std::regex;
using std::regex_match;
using std::size_t;
using std::smatch;
using std::string;
using std::uint16_t;
using std::uint64_t;
using std::unordered_map;

using boost::container::flat_set;
using boost::none;
using boost::optional;

using rapidjson::BaseReaderHandler;
using rapidjson::UTF8;
using rapidjson::SizeType;
using rapidjson::Reader;
using rapidjson::kParseCommentsFlag;
using rapidjson::kParseNumbersAsStringsFlag;
using rapidjson::StringStream;
using rapidjson::ParseResult;

using rgw::auth::Principal;

namespace rgw {
namespace IAM {
#include "rgw_iam_policy_keywords.frag.cc"

struct actpair {
  const char* name;
  const uint64_t bit;
};

namespace {
optional<Partition> to_partition(const smatch::value_type& p,
				 bool wildcards) {
  if (p == "aws")
    return Partition::aws;
  else if (p == "aws-cn")
    return Partition::aws_cn;
  else if (p == "aws-us-gov")
    return Partition::aws_us_gov;
  else if (p == "*" && wildcards)
    return Partition::wildcard;
  else
    return none;

  ceph_abort();
}

optional<Service> to_service(const smatch::value_type& s,
			     bool wildcards) {
  static const unordered_map<string, Service> services = {
    { "acm", Service::acm },
    { "apigateway", Service::apigateway },
    { "appstream", Service::appstream },
    { "artifact", Service::artifact },
    { "autoscaling", Service::autoscaling },
    { "aws-marketplace", Service::aws_marketplace },
    { "aws-marketplace-management",
      Service::aws_marketplace_management },
    { "aws-portal", Service::aws_portal },
    { "cloudformation", Service::cloudformation },
    { "cloudfront", Service::cloudfront },
    { "cloudhsm", Service::cloudhsm },
    { "cloudsearch", Service::cloudsearch },
    { "cloudtrail", Service::cloudtrail },
    { "cloudwatch", Service::cloudwatch },
    { "codebuild", Service::codebuild },
    { "codecommit", Service::codecommit },
    { "codedeploy", Service::codedeploy },
    { "codepipeline", Service::codepipeline },
    { "cognito-identity", Service::cognito_identity },
    { "cognito-idp", Service::cognito_idp },
    { "cognito-sync", Service::cognito_sync },
    { "config", Service::config },
    { "datapipeline", Service::datapipeline },
    { "devicefarm", Service::devicefarm },
    { "directconnect", Service::directconnect },
    { "dms", Service::dms },
    { "ds", Service::ds },
    { "dynamodb", Service::dynamodb },
    { "ec2", Service::ec2 },
    { "ecr", Service::ecr },
    { "ecs", Service::ecs },
    { "elasticache", Service::elasticache },
    { "elasticbeanstalk", Service::elasticbeanstalk },
    { "elasticfilesystem", Service::elasticfilesystem },
    { "elasticloadbalancing", Service::elasticloadbalancing },
    { "elasticmapreduce", Service::elasticmapreduce },
    { "elastictranscoder", Service::elastictranscoder },
    { "es", Service::es },
    { "events", Service::events },
    { "firehose", Service::firehose },
    { "gamelift", Service::gamelift },
    { "glacier", Service::glacier },
    { "health", Service::health },
    { "iam", Service::iam },
    { "importexport", Service::importexport },
    { "inspector", Service::inspector },
    { "iot", Service::iot },
    { "kinesis", Service::kinesis },
    { "kinesisanalytics", Service::kinesisanalytics },
    { "kms", Service::kms },
    { "lambda", Service::lambda },
    { "lightsail", Service::lightsail },
    { "logs", Service::logs },
    { "machinelearning", Service::machinelearning },
    { "mobileanalytics", Service::mobileanalytics },
    { "mobilehub", Service::mobilehub },
    { "opsworks", Service::opsworks },
    { "opsworks-cm", Service::opsworks_cm },
    { "polly", Service::polly },
    { "rds", Service::rds },
    { "redshift", Service::redshift },
    { "route53", Service::route53 },
    { "route53domains", Service::route53domains },
    { "s3", Service::s3 },
    { "sdb", Service::sdb },
    { "servicecatalog", Service::servicecatalog },
    { "ses", Service::ses },
    { "sns", Service::sns },
    { "sqs", Service::sqs },
    { "ssm", Service::ssm },
    { "states", Service::states },
    { "storagegateway", Service::storagegateway },
    { "sts", Service::sts },
    { "support", Service::support },
    { "swf", Service::swf },
    { "trustedadvisor", Service::trustedadvisor },
    { "waf", Service::waf },
    { "workmail", Service::workmail },
    { "workspaces", Service::workspaces }};

  if (wildcards && s == "*")
    return Service::wildcard;

  auto i = services.find(s);
  if (i == services.end())
    return none;
  else
    return i->second;
}
}

ARN::ARN(const rgw_obj& o)
  : partition(Partition::aws),
    service(Service::s3),
    region(),
    account(o.bucket.tenant),
    resource(o.bucket.name)
{
  resource.push_back('/');
  resource.append(o.key.name);
}

ARN::ARN(const rgw_bucket& b)
  : partition(Partition::aws),
    service(Service::s3),
    region(),
    account(b.tenant),
    resource(b.name) { }

ARN::ARN(const rgw_bucket& b, const string& o)
  : partition(Partition::aws),
    service(Service::s3),
    region(),
    account(b.tenant),
    resource(b.name) {
  resource.push_back('/');
  resource.append(o);
}

optional<ARN> ARN::parse(const string& s, bool wildcards) {
  static const regex rx_wild("arn:([^:]*):([^:]*):([^:]*):([^:]*):([^:]*)",
			     std::regex_constants::ECMAScript |
			     std::regex_constants::optimize);
  static const regex rx_no_wild(
    "arn:([^:*]*):([^:*]*):([^:*]*):([^:*]*):([^:*]*)",
    std::regex_constants::ECMAScript |
    std::regex_constants::optimize);

  smatch match;

  if ((s == "*") && wildcards)
    return ARN(Partition::wildcard, Service::wildcard, "*", "*", "*");

  else if (regex_match(s, match, wildcards ? rx_wild : rx_no_wild)) {
    ceph_assert(match.size() == 6);

    ARN a;
    {
      auto p = to_partition(match[1], wildcards);
      if (!p)
	return none;

      a.partition = *p;
    }
    {
      auto s = to_service(match[2], wildcards);
      if (!s) return none;
      a.service = *s;
    }

    a.region = match[3];
    a.account = match[4];
    a.resource = match[5];

    return a;
  }
  return none;
}

string ARN::unparse() {
  string s;

  if (partition == Partition::aws)
    s.append("aws:");
  else if (partition == Partition::aws_cn)
    s.append("aws-cn:");
  else if (partition == Partition::aws_us_gov)
    s.append("aws-us-gov:");
  else
    s.append("*:");

  static const unordered_map<Service, string> services = {
    { Service::acm, "acm" },
    { Service::apigateway, "apigateway" },
    { Service::appstream, "appstream" },
    { Service::artifact, "artifact" },
    { Service::autoscaling, "autoscaling" },
    { Service::aws_marketplace, "aws-marketplace" },
    { Service::aws_marketplace_management, "aws-marketplace-management" },
    { Service::aws_portal, "aws-portal" },
    { Service::cloudformation, "cloudformation" },
    { Service::cloudfront, "cloudfront" },
    { Service::cloudhsm, "cloudhsm" },
    { Service::cloudsearch, "cloudsearch" },
    { Service::cloudtrail, "cloudtrail" },
    { Service::cloudwatch, "cloudwatch" },
    { Service::codebuild, "codebuild" },
    { Service::codecommit, "codecommit" },
    { Service::codedeploy, "codedeploy" },
    { Service::codepipeline, "codepipeline" },
    { Service::cognito_identity, "cognito-identity" },
    { Service::cognito_idp, "cognito-idp" },
    { Service::cognito_sync, "cognito-sync" },
    { Service::config, "config" },
    { Service::datapipeline, "datapipeline" },
    { Service::devicefarm, "devicefarm" },
    { Service::directconnect, "directconnect" },
    { Service::dms, "dms" },
    { Service::ds, "ds" },
    { Service::dynamodb, "dynamodb" },
    { Service::ec2, "ec2" },
    { Service::ecr, "ecr" },
    { Service::ecs, "ecs" },
    { Service::elasticache, "elasticache" },
    { Service::elasticbeanstalk, "elasticbeanstalk" },
    { Service::elasticfilesystem, "elasticfilesystem" },
    { Service::elasticloadbalancing, "elasticloadbalancing" },
    { Service::elasticmapreduce, "elasticmapreduce" },
    { Service::elastictranscoder, "elastictranscoder" },
    { Service::es, "es" },
    { Service::events, "events" },
    { Service::firehose, "firehose" },
    { Service::gamelift, "gamelift" },
    { Service::glacier, "glacier" },
    { Service::health, "health" },
    { Service::iam, "iam" },
    { Service::importexport, "importexport" },
    { Service::inspector, "inspector" },
    { Service::iot, "iot" },
    { Service::kinesis, "kinesis" },
    { Service::kinesisanalytics, "kinesisanalytics" },
    { Service::kms, "kms" },
    { Service::lambda, "lambda" },
    { Service::lightsail, "lightsail" },
    { Service::logs, "logs" },
    { Service::machinelearning, "machinelearning" },
    { Service::mobileanalytics, "mobileanalytics" },
    { Service::mobilehub, "mobilehub" },
    { Service::opsworks, "opsworks" },
    { Service::opsworks_cm, "opsworks-cm" },
    { Service::polly, "polly" },
    { Service::rds, "rds" },
    { Service::redshift, "redshift" },
    { Service::route53, "route53" },
    { Service::route53domains, "route53domains" },
    { Service::s3, "s3" },
    { Service::sdb, "sdb" },
    { Service::servicecatalog, "servicecatalog" },
    { Service::ses, "ses" },
    { Service::sns, "sns" },
    { Service::sqs, "sqs" },
    { Service::ssm, "ssm" },
    { Service::states, "states" },
    { Service::storagegateway, "storagegateway" },
    { Service::sts, "sts" },
    { Service::support, "support" },
    { Service::swf, "swf" },
    { Service::trustedadvisor, "trustedadvisor" },
    { Service::waf, "waf" },
    { Service::workmail, "workmail" },
    { Service::workspaces, "workspaces" }};

  auto i = services.find(service);
  if (i != services.end())
    s.append(i->second);
  else
    s.push_back('*');
  s.push_back(':');

  s.append(region);
  s.push_back(':');

  s.append(account);
  s.push_back(':');

  s.append(resource);

  return s;
}

bool operator ==(const ARN& l, const ARN& r) {
  return ((l.partition == r.partition) &&
	  (l.service == r.service) &&
	  (l.region == r.region) &&
	  (l.account == r.account) &&
	  (l.resource == r.resource));
}
bool operator <(const ARN& l, const ARN& r) {
  return ((l.partition < r.partition) ||
	  (l.service < r.service) ||
	  (l.region < r.region) ||
	  (l.account < r.account) ||
	  (l.resource < r.resource));
}

// The candidate is not allowed to have wildcards. The only way to
// do that sanely would be to use unification rather than matching.
bool ARN::match(const ARN& candidate) const {
  if ((candidate.partition == Partition::wildcard) ||
      (partition != candidate.partition && partition
       != Partition::wildcard))
    return false;

  if ((candidate.service == Service::wildcard) ||
      (service != candidate.service && service != Service::wildcard))
    return false;

  if (!::match(region, candidate.region, MATCH_POLICY_ARN))
    return false;

  if (!::match(account, candidate.account, MATCH_POLICY_ARN))
    return false;

  if (!::match(resource, candidate.resource, MATCH_POLICY_ARN))
    return false;

  return true;
}

namespace {
const actpair actpairs[] =
{{ "s3:AbortMultipartUpload", s3AbortMultipartUpload },
 { "s3:CreateBucket", s3CreateBucket },
 { "s3:DeleteBucketPolicy", s3DeleteBucketPolicy },
 { "s3:DeleteBucket", s3DeleteBucket },
 { "s3:DeleteBucketWebsite", s3DeleteBucketWebsite },
 { "s3:DeleteObject", s3DeleteObject },
 { "s3:DeleteObjectVersion", s3DeleteObjectVersion },
 { "s3:DeleteReplicationConfiguration",
   s3DeleteReplicationConfiguration },
 { "s3:GetAccelerateConfiguration", s3GetAccelerateConfiguration },
 { "s3:GetBucketAcl", s3GetBucketAcl },
 { "s3:GetBucketCORS", s3GetBucketCORS },
 { "s3:GetBucketLocation", s3GetBucketLocation },
 { "s3:GetBucketLogging", s3GetBucketLogging },
 { "s3:GetBucketNotification", s3GetBucketNotification },
 { "s3:GetBucketPolicy", s3GetBucketPolicy },
 { "s3:GetBucketRequestPayment", s3GetBucketRequestPayment },
 { "s3:GetBucketTagging", s3GetBucketTagging },
 { "s3:GetBucketVersioning", s3GetBucketVersioning },
 { "s3:GetBucketWebsite", s3GetBucketWebsite },
 { "s3:GetLifecycleConfiguration", s3GetLifecycleConfiguration },
 { "s3:GetObjectAcl", s3GetObjectAcl },
 { "s3:GetObject", s3GetObject },
 { "s3:GetObjectTorrent", s3GetObjectTorrent },
 { "s3:GetObjectVersionAcl", s3GetObjectVersionAcl },
 { "s3:GetObjectVersion", s3GetObjectVersion },
 { "s3:GetObjectVersionTorrent", s3GetObjectVersionTorrent },
 { "s3:GetReplicationConfiguration", s3GetReplicationConfiguration },
 { "s3:ListAllMyBuckets", s3ListAllMyBuckets },
 { "s3:ListBucketMultiPartUploads", s3ListBucketMultiPartUploads },
 { "s3:ListBucket", s3ListBucket },
 { "s3:ListBucketVersions", s3ListBucketVersions },
 { "s3:ListMultipartUploadParts", s3ListMultipartUploadParts },
 { "s3:PutAccelerateConfiguration", s3PutAccelerateConfiguration },
 { "s3:PutBucketAcl", s3PutBucketAcl },
 { "s3:PutBucketCORS", s3PutBucketCORS },
 { "s3:PutBucketLogging", s3PutBucketLogging },
 { "s3:PutBucketNotification", s3PutBucketNotification },
 { "s3:PutBucketPolicy", s3PutBucketPolicy },
 { "s3:PutBucketRequestPayment", s3PutBucketRequestPayment },
 { "s3:PutBucketTagging", s3PutBucketTagging },
 { "s3:PutBucketVersioning", s3PutBucketVersioning },
 { "s3:PutBucketWebsite", s3PutBucketWebsite },
 { "s3:PutLifecycleConfiguration", s3PutLifecycleConfiguration },
 { "s3:PutObjectAcl",  s3PutObjectAcl },
 { "s3:PutObject", s3PutObject },
 { "s3:PutObjectVersionAcl", s3PutObjectVersionAcl },
 { "s3:PutReplicationConfiguration", s3PutReplicationConfiguration },
 { "s3:RestoreObject", s3RestoreObject }};
}

struct PolicyParser;

const Keyword top[1]{"<Top>", TokenKind::pseudo, TokenID::Top, 0, false,
    false};
const Keyword cond_key[1]{"<Condition Key>", TokenKind::cond_key,
    TokenID::CondKey, 0, true, false};

struct ParseState {
  PolicyParser* pp;
  const Keyword* w;

  bool arraying = false;
  bool objecting = false;

  void reset();

  ParseState(PolicyParser* pp, const Keyword* w)
    : pp(pp), w(w) {}

  bool obj_start();

  bool obj_end();

  bool array_start() {
    if (w->arrayable && !arraying) {
      arraying = true;
      return true;
    }
    return false;
  }

  bool array_end();

  bool key(const char* s, size_t l);
  bool do_string(CephContext* cct, const char* s, size_t l);
  bool number(const char* str, size_t l);
};

struct PolicyParser : public BaseReaderHandler<UTF8<>, PolicyParser> {
  keyword_hash tokens;
  std::vector<ParseState> s;
  CephContext* cct;
  const string& tenant;
  Policy& policy;

  uint32_t seen = 0;

  uint32_t dex(TokenID in) const {
    switch (in) {
    case TokenID::Version:
      return 0x1;
    case TokenID::Id:
      return 0x2;
    case TokenID::Statement:
      return 0x4;
    case TokenID::Sid:
      return 0x8;
    case TokenID::Effect:
      return 0x10;
    case TokenID::Principal:
      return 0x20;
    case TokenID::NotPrincipal:
      return 0x40;
    case TokenID::Action:
      return 0x80;
    case TokenID::NotAction:
      return 0x100;
    case TokenID::Resource:
      return 0x200;
    case TokenID::NotResource:
      return 0x400;
    case TokenID::Condition:
      return 0x800;
    case TokenID::AWS:
      return 0x1000;
    case TokenID::Federated:
      return 0x2000;
    case TokenID::Service:
      return 0x4000;
    case TokenID::CanonicalUser:
      return 0x8000;
    default:
      ceph_abort();
    }
  }
  bool test(TokenID in) {
    return seen & dex(in);
  }
  void set(TokenID in) {
    seen |= dex(in);
  }
  void set(std::initializer_list<TokenID> l) {
    for (auto in : l)
      seen |= dex(in);
  }
  void reset(TokenID in) {
    seen &= ~dex(in);
  }
  void reset(std::initializer_list<TokenID> l) {
    for (auto in : l)
      seen &= ~dex(in);
  }

  PolicyParser(CephContext* cct, const string& tenant, Policy& policy)
    : cct(cct), tenant(tenant), policy(policy) {}
  PolicyParser(const PolicyParser& policy) = delete;

  bool StartObject() {
    if (s.empty()) {
      s.push_back({this, top});
      s.back().objecting = true;
      return true;
    }

    return s.back().obj_start();
  }
  bool EndObject(SizeType memberCount) {
    if (s.empty())
      return false;

    return s.back().obj_end();
  }
  bool Key(const char* str, SizeType length, bool copy) {
    if (s.empty())
      return false;

    return s.back().key(str, length);
  }

  bool String(const char* str, SizeType length, bool copy) {
    if (s.empty())
      return false;

    return s.back().do_string(cct, str, length);
  }
  bool RawNumber(const char* str, SizeType length, bool copy) {
    if (s.empty())
      return false;

    return s.back().number(str, length);
  }
  bool StartArray() {
    if (s.empty())
      return false;

    return s.back().array_start();
  }
  bool EndArray(SizeType) {
    if (s.empty())
      return false;

    return s.back().array_end();
  }

  bool Default() {
    return false;
  }
};


// I really despise this misfeature of C++.
//
bool ParseState::obj_end() {
  if (objecting) {
    objecting = false;
    if (!arraying) {
      pp->s.pop_back();
    } else {
      reset();
    }
    return true;
  }
  return false;
}

bool ParseState::key(const char* s, size_t l) {
  auto k = pp->tokens.lookup(s, l);

  if (!k) {
    if (w->kind == TokenKind::cond_op) {
      auto& t = pp->policy.statements.back();
      pp->s.emplace_back(pp, cond_key);
      t.conditions.emplace_back(w->id, s, l);
      return true;
    }
    else {
      return false;
    }
  }

  if ((((w->id == TokenID::Top) && (k->kind == TokenKind::top)) ||
       ((w->id == TokenID::Statement) &&
	(k->kind == TokenKind::statement)) ||
       ((w->id == TokenID::Principal || w->id == TokenID::NotPrincipal) &&
	(k->kind == TokenKind::princ_type))) &&
      !pp->test(k->id)) {
    pp->set(k->id);
    pp->s.emplace_back(pp, k);
    return true;
  } else if ((w->id == TokenID::Condition) &&
	     (k->kind == TokenKind::cond_op)) {
    pp->s.emplace_back(pp, k);
    return true;
  }
  return false;
}

namespace {
// I should just rewrite a few helper functions to user iterators,
// which will make all of this ever so much nicer.
optional<Principal> parse_principal(CephContext* cct, TokenID t,
				    string&& s) {
  // Wildcard!
  if ((t == TokenID::AWS) && (s == "*")) {
    return Principal::wildcard();

    // Do nothing for now.
  } else if (t == TokenID::CanonicalUser) {

    // AWS ARNs
  } else if (t == TokenID::AWS) {
    auto a = ARN::parse(s);
    if (!a)
      if (std::none_of(s.begin(), s.end(),
		       [](const char& c) {
			 return (c == ':') || (c == '/');
		       })) {
	// Since tenants are simply prefixes, there's no really good
	// way to see if one exists or not. So we return the thing and
	// let them try and match against it.
	return Principal::tenant(std::move(s));
      }

    if (a->resource == "root")
      return Principal::tenant(std::move(a->account));

    static const regex rx("([^/]*)/(.*)",
			  std::regex_constants::ECMAScript |
			  std::regex_constants::optimize);
    smatch match;
    if (regex_match(a->resource, match, rx)) {
      ceph_assert(match.size() == 2);

      if (match[1] == "user")
	return Principal::user(std::move(a->account),
			       match[2]);
      if (match[1] == "role")
	return Principal::role(std::move(a->account),
			       match[2]);
    }
  }

  ldout(cct, 0) << "Supplied principal is discarded: " << s << dendl;
  return boost::none;
}
}

bool ParseState::do_string(CephContext* cct, const char* s, size_t l) {
  auto k = pp->tokens.lookup(s, l);
  Policy& p = pp->policy;
  Statement* t = p.statements.empty() ? nullptr : &(p.statements.back());

  // Top level!
  if ((w->id == TokenID::Version) && k &&
      k->kind == TokenKind::version_key) {
    p.version = static_cast<Version>(k->specific);
  } else if (w->id == TokenID::Id) {
    p.id = string(s, l);

    // Statement

  } else if (w->id == TokenID::Sid) {
    t->sid.emplace(s, l);
  } else if ((w->id == TokenID::Effect) &&
	     k->kind == TokenKind::effect_key) {
    t->effect = static_cast<Effect>(k->specific);
  } else if (w->id == TokenID::Principal && s && *s == '*') {
    t->princ.emplace(Principal::wildcard());
  } else if (w->id == TokenID::NotPrincipal && s && *s == '*') {
    t->noprinc.emplace(Principal::wildcard());
  } else if ((w->id == TokenID::Action) ||
	     (w->id == TokenID::NotAction)) {
    for (auto& p : actpairs)
      if (match({s, l}, p.name, MATCH_POLICY_ACTION))
	(w->id == TokenID::Action ? t->action : t->notaction) |= p.bit;
  } else if (w->id == TokenID::Resource || w->id == TokenID::NotResource) {
    auto a = ARN::parse({s, l}, true);
    // You can't specify resources for someone ELSE'S account.
    if (a && (a->account.empty() || a->account == pp->tenant ||
	      a->account == "*")) {
      if (a->account.empty() || a->account == "*")
	a->account = pp->tenant;
      (w->id == TokenID::Resource ? t->resource : t->notresource)
	.emplace(std::move(*a));
    }
    else
      ldout(cct, 0) << "Supplied resource is discarded: " << string(s, l)
		    << dendl;
  } else if (w->kind == TokenKind::cond_key) {
    auto& t = pp->policy.statements.back();
    t.conditions.back().vals.emplace_back(s, l);

    // Principals

  } else if (w->kind == TokenKind::princ_type) {
    ceph_assert(pp->s.size() > 1);
    auto& pri = pp->s[pp->s.size() - 2].w->id == TokenID::Principal ?
      t->princ : t->noprinc;

    auto o = parse_principal(pp->cct, w->id, string(s, l));
    if (o)
      pri.emplace(std::move(*o));

    // Failure

  } else {
    return false;
  }

  if (!arraying)
    pp->s.pop_back();

  return true;
}

bool ParseState::number(const char* s, size_t l) {
  // Top level!
  if (w->kind == TokenKind::cond_key) {
    auto& t = pp->policy.statements.back();
    t.conditions.back().vals.emplace_back(s, l);

    // Failure

  } else {
    return false;
  }

  if (!arraying)
    pp->s.pop_back();

  return true;
}

void ParseState::reset() {
  pp->reset({TokenID::Sid, TokenID::Effect, TokenID::Principal,
	TokenID::NotPrincipal, TokenID::Action, TokenID::NotAction,
	TokenID::Resource, TokenID::NotResource, TokenID::Condition});
}

bool ParseState::obj_start() {
  if (w->objectable && !objecting) {
    objecting = true;
    if (w->id == TokenID::Statement)
      pp->policy.statements.push_back({});
    return true;
  }

  return false;
}


bool ParseState::array_end() {
  if (arraying && !objecting) {
    pp->s.pop_back();
    return true;
  }

  return false;
}

bool Condition::eval(const Environment& env) const {
  auto i = env.find(key);
  if (op == TokenID::Null)
    return i == env.end() ? true : false;

  if (i == env.end()) {
    return false;
  }
  const auto& s = i->second;

  switch (op) {
    // String!
  case TokenID::StringEquals:
    return orrible(std::equal_to<std::string>(), s, vals);

  case TokenID::StringNotEquals:
    return orrible(std::not2(std::equal_to<std::string>()),
		   s, vals);

  case TokenID::StringEqualsIgnoreCase:
    return orrible(ci_equal_to(), s, vals);

  case TokenID::StringNotEqualsIgnoreCase:
    return orrible(std::not2(ci_equal_to()), s, vals);

    // Implement actual StringLike with wildcarding later
  case TokenID::StringLike:
    return orrible(std::equal_to<std::string>(), s, vals);
  case TokenID::StringNotLike:
    return orrible(std::not2(std::equal_to<std::string>()),
		   s, vals);

    // Numeric
  case TokenID::NumericEquals:
    return shortible(std::equal_to<double>(), as_number, s, vals);

  case TokenID::NumericNotEquals:
    return shortible(std::not2(std::equal_to<double>()),
		     as_number, s, vals);


  case TokenID::NumericLessThan:
    return shortible(std::less<double>(), as_number, s, vals);


  case TokenID::NumericLessThanEquals:
    return shortible(std::less_equal<double>(), as_number, s, vals);

  case TokenID::NumericGreaterThan:
    return shortible(std::greater<double>(), as_number, s, vals);

  case TokenID::NumericGreaterThanEquals:
    return shortible(std::greater_equal<double>(), as_number, s, vals);

    // Date!
  case TokenID::DateEquals:
    return shortible(std::equal_to<ceph::real_time>(), as_date, s, vals);

  case TokenID::DateNotEquals:
    return shortible(std::not2(std::equal_to<ceph::real_time>()),
		     as_date, s, vals);

  case TokenID::DateLessThan:
    return shortible(std::less<ceph::real_time>(), as_date, s, vals);


  case TokenID::DateLessThanEquals:
    return shortible(std::less_equal<ceph::real_time>(), as_date, s, vals);

  case TokenID::DateGreaterThan:
    return shortible(std::greater<ceph::real_time>(), as_date, s, vals);

  case TokenID::DateGreaterThanEquals:
    return shortible(std::greater_equal<ceph::real_time>(), as_date, s,
		     vals);

    // Bool!
  case TokenID::Bool:
    return shortible(std::equal_to<bool>(), as_bool, s, vals);

    // Binary!
  case TokenID::BinaryEquals:
    return shortible(std::equal_to<ceph::bufferlist>(), as_binary, s,
		     vals);

    // IP Address!
  case TokenID::IpAddress:
    return shortible(std::equal_to<MaskedIP>(), as_network, s, vals);

  case TokenID::NotIpAddress:
    return shortible(std::not2(std::equal_to<MaskedIP>()), as_network, s,
		     vals);

#if 0
    // Amazon Resource Names! (Does S3 need this?)
    TokenID::ArnEquals, TokenID::ArnNotEquals, TokenID::ArnLike,
      TokenID::ArnNotLike,
#endif

  default:
    return false;
  }
}

optional<MaskedIP> Condition::as_network(const string& s) {
  MaskedIP m;
  if (s.empty())
    return none;

  m.v6 = s.find(':');
  auto slash = s.find('/');
  if (slash == string::npos) {
    m.prefix = m.v6 ? 128 : 32;
  } else {
    char* end = 0;
    m.prefix = strtoul(s.data() + slash + 1, &end, 10);
    if (*end != 0 || (m.v6 && m.prefix > 128) ||
	(!m.v6 && m.prefix > 32))
      return none;
  }

  string t;
  auto p = &s;

  if (slash != string::npos) {
    t.assign(s, 0, slash);
    p = &t;
  }

  if (m.v6) {
    struct sockaddr_in6 a;
    if (inet_pton(AF_INET6, p->c_str(), static_cast<void*>(&a)) != 1) {
      return none;
    }

    m.addr |= Address(a.sin6_addr.s6_addr[0]) << 0;
    m.addr |= Address(a.sin6_addr.s6_addr[1]) << 8;
    m.addr |= Address(a.sin6_addr.s6_addr[2]) << 16;
    m.addr |= Address(a.sin6_addr.s6_addr[3]) << 24;
    m.addr |= Address(a.sin6_addr.s6_addr[4]) << 32;
    m.addr |= Address(a.sin6_addr.s6_addr[5]) << 40;
    m.addr |= Address(a.sin6_addr.s6_addr[6]) << 48;
    m.addr |= Address(a.sin6_addr.s6_addr[7]) << 56;
    m.addr |= Address(a.sin6_addr.s6_addr[8]) << 64;
    m.addr |= Address(a.sin6_addr.s6_addr[9]) << 72;
    m.addr |= Address(a.sin6_addr.s6_addr[10]) << 80;
    m.addr |= Address(a.sin6_addr.s6_addr[11]) << 88;
    m.addr |= Address(a.sin6_addr.s6_addr[12]) << 96;
    m.addr |= Address(a.sin6_addr.s6_addr[13]) << 104;
    m.addr |= Address(a.sin6_addr.s6_addr[14]) << 112;
    m.addr |= Address(a.sin6_addr.s6_addr[15]) << 120;
  } else {
    struct sockaddr_in a;
    if (inet_pton(AF_INET, p->c_str(), static_cast<void*>(&a)) != 1) {
      return none;
    }
    m.addr = ntohl(a.sin_addr.s_addr);
  }

  return none;
}

Effect Statement::eval(const Environment& e,
		       optional<const rgw::auth::Identity&> ida,
		       uint64_t act, const ARN& res) const {
  if (ida && (!ida->is_identity(princ) || ida->is_identity(noprinc)))
    return Effect::Pass;


  if (!std::any_of(resource.begin(), resource.end(),
		   [&res](const ARN& pattern) {
		     return pattern.match(res);
		   }) ||
      (std::any_of(notresource.begin(), notresource.end(),
		   [&res](const ARN& pattern) {
		     return pattern.match(res);
		   })))
    return Effect::Pass;

  if (!(action & act) || (notaction & act))
    return Effect::Pass;

  if (std::all_of(conditions.begin(),
		  conditions.end(),
		  [&e](const Condition& c) { return c.eval(e);}))
    return effect;

  return Effect::Pass;
}

Policy::Policy(CephContext* cct, const string& tenant,
	       string _text)
  : text(std::move(_text)) {
  StringStream ss(text.data());
  PolicyParser pp(cct, tenant, *this);
  auto pr = Reader{}.Parse<kParseNumbersAsStringsFlag |
			   kParseCommentsFlag>(ss, pp);
  if (!pr)
    throw PolicyParseException(std::move(pr));
}

Effect Policy::eval(const Environment& e,
		    optional<const rgw::auth::Identity&> ida,
		    std::uint64_t action, const ARN& resource) const {
  auto allowed = false;
  for (auto& s : statements) {
    auto g = s.eval(e, ida, action, resource);
    if (g == Effect::Deny)
      return g;
    else if (g == Effect::Allow)
      allowed = true;
  }
  return allowed ? Effect::Allow : Effect::Pass;
}
}
}
