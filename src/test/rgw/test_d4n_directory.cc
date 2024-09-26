#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "driver/d4n/d4n_directory.h"

#define dout_subsys ceph_subsys_rgw

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

class Environment* env;

class Environment : public ::testing::Environment {
  public:
    Environment() {}

    virtual ~Environment() {}

    void SetUp() override {
      std::vector<const char*> args;
      std::string conf_file_list;
      std::string cluster = "";
      CephInitParameters iparams = ceph_argparse_early_args(
	args, CEPH_ENTITY_TYPE_CLIENT,
	&cluster, &conf_file_list);

      cct = common_preinit(iparams, CODE_ENVIRONMENT_UTILITY, {}); 
      dpp = new DoutPrefix(cct->get(), dout_subsys, "D4N Object Directory Test: ");
      
      redisHost = cct->_conf->rgw_d4n_address; 
    }
    
    void TearDown() override {
      delete dpp;
    }

    std::string redisHost;
    CephContext* cct;
    DoutPrefixProvider* dpp;
};

class ObjectDirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      conn = std::make_shared<connection>(boost::asio::make_strand(io));
      dir = new rgw::d4n::ObjectDirectory{conn};
      obj = new rgw::d4n::CacheObj{
	.objName = "testName",
	.bucketName = "testBucket",
	.creationTime = "",
	.dirty = false,
	.hostsList = { env->redisHost }
      };

      ASSERT_NE(obj, nullptr);
      ASSERT_NE(dir, nullptr);
      ASSERT_NE(conn, nullptr);

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = env->redisHost.substr(0, env->redisHost.find(":"));
      cfg.addr.port = env->redisHost.substr(env->redisHost.find(":") + 1, env->redisHost.length()); 

      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete obj;
      delete dir;
    }

    rgw::d4n::CacheObj* obj;
    rgw::d4n::ObjectDirectory* dir;

    net::io_context io;
    std::shared_ptr<connection> conn;

    std::vector<std::string> vals{"testName", "testBucket", "", "0", env->redisHost};
    std::vector<std::string> fields{"objName", "bucketName", "creationTime", "dirty", "hosts"};
};

class BlockDirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      conn = std::make_shared<connection>(boost::asio::make_strand(io));
      dir = new rgw::d4n::BlockDirectory{conn};
      block = new rgw::d4n::CacheBlock{
        .cacheObj = {
	  .objName = "testName",
	  .bucketName = "testBucket",
	  .creationTime = "",
	  .dirty = false,
	  .hostsList = { env->redisHost }
	},
        .blockID = 0,
	.version = "",
	.deleteMarker = false,
	.size = 0
      };

      ASSERT_NE(block, nullptr);
      ASSERT_NE(dir, nullptr);
      ASSERT_NE(conn, nullptr);

      /* Run fixture's connection */
      config cfg;
      cfg.addr.host = env->redisHost.substr(0, env->redisHost.find(":"));
      cfg.addr.port = env->redisHost.substr(env->redisHost.find(":") + 1, env->redisHost.length()); 

      conn->async_run(cfg, {}, net::detached);
    } 

    virtual void TearDown() {
      delete block;
      delete dir;
    }

    rgw::d4n::CacheBlock* block;
    rgw::d4n::BlockDirectory* dir;

    net::io_context io;
    std::shared_ptr<connection> conn;

    std::vector<std::string> vals{"0", "", "0", "0", "0", 
                                   "testName", "testBucket", "", "0", env->redisHost};
    std::vector<std::string> fields{"blockID", "version", "deleteMarker", "size", "globalWeight", 
				     "objName", "bucketName", "creationTime", "dirty", "hosts"};
};

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

TEST_F(ObjectDirectoryFixture, SetYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, obj, yield));

    boost::system::error_code ec;
    request req;
    req.push_range("HMGET", "testBucket_testName", fields);
    req.push("FLUSHALL");

    response< std::vector<std::string>,
	      boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), vals);
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(ObjectDirectoryFixture, GetYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, obj, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "testBucket_testName", "objName", "newoid");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    ASSERT_EQ(0, dir->get(env->dpp, obj, yield));
    EXPECT_EQ(obj->objName, "newoid");

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

/* Does not currently pass on Ubuntu due to incompatible Redis version.
TEST_F(ObjectDirectoryFixture, CopyYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, obj, yield));
    ASSERT_EQ(0, dir->copy(env->dpp, obj, "copyTestName", "copyBucketName", yield));

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "copyBucketName_copyTestName");
    req.push_range("HMGET", "copyBucketName_copyTestName", fields);
    req.push("FLUSHALL");

    response<int, std::vector<std::string>, 
	     boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 1);

    auto copyVals = vals;
    copyVals[0] = "copyTestName";
    copyVals[1] = "copyBucketName";
    EXPECT_EQ(std::get<1>(resp).value(), copyVals);

    conn->cancel();
  }, rethrow);

  io.run();
}
*/

TEST_F(ObjectDirectoryFixture, DelYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, obj, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, dir->del(env->dpp, obj, yield));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName");
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(ObjectDirectoryFixture, UpdateFieldYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, obj, yield));
    ASSERT_EQ(0, dir->update_field(env->dpp, obj, "objName", "newTestName", yield));
    ASSERT_EQ(0, dir->update_field(env->dpp, obj, "hosts", "127.0.0.1:5000", yield));

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "testBucket_testName", "objName", "hosts");
    req.push("FLUSHALL");
    response< std::vector<std::string>, 
	      boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "newTestName");
    EXPECT_EQ(std::get<0>(resp).value()[1], "127.0.0.1:6379_127.0.0.1:5000");

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(ObjectDirectoryFixture, ZAddYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    auto m_time = real_clock::now();
    auto score = ceph::real_clock::to_double(m_time);
    std::string version = "v1";
    ASSERT_EQ(0, dir->zadd(env->dpp, obj, score, version, yield));
    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
    }
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(ObjectDirectoryFixture, ZAddZRevRangeYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    {
      auto m_time = real_clock::now();
      auto score = ceph::real_clock::to_double(m_time);
      std::string version = "v2";
      ASSERT_EQ(0, dir->zadd(env->dpp, obj, score, version, yield));
    }
    {
      auto m_time = real_clock::now();
      auto score = ceph::real_clock::to_double(m_time);
      std::string version = "v1";
      ASSERT_EQ(0, dir->zadd(env->dpp, obj, score, version, yield));
    }
    {
      std::vector<std::string> members;
      ASSERT_EQ(0, dir->zrevrange(env->dpp, obj, 0, 0, members, yield));
      ASSERT_EQ(1, members.size());
      ASSERT_EQ("v1", members[0]);
    }

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
    }
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(ObjectDirectoryFixture, ZAddZRemYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    {
      auto m_time = real_clock::now();
      auto score = ceph::real_clock::to_double(m_time);
      std::cout << "Score for v1: " << score << std::endl;
      std::string version = "v1";
      ASSERT_EQ(0, dir->zadd(env->dpp, obj, score, version, yield));
    }
    {
      auto m_time = real_clock::now();
      auto score = ceph::real_clock::to_double(m_time);
      std::cout << "Score for v2: " << score << std::endl;
      std::string version = "v2";
      ASSERT_EQ(0, dir->zadd(env->dpp, obj, score, version, yield));
    }
    {
      ASSERT_EQ(0, dir->zrem(env->dpp, obj, "v2", yield));
      std::vector<std::string> members;
      ASSERT_EQ(0, dir->zrevrange(env->dpp, obj, 0, 0, members, yield));
      ASSERT_EQ(1, members.size());
      ASSERT_EQ("v1", members[0]);
    }
    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
    }
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, SetYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));

    boost::system::error_code ec;
    request req;
    req.push_range("HMGET", "testBucket_testName_0_0", fields);
    req.push("FLUSHALL");

    response< std::vector<std::string>,
	      boost::redis::ignore_t > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), vals);
    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, GetYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "testBucket_testName_0_0", "objName", "newoid");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    ASSERT_EQ(0, dir->get(env->dpp, block, optional_yield{yield}));
    EXPECT_EQ(block->cacheObj.objName, "newoid");

    {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

/* Does not currently pass on Ubuntu due to incompatible Redis version.
TEST_F(BlockDirectoryFixture, CopyYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));
    ASSERT_EQ(0, dir->copy(env->dpp, block, "copyTestName", "copyBucketName", optional_yield{yield}));

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", "copyBucketName_copyTestName_0_0");
    req.push_range("HMGET", "copyBucketName_copyTestName_0_0", fields);
    req.push("FLUSHALL");

    response<int, std::vector<std::string>, 
	     boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value(), 1);

    auto copyVals = vals;
    copyVals[6] = "copyTestName";
    copyVals[7] = "copyBucketName";
    EXPECT_EQ(std::get<1>(resp).value(), copyVals);

    conn->cancel();
  }, rethrow);

  io.run();
}
*/

TEST_F(BlockDirectoryFixture, DelYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName_0_0");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
    }

    ASSERT_EQ(0, dir->del(env->dpp, block, optional_yield{yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName_0");
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, UpdateFieldYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));
    ASSERT_EQ(0, dir->update_field(env->dpp, block, "objName", "newTestName", optional_yield{yield}));
    ASSERT_EQ(0, dir->update_field(env->dpp, block, "hosts", "127.0.0.1:5000", optional_yield{yield}));

    boost::system::error_code ec;
    request req;
    req.push("HMGET", "testBucket_testName_0_0", "objName", "hosts");
    req.push("FLUSHALL");
    response< std::vector<std::string>, 
	      boost::redis::ignore_t> resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ(std::get<0>(resp).value()[0], "newTestName");
    EXPECT_EQ(std::get<0>(resp).value()[1], "127.0.0.1:6379_127.0.0.1:5000");

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, RemoveHostYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    block->cacheObj.hostsList.insert("127.0.0.1:6000");
    ASSERT_EQ(0, dir->set(env->dpp, block, optional_yield{yield}));
    ASSERT_EQ(0, dir->remove_host(env->dpp, block, "127.0.0.1:6379", optional_yield{yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", "testBucket_testName_0_0", "hosts");
      req.push("HGET", "testBucket_testName_0_0", "hosts");
      response<int, std::string> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
      EXPECT_EQ(std::get<1>(resp).value(), "127.0.0.1:6000");
    }

    ASSERT_EQ(0, dir->remove_host(env->dpp, block, "127.0.0.1:6000", optional_yield{yield}));

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", "testBucket_testName_0");
      req.push("FLUSHALL");
      response<int, boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 0);
    }

    conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, WatchExecuteYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
  {
    boost::system::error_code ec;
    request req;
    req.push("WATCH", "testBucket");
    response<std::string> resp;

    conn->async_exec(req, resp, yield[ec]);
    ASSERT_EQ((bool)ec, false);

    // The number of members added
    EXPECT_EQ(std::get<0>(resp).value(), "OK");
  }

  {
      boost::system::error_code ec;
      request req;
      req.push("HSET", "testBucket", "objName", "newoid");
      response<int> resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), 1);
  }

  {
      boost::system::error_code ec;
      request req;
      req.push("EXEC");
      response<std::vector<std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
  }

  {
      boost::system::error_code ec;
      request req;
      req.push("FLUSHALL");
      response<boost::redis::ignore_t> resp;

      conn->async_exec(req, resp, yield[ec]);
    }

  conn->cancel();
  }, rethrow);

  io.run();
}

TEST_F(BlockDirectoryFixture, IncrYield)
{
  boost::asio::spawn(io, [this] (boost::asio::yield_context yield) {
    for (int i = 0; i < 10; i++) {
      {
        boost::system::error_code ec;
        request req;
        req.push("INCR", "testObject");
        response<std::string> resp;

        conn->async_exec(req, resp, yield[ec]);
        ASSERT_EQ((bool)ec, false);
        std::cout << "thread id: " << std::this_thread::get_id() << std::endl;
        std::cout << "INCR value: " << std::get<0>(resp).value() << std::endl;
      }
    }
    boost::asio::post(conn->get_executor(), [c = conn] { c->cancel(); });
  }, rethrow);

  std::vector<std::thread> threads;

  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&] { io.run(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
