// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/cache/MockImageCache.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ObjectRequest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace util {

inline ImageCtx *get_image_ctx(MockTestImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util

namespace io {

template <>
struct ObjectRequest<librbd::MockTestImageCtx> : public ObjectRequestHandle {
  static ObjectRequest* s_instance;
  Context *on_finish = nullptr;

  static ObjectRequest* create_remove(librbd::MockTestImageCtx *ictx,
                                      const std::string &oid,
                                      uint64_t object_no,
                                      const ::SnapContext &snapc,
                                      Context *completion) {
    assert(s_instance != nullptr);
    s_instance->on_finish = completion;
    return s_instance;
  }

  static ObjectRequest* create_truncate(librbd::MockTestImageCtx *ictx,
                                        const std::string &oid,
                                        uint64_t object_no,
                                        uint64_t object_off,
                                        const ::SnapContext &snapc,
                                        Context *completion) {
    assert(s_instance != nullptr);
    s_instance->on_finish = completion;
    return s_instance;
  }

  static ObjectRequest* create_write(librbd::MockTestImageCtx *ictx,
                                     const std::string &oid,
                                     uint64_t object_no,
                                     uint64_t object_off,
                                     const ceph::bufferlist &data,
                                     const ::SnapContext &snapc,
                                     Context *completion, int op_flags) {
    assert(s_instance != nullptr);
    s_instance->on_finish = completion;
    return s_instance;
  }

  static ObjectRequest* create_zero(librbd::MockTestImageCtx *ictx,
                                    const std::string &oid,
                                    uint64_t object_no, uint64_t object_off,
                                    uint64_t object_len,
                                    const ::SnapContext &snapc,
                                    Context *completion) {
    assert(s_instance != nullptr);
    s_instance->on_finish = completion;
    return s_instance;
  }

  ObjectRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }
  ~ObjectRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD1(complete, void(int));
  MOCK_METHOD0(send, void());
};

template <>
struct ObjectReadRequest<librbd::MockTestImageCtx> : public ObjectRequest<librbd::MockTestImageCtx> {
  typedef std::vector<std::pair<uint64_t, uint64_t> > Extents;
  typedef std::map<uint64_t, uint64_t> ExtentMap;

  static ObjectReadRequest* s_instance;

  static ObjectReadRequest* create(librbd::MockTestImageCtx *ictx,
                                   const std::string &oid,
                                   uint64_t objectno, uint64_t offset,
                                   uint64_t len, Extents &buffer_extents,
                                   librados::snap_t snap_id, bool sparse,
                                   Context *completion, int op_flags) {
    assert(s_instance != nullptr);
    s_instance->on_finish = completion;
    return s_instance;
  }

  ObjectReadRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }
  ~ObjectReadRequest() {
    s_instance = nullptr;
  }

  MOCK_CONST_METHOD0(get_offset, uint64_t());
  MOCK_CONST_METHOD0(get_length, uint64_t());
  MOCK_METHOD0(data, ceph::bufferlist &());
  MOCK_CONST_METHOD0(get_buffer_extents, const Extents &());
  MOCK_METHOD0(get_extent_map, ExtentMap &());

};

ObjectRequest<librbd::MockTestImageCtx>* ObjectRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
ObjectReadRequest<librbd::MockTestImageCtx>* ObjectReadRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace io
} // namespace librbd

#include "librbd/io/ImageRequest.cc"

namespace librbd {
namespace io {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

struct TestMockIoImageRequest : public TestMockFixture {
  typedef ImageRequest<librbd::MockTestImageCtx> MockImageRequest;
  typedef ImageWriteRequest<librbd::MockTestImageCtx> MockImageWriteRequest;
  typedef ImageDiscardRequest<librbd::MockTestImageCtx> MockImageDiscardRequest;
  typedef ImageFlushRequest<librbd::MockTestImageCtx> MockImageFlushRequest;
  typedef ObjectRequest<librbd::MockTestImageCtx> MockObjectRequest;
  typedef ObjectReadRequest<librbd::MockTestImageCtx> MockObjectReadRequest;

  void expect_is_journal_appending(MockJournal &mock_journal, bool appending) {
    EXPECT_CALL(mock_journal, is_journal_appending())
      .WillOnce(Return(appending));
  }

  void expect_write_to_cache(MockImageCtx &mock_image_ctx,
                             const object_t &object,
                             uint64_t offset, uint64_t length,
                             uint64_t journal_tid, int r) {
    EXPECT_CALL(mock_image_ctx, write_to_cache(object, _, length, offset, _, _,
                journal_tid))
      .WillOnce(WithArg<4>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }

  void expect_object_request_send(MockImageCtx &mock_image_ctx,
                                  MockObjectRequest &mock_object_request,
                                  int r) {
    EXPECT_CALL(mock_object_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_object_request, r]() {
                  mock_image_ctx.image_ctx->op_work_queue->queue(
                    mock_object_request.on_finish, r);
                }));
  }

  void expect_user_flushed(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, user_flushed());
  }

  void expect_flush(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_image_ctx, flush(_))
      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }
};

TEST_F(TestMockIoImageRequest, AioWriteJournalAppendDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectRequest mock_aio_object_request;
  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_is_journal_appending(mock_journal, false);
  expect_write_to_cache(mock_image_ctx, ictx->get_object_name(0),
                        0, 1, 0, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_WRITE);

  bufferlist bl;
  bl.append("1");
  MockImageWriteRequest mock_aio_image_write(mock_image_ctx, aio_comp,
                                             {{0, 1}}, std::move(bl), 0);
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_write.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, AioDiscardJournalAppendDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockObjectRequest mock_aio_object_request;
  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_is_journal_appending(mock_journal, false);
  expect_object_request_send(mock_image_ctx, mock_aio_object_request, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_DISCARD);
  MockImageDiscardRequest mock_aio_image_discard(mock_image_ctx, aio_comp, 0, 1);
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, AioFlushJournalAppendDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_user_flushed(mock_image_ctx);
  expect_is_journal_appending(mock_journal, false);
  expect_flush(mock_image_ctx, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_FLUSH);
  MockImageFlushRequest mock_aio_image_flush(mock_image_ctx, aio_comp);
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_flush.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

} // namespace io
} // namespace librbd
