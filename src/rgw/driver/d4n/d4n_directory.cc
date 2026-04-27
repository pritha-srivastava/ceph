#include <algorithm>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory.h"

namespace rgw { namespace d4n {

std::string ObjectDirectory::build_index(CacheObj* object) 
{
  return object->bucketName + "_" + object->objName;
}

std::string BlockDirectory::build_index(CacheBlock* block) 
{
  return block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);
}

} } // namespace rgw::d4n
