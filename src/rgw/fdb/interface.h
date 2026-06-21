// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- // vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025-2026 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#ifndef CEPH_FDB_BINDINGS_H
 #define CEPH_FDB_BINDINGS_H

#include "base.h"
#include "conversion.h"

// JFW: this can go away once iterator types are lifted away:
#include <map> 

// Ditto this, which I believe will be integrated into the standard library:
#include <boost/container/flat_map.hpp>


#include <span>
#include <ranges>
#include <cstdint>
#include <utility>
#include <concepts>
#include <iterator>
#include <generator>
#include <exception>
#include <algorithm>
#include <functional>
#include <filesystem>
#include <type_traits>

namespace ceph::libfdb {

// This needs to be called once and only once during application lifetime,
// when no more FoundationDB calls will be made:
inline void shutdown_libfdb()
{
 // Shutdown the FDB thread:
 ceph::libfdb::detail::database_system::shutdown_fdb();
}

inline database_handle create_database()
{
 return std::make_shared<database>();
}

inline database_handle create_database(const std::filesystem::path dbfile)
{
 return std::make_shared<database>(dbfile);
}

inline database_handle create_database(const std::filesystem::path dbfile, const database_options& dbopts, const network_options& netopts)
{
 return std::make_shared<database>(dbfile, dbopts, netopts);
}

inline database_handle create_database(const database_options& dbopts, const network_options& netopts)
{
 return std::make_shared<database>(dbopts, netopts);
}

inline database_handle create_database(const database_options& opts)
{
 return std::make_shared<database>(opts, network_options{});
}

inline database_handle create_database(const std::filesystem::path dbfile, const database_options& dbopts)
{
 return std::make_shared<database>(dbfile, dbopts, network_options{});
}

inline transaction_handle make_transaction(database_handle dbh)
{
 return std::make_shared<transaction>(dbh);
}

inline transaction_handle make_transaction(database_handle dbh, const transaction_options& opts)
{
 return std::make_shared<transaction>(dbh, opts);
}

// Note: only rarely is a direct call to this needed.
// Note: after a transaction is committed, it cannot be used again; but right now, that is NOT
// an error with respect to the object. So, don't do operations on the object after you've committed
// it or the behavior could be surpising.
// On false, the client should retry the transaction:
[[nodiscard]] inline bool commit(transaction_handle& txn)
{
 return txn->commit(); 
}

} // namespace ceph::libfdb

namespace ceph::libfdb::detail {

inline fdb_error_t do_commit(transaction_handle& txn)
{
 fdb_error_t r = 0;

 if(txn->commit(&r)) {
  return 0;
 }

 return r;
}

// Algorithm helper for gathering converted values into a container (out_values):
auto value_collector(auto& out_values)
{
 return [&out_values](std::span<const std::uint8_t> out_data) {
            ceph::libfdb::from::convert(out_data, out_values);
        }; 
}

// Sadly, a RAII wrapper can't safely capture all of the failure conditions that can occur during an FDB commit without throwing from its dtor.
// Commit operation, possibly replay if FDB requests it.
template <typename FnT>
decltype(auto) maybe_commit(transaction_handle txn, const commit_after_op commit_after, FnT&& fn)
{
 using result_t = std::invoke_result_t<FnT&, transaction_handle>;

 if(commit_after_op::commit != commit_after) {
  return std::invoke(fn, txn);
 }

 for(int attempts = 10; attempts; --attempts) {
  if constexpr (std::is_void_v<result_t>) {
   std::invoke(fn, txn);

   if(ceph::libfdb::commit(txn)) {
    return;
   }
  }

  if constexpr (not std::is_void_v<result_t>) {
   auto result = std::invoke(fn, txn);

   if(ceph::libfdb::commit(txn)) {
    return result;
   }
  }
 }

 throw ceph::libfdb::libfdb_exception("transaction retry limit exceeded");
}

// Commit operation only once; caller responsible for transaction replay.
template <typename FnT>
decltype(auto) commit_noreplay(transaction_handle txn, const commit_after_op commit_after, FnT&& fn)
{
 using result_t = std::invoke_result_t<FnT&, transaction_handle>;

 if(commit_after_op::commit != commit_after) {
  return std::invoke(fn, txn);
 }

 if constexpr (std::is_void_v<result_t>) {
  std::invoke(fn, txn);

  if(fdb_error_t r = do_commit(txn); 0 != r) {
   throw ceph::libfdb::libfdb_exception(r);
  }

  return;
 }

 if constexpr (not std::is_void_v<result_t>) {
  auto result = std::invoke(fn, txn);

  if(fdb_error_t r = do_commit(txn); 0 != r) {
   throw ceph::libfdb::libfdb_exception(r);
  }

  return result;
 }
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb {

inline void set(transaction_handle txn, std::string_view k, const auto& v, const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after, [&](transaction_handle txn) {
            return txn->set(detail::as_fdb_span(k), ceph::libfdb::to::convert(v));
        });
}

// If someone gives us an explicit transaction handle, they almost certainly don't want to commit 
// it (though they can always specify otherwise):
inline void set(transaction_handle txn, std::string_view k, const auto& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

// ...conversely, with a database handle given, we can assume they DO want to auto-commit:
inline void set(database_handle dbh, std::string_view k, const auto& v)
{
 return detail::maybe_commit(make_transaction(dbh), commit_after_op::commit, [&](transaction_handle txn) {
            return txn->set(detail::as_fdb_span(k), ceph::libfdb::to::convert(v));
        });
}

template <template <typename ...> typename AssocT = fdb_flat_map, typename IterT>
requires std::input_iterator<IterT> and requires(const std::iter_value_t<IterT>& kv)
  { kv.first; kv.second; }
inline void set(transaction_handle txn, IterT b, IterT e, const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after, [&](transaction_handle txn) {
          std::for_each(b, e, [&txn](const auto& kv) {
            txn->set(detail::as_fdb_span(kv.first), ceph::libfdb::to::convert(kv.second)); 
           });
        });
}

inline void set(transaction_handle txn, std::map<std::string, std::string>::const_iterator b, std::map<std::string, std::string>::const_iterator e)
{
 return set(txn, b, e, commit_after_op::no_commit);
}

// There's a "sharp edge" in our underlying serializer that exists to provide maximum performance but for us is just
// a source of confusion, and that is that raw character arrays are retrieved without setup for their length-- which will
// cause an obscure-looking exception about numeric ranges. Our own users can handle a string decode with length. Therefore,
// we pass the character array in (as with anything else) forced to a span; these overloads are force us to forward via those
// other calls. Let's force string-like inputs to go down the string-like happy path:
inline void set(transaction_handle txn, std::string_view k, const ceph::libfdb::concepts::stringview_convertible auto& v, const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after, [&](transaction_handle txn) {
            return txn->set(detail::as_fdb_span(k),
                            ceph::libfdb::to::convert(std::string_view(v)));
        });
}

inline void set(transaction_handle txn, std::string_view k, const ceph::libfdb::concepts::stringview_convertible auto& v)
{
 return set(txn, k, v, commit_after_op::no_commit);
}

inline void set(database_handle dbh, std::string_view k, const ceph::libfdb::concepts::stringview_convertible auto& v)
{
 return detail::maybe_commit(make_transaction(dbh), commit_after_op::commit, [&](transaction_handle txn) {
            return txn->set(detail::as_fdb_span(k),
                            ceph::libfdb::to::convert(std::string_view(v)));
        });
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// erase() in libfdb is clear() in FDB parlance:
inline void erase(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after, [&](transaction_handle txn) {
            return txn->erase(key_range);
        });
}

inline void erase(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range)
{
 return erase(txn, key_range, commit_after_op::no_commit);
}

inline void erase(ceph::libfdb::database_handle dbh, const ceph::libfdb::select& key_range)
{
 return detail::maybe_commit(ceph::libfdb::make_transaction(dbh), commit_after_op::commit, [&](transaction_handle txn) {
            return txn->erase(key_range);
        });
}

inline void erase(ceph::libfdb::transaction_handle txn, std::string_view k, const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after, [&](transaction_handle txn) {
            return txn->erase(detail::as_fdb_span(k));
        });
}

inline void erase(ceph::libfdb::transaction_handle txn, std::string_view k)
{
 return erase(txn, k, commit_after_op::no_commit);
}

inline void erase(ceph::libfdb::database_handle dbh, std::string_view k)
{
 return detail::maybe_commit(make_transaction(dbh), commit_after_op::commit, [&](transaction_handle txn) {
            return txn->erase(detail::as_fdb_span(k));
        });
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// get() with selector:
// JFW: Satisfying output_iterator is not as straightforward as it appears, I need to look at this mechanism again; meanwhile, the template doesn't
// /prevent/ future type narrowing, but I'm forcing it to std::string for now:
/*template <template OutIterT<typename I, typename T>, typename I, typename T = std::pair<std::string, std::string>>
requires std::output_iterator<I, T>
inline bool get(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, std::output_iterator<T> auto out_iter, const ceph::libfdb::commit_after_op commit_after)
*/
inline bool get(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, auto out_iter, const ceph::libfdb::commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after, [&](transaction_handle txn) {
            return txn->get(key_range, out_iter);
        });
}

inline bool get(ceph::libfdb::transaction_handle txn, const ceph::libfdb::select& key_range, auto out_iter)
{ 
 return get(txn, key_range, out_iter, commit_after_op::no_commit);
}

inline bool get(ceph::libfdb::database_handle dbh, const ceph::libfdb::select& key_range, auto out_iter)
{
 return get(ceph::libfdb::make_transaction(dbh), key_range, out_iter, ceph::libfdb::commit_after_op::no_commit);
}

inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, auto& out_value, const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after, [&](transaction_handle txn) {
            auto vc = detail::value_collector(out_value);
            return txn->get(detail::as_fdb_span(key), vc);
        });
}

inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, auto& out_value)
{
 return get(txn, key, out_value, commit_after_op::no_commit);
}

inline bool get(ceph::libfdb::database_handle dbh, std::string_view key, auto& out_value)
{
 return get(ceph::libfdb::make_transaction(dbh), key, out_value, commit_after_op::no_commit);
}

// The user can provide an immediate conversion function (no function_ref until C++26):
// The user function must immediately make a copy of the FDB Future's data, before its lifetime ends:
// JFW: std::remove_cvref_t will help:
// JFW: inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, std::invocable auto&& fn, const commit_after_op commit_after)
inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, auto&& fn, const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after, [&](transaction_handle txn) {
            return txn->get(detail::as_fdb_span(key), fn);
        });
}

inline bool get(ceph::libfdb::transaction_handle txn, std::string_view key, auto&& fn)
{
 return get(txn, key, fn, commit_after_op::no_commit);
}

inline bool get(ceph::libfdb::database_handle dbh, std::string_view key, auto&& fn)
{
 return get(ceph::libfdb::make_transaction(dbh), key, fn, commit_after_op::no_commit);
}

} // namespace ceph::libfdb

namespace ceph::libfdb {

// Does a key exist?
inline bool key_exists(transaction_handle txn, std::string_view k, const commit_after_op commit_after)
{
 return detail::commit_noreplay(txn, commit_after, [&](transaction_handle txn) {
            return txn->key_exists(k);
        });
}

inline bool key_exists(transaction_handle txn, std::string_view k)
{
 return key_exists(txn, k, commit_after_op::no_commit);
}

inline bool key_exists(database_handle dbh, std::string_view k)
{
 return key_exists(ceph::libfdb::make_transaction(dbh), k, commit_after_op::no_commit);
}

} // namespace ceph::libfdb

// Basic Generators:
namespace ceph::libfdb {

template <typename ValueT = std::string>
inline auto pair_generator(ceph::libfdb::transaction_handle txn, ceph::libfdb::select key_range) 
  -> std::generator<std::pair<std::string, ValueT>>
{
 for(const std::span<const FDBKeyValue>& kvp_block : ceph::libfdb::detail::generate_FDB_pairs(*txn, key_range)) {
   for(const auto& kvp : kvp_block) {
     co_yield ceph::libfdb::detail::to_decoded_kv_pair(kvp);
   }
 } 
}

template <typename ValueT = std::string>
inline auto pair_generator(ceph::libfdb::database_handle& dbh, ceph::libfdb::select key_range)
{
 return pair_generator<ValueT>(ceph::libfdb::make_transaction(dbh), key_range);
}

template <typename AssocT = boost::container::flat_map<std::string, std::string>>
auto block_generator(ceph::libfdb::database_handle dbh, ceph::libfdb::select selector)
-> std::generator<AssocT>
{
 // Although this is tunable, in my measurement it isn't a large factor so far-- likely 
 // these can move into the select instance itself:
 const auto chunk_size = 2048;

 auto split_points = detail::locate_split_points(dbh, selector, chunk_size);

 for(const auto& sp : split_points) {
    AssocT block;
    for(auto&& p : pair_generator(dbh, sp)) {
      block.emplace(std::move(p));
    }

    co_yield block;
 }
}

} // namespace ceph::libfdb

#endif
