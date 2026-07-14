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

#include <catch2/catch_config.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_adapters.hpp>

#include <catch2/matchers/catch_matchers_all.hpp>

#define CATCH_CONFIG_MAIN

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include "rgw/fdb/fdb.h"

#include "include/random.h"

#include <boost/container/flat_map.hpp>

#include <map>
#include <list>
#include <chrono>
#include <vector>
#include <ranges>
#include <unordered_map>

using Catch::Matchers::AllMatch;

using fmt::format;
using fmt::println;

using std::end;
using std::begin;

using std::string;
using std::string_view;

using std::to_string;

using std::vector;

using namespace std::literals;

namespace lfdb = ceph::libfdb;

// Be nice to Catch2's template-test macros:
using string_pair = std::pair<std::string, std::string>;

/* Importantly, FDB operations are /lexicographically/ ordered. I do not have a bunch of
time to write a fancy generator, so I've taken a "dumb as bones" approach and write a
fixed prefix followed by integers: */

// As we manipulate keys and values quite a bit, it's helpful to have a recipe for them:
std::string make_key(const int n, std::string_view prefix = "key") {
return fmt::format("{}_{:010d}", prefix, n);
}

std::string make_value(const int n) {
 return fmt::format("value_{:010d}", n);
}

// Collect values in selection to out_values:
auto key_counter(auto txn, const auto& selector, auto& out_values) -> auto {
 out_values.clear();

 lfdb::get(txn, selector, 
           std::inserter(out_values, std::end(out_values)));

 return out_values.size();
};

auto key_count(auto& dbh, const auto& selector) {
 std::map<std::string, std::string> _;
 return key_counter(lfdb::make_transaction(dbh), selector, _);
}

inline std::map<std::string, std::string> make_monotonic_kvs(const int N, std::string_view prefix = "key")
{
 std::map<std::string, std::string> kvs;

 for(const auto i : std::ranges::iota_view(0, N)) {
  kvs.insert({ make_key(i, prefix), make_value(i) });
 }

 return kvs;
}

inline auto write_monotonic_kvs(lfdb::database_handle dbh, const int N, std::string_view prefix = "key")
{
 auto kvs = make_monotonic_kvs(N, prefix);

 for(const auto& [k, v] : kvs)
  lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit);

 return kvs;
}

constexpr const char* const msg = "Hello, World!"; 
constexpr const char msg_with_null[] = { '\0', 'H', 'i', '\0', ' ', 't', 'h', 'e', 'r', 'e', '!', '\0'};
constexpr const char * const pearl_msg =
"Perle, plesaunte to prynces paye\n"
"To clanly clos in golde so clere;\n"
"Oute of oryent, I hardyly saye.\n"
"Ne proved I never her precios pere.\n";

// Clean up test keys when we leave scope:
struct janitor final
{
 ceph::libfdb::database_handle dbh_;

 // flip this off if you need artifacts after debugging:
 bool drop_after_scope = true;

 janitor(ceph::libfdb::database_handle dbh_)
  : dbh_(dbh_)
 {
  REQUIRE(nullptr != dbh_);
 }

 janitor()
  : janitor(ceph::libfdb::create_database())
 {}

 ~janitor()
 {
  if(drop_after_scope)
   drop_all(dbh_);
 }

 ceph::libfdb::database_handle dbh() { return dbh_; }

 static void drop_all(ceph::libfdb::database_handle dbh_) {
   lfdb::erase(ceph::libfdb::make_transaction(dbh_),
               lfdb::select { "", "\xFF" },
               lfdb::commit_after_op::commit);
 }

 void drop_all() { 
  return drop_all(dbh()); 
 }

 static void drop_all_keys(ceph::libfdb::database_handle dbh_) {

   // Note: technically, [0x00, 0xFF) is needed to include the system keys (if the transaction's allowed to
   // access these). However, special permissions are needed to access these magical "system keys" and we
   // probably don't actually want to delete them erroneously. So, we stick with our key range...
   // ("500,000,000 records aught to be enough for anybody.") 
   lfdb::erase(ceph::libfdb::make_transaction(dbh_),
               lfdb::select { make_key(0), make_key(500'000'000) },
               lfdb::commit_after_op::commit);
   }

  void drop_all_keys() {
    return drop_all_keys(dbh());
  }

/* This is tempting, but I think it might also *hide* bugs at times. Thoughts?
  operator ceph::libfdb::database_handle() { ... }
*/
};

// Basically, make sure we're actually linking with the library:
TEST_CASE()
{
 REQUIRE_THROWS_AS([] { throw ceph::libfdb::libfdb_exception(0); }(),
                   ceph::libfdb::libfdb_exception);
}

TEST_CASE("fdb simple", "[rgw][fdb]") {
 janitor j;

 auto dbh = j.dbh();

 const string_view k = "key";
 const string v = fmt::format("value-{:%c}", std::chrono::system_clock::now());

 SECTION("read missing key") {
    const string_view missing_key = "missing_key";

    SECTION("with transaction") {
        std::string out_value;

        auto txn_handle = lfdb::make_transaction(dbh);
        REQUIRE(nullptr != txn_handle);
  
        CAPTURE(missing_key); 
        CAPTURE(out_value); 
        REQUIRE_FALSE(lfdb::get(txn_handle, missing_key, out_value, lfdb::commit_after_op::no_commit));
        CHECK(v != out_value);
    }
 }

 SECTION("CRD single-key") {
    std::string out_value;

    // The key initially either exists, or we'll write it anew, either is fine:
    CHECK_NOTHROW(lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit));

    // Make sure that it DOES exist:
    CHECK(lfdb::get(lfdb::make_transaction(dbh), k, out_value, lfdb::commit_after_op::no_commit));
    CHECK(v == out_value); 

    // "erase()" is known as "clear" in FDB parlance, deleting a record:
    REQUIRE_NOTHROW(lfdb::erase(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::commit));

    // ...as this shouldn't be updated again, make sure there isn't an accidental match:
    out_value.clear();

    // ...and, POOF!-- it should be gone:
    CHECK_FALSE(lfdb::get(lfdb::make_transaction(dbh), k, out_value, lfdb::commit_after_op::no_commit));
    CHECK(v != out_value);
 }

 SECTION("read/write single key") {
    REQUIRE(nullptr != dbh);

    // First, be sure we have a valid value written to the database:
    REQUIRE_NOTHROW(lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit));

    SECTION("read transaction") {
      std::string out_value;
     
      CHECK(lfdb::get(lfdb::make_transaction(dbh), k, out_value, lfdb::commit_after_op::no_commit));
      CHECK(v == out_value); 
    }
 }

 SECTION("check for existence of key") {
    REQUIRE(nullptr != dbh);

    // Erase the key if it's already there:
    lfdb::erase(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::commit);

    // Now, we shouldn't find anything:
    CHECK_FALSE(lfdb::key_exists(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::no_commit));

    // Write the key:
    lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit);

    // ...it should magically be there!
    CHECK(lfdb::key_exists(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::no_commit));

    // ...and now it should be gone again:
    lfdb::erase(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::commit);
    CHECK_FALSE(lfdb::key_exists(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::no_commit));
 }
}

TEST_CASE("delete keys in range", "[rgw][fdb]") {
 janitor j;
 auto dbh = j.dbh();

 // Exactly 20 keys, 0-19:
 const auto selector = lfdb::select { make_key(0), make_key(20) };

 // Make sure we're "empty":
 REQUIRE(0 == key_count(dbh, selector));

 // Make sure we have a matching number of keys in our selector range:
 const auto kvs = write_monotonic_kvs(dbh, 20);
 REQUIRE(20 == key_count(dbh, selector));

 // Erase a single value:
 lfdb::erase(dbh, make_key(5));
 CHECK(19 == key_count(dbh, selector));

 // Erase an edge of the range:
 lfdb::erase(dbh, lfdb::select { make_key(0), make_key(1) });
 CHECK(18 == key_count(dbh, selector));

 // ...the other edge: 
 lfdb::erase(dbh, lfdb::select { make_key(19), make_key(20) });
 CHECK(17 == key_count(dbh, selector));

 // Erase the entire range:
 lfdb::erase(lfdb::make_transaction(dbh), selector, lfdb::commit_after_op::commit);
 CHECK(0 == key_count(dbh, selector));
}

TEMPLATE_PRODUCT_TEST_CASE("multi-key ops", "[rgw][fdb]", 
(std::vector, std::list), (string_pair)) 
{
 janitor j;

 auto dbh = j.dbh();

 // Write a sequence of keys so we have some data to work with:
 const auto kvs = write_monotonic_kvs(dbh, 100);

 SECTION("check multiple key write", "[fdb]") {
  auto txn = lfdb::make_transaction(dbh);

  std::string out_value;
 
  CHECK((*(kvs.find(make_key(0)))).second == make_value(0));
  CHECK(lfdb::get(txn, make_key(0), out_value, lfdb::commit_after_op::no_commit));
  CHECK(make_value(0) == out_value);

  out_value.clear();
  CHECK((*(kvs.find(make_key(99)))).second == make_value(99));
  CHECK(lfdb::get(txn, make_key(99), out_value, lfdb::commit_after_op::no_commit));
  CHECK(make_value(99) == out_value);
 }

 SECTION("check multiple key selection", "[fdb]") {
  TestType out_values;

  auto txn = lfdb::make_transaction(dbh);

  lfdb::get(txn, lfdb::select { make_key(0), make_key(100) }, std::back_inserter(out_values), lfdb::commit_after_op::no_commit);

  CHECK(100 == out_values.size());

  // Maybe not the world's most creative test, but the idea is to try getting some random keys:
  for(auto i = ceph::util::generate_random_number(out_values.size() - 1); i; --i) {
    CHECK(std::end(out_values) != std::ranges::find(out_values, string_pair { make_key(i), make_value(i) }));
  }
 }
}

TEST_CASE("check selectors", "[fdb][rgw]") {
 janitor j;

 const int nentries = 10;

 const auto select_all = lfdb::select { make_key(0), make_key(nentries) };

 auto dbh = j.dbh();

 // Make sure that there's nothing in our test range:
 j.drop_all();
 REQUIRE(0 == key_count(dbh, select_all));

 const auto kvs = write_monotonic_kvs(dbh, nentries);

 // Make sure there's exactly as many entries as we added:
 REQUIRE(nentries == key_count(dbh, select_all));

 std::vector<std::pair<std::string, std::string>> out;
 lfdb::get(dbh, select_all, std::back_inserter(out));

 // These /are/ the droids you're looking for:
 CHECK(nentries == out.size());
 CHECK(make_key(0) == out.front().first);
 CHECK(make_key(nentries - 1) == out.back().first);

 // Get exactly no entries:
 out.clear();
 lfdb::get(dbh, lfdb::select { make_key(0), make_key(0) }, std::back_inserter(out));
 CHECK(0 == out.size());

 // Get exactly one entry: 
 out.clear();
 lfdb::get(dbh, lfdb::select { make_key(1), make_key(2) }, std::back_inserter(out));
 REQUIRE(1 == out.size());
 CHECK(make_key(1) == out.front().first);
}

TEST_CASE("fdb conversions (built-in)", "[fdb][rgw]") {
 // Manual tests of conversions to and from supported FDB built-in types.

 SECTION("spanlike") {
  // span<uint8_t> -> vector<uint8_t> -> vector<uint8_t>
  const std::span<const std::uint8_t> n((const std::uint8_t *)msg, sizeof(msg));

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 } 

 SECTION("NULL-as-data") {
  // with NULL data-- const char* -> vector<uint8_t> -> vector<uint8_t>
  const std::span<const std::uint8_t> n((const std::uint8_t *)msg_with_null, sizeof(msg_with_null));

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  REQUIRE_THAT(msg_with_null, Catch::Matchers::RangeEquals(o));
 }
}

TEST_CASE("fdb conversions (round-trip)", "[fdb][rgw]") {
 janitor j;

 auto dbh = j.dbh();

 // string_view -> string
 {
 const std::string_view n = "Hello, World!";
 std::string o;

 lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(dbh), "key", o, lfdb::commit_after_op::no_commit);

 REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }

 // vector<uint8_t> -> vector<uint8_t>
 {
 const std::vector<uint8_t> n = { 1, 2, 3, 4, 5 };
 std::vector<uint8_t> o;

 lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(dbh), "key", o, lfdb::commit_after_op::no_commit);

 REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }
} 

TEST_CASE("fdb conversions (functions)", "[fdb][rgw]")
{
 SECTION("convert with a lambda function")
 {
  std::string_view n { pearl_msg };
  std::string o;

  std::vector<std::uint8_t> x = ceph::libfdb::to::convert(n);

  auto fn = [&o](const char *data, std::size_t sz) -> void { 
    // Because we did /conversion/ on the inbound data, we're still obliged to
    // reverse this (or else we'll see whatever artefacts the conversion produced)-- 
    // the complication is a consequence of dealing with the underlying buffer directly:
    std::span<const std::uint8_t> in_span((const std::uint8_t *)data, sz);
 
    ceph::libfdb::from::convert(in_span, o);
  };

  ceph::libfdb::from::convert(x, fn); 

  CAPTURE(n);
  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }
}

TEST_CASE("basic generators", "[fdb]") {
 janitor j;

 const unsigned nkeys = GENERATE(0, 1, 2, 3, 10, 100, 1'000);

 auto dbh = j.dbh();

 const auto kvs_in = write_monotonic_kvs(dbh, nkeys);
 REQUIRE(nkeys == kvs_in.size());

 SECTION("pair_generator, kv pair return") {
    std::map<std::string, std::string> out;

    // pair_generator returns key-value pairs, keeping the specified transaction (or implicitly created one)
    // alive until exhausted (note that this may cause the transaction to expire if approaching 5s or so):
    for(auto&& kvp : lfdb::pair_generator(dbh, lfdb::select { make_key(0), make_key(nkeys) }))
     out.emplace(kvp);

    REQUIRE(nkeys == out.size());

    // Be sure we captured the head and the tail:
    if(0 < nkeys) {
      CHECK(out.contains(make_key(0)));
      CHECK(out.contains(make_key(nkeys - 1)));
    }
 }
}

TEMPLATE_PRODUCT_TEST_CASE("associative data", "[fdb][rgw]",
(std::map, std::unordered_map, boost::container::flat_map), ((std::string, std::string)))
{
 janitor j;

 auto dbh = j.dbh();

 TestType kvs{
      { "hello", "world" },
      { "lorem", "ipsum" },
      { "perl", "camel" },
      { "pearl", pearl_msg }
    };

 // From the "database" point of view, the structure is now that we have a single 
 // key pointing (p) to an associative array, e.g. map<p, map<k, v>>:
 lfdb::set(lfdb::make_transaction(dbh), "key", kvs, lfdb::commit_after_op::commit);

 TestType out_kvs;

 lfdb::get(lfdb::make_transaction(dbh), "key", out_kvs, lfdb::commit_after_op::no_commit);

 CHECK(pearl_msg == out_kvs["pearl"]);
}

TEST_CASE("person records with name-age storage", "[rgw][fdb]") {
 janitor j;
 auto dbh = j.dbh();

 // Create person records, where each person is a map with "name" and "age"
 std::vector<std::map<std::string, std::string>> people = {
   {{"name", "Alice"}, {"age", "30"}},
   {{"name", "Bob"}, {"age", "25"}},
   {{"name", "Charlie"}, {"age", "35"}},
   {{"name", "Diana"}, {"age", "28"}},
   {{"name", "Eve"}, {"age", "32"}}
 };

 SECTION("store each person under their name as key") {
   // Store each person map using their name as the database key
   for(const auto& person : people) {
     const auto& name = person.at("name");
     lfdb::set(dbh, name, person);
   }

   // Retrieve a single person record by name
   std::map<std::string, std::string> alice_record;
   CHECK(lfdb::get(dbh, "Alice", alice_record));
   CHECK("Alice" == alice_record["name"]);
   CHECK("30" == alice_record["age"]);

   // Retrieve another person
   std::map<std::string, std::string> bob_record;
   CHECK(lfdb::get(dbh, "Bob", bob_record));
   CHECK("Bob" == bob_record["name"]);
   CHECK("25" == bob_record["age"]);
 }

 SECTION("query range of person records") {
   // Store each person individually with name as key
   for(const auto& person : people) {
     const auto& name = person.at("name");
     lfdb::set(dbh, name, person);
   }

   // Query range by retrieving individual keys
   // Check that Bob, Charlie, and Diana exist in the range
   std::map<std::string, std::string> bob_record, charlie_record, diana_record;

   CHECK(lfdb::get(dbh, "Bob", bob_record));
   CHECK("Bob" == bob_record["name"]);
   CHECK("25" == bob_record["age"]);

   CHECK(lfdb::get(dbh, "Charlie", charlie_record));
   CHECK("Charlie" == charlie_record["name"]);
   CHECK("35" == charlie_record["age"]);

   CHECK(lfdb::get(dbh, "Diana", diana_record));
   CHECK("Diana" == diana_record["name"]);
   CHECK("28" == diana_record["age"]);
 }

 SECTION("query all person records") {
   // Store each person individually
   for(const auto& person : people) {
     const auto& name = person.at("name");
     lfdb::set(dbh, name, person);
   }

   // Retrieve all person records individually to verify they exist
   std::map<std::string, std::string> alice_record, bob_record, charlie_record, diana_record, eve_record;

   CHECK(lfdb::get(dbh, "Alice", alice_record));
   CHECK("Alice" == alice_record["name"]);
   CHECK("30" == alice_record["age"]);

   CHECK(lfdb::get(dbh, "Bob", bob_record));
   CHECK("Bob" == bob_record["name"]);
   CHECK("25" == bob_record["age"]);

   CHECK(lfdb::get(dbh, "Charlie", charlie_record));
   CHECK("Charlie" == charlie_record["name"]);
   CHECK("35" == charlie_record["age"]);

   CHECK(lfdb::get(dbh, "Diana", diana_record));
   CHECK("Diana" == diana_record["name"]);
   CHECK("28" == diana_record["age"]);

   CHECK(lfdb::get(dbh, "Eve", eve_record));
   CHECK("Eve" == eve_record["name"]);
   CHECK("32" == eve_record["age"]);
 }

 SECTION("range query using pair_generator") {
   // Store each person individually
   for(const auto& person : people) {
     const auto& name = person.at("name");
     lfdb::set(dbh, name, person);
   }

   // Use pair_generator to iterate through range and collect keys
   // Range: "Bob" to "Eve" (exclusive end, so Bob, Charlie, Diana)
   std::vector<std::string> keys_in_range;

   for(auto&& [key, value] : lfdb::pair_generator(dbh, lfdb::select{"Bob", "Eve"})) {
     keys_in_range.push_back(key);
   }

   // Verify we got exactly 3 keys in lexicographic order
   REQUIRE(3 == keys_in_range.size());
   CHECK("Bob" == keys_in_range[0]);
   CHECK("Charlie" == keys_in_range[1]);
   CHECK("Diana" == keys_in_range[2]);

   // Now retrieve the values for verification
   std::map<std::string, std::string> bob_record, charlie_record, diana_record;
   CHECK(lfdb::get(dbh, "Bob", bob_record));
   CHECK("25" == bob_record["age"]);
   CHECK(lfdb::get(dbh, "Charlie", charlie_record));
   CHECK("35" == charlie_record["age"]);
   CHECK(lfdb::get(dbh, "Diana", diana_record));
   CHECK("28" == diana_record["age"]);
 }

 SECTION("full range query using pair_generator") {
   // Store each person individually
   for(const auto& person : people) {
     const auto& name = person.at("name");
     lfdb::set(dbh, name, person);
   }

   // Use pair_generator to get all keys in range
   std::vector<std::string> all_keys;

   for(auto&& [key, value] : lfdb::pair_generator(dbh, lfdb::select{"Alice", "Zzzz"})) {
     all_keys.push_back(key);
   }

   // Verify we got all 5 keys in lexicographic order
   REQUIRE(5 == all_keys.size());
   CHECK("Alice" == all_keys[0]);
   CHECK("Bob" == all_keys[1]);
   CHECK("Charlie" == all_keys[2]);
   CHECK("Diana" == all_keys[3]);
   CHECK("Eve" == all_keys[4]);

   // Retrieve and verify the values
   std::map<std::string, std::string> record;
   CHECK(lfdb::get(dbh, "Alice", record));
   CHECK("30" == record["age"]);
   CHECK(lfdb::get(dbh, "Eve", record));
   CHECK("32" == record["age"]);
 }

 SECTION("efficient range query with simple string values") {
   // Store person data as simple strings (name:age format)
   // This allows pair_generator to return both key and value in one call!
   lfdb::set(dbh, "Alice", "Alice:30");
   lfdb::set(dbh, "Bob", "Bob:25");
   lfdb::set(dbh, "Charlie", "Charlie:35");
   lfdb::set(dbh, "Diana", "Diana:28");
   lfdb::set(dbh, "Eve", "Eve:32");

   // Now pair_generator returns BOTH key AND value directly - no extra calls needed!
   std::map<std::string, std::string> range_data;

   for(auto&& [key, value] : lfdb::pair_generator(dbh, lfdb::select{"Bob", "Eve"})) {
     range_data[key] = value;  // Value is directly usable!
   }

   // Verify we got 3 records with their values in ONE range query
   REQUIRE(3 == range_data.size());
   CHECK("Bob:25" == range_data["Bob"]);
   CHECK("Charlie:35" == range_data["Charlie"]);
   CHECK("Diana:28" == range_data["Diana"]);

   // Parse the values if needed
   auto parse_age = [](const std::string& value) {
     auto pos = value.find(':');
     return value.substr(pos + 1);
   };

   CHECK("25" == parse_age(range_data["Bob"]));
   CHECK("35" == parse_age(range_data["Charlie"]));
   CHECK("28" == parse_age(range_data["Diana"]));
 }

 SECTION("complex person records with multiple fields") {
   // For complex data with many fields, structured types (maps) are better than concatenation
   // Example: person with name, age, email, city, phone
   std::vector<std::map<std::string, std::string>> complex_people = {
     {{"name", "Alice"}, {"age", "30"}, {"email", "alice@example.com"}, {"city", "NYC"}, {"phone", "555-0001"}},
     {{"name", "Bob"}, {"age", "25"}, {"email", "bob@example.com"}, {"city", "LA"}, {"phone", "555-0002"}},
     {{"name", "Charlie"}, {"age", "35"}, {"email", "charlie@example.com"}, {"city", "Chicago"}, {"phone", "555-0003"}},
     {{"name", "Diana"}, {"age", "28"}, {"email", "diana@example.com"}, {"city", "Boston"}, {"phone", "555-0004"}},
     {{"name", "Eve"}, {"age", "32"}, {"email", "eve@example.com"}, {"city", "Seattle"}, {"phone", "555-0005"}}
   };

   // Store complex records
   for(const auto& person : complex_people) {
     const auto& name = person.at("name");
     lfdb::set(dbh, name, person);
   }

   // Get range of keys first
   std::vector<std::string> keys_in_range;
   for(auto&& [key, value] : lfdb::pair_generator(dbh, lfdb::select{"Bob", "Eve"})) {
     keys_in_range.push_back(key);
   }

   REQUIRE(3 == keys_in_range.size());

   // Retrieve complex records - requires separate get() calls but provides structured data
   std::map<std::string, std::string> bob_data, charlie_data, diana_data;

   CHECK(lfdb::get(dbh, "Bob", bob_data));
   CHECK("Bob" == bob_data["name"]);
   CHECK("25" == bob_data["age"]);
   CHECK("bob@example.com" == bob_data["email"]);
   CHECK("LA" == bob_data["city"]);
   CHECK("555-0002" == bob_data["phone"]);

   CHECK(lfdb::get(dbh, "Charlie", charlie_data));
   CHECK("35" == charlie_data["age"]);
   CHECK("Chicago" == charlie_data["city"]);

   CHECK(lfdb::get(dbh, "Diana", diana_data));
   CHECK("28" == diana_data["age"]);
   CHECK("Boston" == diana_data["city"]);
 }

 SECTION("comparison: simple vs complex data storage") {
   // Example: Simple string for 2 fields
   lfdb::set(dbh, "simple_alice", "Alice:30");

   // Example: Complex map for 5+ fields
   std::map<std::string, std::string> complex_alice = {
     {"name", "Alice"}, {"age", "30"}, {"email", "alice@example.com"},
     {"city", "NYC"}, {"phone", "555-0001"}
   };
   lfdb::set(dbh, "complex_alice", complex_alice);

   // Retrieve simple: direct from pair_generator
   std::string simple_value;
   for(auto&& [key, value] : lfdb::pair_generator(dbh, lfdb::select{"simple_alice", "simple_alicf"})) {
     simple_value = value;
   }
   CHECK("Alice:30" == simple_value);

   // Retrieve complex: requires get() call
   std::map<std::string, std::string> complex_value;
   CHECK(lfdb::get(dbh, "complex_alice", complex_value));
   CHECK("alice@example.com" == complex_value["email"]);
 }

 SECTION("FUTURE: efficient range query with complex types (requires conversion.h modification)") {
   // ═══════════════════════════════════════════════════════════════════════════
   // This section demonstrates the IDEAL usage pattern once from::convert()
   // supports string_view input (see conversion.h modification proposal below)
   // ═══════════════════════════════════════════════════════════════════════════
   //
   // REQUIRED CHANGE in src/rgw/fdb/conversion.h (around line 112):
   // Add this overload to ceph::libfdb::from namespace:
   //
   //   void convert(std::string_view from, auto& to)
   // Once implemented, this enables ONE database call to get all keys AND values!

   // Store complex person records
   for(const auto& person : people) {
     const auto& name = person.at("name");
     lfdb::set(dbh, name, person);
   }

   /* ═══════════════════════════════════════════════════════════════════════════
      COMMENTED OUT - Requires conversion.h modification to work
      ═══════════════════════════════════════════════════════════════════════════

   // ✅ EFFICIENT: Get ALL keys AND values in ONE range query!
   std::map<std::string, std::map<std::string, std::string>> all_people;

   for(auto&& [key, value] : lfdb::pair_generator(dbh, lfdb::select{"Bob", "Eve"})) {
     std::map<std::string, std::string> person_record;
     
     // This line requires the new from::convert(string_view) overload:
     lfdb::from::convert(value, person_record);  // ← KEY LINE: deserialize in-place!
     
     all_people[key] = person_record;
   }

   // Verify we got 3 records with FULL DATA in ONE database call
   REQUIRE(3 == all_people.size());
   CHECK("Bob" == all_people["Bob"]["name"]);
   CHECK("25" == all_people["Bob"]["age"]);
   CHECK("Charlie" == all_people["Charlie"]["name"]);
   CHECK("35" == all_people["Charlie"]["age"]);
   CHECK("Diana" == all_people["Diana"]["name"]);
   CHECK("28" == all_people["Diana"]["age"]);
   */

   // Current workaround (until conversion.h is modified):
   // Use pair_generator for keys, then get() for each value (N+1 queries)
   std::vector<std::string> keys;
   for(auto&& [key, value] : lfdb::pair_generator(dbh, lfdb::select{"Bob", "Eve"})) {
     keys.push_back(key);
   }

   REQUIRE(3 == keys.size());
   CHECK("Bob" == keys[0]);
   CHECK("Charlie" == keys[1]);
   CHECK("Diana" == keys[2]);
 }
}


SCENARIO("implicit transactions", "[fdb][rgw]")
{
 janitor j;

 auto dbh = j.dbh();

 std::string_view k = "hi", v = "there";

 CAPTURE(k);   
 CAPTURE(v);   

 SECTION("implicitly create and complete transactions") {

  REQUIRE_FALSE(lfdb::key_exists(dbh, k));
  CHECK_NOTHROW(lfdb::set(dbh, k, v));
  CHECK(lfdb::key_exists(dbh, k));

  std::string ov;
  CHECK(lfdb::get(dbh, k, ov));

  CAPTURE(ov);   

  REQUIRE(v == ov);

  CHECK_NOTHROW(lfdb::erase(dbh, k));
  REQUIRE_FALSE(lfdb::key_exists(dbh, k));

  REQUIRE_FALSE(lfdb::get(dbh, k, ov));
 }

 SECTION("implicitly create and complete transactions-- selection operations") {
  // With an implicit transaction, mutating transactions should commit by default:
  const auto selector = lfdb::select { make_key(0), make_key(20) };

  const auto kvs = write_monotonic_kvs(dbh, 20);

  lfdb::erase(dbh, lfdb::select { make_key(1), make_key(6) });

  CHECK(15 == key_count(dbh, selector));

  // Let's look around the edge cases of the selection:   
  CHECK_FALSE(lfdb::key_exists(dbh, make_key(1)));
  CHECK_FALSE(lfdb::key_exists(dbh, make_key(5)));

  CHECK(lfdb::key_exists(dbh, make_key(0)));
  CHECK(lfdb::key_exists(dbh, make_key(6)));
 }

 SECTION("test behavior with shared transaction") {
    SECTION("write in uncommitted transaction") {
      using lfdb::commit_after_op;
    
      auto txn = lfdb::make_transaction(dbh);
    
      lfdb::set(txn, "Herman", "Hollerith", commit_after_op::no_commit);
     
      // Key exists with respect to this transaction: 
      CHECK(lfdb::key_exists(txn, "Herman"));
      
      lfdb::set(txn, "John", "Backus", commit_after_op::no_commit);
    
      // Key exists with respect to this transaction: 
      CHECK(lfdb::key_exists(txn, "John", commit_after_op::no_commit));
    
      // transaction is abandoned
    }

  // These were only set in the abandoned transaction:
  CHECK_FALSE(lfdb::key_exists(dbh, "Herman"));
  CHECK_FALSE(lfdb::key_exists(dbh, "John"));
 }

 SECTION("round trip") {
  janitor j(dbh);

  using namespace ceph::libfdb;
  
  set(dbh, "key_0000", "value");
  std::string out;
  get(dbh, "key_0000", out);
  
  CHECK("value" == out);
 }

 SECTION("round trip with raw string") {
  // The underlying serializer can produce some surprising behavior; libfdb
  // works around this so that the "right" thing to do is what gets done, with
  // performance-maximzation left as an available, but explicit operation.

  janitor j(dbh);

  using namespace ceph::libfdb;
 
  // Notice the raw literal going in here: 
  set(dbh, "key_0000", "value");

  std::string out;
  CHECK_NOTHROW(get(dbh, "key_0000", out));

  CHECK(std::string_view("value") == std::string_view(out));

  // Explicit raw buffers:
  char out_buffer[9] = {}; 
  CHECK_NOTHROW(get(dbh, "key_0000", out_buffer));
  
  CHECK(std::string_view("value") == std::string_view(out));
 }
}

SCENARIO("options", "[fdb]")
{
 // For information about options, consult the FoundationDB's source tree's
 // documentation: fdbclient/vexillographer/fdb.options
 SECTION("option types") {

  // check that the types supported for FDB options are supported by
  // the library:
  lfdb::option_value ov;
  ov = true;                              // flag
  ov = 42;                                // integer
  ov = std::string("hi");                 // string
  ov = std::vector<std::uint8_t>(         // data
        (const std::uint8_t *)pearl_msg, 
        (const std::uint8_t *)(pearl_msg + sizeof(pearl_msg)));
 }

  auto dbh0 = lfdb::create_database(
                { { FDB_DB_OPTION_LOCATION_CACHE_SIZE, 200'000 } },  
                { { FDB_NET_OPTION_TRACE_ENABLE, false } });         

  auto dbh1 = lfdb::create_database("fishing for databass!",       // name
               { { FDB_DB_OPTION_LOCATION_CACHE_SIZE, 200'000 } }, // database options
               { { FDB_NET_OPTION_TRACE_ENABLE, false } });        // network options
 
  auto txn = lfdb::make_transaction(dbh0, 
               { { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, true } });

 SECTION("create_database()") {
  lfdb::create_database();
  lfdb::create_database("");
  lfdb::create_database("", {}, {});
  lfdb::create_database(lfdb::database_options {}, lfdb::network_options {});
 }

 SECTION("piecemeal construction") {
  lfdb::network_options netopts;

  // Note that, according to FDB's documentation, this setting's actually deprecated:
  netopts[FDB_NET_OPTION_LOCAL_ADDRESS] = "127.0.0.1:2323"; 

  // The cluster file is in "/etc/foundationdb.fdb.cluster" normally, but we'll point to 
  // nowhere just for fun. The cluster file is the "approved" way to establish a list of
  // addressess, AFAIK, rather than setting the option:
  lfdb::create_database("/dev/null", {}, netopts);
 }
}

TEST_CASE("mini-demo", "[fdb]") {
 janitor j;

 using std::map;
 using std::string;

 map<string, string> bucket_entries = {
    { "objName", "obj" },
    { "bucketName", "bucket" },
    { "creationTime", "2025-11-12T10:00:00" },
    { "dirty", "0" },
    { "hosts", "192.168.1.1:8000_192.168.1.2:8000" },
    { "etag", "abc123def" },
    { "objSize", "1048576" },
    { "userId", "user123" },
    { "displayName", "John Doe" }
  };
  
 auto dbh = j.dbh();

 lfdb::set(dbh, "bucket_obj", bucket_entries);

 map<string, string> out;
 lfdb::get(dbh, "bucket_obj", out);

 CAPTURE(out["userId"]);
 REQUIRE(bucket_entries == out);

 j.drop_all();
}

// Adapted from Catch2 documentation:
#include <catch2/catch_session.hpp>

int main(int argc, char **argv) 
{
  int result = Catch::Session().run(argc, argv);

  // Make sure that FoundationDB is shut down once and only once:
  ceph::libfdb::shutdown_libfdb(); 

  return result;
}
