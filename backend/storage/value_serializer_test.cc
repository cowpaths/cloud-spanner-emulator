//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "backend/storage/value_serializer.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "googlesql/public/type.h"
#include "googlesql/public/value.h"
#include "backend/datamodel/key.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using googlesql::values::Bool;
using googlesql::values::Bytes;
using googlesql::values::Double;
using googlesql::values::Int64;
using googlesql::values::String;

class ValueSerializerTest : public testing::Test {
 protected:
  googlesql::TypeFactory type_factory_;
};

TEST_F(ValueSerializerTest, RoundTripInt64) {
  googlesql::Value original = Int64(42);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(PersistedValue serialized,
                        SerializeValue(original));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value deserialized,
                        DeserializeValue(serialized, &type_factory_));
  EXPECT_EQ(deserialized, original);
}

TEST_F(ValueSerializerTest, RoundTripString) {
  googlesql::Value original = String("hello world");
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(PersistedValue serialized,
                        SerializeValue(original));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value deserialized,
                        DeserializeValue(serialized, &type_factory_));
  EXPECT_EQ(deserialized, original);
}

TEST_F(ValueSerializerTest, RoundTripBool) {
  googlesql::Value original = Bool(true);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(PersistedValue serialized,
                        SerializeValue(original));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value deserialized,
                        DeserializeValue(serialized, &type_factory_));
  EXPECT_EQ(deserialized, original);
}

TEST_F(ValueSerializerTest, RoundTripDouble) {
  googlesql::Value original = Double(3.14159);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(PersistedValue serialized,
                        SerializeValue(original));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value deserialized,
                        DeserializeValue(serialized, &type_factory_));
  EXPECT_EQ(deserialized, original);
}

TEST_F(ValueSerializerTest, RoundTripBytes) {
  std::string binary_data = std::string("binary\x00\x01\x02", 9) + "data";
  googlesql::Value original = Bytes(binary_data);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(PersistedValue serialized,
                        SerializeValue(original));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value deserialized,
                        DeserializeValue(serialized, &type_factory_));
  EXPECT_EQ(deserialized, original);
}

TEST_F(ValueSerializerTest, RoundTripInvalidValue) {
  googlesql::Value original;  // Default-constructed, invalid.
  EXPECT_FALSE(original.is_valid());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(PersistedValue serialized,
                        SerializeValue(original));
  // Empty fields represent an invalid value.
  EXPECT_TRUE(serialized.type_proto().empty());
  EXPECT_TRUE(serialized.value_proto().empty());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value deserialized,
                        DeserializeValue(serialized, &type_factory_));
  EXPECT_FALSE(deserialized.is_valid());
}

TEST_F(ValueSerializerTest, RoundTripNullInt64) {
  googlesql::Value original = googlesql::values::NullInt64();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(PersistedValue serialized,
                        SerializeValue(original));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value deserialized,
                        DeserializeValue(serialized, &type_factory_));
  EXPECT_EQ(deserialized, original);
  EXPECT_TRUE(deserialized.is_null());
}

// Key serialization tests.

TEST_F(ValueSerializerTest, RoundTripSimpleKey) {
  Key original({Int64(100)});
  PersistedKey serialized = SerializeKey(original);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Key deserialized,
                        DeserializeKey(serialized, &type_factory_));
  EXPECT_EQ(deserialized.NumColumns(), 1);
  EXPECT_EQ(deserialized.ColumnValue(0), Int64(100));
  EXPECT_FALSE(deserialized.IsColumnDescending(0));
}

TEST_F(ValueSerializerTest, RoundTripMultiColumnKey) {
  Key original;
  original.AddColumn(Int64(1), /*desc=*/true, /*is_nulls_last=*/true);
  original.AddColumn(String("abc"), /*desc=*/false, /*is_nulls_last=*/false);

  PersistedKey serialized = SerializeKey(original);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Key deserialized,
                        DeserializeKey(serialized, &type_factory_));

  EXPECT_EQ(deserialized.NumColumns(), 2);
  EXPECT_EQ(deserialized.ColumnValue(0), Int64(1));
  EXPECT_TRUE(deserialized.IsColumnDescending(0));
  EXPECT_TRUE(deserialized.IsColumnNullsLast(0));
  EXPECT_EQ(deserialized.ColumnValue(1), String("abc"));
  EXPECT_FALSE(deserialized.IsColumnDescending(1));
  EXPECT_FALSE(deserialized.IsColumnNullsLast(1));
}

TEST_F(ValueSerializerTest, RoundTripEmptyKey) {
  Key original;
  EXPECT_TRUE(original.IsEmpty());

  PersistedKey serialized = SerializeKey(original);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Key deserialized,
                        DeserializeKey(serialized, &type_factory_));
  EXPECT_TRUE(deserialized.IsEmpty());
  EXPECT_EQ(deserialized.NumColumns(), 0);
}

TEST_F(ValueSerializerTest, RoundTripInfinityKey) {
  Key original = Key::Infinity();
  EXPECT_TRUE(original.IsInfinity());

  PersistedKey serialized = SerializeKey(original);
  EXPECT_TRUE(serialized.is_infinity());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Key deserialized,
                        DeserializeKey(serialized, &type_factory_));
  EXPECT_TRUE(deserialized.IsInfinity());
}

TEST_F(ValueSerializerTest, RoundTripPrefixLimitKey) {
  Key base({Int64(42)});
  Key original = base.ToPrefixLimit();
  EXPECT_TRUE(original.IsPrefixLimit());

  PersistedKey serialized = SerializeKey(original);
  EXPECT_TRUE(serialized.is_prefix_limit());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(Key deserialized,
                        DeserializeKey(serialized, &type_factory_));
  EXPECT_TRUE(deserialized.IsPrefixLimit());
  EXPECT_EQ(deserialized.NumColumns(), 1);
  EXPECT_EQ(deserialized.ColumnValue(0), Int64(42));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
