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

#include <string>

#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/public/value.pb.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/datamodel/key.h"
#include "backend/storage/persistence.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::StatusOr<PersistedValue> SerializeValue(const zetasql::Value& value) {
  PersistedValue result;

  // An invalid (default-constructed) Value is represented as empty fields.
  if (!value.is_valid()) {
    return result;
  }

  // Serialize the type to a self-contained TypeProto.
  zetasql::TypeProto type_proto;
  absl::Status type_status =
      value.type()->SerializeToSelfContainedProto(&type_proto);
  if (!type_status.ok()) {
    return type_status;
  }
  result.set_type_proto(type_proto.SerializeAsString());

  // Serialize the value to a ValueProto.
  zetasql::ValueProto value_proto;
  absl::Status value_status = value.Serialize(&value_proto);
  if (!value_status.ok()) {
    return value_status;
  }
  result.set_value_proto(value_proto.SerializeAsString());

  return result;
}

absl::StatusOr<zetasql::Value> DeserializeValue(
    const PersistedValue& proto, zetasql::TypeFactory* type_factory) {
  // Empty fields represent an invalid (default-constructed) Value.
  if (proto.type_proto().empty() && proto.value_proto().empty()) {
    return zetasql::Value();
  }

  // Deserialize the type.
  zetasql::TypeProto type_proto;
  if (!type_proto.ParseFromString(proto.type_proto())) {
    return absl::InternalError("Failed to parse TypeProto from bytes");
  }

  const zetasql::Type* type = nullptr;
  absl::Status type_status =
      type_factory->DeserializeFromSelfContainedProto(type_proto, &type);
  if (!type_status.ok()) {
    return type_status;
  }

  // Deserialize the value.
  zetasql::ValueProto value_proto;
  if (!value_proto.ParseFromString(proto.value_proto())) {
    return absl::InternalError("Failed to parse ValueProto from bytes");
  }

  return zetasql::Value::Deserialize(value_proto, type);
}

PersistedKey SerializeKey(const Key& key) {
  PersistedKey result;

  // Check for special key values.
  if (key.IsInfinity()) {
    result.set_is_infinity(true);
    return result;
  }

  for (int i = 0; i < key.NumColumns(); ++i) {
    PersistedKeyColumn* col = result.add_columns();

    // Serialize the column value. Since SerializeValue can fail, we handle
    // the error by storing an empty PersistedValue for invalid values.
    auto value_or = SerializeValue(key.ColumnValue(i));
    if (value_or.ok()) {
      *col->mutable_value() = *std::move(value_or);
    }

    col->set_is_descending(key.IsColumnDescending(i));
    col->set_is_nulls_last(key.IsColumnNullsLast(i));
  }

  result.set_is_prefix_limit(key.IsPrefixLimit());

  return result;
}

absl::StatusOr<Key> DeserializeKey(const PersistedKey& proto,
                                   zetasql::TypeFactory* type_factory) {
  // Handle special key values.
  if (proto.is_infinity()) {
    return Key::Infinity();
  }

  Key key;
  for (const auto& col : proto.columns()) {
    auto value_or = DeserializeValue(col.value(), type_factory);
    if (!value_or.ok()) {
      return value_or.status();
    }
    key.AddColumn(*std::move(value_or), col.is_descending(),
                  col.is_nulls_last());
  }

  if (proto.is_prefix_limit()) {
    key = key.ToPrefixLimit();
  }

  return key;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
