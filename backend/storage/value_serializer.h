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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_VALUE_SERIALIZER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_VALUE_SERIALIZER_H_

#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/public/value.pb.h"
#include "absl/status/statusor.h"
#include "backend/datamodel/key.h"
#include "backend/storage/persistence.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Serializes a zetasql::Value to a PersistedValue proto containing both the
// value and its type information. Returns an error if serialization fails.
// An invalid (default-constructed) Value is serialized as a PersistedValue with
// empty fields.
absl::StatusOr<PersistedValue> SerializeValue(const zetasql::Value& value);

// Deserializes a zetasql::Value from a PersistedValue proto. Requires a
// TypeFactory for reconstructing the type. Returns an invalid Value if the
// proto has empty fields.
absl::StatusOr<zetasql::Value> DeserializeValue(
    const PersistedValue& proto, zetasql::TypeFactory* type_factory);

// Serializes a Key to a PersistedKey proto.
PersistedKey SerializeKey(const Key& key);

// Deserializes a Key from a PersistedKey proto. Requires a TypeFactory for
// reconstructing column values.
absl::StatusOr<Key> DeserializeKey(const PersistedKey& proto,
                                   zetasql::TypeFactory* type_factory);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_VALUE_SERIALIZER_H_
