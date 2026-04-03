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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_SNAPSHOT_WRITER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_SNAPSHOT_WRITER_H_

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/storage/persistence.pb.h"
#include "frontend/collections/database_manager.h"
#include "frontend/collections/instance_manager.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Database;

// SnapshotWriter writes a full snapshot of the emulator state to disk.
//
// The snapshot captures all instances, databases (schema + data), and is
// written atomically (write to .tmp, then rename). On restore, the DDL is
// replayed to recreate the schema, and data is written back into storage.
//
// Limitations:
// - Only the latest version of each row is captured (version history is lost).
//   This is acceptable for an emulator snapshot -- on restore all data appears
//   at a single timestamp.
// - The backend::Database does not expose storage() or database_id() publicly,
//   so we read data through a ReadOnlyTransaction instead.
class SnapshotWriter {
 public:
  // Write a full snapshot of the emulator state to the given path.
  // Serializes all instances, databases (schema + data) to an
  // EmulatorSnapshot proto and writes it atomically.
  static absl::Status WriteSnapshot(
      const std::string& snapshot_path,
      frontend::InstanceManager* instance_manager,
      frontend::DatabaseManager* database_manager);

 private:
  // Serialize one database's storage contents by reading all tables
  // through a ReadOnlyTransaction at the given timestamp.
  static absl::StatusOr<PersistedStorage> SerializeStorage(
      backend::Database* database, absl::Time read_timestamp);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_SNAPSHOT_WRITER_H_
