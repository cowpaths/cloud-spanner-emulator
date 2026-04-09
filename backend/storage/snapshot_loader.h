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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_SNAPSHOT_LOADER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_SNAPSHOT_LOADER_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "frontend/server/environment.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// SnapshotLoader restores emulator state from a snapshot file on disk.
//
// It reads an EmulatorSnapshot proto, recreates all instances and databases
// (by replaying DDL), and populates storage with the persisted data.
// Returns the snapshot timestamp so the caller can advance the clock past it.
class SnapshotLoader {
 public:
  // Load a snapshot and populate the ServerEnv with the restored state.
  // Returns the snapshot timestamp (for clock advancement).
  static absl::StatusOr<absl::Time> LoadSnapshot(
      const std::string& snapshot_path,
      frontend::ServerEnv* env);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_SNAPSHOT_LOADER_H_
