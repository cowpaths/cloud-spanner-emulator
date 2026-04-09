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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_SERVER_PERSISTENCE_MANAGER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_SERVER_PERSISTENCE_MANAGER_H_

#include <memory>
#include <string>
#include <thread>  // NOLINT

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/storage/persistence.pb.h"
#include "backend/storage/wal_writer.h"
#include "frontend/server/environment.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// PersistenceManager coordinates persistence lifecycle for the emulator.
// It manages loading state on startup, WAL writing during operation,
// and snapshot creation on shutdown.
class PersistenceManager {
 public:
  // Creates a PersistenceManager for the given data directory.
  // Returns nullptr if data_dir is empty (persistence disabled).
  static std::unique_ptr<PersistenceManager> Create(
      const std::string& data_dir);

  // Restore state from disk into the given ServerEnv.
  // Must be called before the gRPC server starts accepting requests.
  // Loads snapshot + replays WAL.
  absl::Status RestoreState(ServerEnv* env);

  // Save state to disk. Called on graceful shutdown.
  // Writes a snapshot and clears the WAL.
  absl::Status SaveState(ServerEnv* env);

  // Starts periodic background snapshots at the given interval.
  // Does nothing if interval is zero or negative.
  void StartPeriodicSnapshots(ServerEnv* env, absl::Duration interval);

  // Stops the background snapshot thread. Called before shutdown.
  void StopPeriodicSnapshots();

  // Returns the WAL writer (used by Database creation to wrap storage).
  std::shared_ptr<backend::WalWriter> wal_writer() { return wal_writer_; }

  const std::string& data_dir() const { return data_dir_; }
  std::string snapshot_path() const;
  std::string wal_directory() const;

 private:
  explicit PersistenceManager(const std::string& data_dir);

  // WAL replay helpers for each record type.
  absl::Status ReplayMetadataChange(
      const backend::WalMetadataChange& change, ServerEnv* env);
  absl::Status ReplaySchemaChange(
      const backend::WalSchemaChange& change, ServerEnv* env);
  absl::Status ReplayEntry(
      const backend::WalEntry& entry, ServerEnv* env);

  void SnapshotLoop(ServerEnv* env, absl::Duration interval);

  std::string data_dir_;
  std::shared_ptr<backend::WalWriter> wal_writer_;

  // Background snapshot thread state.
  absl::Mutex snapshot_mu_;
  bool snapshot_stop_ ABSL_GUARDED_BY(snapshot_mu_) = false;
  std::thread snapshot_thread_;
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_SERVER_PERSISTENCE_MANAGER_H_
