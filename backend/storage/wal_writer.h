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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_WAL_WRITER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_WAL_WRITER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "backend/storage/persistence.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// WAL writer for the Cloud Spanner Emulator persistence layer.
//
// Appends serialized WalRecord protos to log files in a configured directory.
// Each record is written as:
//   [4-byte little-endian length][serialized WalRecord proto][4-byte CRC32C]
//
// WAL files are named wal-NNNNNN.log with zero-padded segment numbers.
//
// Thread-safe.
class WalWriter {
 public:
  // Creates a WalWriter that writes to the given directory.
  // Creates the directory if it doesn't exist.
  // Scans for existing WAL files to resume sequence numbering.
  static absl::StatusOr<std::unique_ptr<WalWriter>> Create(
      const std::string& wal_directory);

  ~WalWriter();

  // Append a WAL record. Thread-safe. Sets the sequence number on the
  // record's entry (if it has one) before writing.
  absl::Status Append(const WalRecord& record);

  // Force flush pending writes to disk.
  absl::Status Sync();

  // Rotate to a new WAL segment file. Returns the path of the old file.
  absl::StatusOr<std::string> Rotate();

  // Returns the current sequence number.
  int64_t current_sequence_number() const;

  // Read all WAL records from the directory, in order.
  // Static method - can be used before creating a WalWriter.
  static absl::StatusOr<std::vector<WalRecord>> ReadAll(
      const std::string& wal_directory);

  // Delete all WAL files in the directory (after snapshot).
  static absl::Status Clear(const std::string& wal_directory);

 private:
  explicit WalWriter(const std::string& wal_directory);
  absl::Status OpenNewSegment() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  std::string CurrentSegmentPath() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Scan existing WAL files to determine the next segment and sequence numbers.
  absl::Status ScanExistingFiles() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // List WAL files in the directory, sorted by name.
  static absl::StatusOr<std::vector<std::string>> ListWalFiles(
      const std::string& wal_directory);

  // Read records from a single WAL file.
  static absl::StatusOr<std::vector<WalRecord>> ReadFile(
      const std::string& path);

  std::string wal_directory_;
  int64_t next_sequence_number_ ABSL_GUARDED_BY(mu_) = 0;
  int segment_number_ ABSL_GUARDED_BY(mu_) = 0;
  int fd_ ABSL_GUARDED_BY(mu_) = -1;
  mutable absl::Mutex mu_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_WAL_WRITER_H_
