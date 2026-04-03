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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_PERSISTENT_STORAGE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_PERSISTENT_STORAGE_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/storage/in_memory_storage.h"
#include "backend/storage/iterator.h"
#include "backend/storage/storage.h"
#include "backend/storage/wal_writer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// PersistentStorage wraps InMemoryStorage with write-ahead logging.
//
// All reads are served from the in-memory layer (fast).
// All writes are logged to WAL before being applied to memory, ensuring
// durability across emulator restarts.
//
// This class is thread-safe (inherits thread-safety from InMemoryStorage
// and WalWriter).
class PersistentStorage : public Storage {
 public:
  // Create a PersistentStorage wrapping a new InMemoryStorage with WAL.
  static absl::StatusOr<std::unique_ptr<PersistentStorage>> Create(
      const std::string& database_uri, std::shared_ptr<WalWriter> wal_writer);

  // Access the underlying in-memory storage (for snapshot serialization).
  InMemoryStorage* inner() { return inner_.get(); }

  // Storage interface - reads delegate to inner_.
  absl::Status Lookup(absl::Time timestamp, const TableID& table_id,
                      const Key& key, const std::vector<ColumnID>& column_ids,
                      std::vector<zetasql::Value>* values) const override;

  absl::Status Read(absl::Time timestamp, const TableID& table_id,
                    const KeyRange& key_range,
                    const std::vector<ColumnID>& column_ids,
                    std::unique_ptr<StorageIterator>* itr) const override;

  // Storage interface - writes log to WAL then delegate to inner_.
  absl::Status Write(absl::Time timestamp, const TableID& table_id,
                     const Key& key, const std::vector<ColumnID>& column_ids,
                     const std::vector<zetasql::Value>& values) override;

  absl::Status Delete(absl::Time timestamp, const TableID& table_id,
                      const KeyRange& key_range) override;

  // Storage interface - metadata operations delegate to inner_.
  void SetVersionRetentionPeriod(
      absl::Duration version_retention_period) override;

  void CleanUpDeletedTables(absl::Time timestamp) override;
  void CleanUpDeletedColumns(absl::Time timestamp) override;

  void MarkDroppedTable(absl::Time timestamp,
                        TableID dropped_table_id) override;

  void MarkDroppedColumn(absl::Time timestamp, TableID dropped_table_id,
                         ColumnID dropped_column_id) override;

 private:
  PersistentStorage(const std::string& database_uri,
                    std::unique_ptr<InMemoryStorage> inner,
                    std::shared_ptr<WalWriter> wal_writer);

  std::string database_uri_;
  std::unique_ptr<InMemoryStorage> inner_;
  std::shared_ptr<WalWriter> wal_writer_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_PERSISTENT_STORAGE_H_
