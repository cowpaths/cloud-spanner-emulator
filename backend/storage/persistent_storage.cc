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

#include "backend/storage/persistent_storage.h"

#include <memory>
#include <string>
#include <vector>

#include "googlesql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_graph.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/storage/in_memory_storage.h"
#include "backend/storage/iterator.h"
#include "backend/storage/persistence.pb.h"
#include "backend/storage/value_serializer.h"
#include "backend/storage/wal_writer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Finds the table with the given id anywhere in `schema`, including
// internal tables (index backing tables, change stream data/partition
// tables) that `Schema::tables()`/`FindTable()` deliberately exclude since
// they're not user-visible. WAL writes happen for these tables too (e.g.
// index maintenance, change stream production), so name resolution must
// cover them or their WAL entries silently stay id-only forever, even on a
// build with this fix. Schema::GetSchemaGraph() exposes every schema node,
// which is how Schema itself builds its public-only tables_ list.
const Table* FindAnyTableById(const Schema* schema, const TableID& table_id) {
  for (const SchemaNode* node : schema->GetSchemaGraph()->GetSchemaNodes()) {
    const Table* table = node->As<const Table>();
    if (table != nullptr && table->id() == table_id) {
      return table;
    }
  }
  return nullptr;
}

// Best-effort resolution of table_id/column_ids to their current names in
// `schema`. Ids are reallocated whenever the schema is replayed from DDL, so
// names captured here at write time are what later replay uses to reconcile
// against a schema whose ids may have shifted. Returns false (leaving the
// out params untouched) if the schema or any id can't be resolved -- the
// caller then just omits the name fields, and the record behaves as a
// legacy (id-only) entry on replay.
bool TryResolveNames(const Schema* schema, const TableID& table_id,
                     const std::vector<ColumnID>& column_ids,
                     std::string* table_name,
                     std::vector<std::string>* column_names) {
  if (schema == nullptr) return false;
  const Table* table = FindAnyTableById(schema, table_id);
  if (table == nullptr) return false;

  std::vector<std::string> names;
  names.reserve(column_ids.size());
  for (const auto& col_id : column_ids) {
    const Column* found = nullptr;
    for (const auto* col : table->columns()) {
      if (col->id() == col_id) {
        found = col;
        break;
      }
    }
    if (found == nullptr) return false;
    names.push_back(found->Name());
  }

  *table_name = table->Name();
  *column_names = std::move(names);
  return true;
}

// Best-effort resolution of just a table's current name, for deletes.
bool TryResolveTableName(const Schema* schema, const TableID& table_id,
                         std::string* table_name) {
  if (schema == nullptr) return false;
  const Table* table = FindAnyTableById(schema, table_id);
  if (table == nullptr) return false;
  *table_name = table->Name();
  return true;
}

}  // namespace

PersistentStorage::PersistentStorage(const std::string& database_uri,
                                     std::unique_ptr<InMemoryStorage> inner,
                                     std::shared_ptr<WalWriter> wal_writer)
    : database_uri_(database_uri),
      inner_(std::move(inner)),
      wal_writer_(std::move(wal_writer)) {}

absl::StatusOr<std::unique_ptr<PersistentStorage>> PersistentStorage::Create(
    const std::string& database_uri, std::shared_ptr<WalWriter> wal_writer) {
  auto inner = std::make_unique<InMemoryStorage>();
  return std::unique_ptr<PersistentStorage>(
      new PersistentStorage(database_uri, std::move(inner),
                            std::move(wal_writer)));
}

std::unique_ptr<PersistentStorage> PersistentStorage::Wrap(
    const std::string& database_uri,
    std::unique_ptr<InMemoryStorage> inner,
    std::shared_ptr<WalWriter> wal_writer) {
  return std::unique_ptr<PersistentStorage>(
      new PersistentStorage(database_uri, std::move(inner),
                            std::move(wal_writer)));
}

absl::Status PersistentStorage::Lookup(
    absl::Time timestamp, const TableID& table_id, const Key& key,
    const std::vector<ColumnID>& column_ids,
    std::vector<googlesql::Value>* values) const {
  return inner_->Lookup(timestamp, table_id, key, column_ids, values);
}

absl::Status PersistentStorage::Read(
    absl::Time timestamp, const TableID& table_id,
    const KeyRange& key_range, const std::vector<ColumnID>& column_ids,
    std::unique_ptr<StorageIterator>* itr) const {
  return inner_->Read(timestamp, table_id, key_range, column_ids, itr);
}

absl::Status PersistentStorage::Write(
    absl::Time timestamp, const TableID& table_id, const Key& key,
    const std::vector<ColumnID>& column_ids,
    const std::vector<googlesql::Value>& values) {
  // Build the WAL record.
  WalRecord wal_record;
  WalEntry* entry = wal_record.mutable_entry();
  entry->set_commit_timestamp_micros(
      absl::ToUnixMicros(timestamp));
  entry->set_database_uri(database_uri_);

  WalMutation* mutation = entry->add_mutations();
  WalWrite* write = mutation->mutable_write();
  write->set_table_id(table_id);
  *write->mutable_key() = SerializeKey(key);

  for (const auto& col_id : column_ids) {
    write->add_column_ids(col_id);
  }

  if (schema_accessor_) {
    std::string table_name;
    std::vector<std::string> column_names;
    if (TryResolveNames(schema_accessor_(), table_id, column_ids,
                        &table_name, &column_names)) {
      write->set_table_name(table_name);
      for (const auto& name : column_names) {
        write->add_column_names(name);
      }
    }
  }

  for (const auto& value : values) {
    auto serialized_or = SerializeValue(value);
    if (!serialized_or.ok()) return serialized_or.status();
    *write->add_values() = std::move(serialized_or.value());
  }

  // Write to WAL first (write-ahead).
  absl::Status wal_status = wal_writer_->Append(wal_record);
  if (!wal_status.ok()) return wal_status;

  // Then apply to in-memory storage.
  return inner_->Write(timestamp, table_id, key, column_ids, values);
}

absl::Status PersistentStorage::Delete(absl::Time timestamp,
                                       const TableID& table_id,
                                       const KeyRange& key_range) {
  // Build the WAL record.
  WalRecord wal_record;
  WalEntry* entry = wal_record.mutable_entry();
  entry->set_commit_timestamp_micros(
      absl::ToUnixMicros(timestamp));
  entry->set_database_uri(database_uri_);

  WalMutation* mutation = entry->add_mutations();
  WalDelete* delete_op = mutation->mutable_delete_op();
  delete_op->set_table_id(table_id);
  *delete_op->mutable_start_key() = SerializeKey(key_range.start_key());
  *delete_op->mutable_end_key() = SerializeKey(key_range.limit_key());

  if (schema_accessor_) {
    std::string table_name;
    if (TryResolveTableName(schema_accessor_(), table_id, &table_name)) {
      delete_op->set_table_name(table_name);
    }
  }

  // Write to WAL first (write-ahead).
  absl::Status wal_status = wal_writer_->Append(wal_record);
  if (!wal_status.ok()) return wal_status;

  // Then apply to in-memory storage.
  return inner_->Delete(timestamp, table_id, key_range);
}

void PersistentStorage::SetVersionRetentionPeriod(
    absl::Duration version_retention_period) {
  inner_->SetVersionRetentionPeriod(version_retention_period);
}

void PersistentStorage::CleanUpDeletedTables(absl::Time timestamp) {
  inner_->CleanUpDeletedTables(timestamp);
}

void PersistentStorage::CleanUpDeletedColumns(absl::Time timestamp) {
  inner_->CleanUpDeletedColumns(timestamp);
}

void PersistentStorage::MarkDroppedTable(absl::Time timestamp,
                                         TableID dropped_table_id) {
  inner_->MarkDroppedTable(timestamp, dropped_table_id);
}

void PersistentStorage::MarkDroppedColumn(absl::Time timestamp,
                                          TableID dropped_table_id,
                                          ColumnID dropped_column_id) {
  inner_->MarkDroppedColumn(timestamp, dropped_table_id, dropped_column_id);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
