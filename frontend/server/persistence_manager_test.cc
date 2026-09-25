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

#include "frontend/server/persistence_manager.h"

#include <ftw.h>

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/storage/persistence.pb.h"
#include "backend/storage/snapshot_loader.h"
#include "backend/storage/snapshot_writer.h"
#include "backend/storage/wal_writer.h"
#include "backend/transaction/options.h"
#include "frontend/server/environment.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

namespace instance_api = ::google::spanner::admin::instance::v1;
namespace database_api = ::google::spanner::admin::database::v1;

const char kInstanceUri[] =
    "projects/test-project/instances/test-instance";
const char kDatabaseUri[] =
    "projects/test-project/instances/test-instance/databases/test-db";

// Helper to create an instance proto.
instance_api::Instance MakeInstanceProto() {
  instance_api::Instance proto;
  proto.set_name(kInstanceUri);
  proto.set_config("emulator-config");
  proto.set_display_name("Test Instance");
  proto.set_processing_units(1000);
  return proto;
}

// Helper to create a schema with a simple table.
std::vector<std::string> SimpleSchema() {
  return {
      R"(CREATE TABLE TestTable (
           key INT64 NOT NULL,
           value STRING(MAX)
         ) PRIMARY KEY(key))"};
}

// Helper to create an instance and database with data in a ServerEnv.
// Returns the commit timestamp of the data write.
absl::Status SetUpInstanceAndDatabase(ServerEnv* env,
                                      std::shared_ptr<backend::WalWriter>
                                          wal_writer = nullptr) {
  // Create instance.
  GOOGLESQL_RETURN_IF_ERROR(
      env->instance_manager()
          ->CreateInstance(kInstanceUri, MakeInstanceProto())
          .status());

  // Create database with schema.
  backend::SchemaChangeOperation schema_op;
  auto ddl = SimpleSchema();
  schema_op.statements = ddl;
  schema_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;

  GOOGLESQL_ASSIGN_OR_RETURN(
      auto database,
      env->database_manager()->CreateDatabase(kDatabaseUri, schema_op,
                                              wal_writer));

  // Insert data via a read-write transaction.
  backend::ReadWriteOptions rw_options;
  backend::RetryState retry_state;
  GOOGLESQL_ASSIGN_OR_RETURN(
      auto txn,
      database->backend()->CreateReadWriteTransaction(rw_options, retry_state));

  backend::Mutation mutation;
  std::vector<std::string> columns = {"key", "value"};
  std::vector<backend::ValueList> rows;
  rows.push_back({googlesql::values::Int64(1),
                  googlesql::values::String("hello")});
  rows.push_back({googlesql::values::Int64(2),
                  googlesql::values::String("world")});
  rows.push_back({googlesql::values::Int64(3),
                  googlesql::values::String("foo")});
  mutation.AddWriteOp(backend::MutationOpType::kInsert, "TestTable",
                      std::move(columns), std::move(rows));
  GOOGLESQL_RETURN_IF_ERROR(txn->Write(mutation));
  GOOGLESQL_RETURN_IF_ERROR(txn->Commit());

  return absl::OkStatus();
}

// Helper to read all rows from TestTable in the given database.
// Returns a map from key (int64) to value (string).
absl::StatusOr<std::map<int64_t, std::string>> ReadAllRows(
    backend::Database* db) {
  backend::ReadOnlyOptions ro_options;
  ro_options.bound = backend::TimestampBound::kStrongRead;
  GOOGLESQL_ASSIGN_OR_RETURN(auto txn, db->CreateReadOnlyTransaction(ro_options));

  backend::ReadArg read_arg;
  read_arg.table = "TestTable";
  read_arg.key_set = backend::KeySet::All();
  read_arg.columns = {"key", "value"};

  std::unique_ptr<backend::RowCursor> cursor;
  GOOGLESQL_RETURN_IF_ERROR(txn->Read(read_arg, &cursor));

  std::map<int64_t, std::string> result;
  while (cursor->Next()) {
    int64_t key = cursor->ColumnValue(0).int64_value();
    std::string value = cursor->ColumnValue(1).string_value();
    result[key] = value;
  }
  GOOGLESQL_RETURN_IF_ERROR(cursor->Status());
  return result;
}

// Generic helper for the ALTER TABLE / round-trip regression tests below:
// reads `table`, whose first column must be an INT64 primary key named
// "id", and returns a map from id to the remaining columns' string values
// ("<NULL>" standing in for a SQL NULL, so it round-trips through EXPECT_EQ
// on a plain std::string).
absl::StatusOr<std::map<int64_t, std::vector<std::string>>> ReadTable(
    backend::Database* db, const std::string& table,
    const std::vector<std::string>& columns) {
  backend::ReadOnlyOptions ro_options;
  ro_options.bound = backend::TimestampBound::kStrongRead;
  GOOGLESQL_ASSIGN_OR_RETURN(auto txn, db->CreateReadOnlyTransaction(ro_options));

  backend::ReadArg read_arg;
  read_arg.table = table;
  read_arg.key_set = backend::KeySet::All();
  read_arg.columns = columns;

  std::unique_ptr<backend::RowCursor> cursor;
  GOOGLESQL_RETURN_IF_ERROR(txn->Read(read_arg, &cursor));

  std::map<int64_t, std::vector<std::string>> result;
  while (cursor->Next()) {
    int64_t id = cursor->ColumnValue(0).int64_value();
    std::vector<std::string> values;
    for (int i = 1; i < static_cast<int>(columns.size()); ++i) {
      auto value = cursor->ColumnValue(i);
      values.push_back(value.is_null() ? "<NULL>" : value.string_value());
    }
    result[id] = std::move(values);
  }
  GOOGLESQL_RETURN_IF_ERROR(cursor->Status());
  return result;
}

// Rewrites the snapshot at `snapshot_path` to clear every PersistedTable's
// table_name/column_names, simulating a snapshot written by a binary that
// predates that field (e.g. fs.1) -- the loader's legacy id-based fallback
// then has to recover names from the raw table/column ids instead.
void StripPersistedNamesForLegacyUpgradeTest(const std::string& snapshot_path) {
  backend::EmulatorSnapshot snapshot;
  {
    std::ifstream in(snapshot_path, std::ios::binary);
    ASSERT_TRUE(snapshot.ParseFromIstream(&in));
  }
  for (auto& db : *snapshot.mutable_databases()) {
    for (auto& table : *db.mutable_storage()->mutable_tables()) {
      table.clear_table_name();
      table.clear_column_names();
    }
  }
  std::ofstream out(snapshot_path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(snapshot.SerializeToOstream(&out));
}

class PersistenceManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = testing::TempDir() + "/persistence_manager_test";
    mkdir(test_dir_.c_str(), 0755);
  }

  void TearDown() override {
    // Clean up test directory recursively.
    nftw(test_dir_.c_str(),
         [](const char* path, const struct stat*, int, struct FTW*) {
           return remove(path);
         },
         64, FTW_DEPTH | FTW_PHYS);
  }

  std::string test_dir_;
};

// ---------------------------------------------------------------------------
// Test 1: Snapshot round-trip
//
// Create instance + database + data → write snapshot → load into fresh env →
// verify schema and data match.
// ---------------------------------------------------------------------------
TEST_F(PersistenceManagerTest, SnapshotRoundTrip) {
  std::string snapshot_path = test_dir_ + "/snapshot.pb";

  // Set up source env with data.
  auto src_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(SetUpInstanceAndDatabase(src_env.get()));

  // Verify data was written.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto src_db,
      src_env->database_manager()->GetDatabase(kDatabaseUri));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto src_rows, ReadAllRows(src_db->backend()));
  ASSERT_EQ(src_rows.size(), 3);

  // Write snapshot.
  GOOGLESQL_ASSERT_OK(backend::SnapshotWriter::WriteSnapshot(
      snapshot_path, src_env->instance_manager(),
      src_env->database_manager()));

  // Load snapshot into a fresh env.
  auto dst_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      backend::SnapshotLoader::LoadSnapshot(snapshot_path, dst_env.get())
          .status());

  // Verify instance was restored.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restored_instance,
      dst_env->instance_manager()->GetInstance(kInstanceUri));
  EXPECT_EQ(restored_instance->instance_uri(), kInstanceUri);

  // Verify database was restored with correct schema.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restored_db,
      dst_env->database_manager()->GetDatabase(kDatabaseUri));
  const backend::Schema* schema = restored_db->backend()->GetLatestSchema();
  ASSERT_NE(schema, nullptr);

  // Verify the table exists in the schema.
  const backend::Table* table = schema->FindTable("TestTable");
  ASSERT_NE(table, nullptr);
  EXPECT_NE(table->FindColumn("key"), nullptr);
  EXPECT_NE(table->FindColumn("value"), nullptr);

  // Verify data was restored.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto dst_rows,
                        ReadAllRows(restored_db->backend()));
  EXPECT_EQ(dst_rows.size(), 3);
  EXPECT_EQ(dst_rows[1], "hello");
  EXPECT_EQ(dst_rows[2], "world");
  EXPECT_EQ(dst_rows[3], "foo");
}

// ---------------------------------------------------------------------------
// Test: Snapshot round-trip with a generated column
//
// Regression test: restoring a snapshot must not attempt to write a value
// into a non-key generated column, since that is rejected by
// ValidateGeneratedColumnsNotPresent on insert.
// ---------------------------------------------------------------------------
TEST_F(PersistenceManagerTest, SnapshotRoundTripGeneratedColumn) {
  std::string snapshot_path = test_dir_ + "/snapshot.pb";

  auto src_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      src_env->instance_manager()
          ->CreateInstance(kInstanceUri, MakeInstanceProto())
          .status());

  std::vector<std::string> ddl = {
      R"(CREATE TABLE GenTable (
           key INT64 NOT NULL,
           value INT64,
           computed INT64 NOT NULL AS (key + value) STORED
         ) PRIMARY KEY(key))"};
  backend::SchemaChangeOperation schema_op;
  schema_op.statements = ddl;
  schema_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto src_db,
      src_env->database_manager()->CreateDatabase(kDatabaseUri, schema_op,
                                                   nullptr));

  // Insert rows, supplying only the non-generated columns. `computed` is
  // derived by the engine.
  {
    backend::ReadWriteOptions rw_options;
    backend::RetryState retry_state;
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        auto txn, src_db->backend()->CreateReadWriteTransaction(
                      rw_options, retry_state));

    backend::Mutation mutation;
    std::vector<std::string> columns = {"key", "value"};
    std::vector<backend::ValueList> rows;
    rows.push_back({googlesql::values::Int64(1), googlesql::values::Int64(10)});
    rows.push_back({googlesql::values::Int64(2), googlesql::values::Int64(20)});
    mutation.AddWriteOp(backend::MutationOpType::kInsert, "GenTable",
                        std::move(columns), std::move(rows));
    GOOGLESQL_ASSERT_OK(txn->Write(mutation));
    GOOGLESQL_ASSERT_OK(txn->Commit());
  }

  // Write snapshot.
  GOOGLESQL_ASSERT_OK(backend::SnapshotWriter::WriteSnapshot(
      snapshot_path, src_env->instance_manager(),
      src_env->database_manager()));

  // Load snapshot into a fresh env. Before the fix, this failed with
  // FAILED_PRECONDITION: "Cannot write into generated column
  // `GenTable.computed`."
  auto dst_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      backend::SnapshotLoader::LoadSnapshot(snapshot_path, dst_env.get())
          .status());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restored_db,
      dst_env->database_manager()->GetDatabase(kDatabaseUri));

  backend::ReadOnlyOptions ro_options;
  ro_options.bound = backend::TimestampBound::kStrongRead;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto read_txn, restored_db->backend()->CreateReadOnlyTransaction(
                         ro_options));

  backend::ReadArg read_arg;
  read_arg.table = "GenTable";
  read_arg.key_set = backend::KeySet::All();
  read_arg.columns = {"key", "computed"};

  std::unique_ptr<backend::RowCursor> cursor;
  GOOGLESQL_ASSERT_OK(read_txn->Read(read_arg, &cursor));

  std::map<int64_t, int64_t> computed_by_key;
  while (cursor->Next()) {
    computed_by_key[cursor->ColumnValue(0).int64_value()] =
        cursor->ColumnValue(1).int64_value();
  }
  GOOGLESQL_ASSERT_OK(cursor->Status());

  EXPECT_EQ(computed_by_key.size(), 2);
  EXPECT_EQ(computed_by_key[1], 11);
  EXPECT_EQ(computed_by_key[2], 22);
}

// ---------------------------------------------------------------------------
// Regression test for: snapshot restore drops data for tables declared
// after one whose schema included ALTER TABLE ... ADD COLUMN.
//
// The snapshot's DDL is compacted (PrintDDLStatements folds the added column
// into its CREATE TABLE), so replaying it during restore reallocates every
// table/column id from that point on in the one shared, database-wide id
// sequence. TableB, declared after the altered TableA, is exactly the shape
// that lost all of its rows in production.
// ---------------------------------------------------------------------------
TEST_F(PersistenceManagerTest, SnapshotRoundTripAlterAddColumnTwoTables) {
  std::string snapshot_path = test_dir_ + "/snapshot.pb";

  auto src_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      src_env->instance_manager()
          ->CreateInstance(kInstanceUri, MakeInstanceProto())
          .status());

  std::vector<std::string> ddl = {
      R"(CREATE TABLE TableA (
           id INT64 NOT NULL,
           name STRING(MAX)
         ) PRIMARY KEY(id))",
      R"(CREATE TABLE TableB (
           id INT64 NOT NULL,
           val STRING(MAX)
         ) PRIMARY KEY(id))"};
  backend::SchemaChangeOperation schema_op;
  schema_op.statements = ddl;
  schema_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto src_db,
      src_env->database_manager()->CreateDatabase(kDatabaseUri, schema_op,
                                                   nullptr));

  // Add a column to TableA as a separate schema change, as a real migration
  // would -- this is what the compacted snapshot DDL later folds away.
  {
    std::vector<std::string> alter_ddl = {
        "ALTER TABLE TableA ADD COLUMN extra STRING(MAX)"};
    backend::SchemaChangeOperation alter_op;
    alter_op.statements = alter_ddl;
    alter_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
    int num_successful = 0;
    absl::Time commit_timestamp;
    absl::Status backfill_status;
    GOOGLESQL_ASSERT_OK(src_db->backend()->UpdateSchema(
        alter_op, &num_successful, &commit_timestamp, &backfill_status));
    GOOGLESQL_ASSERT_OK(backfill_status);
  }

  {
    backend::ReadWriteOptions rw_options;
    backend::RetryState retry_state;
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        auto txn, src_db->backend()->CreateReadWriteTransaction(
                      rw_options, retry_state));

    backend::Mutation mutation;
    mutation.AddWriteOp(
        backend::MutationOpType::kInsert, "TableA", {"id", "name", "extra"},
        {{googlesql::values::Int64(1), googlesql::values::String("a1"),
          googlesql::values::String("extra1")}});
    mutation.AddWriteOp(backend::MutationOpType::kInsert, "TableB",
                        {"id", "val"},
                        {{googlesql::values::Int64(1),
                          googlesql::values::String("b1")}});
    GOOGLESQL_ASSERT_OK(txn->Write(mutation));
    GOOGLESQL_ASSERT_OK(txn->Commit());
  }

  GOOGLESQL_ASSERT_OK(backend::SnapshotWriter::WriteSnapshot(
      snapshot_path, src_env->instance_manager(),
      src_env->database_manager()));

  auto dst_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      backend::SnapshotLoader::LoadSnapshot(snapshot_path, dst_env.get())
          .status());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restored_db,
      dst_env->database_manager()->GetDatabase(kDatabaseUri));

  const backend::Schema* schema = restored_db->backend()->GetLatestSchema();
  ASSERT_NE(schema, nullptr);
  ASSERT_NE(schema->FindTable("TableA"), nullptr);
  EXPECT_NE(schema->FindTable("TableA")->FindColumn("extra"), nullptr);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto table_a,
      ReadTable(restored_db->backend(), "TableA", {"id", "name", "extra"}));
  ASSERT_EQ(table_a.size(), 1);
  EXPECT_EQ(table_a[1], (std::vector<std::string>{"a1", "extra1"}));

  // TableB is declared after the altered TableA, so its ids shift under the
  // bug -- it lost all of its rows entirely.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto table_b, ReadTable(restored_db->backend(), "TableB", {"id", "val"}));
  ASSERT_EQ(table_b.size(), 1);
  EXPECT_EQ(table_b[1], (std::vector<std::string>{"b1"}));
}

// ---------------------------------------------------------------------------
// Same regression as SnapshotRoundTripAlterAddColumnTwoTables, but the
// snapshot on disk is stripped of table_name/column_names first, simulating
// one written by a pre-upgrade binary (fs.1) that predates those fields.
// The loader's legacy id-based fallback must recover names from the raw
// ids (format "<name>:<seq>" / "<table>.<column>:<seq>", see
// backend/common/ids.h) rather than compare ids literally, since schema
// replay reallocates fresh sequence numbers.
// ---------------------------------------------------------------------------
TEST_F(PersistenceManagerTest, SnapshotRoundTripLegacyFormatAfterAddColumn) {
  std::string snapshot_path = test_dir_ + "/snapshot.pb";

  auto src_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      src_env->instance_manager()
          ->CreateInstance(kInstanceUri, MakeInstanceProto())
          .status());

  std::vector<std::string> ddl = {
      R"(CREATE TABLE TableA (
           id INT64 NOT NULL,
           name STRING(MAX)
         ) PRIMARY KEY(id))",
      R"(CREATE TABLE TableB (
           id INT64 NOT NULL,
           val STRING(MAX)
         ) PRIMARY KEY(id))"};
  backend::SchemaChangeOperation schema_op;
  schema_op.statements = ddl;
  schema_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto src_db,
      src_env->database_manager()->CreateDatabase(kDatabaseUri, schema_op,
                                                   nullptr));

  {
    std::vector<std::string> alter_ddl = {
        "ALTER TABLE TableA ADD COLUMN extra STRING(MAX)"};
    backend::SchemaChangeOperation alter_op;
    alter_op.statements = alter_ddl;
    alter_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
    int num_successful = 0;
    absl::Time commit_timestamp;
    absl::Status backfill_status;
    GOOGLESQL_ASSERT_OK(src_db->backend()->UpdateSchema(
        alter_op, &num_successful, &commit_timestamp, &backfill_status));
    GOOGLESQL_ASSERT_OK(backfill_status);
  }

  {
    backend::ReadWriteOptions rw_options;
    backend::RetryState retry_state;
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        auto txn, src_db->backend()->CreateReadWriteTransaction(
                      rw_options, retry_state));

    backend::Mutation mutation;
    mutation.AddWriteOp(
        backend::MutationOpType::kInsert, "TableA", {"id", "name", "extra"},
        {{googlesql::values::Int64(1), googlesql::values::String("a1"),
          googlesql::values::String("extra1")}});
    mutation.AddWriteOp(backend::MutationOpType::kInsert, "TableB",
                        {"id", "val"},
                        {{googlesql::values::Int64(1),
                          googlesql::values::String("b1")}});
    GOOGLESQL_ASSERT_OK(txn->Write(mutation));
    GOOGLESQL_ASSERT_OK(txn->Commit());
  }

  GOOGLESQL_ASSERT_OK(backend::SnapshotWriter::WriteSnapshot(
      snapshot_path, src_env->instance_manager(),
      src_env->database_manager()));
  StripPersistedNamesForLegacyUpgradeTest(snapshot_path);

  auto dst_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      backend::SnapshotLoader::LoadSnapshot(snapshot_path, dst_env.get())
          .status());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restored_db,
      dst_env->database_manager()->GetDatabase(kDatabaseUri));

  const backend::Schema* schema = restored_db->backend()->GetLatestSchema();
  ASSERT_NE(schema, nullptr);
  ASSERT_NE(schema->FindTable("TableA"), nullptr);
  EXPECT_NE(schema->FindTable("TableA")->FindColumn("extra"), nullptr);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto table_a,
      ReadTable(restored_db->backend(), "TableA", {"id", "name", "extra"}));
  ASSERT_EQ(table_a.size(), 1);
  EXPECT_EQ(table_a[1], (std::vector<std::string>{"a1", "extra1"}));

  // TableB is declared after the altered TableA, so its ids shift under the
  // original bug -- with literal id-based matching it would lose all rows.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto table_b, ReadTable(restored_db->backend(), "TableB", {"id", "val"}));
  ASSERT_EQ(table_b.size(), 1);
  EXPECT_EQ(table_b[1], (std::vector<std::string>{"b1"}));
}

// ---------------------------------------------------------------------------
// Same regression, but with the table declared after the altered one
// interleaved as a child -- the exact shape that lost 87 rows in production.
// ---------------------------------------------------------------------------
TEST_F(PersistenceManagerTest,
       SnapshotRoundTripAlterAddColumnWithInterleavedChild) {
  std::string snapshot_path = test_dir_ + "/snapshot.pb";

  auto src_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      src_env->instance_manager()
          ->CreateInstance(kInstanceUri, MakeInstanceProto())
          .status());

  std::vector<std::string> ddl = {
      R"(CREATE TABLE Parent (
           id INT64 NOT NULL,
           name STRING(MAX)
         ) PRIMARY KEY(id))",
      R"(CREATE TABLE Child (
           id INT64 NOT NULL,
           child_id INT64 NOT NULL,
           data STRING(MAX)
         ) PRIMARY KEY(id, child_id),
         INTERLEAVE IN PARENT Parent ON DELETE CASCADE)"};
  backend::SchemaChangeOperation schema_op;
  schema_op.statements = ddl;
  schema_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto src_db,
      src_env->database_manager()->CreateDatabase(kDatabaseUri, schema_op,
                                                   nullptr));

  {
    std::vector<std::string> alter_ddl = {
        "ALTER TABLE Parent ADD COLUMN extra STRING(MAX)"};
    backend::SchemaChangeOperation alter_op;
    alter_op.statements = alter_ddl;
    alter_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
    int num_successful = 0;
    absl::Time commit_timestamp;
    absl::Status backfill_status;
    GOOGLESQL_ASSERT_OK(src_db->backend()->UpdateSchema(
        alter_op, &num_successful, &commit_timestamp, &backfill_status));
    GOOGLESQL_ASSERT_OK(backfill_status);
  }

  {
    backend::ReadWriteOptions rw_options;
    backend::RetryState retry_state;
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        auto txn, src_db->backend()->CreateReadWriteTransaction(
                      rw_options, retry_state));

    backend::Mutation mutation;
    mutation.AddWriteOp(
        backend::MutationOpType::kInsert, "Parent", {"id", "name", "extra"},
        {{googlesql::values::Int64(1), googlesql::values::String("p1"),
          googlesql::values::String("extra1")}});
    mutation.AddWriteOp(
        backend::MutationOpType::kInsert, "Child",
        {"id", "child_id", "data"},
        {{googlesql::values::Int64(1), googlesql::values::Int64(1),
          googlesql::values::String("c1")}});
    GOOGLESQL_ASSERT_OK(txn->Write(mutation));
    GOOGLESQL_ASSERT_OK(txn->Commit());
  }

  GOOGLESQL_ASSERT_OK(backend::SnapshotWriter::WriteSnapshot(
      snapshot_path, src_env->instance_manager(),
      src_env->database_manager()));

  auto dst_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      backend::SnapshotLoader::LoadSnapshot(snapshot_path, dst_env.get())
          .status());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restored_db,
      dst_env->database_manager()->GetDatabase(kDatabaseUri));

  backend::ReadOnlyOptions ro_options;
  ro_options.bound = backend::TimestampBound::kStrongRead;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto read_txn,
      restored_db->backend()->CreateReadOnlyTransaction(ro_options));

  backend::ReadArg read_arg;
  read_arg.table = "Child";
  read_arg.key_set = backend::KeySet::All();
  read_arg.columns = {"id", "child_id", "data"};

  std::unique_ptr<backend::RowCursor> cursor;
  GOOGLESQL_ASSERT_OK(read_txn->Read(read_arg, &cursor));

  int child_rows = 0;
  while (cursor->Next()) {
    ++child_rows;
    EXPECT_EQ(cursor->ColumnValue(0).int64_value(), 1);
    EXPECT_EQ(cursor->ColumnValue(1).int64_value(), 1);
    EXPECT_EQ(cursor->ColumnValue(2).string_value(), "c1");
  }
  GOOGLESQL_ASSERT_OK(cursor->Status());
  EXPECT_EQ(child_rows, 1);
}

// ---------------------------------------------------------------------------
// Test 2: WAL replay for data mutations
//
// Write data mutations through PersistentStorage → save WAL → create fresh
// env with the same schema → replay WAL → verify data matches.
// ---------------------------------------------------------------------------
TEST_F(PersistenceManagerTest, WalReplayDataMutations) {
  auto manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);

  // Set up source env without WAL first (creates InMemoryStorage).
  auto src_env = std::make_unique<ServerEnv>();
  src_env->set_wal_writer(manager->wal_writer());
  GOOGLESQL_ASSERT_OK(SetUpInstanceAndDatabase(src_env.get()));

  // Verify source data.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto src_db,
      src_env->database_manager()->GetDatabase(kDatabaseUri));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto src_rows, ReadAllRows(src_db->backend()));
  ASSERT_EQ(src_rows.size(), 3);

  // Write a snapshot to capture the initial 3 rows + schema + instances.
  GOOGLESQL_ASSERT_OK(manager->SaveState(src_env.get()));

  // Recreate manager (SaveState cleared the WAL).
  manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);
  src_env->set_wal_writer(manager->wal_writer());

  // Now enable persistence so the next write goes to WAL.
  GOOGLESQL_ASSERT_OK(src_db->backend()->EnablePersistence(
      kDatabaseUri, manager->wal_writer()));

  // Write another row — this will only be in the WAL, not in the snapshot.
  {
    backend::ReadWriteOptions rw_options;
    backend::RetryState retry_state;
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        auto txn,
        src_db->backend()->CreateReadWriteTransaction(rw_options, retry_state));

    backend::Mutation mutation;
    std::vector<std::string> columns = {"key", "value"};
    std::vector<backend::ValueList> rows;
    rows.push_back({googlesql::values::Int64(4),
                    googlesql::values::String("bar")});
    mutation.AddWriteOp(backend::MutationOpType::kInsert, "TestTable",
                        std::move(columns), std::move(rows));
    GOOGLESQL_ASSERT_OK(txn->Write(mutation));
    GOOGLESQL_ASSERT_OK(txn->Commit());
  }

  // Load into fresh env. Snapshot has 3 rows, WAL has the 4th.
  auto dst_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(manager->RestoreState(dst_env.get()));

  // Verify all 4 rows are present.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restored_db,
      dst_env->database_manager()->GetDatabase(kDatabaseUri));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto dst_rows,
                        ReadAllRows(restored_db->backend()));
  EXPECT_EQ(dst_rows.size(), 4);
  EXPECT_EQ(dst_rows[1], "hello");
  EXPECT_EQ(dst_rows[2], "world");
  EXPECT_EQ(dst_rows[3], "foo");
  EXPECT_EQ(dst_rows[4], "bar");
}

// ---------------------------------------------------------------------------
// Test 3: WAL replay for metadata changes
//
// Log create/delete instance and database to WAL → replay into fresh env →
// verify instances and databases exist or are deleted as expected.
// ---------------------------------------------------------------------------
TEST_F(PersistenceManagerTest, WalReplayMetadataCreateInstance) {
  auto manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);

  // Manually write a create_instance metadata record to WAL.
  backend::WalRecord record;
  record.set_sequence_number(0);
  auto* meta = record.mutable_metadata_change();
  auto* ci = meta->mutable_create_instance();
  ci->set_instance_uri(kInstanceUri);
  instance_api::Instance instance_proto = MakeInstanceProto();
  ci->set_instance_proto(instance_proto.SerializeAsString());

  GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(manager->RestoreState(env.get()));

  // Verify instance was created.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto instance, env->instance_manager()->GetInstance(kInstanceUri));
  EXPECT_EQ(instance->instance_uri(), kInstanceUri);
}

TEST_F(PersistenceManagerTest, WalReplayMetadataCreateDatabase) {
  auto manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);

  // First, create instance metadata in WAL.
  {
    backend::WalRecord record;
    record.set_sequence_number(0);
    auto* meta = record.mutable_metadata_change();
    auto* ci = meta->mutable_create_instance();
    ci->set_instance_uri(kInstanceUri);
    instance_api::Instance instance_proto = MakeInstanceProto();
    ci->set_instance_proto(instance_proto.SerializeAsString());
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Then, create database metadata in WAL.
  {
    backend::WalRecord record;
    record.set_sequence_number(1);
    auto* meta = record.mutable_metadata_change();
    auto* cd = meta->mutable_create_database();
    cd->set_database_uri(kDatabaseUri);
    cd->set_database_id("test-db");
    cd->set_dialect(static_cast<int32_t>(database_api::GOOGLE_STANDARD_SQL));
    for (const auto& stmt : SimpleSchema()) {
      cd->add_ddl_statements(stmt);
    }
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(manager->RestoreState(env.get()));

  // Verify database was created with schema.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db, env->database_manager()->GetDatabase(kDatabaseUri));
  const backend::Schema* schema = db->backend()->GetLatestSchema();
  ASSERT_NE(schema, nullptr);
  EXPECT_NE(schema->FindTable("TestTable"), nullptr);
}

TEST_F(PersistenceManagerTest, WalReplayMetadataDeleteInstance) {
  auto manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);

  // Create instance, then delete it, all in WAL.
  {
    backend::WalRecord record;
    record.set_sequence_number(0);
    auto* meta = record.mutable_metadata_change();
    auto* ci = meta->mutable_create_instance();
    ci->set_instance_uri(kInstanceUri);
    instance_api::Instance instance_proto = MakeInstanceProto();
    ci->set_instance_proto(instance_proto.SerializeAsString());
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }
  {
    backend::WalRecord record;
    record.set_sequence_number(1);
    auto* meta = record.mutable_metadata_change();
    meta->set_delete_instance_uri(kInstanceUri);
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(manager->RestoreState(env.get()));

  // Instance should not exist.
  auto result = env->instance_manager()->GetInstance(kInstanceUri);
  EXPECT_FALSE(result.ok());
}

TEST_F(PersistenceManagerTest, WalReplayMetadataDeleteDatabase) {
  auto manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);

  // Create instance + database, then delete database, all in WAL.
  {
    backend::WalRecord record;
    record.set_sequence_number(0);
    auto* meta = record.mutable_metadata_change();
    auto* ci = meta->mutable_create_instance();
    ci->set_instance_uri(kInstanceUri);
    instance_api::Instance instance_proto = MakeInstanceProto();
    ci->set_instance_proto(instance_proto.SerializeAsString());
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }
  {
    backend::WalRecord record;
    record.set_sequence_number(1);
    auto* meta = record.mutable_metadata_change();
    auto* cd = meta->mutable_create_database();
    cd->set_database_uri(kDatabaseUri);
    cd->set_database_id("test-db");
    cd->set_dialect(static_cast<int32_t>(database_api::GOOGLE_STANDARD_SQL));
    for (const auto& stmt : SimpleSchema()) {
      cd->add_ddl_statements(stmt);
    }
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }
  {
    backend::WalRecord record;
    record.set_sequence_number(2);
    auto* meta = record.mutable_metadata_change();
    meta->set_delete_database_uri(kDatabaseUri);
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(manager->RestoreState(env.get()));

  // Instance should exist but database should not.
  GOOGLESQL_ASSERT_OK(env->instance_manager()->GetInstance(kInstanceUri).status());
  auto result = env->database_manager()->GetDatabase(kDatabaseUri);
  EXPECT_FALSE(result.ok());
}

TEST_F(PersistenceManagerTest, WalReplaySchemaChange) {
  auto manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);

  // Create instance + database in WAL, then apply a schema change.
  {
    backend::WalRecord record;
    record.set_sequence_number(0);
    auto* meta = record.mutable_metadata_change();
    auto* ci = meta->mutable_create_instance();
    ci->set_instance_uri(kInstanceUri);
    instance_api::Instance instance_proto = MakeInstanceProto();
    ci->set_instance_proto(instance_proto.SerializeAsString());
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }
  {
    backend::WalRecord record;
    record.set_sequence_number(1);
    auto* meta = record.mutable_metadata_change();
    auto* cd = meta->mutable_create_database();
    cd->set_database_uri(kDatabaseUri);
    cd->set_database_id("test-db");
    cd->set_dialect(static_cast<int32_t>(database_api::GOOGLE_STANDARD_SQL));
    for (const auto& stmt : SimpleSchema()) {
      cd->add_ddl_statements(stmt);
    }
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Add a new column via schema change in WAL.
  {
    backend::WalRecord record;
    record.set_sequence_number(2);
    auto* sc = record.mutable_schema_change();
    sc->set_database_uri(kDatabaseUri);
    sc->set_dialect(static_cast<int32_t>(database_api::GOOGLE_STANDARD_SQL));
    sc->add_ddl_statements(
        "ALTER TABLE TestTable ADD COLUMN extra STRING(MAX)");
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(manager->RestoreState(env.get()));

  // Verify the schema has the new column.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db, env->database_manager()->GetDatabase(kDatabaseUri));
  const backend::Schema* schema = db->backend()->GetLatestSchema();
  ASSERT_NE(schema, nullptr);
  const backend::Table* table = schema->FindTable("TestTable");
  ASSERT_NE(table, nullptr);
  EXPECT_NE(table->FindColumn("extra"), nullptr);
}

// ---------------------------------------------------------------------------
// Test: Full SaveState / RestoreState round-trip via PersistenceManager
// ---------------------------------------------------------------------------
TEST_F(PersistenceManagerTest, SaveAndRestoreState) {
  // Set up source env with persistence enabled.
  auto manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);

  auto src_env = std::make_unique<ServerEnv>();
  src_env->set_wal_writer(manager->wal_writer());
  GOOGLESQL_ASSERT_OK(SetUpInstanceAndDatabase(src_env.get(), manager->wal_writer()));

  // Save state (snapshot + clear WAL).
  GOOGLESQL_ASSERT_OK(manager->SaveState(src_env.get()));

  // Restore into fresh env with a new manager.
  auto manager2 = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager2, nullptr);

  auto dst_env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(manager2->RestoreState(dst_env.get()));

  // Verify instance was restored.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto instance, dst_env->instance_manager()->GetInstance(kInstanceUri));
  EXPECT_EQ(instance->instance_uri(), kInstanceUri);

  // Verify data.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db, dst_env->database_manager()->GetDatabase(kDatabaseUri));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto rows, ReadAllRows(db->backend()));
  EXPECT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[1], "hello");
  EXPECT_EQ(rows[2], "world");
  EXPECT_EQ(rows[3], "foo");
}

// ---------------------------------------------------------------------------
// Comprehensive round-trip regression test.
//
// Exercises writes, updates, deletes, and ALTER TABLE ADD COLUMN both baked
// into a snapshot and replayed from the WAL on top of it, then restores from
// that same (never re-snapshotted) on-disk state twice in a row -- the
// "crash-loop before a fresh snapshot ever completes" scenario. Covers, in
// one test:
//   - snapshot restore matching tables/columns by name after ADD COLUMN
//     folds into the compacted DDL (bug: IDs are reallocated on replay).
//   - WAL replay matching by name after its own ADD COLUMN (a second,
//     independent ID reallocation on top of the snapshot's).
//   - WAL updates/deletes are not shadowed by snapshot-restored rows (bug:
//     PopulateStorage used to commit restored rows at "now" instead of the
//     snapshot's timestamp, making them look newer than every WAL entry
//     layered on top of them).
//   - repeated restores of the same unconsolidated (snapshot + WAL) state
//     are deterministic.
// ---------------------------------------------------------------------------
TEST_F(PersistenceManagerTest, FullRoundTripAcrossRepeatedRestartsWithoutFreshSnapshot) {
  auto manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);

  auto env = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(
      env->instance_manager()->CreateInstance(kInstanceUri, MakeInstanceProto())
          .status());

  std::vector<std::string> ddl = {
      R"(CREATE TABLE TableA (
           id INT64 NOT NULL,
           name STRING(MAX)
         ) PRIMARY KEY(id))",
      R"(CREATE TABLE TableB (
           id INT64 NOT NULL,
           val STRING(MAX)
         ) PRIMARY KEY(id))"};
  backend::SchemaChangeOperation schema_op;
  schema_op.statements = ddl;
  schema_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
  // No wal_writer yet -- these initial writes and the first ALTER all land
  // directly in the eventual snapshot, not the WAL.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto db, env->database_manager()->CreateDatabase(kDatabaseUri, schema_op,
                                                       nullptr));

  // --- Phase 1: writes + an ALTER, all before the (only) snapshot. ---
  {
    backend::ReadWriteOptions rw_options;
    backend::RetryState retry_state;
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        auto txn,
        db->backend()->CreateReadWriteTransaction(rw_options, retry_state));
    backend::Mutation mutation;
    mutation.AddWriteOp(
        backend::MutationOpType::kInsert, "TableA", {"id", "name"},
        {{googlesql::values::Int64(1), googlesql::values::String("a1")},
         {googlesql::values::Int64(2), googlesql::values::String("a2")},
         {googlesql::values::Int64(3), googlesql::values::String("a3")}});
    mutation.AddWriteOp(
        backend::MutationOpType::kInsert, "TableB", {"id", "val"},
        {{googlesql::values::Int64(1), googlesql::values::String("b1")},
         {googlesql::values::Int64(2), googlesql::values::String("b2")},
         {googlesql::values::Int64(3), googlesql::values::String("b3")}});
    GOOGLESQL_ASSERT_OK(txn->Write(mutation));
    GOOGLESQL_ASSERT_OK(txn->Commit());
  }
  {
    std::vector<std::string> alter_ddl = {
        "ALTER TABLE TableA ADD COLUMN extra STRING(MAX)"};
    backend::SchemaChangeOperation alter_op;
    alter_op.statements = alter_ddl;
    alter_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
    int num_successful = 0;
    absl::Time commit_timestamp;
    absl::Status backfill_status;
    GOOGLESQL_ASSERT_OK(db->backend()->UpdateSchema(
        alter_op, &num_successful, &commit_timestamp, &backfill_status));
    GOOGLESQL_ASSERT_OK(backfill_status);
  }
  {
    // Insert using the new column, update an existing row, delete another --
    // all baked into the snapshot below.
    backend::ReadWriteOptions rw_options;
    backend::RetryState retry_state;
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        auto txn,
        db->backend()->CreateReadWriteTransaction(rw_options, retry_state));
    backend::Mutation mutation;
    mutation.AddWriteOp(
        backend::MutationOpType::kInsert, "TableA", {"id", "name", "extra"},
        {{googlesql::values::Int64(4), googlesql::values::String("a4"),
          googlesql::values::String("ex4")}});
    mutation.AddWriteOp(
        backend::MutationOpType::kUpdate, "TableA", {"id", "name"},
        {{googlesql::values::Int64(1),
          googlesql::values::String("a1-upd1")}});
    mutation.AddDeleteOp("TableB",
                         backend::KeySet(backend::Key(
                             {googlesql::values::Int64(2)})));
    GOOGLESQL_ASSERT_OK(txn->Write(mutation));
    GOOGLESQL_ASSERT_OK(txn->Commit());
  }

  // State right before the snapshot:
  //   TableA: 1->(a1-upd1, NULL), 2->(a2, NULL), 3->(a3, NULL), 4->(a4, ex4)
  //   TableB: 1->b1, 3->b3   (2 deleted)
  GOOGLESQL_ASSERT_OK(manager->SaveState(env.get()));

  // --- Phase 2: a second ALTER plus more writes, living only in the WAL. ---
  manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);
  env->set_wal_writer(manager->wal_writer());
  GOOGLESQL_ASSERT_OK(
      db->backend()->EnablePersistence(kDatabaseUri, manager->wal_writer()));

  {
    std::vector<std::string> alter_ddl = {
        "ALTER TABLE TableB ADD COLUMN tag STRING(MAX)"};
    backend::SchemaChangeOperation alter_op;
    alter_op.statements = alter_ddl;
    alter_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;
    int num_successful = 0;
    absl::Time commit_timestamp;
    absl::Status backfill_status;
    GOOGLESQL_ASSERT_OK(db->backend()->UpdateSchema(
        alter_op, &num_successful, &commit_timestamp, &backfill_status));
    GOOGLESQL_ASSERT_OK(backfill_status);

    // Real DDL replication logs the schema change to the WAL alongside
    // applying it live (see frontend/handlers/databases.cc); reproduce that
    // here since this test operates below that layer.
    backend::WalRecord record;
    auto* sc = record.mutable_schema_change();
    sc->set_database_uri(kDatabaseUri);
    sc->set_dialect(static_cast<int32_t>(database_api::GOOGLE_STANDARD_SQL));
    for (const auto& stmt : alter_op.statements) {
      sc->add_ddl_statements(std::string(stmt));
    }
    GOOGLESQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }
  {
    backend::ReadWriteOptions rw_options;
    backend::RetryState retry_state;
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        auto txn,
        db->backend()->CreateReadWriteTransaction(rw_options, retry_state));
    backend::Mutation mutation;
    mutation.AddWriteOp(
        backend::MutationOpType::kInsert, "TableB", {"id", "val", "tag"},
        {{googlesql::values::Int64(4), googlesql::values::String("b4"),
          googlesql::values::String("tagB4")}});
    mutation.AddWriteOp(
        backend::MutationOpType::kUpdate, "TableA", {"id", "name"},
        {{googlesql::values::Int64(2),
          googlesql::values::String("a2-upd2")}});
    mutation.AddDeleteOp("TableA",
                         backend::KeySet(backend::Key(
                             {googlesql::values::Int64(3)})));
    GOOGLESQL_ASSERT_OK(txn->Write(mutation));
    GOOGLESQL_ASSERT_OK(txn->Commit());
  }

  // Final expected state after both phases:
  //   TableA: 1->(a1-upd1, NULL), 2->(a2-upd2, NULL), 4->(a4, ex4) (3 deleted)
  //   TableB: 1->(b1, NULL), 3->(b3, NULL), 4->(b4, tagB4) (2 deleted)
  std::map<int64_t, std::vector<std::string>> want_table_a = {
      {1, {"a1-upd1", "<NULL>"}},
      {2, {"a2-upd2", "<NULL>"}},
      {4, {"a4", "ex4"}},
  };
  std::map<int64_t, std::vector<std::string>> want_table_b = {
      {1, {"b1", "<NULL>"}},
      {3, {"b3", "<NULL>"}},
      {4, {"b4", "tagB4"}},
  };

  // --- Restart #1: no fresh snapshot was ever taken after phase 2. ---
  auto restart1 = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(manager->RestoreState(restart1.get()));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restart1_db, restart1->database_manager()->GetDatabase(kDatabaseUri));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restart1_a,
      ReadTable(restart1_db->backend(), "TableA", {"id", "name", "extra"}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restart1_b,
      ReadTable(restart1_db->backend(), "TableB", {"id", "val", "tag"}));
  EXPECT_EQ(restart1_a, want_table_a);
  EXPECT_EQ(restart1_b, want_table_b);

  // --- Restart #2: restore again from the exact same on-disk state (still
  // no fresh snapshot in between) -- must reproduce identical results. ---
  auto restart2 = std::make_unique<ServerEnv>();
  GOOGLESQL_ASSERT_OK(manager->RestoreState(restart2.get()));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restart2_db, restart2->database_manager()->GetDatabase(kDatabaseUri));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restart2_a,
      ReadTable(restart2_db->backend(), "TableA", {"id", "name", "extra"}));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto restart2_b,
      ReadTable(restart2_db->backend(), "TableB", {"id", "val", "tag"}));
  EXPECT_EQ(restart2_a, want_table_a);
  EXPECT_EQ(restart2_b, want_table_b);

  // Compare the two restarts against each other directly, not just against
  // the expected values.
  EXPECT_EQ(restart1_a, restart2_a);
  EXPECT_EQ(restart1_b, restart2_b);
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
