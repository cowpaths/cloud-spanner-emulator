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

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/updater/schema_updater.h"
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
  ZETASQL_RETURN_IF_ERROR(
      env->instance_manager()
          ->CreateInstance(kInstanceUri, MakeInstanceProto())
          .status());

  // Create database with schema.
  backend::SchemaChangeOperation schema_op;
  auto ddl = SimpleSchema();
  schema_op.statements = ddl;
  schema_op.database_dialect = database_api::GOOGLE_STANDARD_SQL;

  ZETASQL_ASSIGN_OR_RETURN(
      auto database,
      env->database_manager()->CreateDatabase(kDatabaseUri, schema_op,
                                              wal_writer));

  // Insert data via a read-write transaction.
  backend::ReadWriteOptions rw_options;
  backend::RetryState retry_state;
  ZETASQL_ASSIGN_OR_RETURN(
      auto txn,
      database->backend()->CreateReadWriteTransaction(rw_options, retry_state));

  backend::Mutation mutation;
  std::vector<std::string> columns = {"key", "value"};
  std::vector<backend::ValueList> rows;
  rows.push_back({zetasql::values::Int64(1),
                  zetasql::values::String("hello")});
  rows.push_back({zetasql::values::Int64(2),
                  zetasql::values::String("world")});
  rows.push_back({zetasql::values::Int64(3),
                  zetasql::values::String("foo")});
  mutation.AddWriteOp(backend::MutationOpType::kInsert, "TestTable",
                      std::move(columns), std::move(rows));
  ZETASQL_RETURN_IF_ERROR(txn->Write(mutation));
  ZETASQL_RETURN_IF_ERROR(txn->Commit());

  return absl::OkStatus();
}

// Helper to read all rows from TestTable in the given database.
// Returns a map from key (int64) to value (string).
absl::StatusOr<std::map<int64_t, std::string>> ReadAllRows(
    backend::Database* db) {
  backend::ReadOnlyOptions ro_options;
  ro_options.bound = backend::TimestampBound::kStrongRead;
  ZETASQL_ASSIGN_OR_RETURN(auto txn, db->CreateReadOnlyTransaction(ro_options));

  backend::ReadArg read_arg;
  read_arg.table = "TestTable";
  read_arg.key_set = backend::KeySet::All();
  read_arg.columns = {"key", "value"};

  std::unique_ptr<backend::RowCursor> cursor;
  ZETASQL_RETURN_IF_ERROR(txn->Read(read_arg, &cursor));

  std::map<int64_t, std::string> result;
  while (cursor->Next()) {
    int64_t key = cursor->ColumnValue(0).int64_value();
    std::string value = cursor->ColumnValue(1).string_value();
    result[key] = value;
  }
  ZETASQL_RETURN_IF_ERROR(cursor->Status());
  return result;
}

class PersistenceManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = testing::TempDir() + "/persistence_manager_test";
    mkdir(test_dir_.c_str(), 0755);
  }

  void TearDown() override {
    // Clean up test directory recursively.
    std::string cmd = "rm -rf " + test_dir_;
    system(cmd.c_str());
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
  ZETASQL_ASSERT_OK(SetUpInstanceAndDatabase(src_env.get()));

  // Verify data was written.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto src_db,
      src_env->database_manager()->GetDatabase(kDatabaseUri));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto src_rows, ReadAllRows(src_db->backend()));
  ASSERT_EQ(src_rows.size(), 3);

  // Write snapshot.
  ZETASQL_ASSERT_OK(backend::SnapshotWriter::WriteSnapshot(
      snapshot_path, src_env->instance_manager(),
      src_env->database_manager()));

  // Load snapshot into a fresh env.
  auto dst_env = std::make_unique<ServerEnv>();
  ZETASQL_ASSERT_OK(
      backend::SnapshotLoader::LoadSnapshot(snapshot_path, dst_env.get())
          .status());

  // Verify instance was restored.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto restored_instance,
      dst_env->instance_manager()->GetInstance(kInstanceUri));
  EXPECT_EQ(restored_instance->instance_uri(), kInstanceUri);

  // Verify database was restored with correct schema.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto dst_rows,
                        ReadAllRows(restored_db->backend()));
  EXPECT_EQ(dst_rows.size(), 3);
  EXPECT_EQ(dst_rows[1], "hello");
  EXPECT_EQ(dst_rows[2], "world");
  EXPECT_EQ(dst_rows[3], "foo");
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
  ZETASQL_ASSERT_OK(SetUpInstanceAndDatabase(src_env.get()));

  // Verify source data.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto src_db,
      src_env->database_manager()->GetDatabase(kDatabaseUri));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto src_rows, ReadAllRows(src_db->backend()));
  ASSERT_EQ(src_rows.size(), 3);

  // Write a snapshot to capture the initial 3 rows + schema + instances.
  ZETASQL_ASSERT_OK(manager->SaveState(src_env.get()));

  // Recreate manager (SaveState cleared the WAL).
  manager = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager, nullptr);
  src_env->set_wal_writer(manager->wal_writer());

  // Now enable persistence so the next write goes to WAL.
  ZETASQL_ASSERT_OK(src_db->backend()->EnablePersistence(
      kDatabaseUri, manager->wal_writer()));

  // Write another row — this will only be in the WAL, not in the snapshot.
  {
    backend::ReadWriteOptions rw_options;
    backend::RetryState retry_state;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto txn,
        src_db->backend()->CreateReadWriteTransaction(rw_options, retry_state));

    backend::Mutation mutation;
    std::vector<std::string> columns = {"key", "value"};
    std::vector<backend::ValueList> rows;
    rows.push_back({zetasql::values::Int64(4),
                    zetasql::values::String("bar")});
    mutation.AddWriteOp(backend::MutationOpType::kInsert, "TestTable",
                        std::move(columns), std::move(rows));
    ZETASQL_ASSERT_OK(txn->Write(mutation));
    ZETASQL_ASSERT_OK(txn->Commit());
  }

  // Load into fresh env. Snapshot has 3 rows, WAL has the 4th.
  auto dst_env = std::make_unique<ServerEnv>();
  ZETASQL_ASSERT_OK(manager->RestoreState(dst_env.get()));

  // Verify all 4 rows are present.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto restored_db,
      dst_env->database_manager()->GetDatabase(kDatabaseUri));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto dst_rows,
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

  ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  ZETASQL_ASSERT_OK(manager->RestoreState(env.get()));

  // Verify instance was created.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
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
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
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
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  ZETASQL_ASSERT_OK(manager->RestoreState(env.get()));

  // Verify database was created with schema.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
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
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }
  {
    backend::WalRecord record;
    record.set_sequence_number(1);
    auto* meta = record.mutable_metadata_change();
    meta->set_delete_instance_uri(kInstanceUri);
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  ZETASQL_ASSERT_OK(manager->RestoreState(env.get()));

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
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
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
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }
  {
    backend::WalRecord record;
    record.set_sequence_number(2);
    auto* meta = record.mutable_metadata_change();
    meta->set_delete_database_uri(kDatabaseUri);
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  ZETASQL_ASSERT_OK(manager->RestoreState(env.get()));

  // Instance should exist but database should not.
  ZETASQL_ASSERT_OK(env->instance_manager()->GetInstance(kInstanceUri).status());
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
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
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
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
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
    ZETASQL_ASSERT_OK(manager->wal_writer()->Append(record));
  }

  // Replay into fresh env.
  auto env = std::make_unique<ServerEnv>();
  ZETASQL_ASSERT_OK(manager->RestoreState(env.get()));

  // Verify the schema has the new column.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
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
  ZETASQL_ASSERT_OK(SetUpInstanceAndDatabase(src_env.get(), manager->wal_writer()));

  // Save state (snapshot + clear WAL).
  ZETASQL_ASSERT_OK(manager->SaveState(src_env.get()));

  // Restore into fresh env with a new manager.
  auto manager2 = PersistenceManager::Create(test_dir_);
  ASSERT_NE(manager2, nullptr);

  auto dst_env = std::make_unique<ServerEnv>();
  ZETASQL_ASSERT_OK(manager2->RestoreState(dst_env.get()));

  // Verify instance was restored.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto instance, dst_env->instance_manager()->GetInstance(kInstanceUri));
  EXPECT_EQ(instance->instance_uri(), kInstanceUri);

  // Verify data.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto db, dst_env->database_manager()->GetDatabase(kDatabaseUri));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto rows, ReadAllRows(db->backend()));
  EXPECT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[1], "hello");
  EXPECT_EQ(rows[2], "world");
  EXPECT_EQ(rows[3], "foo");
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
