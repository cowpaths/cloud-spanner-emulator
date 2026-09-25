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

#include "backend/storage/wal_writer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "backend/storage/persistence.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

class WalWriterTest : public testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = testing::TempDir() + "/wal_writer_test";
    mkdir(test_dir_.c_str(), 0755);
  }

  void TearDown() override {
    // Clean up test directory.
    WalWriter::Clear(test_dir_).IgnoreError();
    rmdir(test_dir_.c_str());
  }

  // Helper to create a simple WalRecord with an entry.
  WalRecord MakeRecord(const std::string& db_uri) {
    WalRecord record;
    WalEntry* entry = record.mutable_entry();
    entry->set_database_uri(db_uri);
    entry->set_commit_timestamp_micros(1000);
    return record;
  }

  std::string test_dir_;
};

TEST_F(WalWriterTest, CreateInTempDirectory) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
  EXPECT_NE(writer, nullptr);
  EXPECT_EQ(writer->current_sequence_number(), 0);
}

TEST_F(WalWriterTest, CreateCreatesDirectoryIfMissing) {
  std::string new_dir = test_dir_ + "/subdir";
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(new_dir));
  EXPECT_NE(writer, nullptr);

  // Clean up the extra subdir.
  WalWriter::Clear(new_dir).IgnoreError();
  rmdir(new_dir.c_str());
}

TEST_F(WalWriterTest, AppendIncrementsSequenceNumber) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
  EXPECT_EQ(writer->current_sequence_number(), 0);

  GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db1")));
  EXPECT_EQ(writer->current_sequence_number(), 1);

  GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db2")));
  EXPECT_EQ(writer->current_sequence_number(), 2);

  GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db3")));
  EXPECT_EQ(writer->current_sequence_number(), 3);
}

TEST_F(WalWriterTest, ReadAllReturnsRecordsInOrder) {
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));

    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db_first")));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db_second")));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db_third")));
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  ASSERT_EQ(records.size(), 3);

  EXPECT_EQ(records[0].entry().database_uri(), "db_first");
  EXPECT_EQ(records[0].entry().sequence_number(), 0);

  EXPECT_EQ(records[1].entry().database_uri(), "db_second");
  EXPECT_EQ(records[1].entry().sequence_number(), 1);

  EXPECT_EQ(records[2].entry().database_uri(), "db_third");
  EXPECT_EQ(records[2].entry().sequence_number(), 2);
}

TEST_F(WalWriterTest, AppendSetsTopLevelSequenceNumberOnAllRecordTypes) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));

  // Non-entry records (metadata/schema changes) carry no sequence number of
  // their own -- RestoreState orders replay using WalRecord's top-level
  // sequence_number, so Append must set it regardless of which oneof field
  // is populated.
  WalRecord metadata_record;
  metadata_record.mutable_metadata_change()->set_delete_instance_uri(
      "instance1");
  GOOGLESQL_EXPECT_OK(writer->Append(metadata_record));

  WalRecord schema_record;
  schema_record.mutable_schema_change()->set_database_uri("db1");
  GOOGLESQL_EXPECT_OK(writer->Append(schema_record));

  GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db1")));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  ASSERT_EQ(records.size(), 3);

  EXPECT_EQ(records[0].sequence_number(), 0);
  EXPECT_TRUE(records[0].has_metadata_change());

  EXPECT_EQ(records[1].sequence_number(), 1);
  EXPECT_TRUE(records[1].has_schema_change());

  EXPECT_EQ(records[2].sequence_number(), 2);
  EXPECT_TRUE(records[2].has_entry());

  // Records must sort strictly by increasing top-level sequence number, as
  // RestoreState relies on this for deterministic, causally-ordered replay.
  EXPECT_LT(records[0].sequence_number(), records[1].sequence_number());
  EXPECT_LT(records[1].sequence_number(), records[2].sequence_number());
}

TEST_F(WalWriterTest, CrcIntegrityCheckDetectsCorruption) {
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db1")));
  }

  // Corrupt the WAL file by flipping a byte in the serialized data.
  std::string wal_path = test_dir_ + "/wal-000000.log";
  int fd = ::open(wal_path.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);

  // The file format is [4-byte length][data][4-byte CRC].
  // Seek to offset 5 (inside the serialized data) and flip a byte.
  ASSERT_EQ(::lseek(fd, 5, SEEK_SET), 5);
  char byte;
  ASSERT_EQ(::read(fd, &byte, 1), 1);
  byte ^= 0xFF;
  ASSERT_EQ(::lseek(fd, 5, SEEK_SET), 5);
  ASSERT_EQ(::write(fd, &byte, 1), 1);
  ::close(fd);

  // ReadAll should detect the corruption via CRC mismatch.
  auto result = WalWriter::ReadAll(test_dir_);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kDataLoss);
}

TEST_F(WalWriterTest, RotateCreatesNewSegmentFiles) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));

  GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db_seg0")));

  // Rotate to a new segment.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::string old_path, writer->Rotate());
  EXPECT_THAT(old_path, testing::HasSubstr("wal-000000.log"));

  GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db_seg1")));

  // Verify both segments have records.
  writer.reset();  // Close the writer to flush.

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  ASSERT_EQ(records.size(), 2);
  EXPECT_EQ(records[0].entry().database_uri(), "db_seg0");
  EXPECT_EQ(records[1].entry().database_uri(), "db_seg1");
}

TEST_F(WalWriterTest, ClearRemovesAllWalFiles) {
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db1")));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db2")));
  }

  // Verify files exist.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto records_before,
                        WalWriter::ReadAll(test_dir_));
  EXPECT_EQ(records_before.size(), 2);

  // Clear all WAL files.
  GOOGLESQL_EXPECT_OK(WalWriter::Clear(test_dir_));

  // Verify no records remain.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto records_after,
                        WalWriter::ReadAll(test_dir_));
  EXPECT_EQ(records_after.size(), 0);
}

TEST_F(WalWriterTest, InstanceClearLetsLiveWriterKeepAppending) {
  // Regression test: after a snapshot, the writer instance's Clear() must
  // leave the writer able to durably append -- not still holding an fd to
  // an unlinked file (the WAL-writer-keeps-writing-to-a-deleted-file bug).
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
  GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("pre_snapshot")));

  // Simulate the snapshot's WAL clear while the writer is still live.
  GOOGLESQL_EXPECT_OK(writer->Clear());

  // A write after the clear must land in a real, readable WAL file.
  GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("post_snapshot")));
  GOOGLESQL_EXPECT_OK(writer->Sync());

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(records[0].entry().database_uri(), "post_snapshot");
}

TEST_F(WalWriterTest, CreateResumesSequenceNumbering) {
  // Write some records with the first writer.
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db1")));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db2")));
    EXPECT_EQ(writer->current_sequence_number(), 2);
  }

  // Create a new writer pointing to the same directory.
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
    // Should resume from where the previous writer left off.
    EXPECT_EQ(writer->current_sequence_number(), 2);

    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db3")));
    EXPECT_EQ(writer->current_sequence_number(), 3);
  }

  // Verify all records are present with correct sequence numbers.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  ASSERT_EQ(records.size(), 3);
  EXPECT_EQ(records[0].entry().sequence_number(), 0);
  EXPECT_EQ(records[1].entry().sequence_number(), 1);
  EXPECT_EQ(records[2].entry().sequence_number(), 2);
}

TEST_F(WalWriterTest, CreateSkipsUnreadableWalFileAndContinues) {
  // Write records across two segments.
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db1")));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db2")));
    GOOGLESQL_ASSERT_OK(writer->Rotate());
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db3")));
    EXPECT_EQ(writer->current_sequence_number(), 3);
  }

  // Make the second segment unreadable.
  std::string wal_path = test_dir_ + "/wal-000001.log";
  ASSERT_EQ(chmod(wal_path.c_str(), 0000), 0);

  // Create should succeed — it skips the unreadable file and picks up the
  // sequence number from the first segment (which had records 0 and 1).
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
  // Sequence should resume from the readable segment's max (1) + 1 = 2,
  // not reset to 0.
  EXPECT_GE(writer->current_sequence_number(), 2);

  // Restore permissions for cleanup.
  chmod(wal_path.c_str(), 0644);
}

TEST_F(WalWriterTest, CreateHandlesSingleUnreadableWalFile) {
  // Write records to a single segment.
  {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db1")));
    GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db2")));
    EXPECT_EQ(writer->current_sequence_number(), 2);
  }

  // Make the only WAL file unreadable.
  std::string wal_path = test_dir_ + "/wal-000000.log";
  ASSERT_EQ(chmod(wal_path.c_str(), 0000), 0);

  // Create should still succeed — no readable files means sequence starts
  // at 0, but a new segment is opened (segment 1), so the unreadable
  // segment 0 won't be overwritten.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto writer, WalWriter::Create(test_dir_));

  // New records get written to a new segment file.
  GOOGLESQL_EXPECT_OK(writer->Append(MakeRecord("db3")));

  // Restore permissions for cleanup.
  chmod(wal_path.c_str(), 0644);

  // ReadAll can now read both files — the old records plus the new one.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  EXPECT_EQ(records.size(), 3);
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
