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

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "absl/crc/crc32c.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "backend/storage/persistence.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

constexpr char kWalFilePrefix[] = "wal-";
constexpr char kWalFileSuffix[] = ".log";

// Write all bytes to a file descriptor, handling partial writes.
absl::Status WriteAll(int fd, const char* data, size_t size) {
  size_t written = 0;
  while (written < size) {
    ssize_t result = ::write(fd, data + written, size - written);
    if (result < 0) {
      if (errno == EINTR) continue;
      return absl::InternalError(
          absl::StrCat("Failed to write to WAL file: ", strerror(errno)));
    }
    written += result;
  }
  return absl::OkStatus();
}

// Read exactly `size` bytes from fd into `data`. Returns false on EOF/error.
bool ReadExact(int fd, char* data, size_t size) {
  size_t total = 0;
  while (total < size) {
    ssize_t result = ::read(fd, data + total, size - total);
    if (result <= 0) {
      if (result < 0 && errno == EINTR) continue;
      return false;
    }
    total += result;
  }
  return true;
}

// Encode a uint32 in little-endian format.
void EncodeLittleEndian32(uint32_t value, char* buf) {
  buf[0] = static_cast<char>(value & 0xFF);
  buf[1] = static_cast<char>((value >> 8) & 0xFF);
  buf[2] = static_cast<char>((value >> 16) & 0xFF);
  buf[3] = static_cast<char>((value >> 24) & 0xFF);
}

// Decode a uint32 from little-endian format.
uint32_t DecodeLittleEndian32(const char* buf) {
  return static_cast<uint32_t>(static_cast<unsigned char>(buf[0])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(buf[3])) << 24);
}

// Compute CRC32C of a byte buffer.
uint32_t ComputeCrc32c(const std::string& data) {
  absl::crc32c_t crc = absl::ComputeCrc32c(data);
  return static_cast<uint32_t>(crc);
}

// Format a WAL file name from a segment number.
std::string WalFileName(int segment_number) {
  return absl::StrFormat("%s%06d%s", kWalFilePrefix, segment_number,
                         kWalFileSuffix);
}

// Parse segment number from a WAL file name. Returns -1 on failure.
int ParseSegmentNumber(const std::string& filename) {
  // Expected format: wal-NNNNNN.log
  if (filename.size() != 14) return -1;
  if (filename.substr(0, 4) != kWalFilePrefix) return -1;
  if (filename.substr(10) != kWalFileSuffix) return -1;
  std::string num_str = filename.substr(4, 6);
  for (char c : num_str) {
    if (c < '0' || c > '9') return -1;
  }
  return std::stoi(num_str);
}

}  // namespace

WalWriter::WalWriter(const std::string& wal_directory)
    : wal_directory_(wal_directory) {}

WalWriter::~WalWriter() {
  absl::MutexLock lock(&mu_);
  if (fd_ >= 0) {
    ::fsync(fd_);
    ::close(fd_);
    fd_ = -1;
  }
}

absl::StatusOr<std::unique_ptr<WalWriter>> WalWriter::Create(
    const std::string& wal_directory) {
  // Create directory if it doesn't exist.
  if (::mkdir(wal_directory.c_str(), 0755) != 0 && errno != EEXIST) {
    return absl::InternalError(
        absl::StrCat("Failed to create WAL directory '", wal_directory,
                      "': ", strerror(errno)));
  }

  auto writer = std::unique_ptr<WalWriter>(new WalWriter(wal_directory));

  absl::MutexLock lock(&writer->mu_);
  // Scan existing files to determine starting segment and sequence numbers.
  absl::Status scan_status = writer->ScanExistingFiles();
  if (!scan_status.ok()) return scan_status;

  // Open the first (or next) segment file.
  absl::Status open_status = writer->OpenNewSegment();
  if (!open_status.ok()) return open_status;

  return writer;
}

absl::Status WalWriter::ScanExistingFiles() {
  auto files_or = ListWalFiles(wal_directory_);
  if (!files_or.ok()) return files_or.status();
  const auto& files = files_or.value();

  if (files.empty()) {
    segment_number_ = 0;
    next_sequence_number_ = 0;
    return absl::OkStatus();
  }

  // Find the highest segment number.
  int max_segment = -1;
  for (const auto& file : files) {
    int seg = ParseSegmentNumber(file);
    if (seg > max_segment) max_segment = seg;
  }
  segment_number_ = max_segment + 1;

  // Read the last file to find the highest sequence number.
  std::string last_file_path =
      absl::StrCat(wal_directory_, "/", files.back());
  auto records_or = ReadFile(last_file_path);
  if (!records_or.ok()) {
    // If we can't read, start fresh from this segment.
    LOG(WARNING) << "Could not read last WAL file: "
                 << records_or.status().message();
    next_sequence_number_ = 0;
    return absl::OkStatus();
  }

  // Find max sequence number across all records in the last file.
  int64_t max_seq = 0;
  for (const auto& record : records_or.value()) {
    if (record.has_entry() && record.entry().sequence_number() > max_seq) {
      max_seq = record.entry().sequence_number();
    }
  }

  // Also scan earlier files if needed (the last file might be empty).
  if (records_or.value().empty() && files.size() > 1) {
    for (int i = static_cast<int>(files.size()) - 2; i >= 0; --i) {
      std::string path = absl::StrCat(wal_directory_, "/", files[i]);
      auto prev_records_or = ReadFile(path);
      if (!prev_records_or.ok()) continue;
      for (const auto& record : prev_records_or.value()) {
        if (record.has_entry() && record.entry().sequence_number() > max_seq) {
          max_seq = record.entry().sequence_number();
        }
      }
      if (!prev_records_or.value().empty()) break;
    }
  }

  next_sequence_number_ = max_seq + 1;
  return absl::OkStatus();
}

absl::Status WalWriter::OpenNewSegment() {
  if (fd_ >= 0) {
    ::fsync(fd_);
    ::close(fd_);
    fd_ = -1;
  }

  std::string path = CurrentSegmentPath();
  fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) {
    return absl::InternalError(
        absl::StrCat("Failed to open WAL file '", path,
                      "': ", strerror(errno)));
  }
  return absl::OkStatus();
}

std::string WalWriter::CurrentSegmentPath() const {
  return absl::StrCat(wal_directory_, "/", WalFileName(segment_number_));
}

absl::Status WalWriter::Append(const WalRecord& record) {
  absl::MutexLock lock(&mu_);

  // Make a mutable copy so we can set the sequence number.
  WalRecord mutable_record = record;
  if (mutable_record.has_entry()) {
    mutable_record.mutable_entry()->set_sequence_number(
        next_sequence_number_);
  }

  // Serialize the record.
  std::string serialized;
  if (!mutable_record.SerializeToString(&serialized)) {
    return absl::InternalError("Failed to serialize WAL record");
  }

  // Write: [4-byte length][serialized data][4-byte CRC32C]
  uint32_t length = static_cast<uint32_t>(serialized.size());
  char length_buf[4];
  EncodeLittleEndian32(length, length_buf);

  uint32_t crc = ComputeCrc32c(serialized);
  char crc_buf[4];
  EncodeLittleEndian32(crc, crc_buf);

  absl::Status status = WriteAll(fd_, length_buf, 4);
  if (!status.ok()) return status;

  status = WriteAll(fd_, serialized.data(), serialized.size());
  if (!status.ok()) return status;

  status = WriteAll(fd_, crc_buf, 4);
  if (!status.ok()) return status;

  ++next_sequence_number_;
  return absl::OkStatus();
}

absl::Status WalWriter::Sync() {
  absl::MutexLock lock(&mu_);
  if (fd_ >= 0) {
    if (::fsync(fd_) != 0) {
      return absl::InternalError(
          absl::StrCat("Failed to fsync WAL file: ", strerror(errno)));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::string> WalWriter::Rotate() {
  absl::MutexLock lock(&mu_);
  std::string old_path = CurrentSegmentPath();

  ++segment_number_;
  absl::Status status = OpenNewSegment();
  if (!status.ok()) return status;

  return old_path;
}

int64_t WalWriter::current_sequence_number() const {
  absl::MutexLock lock(&mu_);
  return next_sequence_number_;
}

absl::StatusOr<std::vector<std::string>> WalWriter::ListWalFiles(
    const std::string& wal_directory) {
  std::vector<std::string> files;

  DIR* dir = ::opendir(wal_directory.c_str());
  if (dir == nullptr) {
    if (errno == ENOENT) return files;  // Directory doesn't exist yet.
    return absl::InternalError(
        absl::StrCat("Failed to open WAL directory '", wal_directory,
                      "': ", strerror(errno)));
  }

  struct dirent* entry;
  while ((entry = ::readdir(dir)) != nullptr) {
    std::string name(entry->d_name);
    if (ParseSegmentNumber(name) >= 0) {
      files.push_back(name);
    }
  }
  ::closedir(dir);

  std::sort(files.begin(), files.end());
  return files;
}

absl::StatusOr<std::vector<WalRecord>> WalWriter::ReadFile(
    const std::string& path) {
  std::vector<WalRecord> records;

  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return absl::InternalError(
        absl::StrCat("Failed to open WAL file '", path,
                      "': ", strerror(errno)));
  }

  while (true) {
    // Read 4-byte length.
    char length_buf[4];
    if (!ReadExact(fd, length_buf, 4)) break;  // EOF
    uint32_t length = DecodeLittleEndian32(length_buf);

    // Sanity check on length (max 64MB per record).
    if (length > 64 * 1024 * 1024) {
      ::close(fd);
      return absl::DataLossError(
          absl::StrCat("WAL record too large (", length, " bytes) in ", path));
    }

    // Read serialized data.
    std::string data(length, '\0');
    if (!ReadExact(fd, data.data(), length)) {
      ::close(fd);
      return absl::DataLossError(
          absl::StrCat("Truncated WAL record in ", path));
    }

    // Read 4-byte CRC32C.
    char crc_buf[4];
    if (!ReadExact(fd, crc_buf, 4)) {
      ::close(fd);
      return absl::DataLossError(
          absl::StrCat("Truncated CRC in WAL record in ", path));
    }
    uint32_t stored_crc = DecodeLittleEndian32(crc_buf);
    uint32_t computed_crc = ComputeCrc32c(data);

    if (stored_crc != computed_crc) {
      ::close(fd);
      return absl::DataLossError(
          absl::StrCat("CRC mismatch in WAL record in ", path));
    }

    // Deserialize.
    WalRecord record;
    if (!record.ParseFromString(data)) {
      ::close(fd);
      return absl::DataLossError(
          absl::StrCat("Failed to parse WAL record in ", path));
    }

    records.push_back(std::move(record));
  }

  ::close(fd);
  return records;
}

absl::StatusOr<std::vector<WalRecord>> WalWriter::ReadAll(
    const std::string& wal_directory) {
  auto files_or = ListWalFiles(wal_directory);
  if (!files_or.ok()) return files_or.status();

  std::vector<WalRecord> all_records;
  for (const auto& file : files_or.value()) {
    std::string path = absl::StrCat(wal_directory, "/", file);
    auto records_or = ReadFile(path);
    if (!records_or.ok()) return records_or.status();
    for (auto& record : records_or.value()) {
      all_records.push_back(std::move(record));
    }
  }

  return all_records;
}

absl::Status WalWriter::Clear(const std::string& wal_directory) {
  auto files_or = ListWalFiles(wal_directory);
  if (!files_or.ok()) return files_or.status();

  for (const auto& file : files_or.value()) {
    std::string path = absl::StrCat(wal_directory, "/", file);
    if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
      return absl::InternalError(
          absl::StrCat("Failed to delete WAL file '", path,
                        "': ", strerror(errno)));
    }
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
