// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>

#include "column/chunk.h"
#include "common/status.h"
#include "connector/connector.h"
#include "connector/partition_chunk_writer.h"
#include "formats/file_writer.h"

namespace starrocks {
class RuntimeState;

namespace connector {
struct SortOrdering;

// Context for IcebergMergeSink
// Contains configuration needed to write delete files
struct IcebergMergeSinkContext : public ConnectorChunkSinkContext {
    std::string path;
    std::vector<std::string> column_names;
    std::vector<std::string> partition_column_names;
    std::vector<std::string> transform_exprs;
    std::vector<std::unique_ptr<ColumnEvaluator>> column_evaluators;
    std::vector<std::unique_ptr<ColumnEvaluator>> partition_evaluators;

    // Compression type for Parquet files
    starrocks::TCompressionType::type compression_type;
    std::map<std::string, std::string> options;

    // Maximum size of delete files before rolling to new file
    int64_t max_file_size = 128L * 1024 * 1024;  // 128MB default

    // Tuple descriptor id (contains 3 columns: file_path, pos, op)
    int tuple_desc_id = -1;

    // Cloud configuration (S3/HDFS credentials)
    starrocks::TCloudConfiguration cloud_configuration;

    // Fragment context (for async operations)
    pipeline::FragmentContext* fragment_context = nullptr;

    // Thread pool for async IO
    PriorityThreadPool* executor = nullptr;

    // Sort ordering (required by Iceberg spec: sorted by file_path, then pos)
    std::shared_ptr<SortOrdering> sort_ordering;
};

// IcebergMergeSinkProvider creates IcebergMergeSink for writing position delete files
// This is used for DELETE operations in Iceberg Merge-On-Read pattern
class IcebergMergeSinkProvider final : public ConnectorChunkSinkProvider {
public:
    ~IcebergMergeSinkProvider() override = default;

    // Create a sink for writing delete files
    StatusOr<std::unique_ptr<ConnectorChunkSink>> create_chunk_sink(
            std::shared_ptr<ConnectorChunkSinkContext> context,
            int32_t driver_id) override;
};

// IcebergMergeSink writes position delete files for Iceberg Merge-On-Read operations.
// It receives chunks with columns: file_path, row_position and writes them to Parquet delete files.
class IcebergMergeSink final : public ConnectorChunkSink {
public:
    IcebergMergeSink(std::vector<std::string> partition_columns,
                     std::vector<std::string> transform_exprs,
                     std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                     std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory,
                     RuntimeState* state);
    ~IcebergMergeSink() override = default;
    
    void callback_on_commit(const CommitResult& result) override;

    Status add(const ChunkPtr& chunk) override;
    
private:
    std::vector<std::string> _transform_exprs;
};

} // namespace connector
} // namespace starrocks
