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

#include "connector/iceberg_merge_sink.h"

#include <future>

#include "column/datum.h"
#include "connector/async_flush_stream_poller.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/expr.h"
#include "formats/column_evaluator.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/utils.h"
#include "util/url_coding.h"
#include "utils.h"

namespace starrocks::connector {

// IcebergMergeSink implementation
IcebergMergeSink::IcebergMergeSink(std::vector<std::string> partition_columns,
                                   std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                                   std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory,
                                   RuntimeState* state)
        : ConnectorChunkSink(std::move(partition_columns), std::move(partition_column_evaluators),
                             std::move(partition_chunk_writer_factory), state, true) {}

Status IcebergMergeSink::add(const ChunkPtr& chunk) {
    std::string partition = DEFAULT_PARTITION;
    bool partitioned = !_partition_column_names.empty();
    std::vector<int8_t> partition_field_null_list;

    // If table is partitioned, compute the partition name
    if (partitioned) {
        ASSIGN_OR_RETURN(partition, HiveUtils::iceberg_make_partition_name(
                                            _partition_column_names, _partition_column_evaluators,
                                            std::vector<std::string>(), chunk.get(), _support_null_partition,
                                            partition_field_null_list));
    }

    // Write the chunk to delete file
    RETURN_IF_ERROR(ConnectorChunkSink::write_partition_chunk(partition, partition_field_null_list, chunk));
    return Status::OK();
}

// IcebergMergeSinkProvider implementation
StatusOr<std::unique_ptr<ConnectorChunkSink>> IcebergMergeSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<IcebergMergeSinkContext>(context);
    if (ctx == nullptr) {
        return Status::InternalError("IcebergMergeSinkProvider: context is not IcebergMergeSinkContext");
    }

    auto runtime_state = ctx->fragment_context->runtime_state();

    // Create filesystem
    std::shared_ptr<FileSystem> fs = FileSystem::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_configuration))
                                               .value();

    // For delete files, we only need file_path and row_position columns
    // These should already be in the chunk when it reaches the sink
    std::vector<std::string> column_names = {"file_path", "pos"};

    // Clone column evaluators if any
    auto column_evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>(
            ColumnEvaluator::clone(ctx->column_evaluators));

    // Create location provider for delete files
    // Delete files are stored in metadata/ directory
    auto location_provider = std::make_shared<connector::LocationProvider>(
            ctx->delete_location.empty() ? ctx->path + "/metadata" : ctx->delete_location,
            print_id(ctx->fragment_context->query_id()), runtime_state->be_number(), driver_id, "parquet");

    // Create Parquet writer factory for delete files
    auto file_writer_factory = std::make_shared<formats::ParquetFileWriterFactory>(
            fs, ctx->compression_type, ctx->options, column_names, column_evaluators,
            std::vector<formats::FileColumnId>(), ctx->executor, runtime_state);

    // Create partition chunk writer factory
    std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory;
    if (config::enable_connector_sink_spill) {
        auto writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                {file_writer_factory, location_provider, ctx->max_file_size, ctx->partition_column_names.empty()},
                fs,
                ctx->fragment_context,
                runtime_state->desc_tbl().get_tuple_descriptor(ctx->tuple_desc_id),
                column_evaluators,
                nullptr});
        partition_chunk_writer_factory = std::make_unique<SpillPartitionChunkWriterFactory>(writer_ctx);
    } else {
        auto writer_ctx =
                std::make_shared<BufferPartitionChunkWriterContext>(BufferPartitionChunkWriterContext{
                        {file_writer_factory, location_provider, ctx->max_file_size, ctx->partition_column_names.empty()}});
        partition_chunk_writer_factory = std::make_unique<BufferPartitionChunkWriterFactory>(writer_ctx);
    }

    // Create the merge sink
    return std::make_unique<IcebergMergeSink>(ctx->partition_column_names,
                                              ColumnEvaluator::clone(ctx->partition_evaluators),
                                              std::move(partition_chunk_writer_factory), runtime_state);
}

} // namespace starrocks::connector
