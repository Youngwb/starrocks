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
                                   std::vector<std::string> transform_exprs,
                                   std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                                   std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory,
                                   RuntimeState* state)
        : ConnectorChunkSink(std::move(partition_columns), std::move(partition_column_evaluators),
                             std::move(partition_chunk_writer_factory), state, true),
          _transform_exprs(std::move(transform_exprs)) {}

void IcebergMergeSink::callback_on_commit(const CommitResult& result) {
    push_rollback_action(std::move(result.rollback_action));
    if (result.io_status.ok()) {
        _state->update_num_rows_load_sink(result.file_statistics.record_count);

        TIcebergColumnStats iceberg_column_stats;
        if (result.file_statistics.column_sizes.has_value()) {
            iceberg_column_stats.__set_column_sizes(result.file_statistics.column_sizes.value());
        }
        if (result.file_statistics.value_counts.has_value()) {
            iceberg_column_stats.__set_value_counts(result.file_statistics.value_counts.value());
        }
        if (result.file_statistics.null_value_counts.has_value()) {
            iceberg_column_stats.__set_null_value_counts(result.file_statistics.null_value_counts.value());
        }
        if (result.file_statistics.lower_bounds.has_value()) {
            iceberg_column_stats.__set_lower_bounds(result.file_statistics.lower_bounds.value());
        }
        if (result.file_statistics.upper_bounds.has_value()) {
            iceberg_column_stats.__set_upper_bounds(result.file_statistics.upper_bounds.value());
        }

        TIcebergDataFile iceberg_delete_file;
        iceberg_delete_file.__set_column_stats(iceberg_column_stats);
        iceberg_delete_file.__set_partition_path(PathUtils::get_parent_path(result.location));
        iceberg_delete_file.__set_path(result.location);
        iceberg_delete_file.__set_format(result.format);
        iceberg_delete_file.__set_record_count(result.file_statistics.record_count);
        iceberg_delete_file.__set_file_size_in_bytes(result.file_statistics.file_size);
        iceberg_delete_file.__set_partition_null_fingerprint(result.extra_data);
        iceberg_delete_file.__set_file_content(TIcebergFileContent::POSITION_DELETES);

        TSinkCommitInfo commit_info;
        commit_info.__set_iceberg_data_file(iceberg_delete_file);
        _state->add_sink_commit_info(commit_info);
    }
}

Status IcebergMergeSink::add(const ChunkPtr& chunk) {
    int num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }
    std::string partition = DEFAULT_PARTITION;
    bool partitioned = !_partition_column_names.empty();
    std::vector<int8_t> partition_field_null_list;
    
    LOG(INFO) << "partition columns size: " << _partition_column_names.size();
    for (auto partiton_col : _partition_column_names) {
        LOG(INFO) << "partition column: " << partiton_col;
    }
    // If table is partitioned, compute the partition name
    if (partitioned) {
        ASSIGN_OR_RETURN(partition, HiveUtils::iceberg_make_partition_name(
                                            _partition_column_names, _partition_column_evaluators,
                                            _transform_exprs, chunk.get(), _support_null_partition,
                                            partition_field_null_list));
    }
    
    LOG(INFO) << "IcebergMergeSink: partition=" << partition;
    LOG(INFO) << "chunk debug columns: " << chunk->debug_columns();
    LOG(INFO) << "chunk row 0" << chunk->debug_row(0);
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
    // We only need evaluators for the two columns: file_path and pos
    std::vector<std::unique_ptr<ColumnEvaluator>> column_evaluators_vec;
    column_evaluators_vec.push_back(ctx->column_evaluators[0]->clone());
    column_evaluators_vec.push_back(ctx->column_evaluators[1]->clone());
    auto column_evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>(std::move(column_evaluators_vec));

    // Create location provider for delete files
    // Delete files are stored in metadata/ directory
    auto location_provider = std::make_shared<connector::LocationProvider>(
            ctx->path, print_id(ctx->fragment_context->query_id()), runtime_state->be_number(), driver_id, "parquet");

    std::vector<formats::FileColumnId> file_column_ids(column_names.size());
    // file_path column (index 0)
    file_column_ids[0].field_id = INT32_MAX - 101;  
    // pos column (index 1)  
    file_column_ids[1].field_id = INT32_MAX - 102; 

    // Create Parquet writer factory for delete files
    auto file_writer_factory = std::make_shared<formats::ParquetFileWriterFactory>(
            fs, ctx->compression_type, ctx->options, column_names, column_evaluators,
            file_column_ids, ctx->executor, runtime_state);

    LOG(INFO) << "partition columns size: " << ctx->partition_column_names.size();

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
    return std::make_unique<IcebergMergeSink>(ctx->partition_column_names, ctx->transform_exprs,
                                              ColumnEvaluator::clone(ctx->partition_evaluators),
                                              std::move(partition_chunk_writer_factory), runtime_state);
}

} // namespace starrocks::connector
