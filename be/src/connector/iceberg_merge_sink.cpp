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

#include <algorithm>
#include <future>
#include <fmt/format.h>

#include "column/datum.h"
#include "connector/async_flush_stream_poller.h"
#include "connector/partition_chunk_writer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/sorting/sorting.h"
#include "exprs/expr.h"
#include "formats/column_evaluator.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/utils.h"
#include "gutil/strings/fastmem.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"
#include "util/url_coding.h"
#include "utils.h"

namespace starrocks::connector {

// IcebergMergeSink implementation
IcebergMergeSink::IcebergMergeSink(std::vector<std::string> partition_columns,
                                   std::vector<std::string> transform_exprs,
                                   std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                                   std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory,
                                   RuntimeState* state,
                                   const std::unordered_map<std::string, SlotId>& column_slot_map)
        : ConnectorChunkSink(std::move(partition_columns), std::move(partition_column_evaluators),
                             std::move(partition_chunk_writer_factory), state, true),
          _transform_exprs(std::move(transform_exprs)),
          _column_slot_map(column_slot_map),
          _state(state) {}

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
    LOG(INFO) << "chunk num columns: " << chunk->num_columns();

    // Find file_path column slot_id from the mapping
    SlotId file_path_slot_id = -1;
    auto it = _column_slot_map.find("$file_path");
    if (it != _column_slot_map.end()) {
        file_path_slot_id = it->second;
    }

    if (file_path_slot_id == -1) {
        return Status::InternalError("Could not find file_path column in column_slot_map");
    }

    LOG(INFO) << "file_path_slot_id: " << file_path_slot_id;

    // Get file_path column directly using slot_id (from FE)
    ColumnPtr file_path_column = chunk->get_column_by_slot_id(file_path_slot_id);
    if (file_path_column == nullptr) {
        return Status::InternalError(fmt::format("Could not find file_path column with slot_id {} in chunk",
                                                  file_path_slot_id));
    }

    // If it's a nullable column, get the data column
    Column* data_column = file_path_column.get();
    if (file_path_column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(file_path_column.get());
        data_column = nullable_column->data_column().get();
    }

    // Verify the data column is a BinaryColumn
    auto* binary_column = down_cast<BinaryColumn*>(data_column);

    // Group rows by file_path to create file-level delete files
    // Map: file_path -> row indices
    std::unordered_map<std::string, std::vector<uint32_t>> file_path_to_indices;
    for (int i = 0; i < num_rows; ++i) {
        // Handle NULL values - skip them as they don't reference any file
        if (file_path_column->is_null(i)) {
            LOG(WARNING) << "Encountered NULL file_path at row " << i << ", skipping";
            continue;
        }
        auto file_path = binary_column->get_slice(i).to_string();
        file_path_to_indices[file_path].push_back(i);
    }

    // Write separate delete files for each file_path
    for (auto& [file_path, indices] : file_path_to_indices) {
        // Create a chunk containing only rows for this file_path
        ChunkPtr file_chunk = chunk->clone_empty_with_slot();
        file_chunk->append_selective(*chunk, indices.data(), 0, indices.size());

        LOG(INFO) << "Writing delete file for data file: " << file_path
                  << " with " << indices.size() << " delete rows";

        // Write using file-level writer for this (partition, file_path)
        RETURN_IF_ERROR(write_file_level_chunk(partition, partition_field_null_list, file_chunk, file_path));
    }

    return Status::OK();
}

Status IcebergMergeSink::finish() {
    LOG(INFO) << "IcebergMergeSink: finish";
    // Flush and finish all file-level writers
    for (auto& [key, writer] : _file_writers) {
        RETURN_IF_ERROR(writer->flush());
    }
    for (auto& [key, writer] : _file_writers) {
        RETURN_IF_ERROR(writer->wait_flush());
    }
    for (auto& [key, writer] : _file_writers) {
        RETURN_IF_ERROR(writer->finish());
    }
    LOG(INFO) << "IcebergMergeSink: finish completed";
    return Status::OK();
}

bool IcebergMergeSink::is_finished() {
    LOG(INFO) << "IcebergMergeSink: is_finished";
    for (auto& [key, writer] : _file_writers) {
        if (!writer->is_finished()) {
            LOG(INFO) << "IcebergMergeSink: is_finished false";
            return false;
        }
    }
    LOG(INFO) << "IcebergMergeSink: is_finished true";
    return true;
}

// IcebergMergeSinkProvider implementation
StatusOr<std::unique_ptr<ConnectorChunkSink>> IcebergMergeSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<IcebergMergeSinkContext>(context);
    if (ctx == nullptr) {
        return Status::InternalError("IcebergMergeSinkProvider: context is not IcebergMergeSinkContext");
    }

    auto runtime_state = ctx->fragment_context->runtime_state();

    TupleDescriptor* tuple_desc = runtime_state->desc_tbl().get_tuple_descriptor(ctx->tuple_desc_id);
    if (tuple_desc == nullptr) {
        return Status::InternalError(fmt::format("Failed to find tuple descriptor with id {}", ctx->tuple_desc_id));
    }
    LOG(INFO) << "tuple_desc: " << tuple_desc->debug_string();
    DCHECK(tuple_desc->slots().size() == ctx->output_exprs.size());

    // Build column name to slot_id mapping from output expressions
    // For DELETE operations, we expect columns: file_path, pos, (optional) op
    for (size_t i = 0; i < tuple_desc->slots().size() && i < ctx->output_exprs.size(); ++i) {
        const auto* slot = tuple_desc->slots()[i];
        const auto& expr = ctx->output_exprs[i];

        // Check if this expression contains a slot reference (should be a direct slot reference for delete columns)
        for (const auto& node : expr.nodes) {
            if (node.node_type == TExprNodeType::SLOT_REF) {
                // Extract column name from slot descriptor
                std::string col_name = slot->col_name();
                SlotId slot_id = node.slot_ref.slot_id;

                ctx->column_slot_map[col_name] = slot_id;
                LOG(INFO) << "Mapped column '" << col_name << "' to slot_id " << slot_id;
                break;
            }
        }
    }

    // Verify we found the required columns
    LOG(INFO) << "Built column_slot_map with " << ctx->column_slot_map.size() << " entries";

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

    // Initialize sort ordering for position delete files (required by Iceberg spec)
    // Sort by: file_path (col 0) ASC, then pos (col 1) ASC
    std::shared_ptr<SortOrdering> sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0, 1};  // file_path, pos
    sort_ordering->sort_descs.descs.emplace_back(true, false);   // file_path: ASC, nulls last
    sort_ordering->sort_descs.descs.emplace_back(true, false);   // pos: ASC, nulls last

    // Create partition chunk writer factory
    std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory;

    // Create a custom tuple descriptor with only file_path and pos columns
    // Use DescriptorTbl::create to properly create tuple and slot descriptors
    // since TupleDescriptor::add_slot is private
    TSlotDescriptorBuilder slot_builder;
    TTupleDescriptorBuilder tuple_builder;

    // Add file_path column (STRING/VARCHAR)
    tuple_builder.add_slot(
        slot_builder.id(1)
            .type(TYPE_VARCHAR)
            .nullable(true)
            .is_materialized(true)
            .column_name("file_path")
            .build());

    // Add pos column (BIGINT)
    tuple_builder.add_slot(
        slot_builder.id(2)
            .type(TYPE_BIGINT)
            .nullable(true)
            .is_materialized(true)
            .column_name("pos")
            .build());

    // Create descriptor table and tuple
    TDescriptorTableBuilder desc_tbl_builder;
    tuple_builder.build(&desc_tbl_builder);
    TDescriptorTable t_desc_tbl = desc_tbl_builder.desc_tbl();

    // Use DescriptorTbl::create to properly create the tuple and slot descriptors
    // This handles the private add_slot method for us
    DescriptorTbl* desc_tbl = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(runtime_state, runtime_state->obj_pool(), t_desc_tbl,
                                                        &desc_tbl, config::vector_chunk_size));

    // Extract the tuple descriptor we just created (it will be the first one)
    TupleDescriptor* delete_tuple_desc = desc_tbl->get_tuple_descriptor(0);
    DCHECK(delete_tuple_desc != nullptr);
    DCHECK_EQ(delete_tuple_desc->slots().size(), 2);

    auto writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
            {file_writer_factory, location_provider, ctx->max_file_size, ctx->partition_column_names.empty()},
            fs,
            ctx->fragment_context,
            delete_tuple_desc,
            column_evaluators,
            sort_ordering});
    partition_chunk_writer_factory = std::make_unique<SpillPartitionChunkWriterFactory>(writer_ctx);
    
    // Create the merge sink
    return std::make_unique<IcebergMergeSink>(ctx->partition_column_names, ctx->transform_exprs,
                                              ColumnEvaluator::clone(ctx->partition_evaluators),
                                              std::move(partition_chunk_writer_factory), runtime_state,
                                              ctx->column_slot_map);
}

Status IcebergMergeSink::write_file_level_chunk(const std::string& partition,
                                                const std::vector<int8_t>& partition_field_null_list,
                                                const ChunkPtr& chunk,
                                                const std::string& file_path) {
    // Key: (partition, file_path)
    auto key = std::make_pair(partition, file_path);

    auto it = _file_writers.find(key);
    if (it != _file_writers.end()) {
        return it->second->write(chunk);
    }

    // Create new writer for this (partition, file_path)
    auto writer = _partition_chunk_writer_factory->create(partition, partition_field_null_list);

    auto commit_callback = [this](const CommitResult& r) { this->callback_on_commit(r); };
    auto error_handler = [this](const Status& s) { this->set_status(s); };
    writer->set_commit_callback(commit_callback);
    writer->set_error_handler(error_handler);
    writer->set_io_poller(_io_poller);

    RETURN_IF_ERROR(writer->init());

    _file_writers[key] = writer;
    return writer->write(chunk);
}

} // namespace starrocks::connector
