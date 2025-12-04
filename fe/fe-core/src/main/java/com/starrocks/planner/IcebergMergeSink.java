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

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TIcebergTableSink;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import org.apache.iceberg.Table;

import static com.starrocks.sql.ast.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;
import static org.aspectj.lang.reflect.DeclareAnnotation.Kind.Type;

/**
 * IcebergMergeSink is used to write merge-on-read operations (MERGE, UPDATE, DELETE) to Iceberg tables.
 * It supports position deletes and equality deletes for MERGE operations.
 *
 * For DELETE operations: receives __file_path__ and __pos__ columns
 * For MERGE/UPDATE operations: receives __file_path__, __pos__, and operation columns
 */
public class IcebergMergeSink extends DataSink {
    public final static int ICEBERG_MERGE_SINK_MAX_DOP = 32;
    protected final TupleDescriptor desc;
    private final long targetTableId;
    private final String tableLocation;
    private final String dataLocation;
    private final String deleteLocation;
    private final String compressionType;
    private final long targetMaxFileSize;
    private final String tableIdentifier;
    private final CloudConfiguration cloudConfiguration;
    private String targetBranch;

    /**
     * Constructor for IcebergMergeSink
     * @param icebergTable The target Iceberg table
     * @param desc Tuple descriptor containing operation columns
     * @param sessionVariable Session variables for configuration
     */
    public IcebergMergeSink(IcebergTable icebergTable, TupleDescriptor desc,
                            SessionVariable sessionVariable) {
        Table nativeTable = icebergTable.getNativeTable();
        this.desc = desc;
        this.tableLocation = nativeTable.location();
        this.dataLocation = IcebergUtil.tableDataLocation(nativeTable);
        // Delete files are stored in metadata/ directory
        this.deleteLocation = this.tableLocation + "/metadata";
        this.targetTableId = icebergTable.getId();
        this.tableIdentifier = icebergTable.getUUID();
        this.compressionType = sessionVariable.getConnectorSinkCompressionCodec();
        this.targetMaxFileSize = sessionVariable.getConnectorSinkTargetMaxFileSize() > 0 ?
            sessionVariable.getConnectorSinkTargetMaxFileSize() : 1024L * 1024 * 1024;

        String catalogName = icebergTable.getCatalogName();
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalogName));

        // Try to set for tabular
        CloudConfiguration tabularTempCloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForVendedCredentials(icebergTable.getNativeTable().io().properties(),
                        this.tableLocation);
        if (tabularTempCloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            this.cloudConfiguration = tabularTempCloudConfiguration;
        } else {
            this.cloudConfiguration = connector.getMetadata().getCloudConfiguration();
        }

        Preconditions.checkState(cloudConfiguration != null,
                String.format("cloudConfiguration of catalog %s should not be null", catalogName));

        // Validate tuple descriptor contains required columns
        validateMergeTuple(desc);
    }

    /**
     * Validate that the tuple descriptor contains required columns
     * @param desc The tuple descriptor to validate
     */
    private void validateMergeTuple(TupleDescriptor desc) {
        boolean hasFilePathColumn = false;
        boolean hasPosColumn = false;

        for (SlotDescriptor slot : desc.getSlots()) {
            if (slot.getColumn() != null) {
                String colName = slot.getColumn().getName();
                if ("__file_path__".equals(colName)) {
                    hasFilePathColumn = true;
                    Preconditions.checkState(slot.getType().equals(StringType.STRING),
                            "__file_path__ column must be of type STRING");
                } else if ("__pos__".equals(colName)) {
                    hasPosColumn = true;
                    Preconditions.checkState(slot.getType().equals(IntegerType.BIGINT),
                            "__pos__ column must be of type BIGINT");
                }
            }
        }

        Preconditions.checkState(hasFilePathColumn && hasPosColumn,
                "IcebergMergeSink requires __file_path__ and __pos__ columns in tuple descriptor");
    }


    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "ICEBERG MERGE SINK");
        strBuilder.append("\n");
        strBuilder.append(prefix + "  TABLE: " + tableIdentifier + "\n");
        strBuilder.append(prefix + "  LOCATION: " + tableLocation + "\n");
        strBuilder.append(prefix + "  DELETE LOCATION: " + deleteLocation + "\n");
        strBuilder.append(prefix + "  TUPLE ID: " + desc.getId() + "\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.ICEBERG_MERGE_SINK);
        TIcebergTableSink tIcebergTableSink = new TIcebergTableSink();
        tIcebergTableSink.setTarget_table_id(targetTableId);
        tIcebergTableSink.setTuple_id(desc.getId().asInt());
        tIcebergTableSink.setLocation(tableLocation);
        // For merge sink, we set both data and delete locations
        tIcebergTableSink.setData_location(dataLocation);
        tIcebergTableSink.setFile_format("parquet"); // Delete files are always parquet
        tIcebergTableSink.setIs_static_partition_sink(false);
        TCompressionType compression = PARQUET_COMPRESSION_TYPE_MAP.get(compressionType);
        tIcebergTableSink.setCompression_type(compression);
        tIcebergTableSink.setTarget_max_file_size(targetMaxFileSize);
        com.starrocks.thrift.TCloudConfiguration tCloudConfiguration = new com.starrocks.thrift.TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        tIcebergTableSink.setCloud_configuration(tCloudConfiguration);

        tDataSink.setIceberg_merge_sink(tIcebergTableSink);
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
