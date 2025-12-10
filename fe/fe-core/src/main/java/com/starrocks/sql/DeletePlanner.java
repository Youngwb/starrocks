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

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.TableName;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.load.Load;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.IcebergMergeSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.IntegerType;

import java.util.List;

public class DeletePlanner {
    public ExecPlan plan(DeleteStmt deleteStatement, ConnectContext session) {
        if (deleteStatement.shouldHandledByDeleteHandler()) {
            // executor will use DeleteHandler to handle delete statement
            // so just return empty plan here
            return null;
        }

        // Handle Iceberg delete operation
        if (deleteStatement.isIcebergDelete()) {
            return planIcebergDelete(deleteStatement, session);
        }

        QueryRelation query = deleteStatement.getQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(deleteStatement.getTable());
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            // Non-query must use the strategy assign scan ranges per driver sequence, which local shuffle agg cannot use.
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            Optimizer optimizer = OptimizerFactory.create(OptimizerFactory.initContext(session, columnRefFactory));
            OptExpression optimizedPlan = optimizer.optimize(
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    logicalPlan.getOutputColumn(), columnRefFactory,
                    colNames, TResultSinkType.MYSQL_PROTOCAL, false);
            DescriptorTable descriptorTable = execPlan.getDescTbl();
            TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();

            OlapTable table = (OlapTable) deleteStatement.getTable();
            for (Column column : table.getBaseSchema()) {
                if (column.isKey() || column.isNameWithPrefix(FeConstants.GENERATED_PARTITION_COLUMN_PREFIX)) {
                    SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
                    slotDescriptor.setIsMaterialized(true);
                    slotDescriptor.setType(column.getType());
                    slotDescriptor.setColumn(column);
                    slotDescriptor.setIsNullable(column.isAllowNull());
                } else {
                    continue;
                }
            }
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setType(IntegerType.TINYINT);
            slotDescriptor.setColumn(new Column(Load.LOAD_OP_COLUMN, IntegerType.TINYINT));
            slotDescriptor.setIsNullable(false);
            olapTuple.computeMemLayout();

            List<Long> partitionIds = Lists.newArrayList();
            for (Partition partition : table.getPartitions()) {
                partitionIds.add(partition.getId());
            }
            DataSink dataSink = new OlapTableSink(table, olapTuple, partitionIds, table.writeQuorum(),
                    table.enableReplicatedStorage(), false, false,
                    session.getCurrentComputeResource());
            execPlan.getFragments().get(0).setSink(dataSink);
            if (session.getTxnId() != 0) {
                ((OlapTableSink) dataSink).setIsMultiStatementsTxn(true);
            }

            // if sink is OlapTableSink Assigned to Be execute this sql [cn execute OlapTableSink will crash]
            session.getSessionVariable().setPreferComputeNode(false);
            session.getSessionVariable().setUseComputeNodes(0);
            OlapTableSink olapTableSink = (OlapTableSink) dataSink;
            TableName catalogDbTable = deleteStatement.getTableName();
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getDb(session, catalogDbTable.getCatalog(), catalogDbTable.getDb());
            try {
                olapTableSink.init(session.getExecutionId(), deleteStatement.getTxnId(), db.getId(), session.getExecTimeout());
                olapTableSink.complete();
            } catch (StarRocksException e) {
                throw new SemanticException(e.getMessage());
            }

            if (canUsePipeline) {
                PlanFragment sinkFragment = execPlan.getFragments().get(0);
                if (ConnectContext.get().getSessionVariable().getEnableAdaptiveSinkDop()) {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getSinkDegreeOfParallelism());
                } else {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getParallelExecInstanceNum());
                }
                sinkFragment.setHasOlapTableSink();
                sinkFragment.setForceSetTableSinkDop();
                sinkFragment.setForceAssignScanRangesPerDriverSeq();
                sinkFragment.disableRuntimeAdaptiveDop();
            } else {
                execPlan.getFragments().get(0).setPipelineDop(1);
            }
            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }

    /**
     * Plan Iceberg delete operation.
     * For Iceberg DELETE, we will:
     * 1. Generate SELECT $file_path, $row_pos, $op FROM table WHERE condition
     * 2. Write results to Parquet delete file using IcebergMergeSink
     * <p>
     * The $op column is added to support future MERGE/UPDATE operations.
     * For DELETE operations, $op is always set to 1 (DELETE).
     */
    private ExecPlan planIcebergDelete(DeleteStmt deleteStatement, ConnectContext session) {
        QueryRelation query = deleteStatement.getQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);

        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(deleteStatement.getTable());
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            List<ColumnRefOperator> outputColumns = logicalPlan.getOutputColumn();
            IcebergTable icebergTable = (IcebergTable) deleteStatement.getTable();

            // Create shuffle property based on session variables and table partitioning
            PhysicalPropertySet requiredProperty = createShuffleProperty(icebergTable, outputColumns, session);

            Optimizer optimizer = OptimizerFactory.create(OptimizerFactory.initContext(session, columnRefFactory));
            OptExpression optimizedPlan = optimizer.optimize(
                    logicalPlan.getRoot(),
                    requiredProperty,
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    logicalPlan.getOutputColumn(), columnRefFactory,
                    colNames, TResultSinkType.MYSQL_PROTOCAL, false);

            // Create IcebergMergeSink for delete operation
            DescriptorTable descriptorTable = execPlan.getDescTbl();
            TupleDescriptor mergeTuple = descriptorTable.createTupleDescriptor();

            List<Expr> outputExprs = execPlan.getOutputExprs();
            Preconditions.checkArgument(colNames.size() == outputExprs.size(),
                    "output column size mismatch");
            for (int index = 0; index < colNames.size(); ++index) {
                SlotDescriptor slot = descriptorTable.addSlotDescriptor(mergeTuple);
                slot.setIsMaterialized(true);
                slot.setType(outputExprs.get(index).getType());
                slot.setColumn(new Column(colNames.get(index), outputExprs.get(index).getType()));
                slot.setIsNullable(outputExprs.get(index).isNullable());
            }
            mergeTuple.computeMemLayout();

            // Initialize IcebergMergeSink
            descriptorTable.addReferencedTable(icebergTable);
            DataSink dataSink = new IcebergMergeSink(
                    icebergTable,
                    mergeTuple,
                    session.getSessionVariable()
            );
            execPlan.getFragments().get(0).setSink(dataSink);

            if (canUsePipeline) {
                PlanFragment sinkFragment = execPlan.getFragments().get(0);
                if (ConnectContext.get().getSessionVariable().getEnableAdaptiveSinkDop()) {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getSinkDegreeOfParallelism());
                } else {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getParallelExecInstanceNum());
                }
                sinkFragment.setForceSetTableSinkDop();
                sinkFragment.setForceAssignScanRangesPerDriverSeq();
                sinkFragment.disableRuntimeAdaptiveDop();
            } else {
                execPlan.getFragments().get(0).setPipelineDop(1);
            }
            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }

    /**
     * Create shuffle property for Iceberg DELETE operation.
     * Determines whether to shuffle data by partition columns based on:
     * 1. Whether the table is partitioned
     * 2. Session variable enable_iceberg_delete_shuffle
     * 3. Estimated data size (TODO: implement estimation)
     *
     * @param icebergTable  The Iceberg table
     * @param outputColumns Output columns from the logical plan (includes virtual columns + partition columns)
     * @param session       Connect context with session variables
     * @return PhysicalPropertySet with shuffle requirement or empty property
     */
    private PhysicalPropertySet createShuffleProperty(IcebergTable icebergTable,
                                                      List<ColumnRefOperator> outputColumns,
                                                      ConnectContext session) {
        // Check if table is partitioned
        if (!icebergTable.isPartitioned()) {
            return new PhysicalPropertySet(); // No shuffle for non-partitioned tables
        }

        List<String> partitionColNames = icebergTable.getPartitionColumnNames();
        List<Integer> partitionColumnIDs = Lists.newArrayList();
        for (String partCol : partitionColNames) {
            boolean found = false;
            for (ColumnRefOperator outputCol : outputColumns) {
                if (outputCol.getName().equalsIgnoreCase(partCol)) {
                    found = true;
                    partitionColumnIDs.add(outputCol.getId());
                    break;
                }
            }
            if (!found) {
                // Partition column not in output, cannot shuffle
                return new PhysicalPropertySet();
            }
        }

        // Create HASH distribution spec
        HashDistributionDesc distributionDesc = new HashDistributionDesc(
                partitionColumnIDs,
                HashDistributionDesc.SourceType.SHUFFLE_AGG
        );

        DistributionProperty distributionProperty = DistributionProperty.createProperty(
                DistributionSpec.createHashDistributionSpec(distributionDesc));

        return new PhysicalPropertySet(distributionProperty);
    }
}
