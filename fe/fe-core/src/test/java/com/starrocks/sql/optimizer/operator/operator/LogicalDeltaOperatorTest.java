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

package com.starrocks.sql.optimizer.operator.operator;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogicalDeltaOperatorTest {

    @Test
    void testDefaultConstructor() {
        LogicalDeltaOperator op = new LogicalDeltaOperator();
        assertEquals(OperatorType.LOGICAL_DELTA, op.getOpType());
        assertFalse(op.isRootDelta());
        assertNull(op.getActionColumn());
        assertNotNull(op.getMvColumnMapping());
        assertTrue(op.getMvColumnMapping().isEmpty());
    }

    @Test
    void testConstructorWithActionColumn() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator op = new LogicalDeltaOperator(true, actionCol);
        assertTrue(op.isRootDelta());
        assertEquals(actionCol, op.getActionColumn());
        assertTrue(op.getMvColumnMapping().isEmpty());
    }

    @Test
    void testConstructorWithMvColumnMapping() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        ColumnRefOperator col1 = new ColumnRefOperator(2, IntegerType.INT, "c1", false);
        Column mvCol = new Column("c1", IntegerType.INT);
        Map<ColumnRefOperator, Column> mapping = Maps.newHashMap();
        mapping.put(col1, mvCol);

        LogicalDeltaOperator op = new LogicalDeltaOperator(false, actionCol, mapping);
        assertFalse(op.isRootDelta());
        assertEquals(actionCol, op.getActionColumn());
        assertEquals(1, op.getMvColumnMapping().size());
    }

    @Test
    void testMvColumnMappingIsImmutable() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        Map<ColumnRefOperator, Column> mapping = Maps.newHashMap();
        LogicalDeltaOperator op = new LogicalDeltaOperator(true, actionCol, mapping);

        assertThrows(UnsupportedOperationException.class, () ->
                op.getMvColumnMapping().put(
                        new ColumnRefOperator(2, IntegerType.INT, "c2", false),
                        new Column("c2", IntegerType.INT)));
    }

    @Test
    void testNullableActionColumnRejected() {
        ColumnRefOperator nullableCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", true);
        assertThrows(IllegalArgumentException.class, () ->
                new LogicalDeltaOperator(true, nullableCol));
    }

    @Test
    void testBuilder() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator original = new LogicalDeltaOperator(true, actionCol);

        LogicalDeltaOperator.Builder builder = new LogicalDeltaOperator.Builder();
        LogicalDeltaOperator copy = builder.withOperator(original).build();

        assertEquals(original.isRootDelta(), copy.isRootDelta());
        assertEquals(original.getActionColumn(), copy.getActionColumn());
    }

    @Test
    void testBuilderSetters() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator op = new LogicalDeltaOperator.Builder()
                .withOperator(new LogicalDeltaOperator())
                .setRootDelta(true)
                .setActionColumn(actionCol)
                .build();

        assertTrue(op.isRootDelta());
        assertEquals(actionCol, op.getActionColumn());
    }
}
