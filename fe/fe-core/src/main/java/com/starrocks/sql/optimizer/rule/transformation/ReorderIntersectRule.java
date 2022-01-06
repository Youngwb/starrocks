// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ReorderIntersectRule extends TransformationRule {
    public ReorderIntersectRule() {
        super(RuleType.TF_INTERSECT_REORDER, Pattern.create(OperatorType.LOGICAL_INTERSECT).
                addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalIntersectOperator intersectOperator = (LogicalIntersectOperator) input.getOp();
        OptExpression intersectOpt = input.getGroupExpression().getGroup().extractLogicalTree();
        OptExpression o = intersectOpt.getInputs().stream().min(
                Comparator.comparingDouble(c -> c.getGroupExpression().getGroup().getStatistics().getOutputRowCount()))
                .get();

        int index = intersectOpt.getInputs().indexOf(o);

        List<OptExpression> newChildList = new ArrayList<>();
        List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
        newChildList.add(intersectOpt.getInputs().get(index));
        childOutputColumns.add(intersectOperator.getChildOutputColumns().get(index));

        for (int idx = 0; idx < intersectOpt.arity(); ++idx) {
            OptExpression child = intersectOpt.inputAt(idx);
            if (!child.equals(o)) {
                newChildList.add(child);
                childOutputColumns.add(intersectOperator.getChildOutputColumns().get(idx));
            }
        }
        return Lists.newArrayList(OptExpression.create(
                new LogicalIntersectOperator.Builder().withOperator(intersectOperator)
                        .setChildOutputColumns(childOutputColumns).build(), newChildList));
    }

}
