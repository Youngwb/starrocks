// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;

public class CTEUtils {

    /*
     * Collect CTE operators info, why don't use TransformRule to search?
     *
     * like SQL:
     *  with cte1 as (select * from A), cte2 as (select * from cte1) select * from cte2 union all select * from cte2;
     * Now plan tree which contains CTE:
     *                      Anchor1
     *                    /         \
     *              Produce1        Anchor2
     *                /           /         \
     *             ScanA     Produce2        UNION
     *                         /            /      \
     *                      Consume1  Consume2    Consume2
     *                       /           |           |
     *                     ScanA      Consume1    Consume1
     *                                   |           |
     *                                 ScanA        ScanA
     *
     * Based on the plan structure, there are such advantages:
     * 1. Can push down consume predicate/limit for CTEs with forward dependencies, else need deduction
     *    of predicate and limit in a loop
     * 2. Inline CTE easy, don't need consider column-ref rewrite, call logical rule rewrite after inline
     *
     * But there are also questions:
     * 1. Collect CTE operator is inaccurate, like the demo, the Consume1 only use once in `cte2`,
     *    but it's appear there in the plan tree, it's not friendly to InlineRule check
     * 2. CTE consume cost compute is inaccurate.
     *
     * So we need collect CTE operators info by plan tree, avoid collect repeat consume operator
     *
     * @Todo: move CTE inline into memo optimize phase
     * */
    public static void collectCteOperators(Memo memo, CTEContext context) {
        context.reset();
        collectCteProduce(memo.getRootGroup(), context);
        collectCteConsume(memo.getRootGroup(), context);
    }

    private static void collectCteProduce(Group root, CTEContext context) {
        GroupExpression expression = root.getFirstLogicalExpression();

        if (OperatorType.LOGICAL_CTE_PRODUCE.equals(expression.getOp().getOpType())) {
            // produce
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) expression.getOp();
            OptExpression opt = OptExpression.create(produce);
            opt.attachGroupExpression(expression);
            context.addCTEProduce(produce.getCteId(), opt);

            // costs
            context.addCTEProduceCost(produce.getCteId(), calculateOutputSize(root));
        }

        for (Group group : expression.getInputs()) {
            // reduce tree search
            OperatorType childType = group.getFirstLogicalExpression().getOp().getOpType();
            if (OperatorType.LOGICAL_CTE_PRODUCE.equals(childType) ||
                    OperatorType.LOGICAL_CTE_ANCHOR.equals(childType)) {
                collectCteProduce(group, context);
            }
        }
    }

    private static void collectCteConsume(Group root, CTEContext context) {
        GroupExpression expression = root.getFirstLogicalExpression();

        if (OperatorType.LOGICAL_CTE_CONSUME.equals(expression.getOp().getOpType())) {
            // consumer
            LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) expression.getOp();
            context.addCTEConsume(consume.getCteId());

            // required columns
            ColumnRefSet requiredColumnRef =
                    context.getRequiredColumns().getOrDefault(consume.getCteId(), new ColumnRefSet());
            consume.getCteOutputColumnRefMap().values().forEach(requiredColumnRef::union);
            context.getRequiredColumns().put(consume.getCteId(), requiredColumnRef);

            // inline costs
            context.addCTEConsumeInlineCost(consume.getCteId(), calculateOutputSize(expression.getInputs().get(0)));

            // not ask children
            return;
        }

        for (Group group : expression.getInputs()) {
            collectCteConsume(group, context);
        }
    }

    private static double calculateOutputSize(Group root) {
        if (root.getStatistics() == null) {
            return -1;
        }

        /*
         * Because logical operator can't compute costs in rewrite phase now, so
         * there temporary use rows size
         *
         * */
        return root.getStatistics().getComputeSize();
    }
}
