package sqlancer.duckdb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.ast.newast.NewTernaryNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBDataType;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBFunction;

public final class DuckDBExpressionGenerator extends UntypedExpressionGenerator<Node<DuckDBExpression>, DuckDBColumn> {

    private final DuckDBGlobalState globalState;

    public DuckDBExpressionGenerator(DuckDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, CAST, FUNC, BETWEEN, CASE,
        IN, COLLATE, LIKE_ESCAPE
    }

    @Override
    protected Node<DuckDBExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            DuckDBAggregateFunction aggregate = DuckDBAggregateFunction.getRandom();
            allowAggregates = false;
            return new NewFunctionNode<>(generateExpressions(aggregate.getNrArgs(), depth + 1), aggregate);
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        if (!globalState.getDbmsSpecificOptions().testCollate) {
            possibleOptions.remove(Expression.COLLATE);
        }
        if (!globalState.getDbmsSpecificOptions().testFunctions) {
            possibleOptions.remove(Expression.FUNC);
        }
        if (!globalState.getDbmsSpecificOptions().testCasts) {
            possibleOptions.remove(Expression.CAST);
        }
        if (!globalState.getDbmsSpecificOptions().testBetween) {
            possibleOptions.remove(Expression.BETWEEN);
        }
        if (!globalState.getDbmsSpecificOptions().testIn) {
            possibleOptions.remove(Expression.IN);
        }
        if (!globalState.getDbmsSpecificOptions().testCase) {
            possibleOptions.remove(Expression.CASE);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryComparisons) {
            possibleOptions.remove(Expression.BINARY_COMPARISON);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryLogicals) {
            possibleOptions.remove(Expression.BINARY_LOGICAL);
        }
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case COLLATE:
            return new NewUnaryPostfixOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
                    DuckDBCollate.getRandom());
        case UNARY_PREFIX:
            return new NewUnaryPrefixOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
                    DuckDBUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new NewUnaryPostfixOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
                    DuckDBUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            Operator op = DuckDBBinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = DuckDBBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new NewBinaryOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), DuckDBBinaryArithmeticOperator.getRandom());
        case CAST:
            return new DuckDBCastOperation(generateExpression(depth + 1),
                    DuckDBCompositeDataType.getRandomWithoutNull());
        case FUNC:
            DuckDBFunction func = globalState.getRandomFunction();
            return new NewFunctionNode<DuckDBExpression, DuckDBFunction>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new NewBetweenOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new NewInOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new NewCaseOperatorNode<DuckDBExpression>(generateExpression(depth + 1),
                    generateExpressions(nr, depth + 1), generateExpressions(nr, depth + 1),
                    generateExpression(depth + 1));
        case LIKE_ESCAPE:
            return new NewTernaryNode<DuckDBExpression>(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), "LIKE", "ESCAPE");
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Node<DuckDBExpression> generateColumn() {
        DuckDBColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<DuckDBExpression, DuckDBColumn>(column);
    }

    @Override
    public Node<DuckDBExpression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return DuckDBConstant.createNullConstant();
        }
        DuckDBDataType type = DuckDBDataType.getRandomWithoutNull();
        switch (type) {
        case INT:
            if (!globalState.getDbmsSpecificOptions().testIntConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        case UINT:
            if (!globalState.getDbmsSpecificOptions().testIntConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        case DATE:
            if (!globalState.getDbmsSpecificOptions().testDateConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createDateConstant(globalState.getRandomly().getInteger());
        case TIMESTAMP:
            if (!globalState.getDbmsSpecificOptions().testTimestampConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        case VARCHAR:
            if (!globalState.getDbmsSpecificOptions().testStringConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOLEAN:
            if (!globalState.getDbmsSpecificOptions().testBooleanConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            if (!globalState.getDbmsSpecificOptions().testFloatConstants) {
                throw new IgnoreMeException();
            }
            return DuckDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
        default:
            throw new AssertionError();
        }
    }

    @Override
    public List<Node<DuckDBExpression>> generateOrderBys() {
        List<Node<DuckDBExpression>> expr = super.generateOrderBys();
        List<Node<DuckDBExpression>> newExpr = new ArrayList<>(expr.size());
        for (Node<DuckDBExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new NewOrderingTerm<>(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    public static class DuckDBCastOperation extends NewUnaryPostfixOperatorNode<DuckDBExpression> {

        public DuckDBCastOperation(Node<DuckDBExpression> expr, DuckDBCompositeDataType type) {
            super(expr, new Operator() {

                @Override
                public String getTextRepresentation() {
                    return "::" + type.toString();
                }
            });
        }

    }

    public enum DuckDBAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), STRING_AGG(1), FIRST(1), SUM(1), STDDEV_SAMP(1), STDDEV_POP(1), VAR_POP(1),
        VAR_SAMP(1), COVAR_POP(1), COVAR_SAMP(1);

        private int nrArgs;

        DuckDBAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static DuckDBAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public enum DuckDBUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        DuckDBUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static DuckDBUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static final class DuckDBCollate implements Operator {

        private final String textRepr;

        private DuckDBCollate(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return "COLLATE " + textRepr;
        }

        public static DuckDBCollate getRandom() {
            return new DuckDBCollate(DuckDBTableGenerator.getRandomCollate());
        }

    }

    public enum DuckDBUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        DuckDBUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static DuckDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum DuckDBBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum DuckDBBinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"), LSHIFT("<<"), RSHIFT(">>");

        private String textRepr;

        DuckDBBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum DuckDBBinaryComparisonOperator implements Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        LIKE("LIKE"), NOT_LIKE("NOT LIKE"), SIMILAR_TO("SIMILAR TO"), NOT_SIMILAR_TO("NOT SIMILAR TO"),
        REGEX_POSIX("~"), REGEX_POSIT_NOT("!~"), IS_DISTINCT_FROM("IS DISTINCT FROM"), IS_NOT_DISTINCT_FROM("IS NOT DISTINCT FROM");

        private String textRepr;

        DuckDBBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public NewFunctionNode<DuckDBExpression, DuckDBAggregateFunction> generateArgsForAggregate(
            DuckDBAggregateFunction aggregateFunction) {
        return new NewFunctionNode<DuckDBExpression, DuckDBExpressionGenerator.DuckDBAggregateFunction>(
                generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public Node<DuckDBExpression> generateAggregate() {
        DuckDBAggregateFunction aggrFunc = DuckDBAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public Node<DuckDBExpression> negatePredicate(Node<DuckDBExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, DuckDBUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<DuckDBExpression> isNull(Node<DuckDBExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, DuckDBUnaryPostfixOperator.IS_NULL);
    }

}
