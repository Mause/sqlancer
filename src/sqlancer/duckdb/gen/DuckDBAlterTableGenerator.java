package sqlancer.duckdb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;

public final class DuckDBAlterTableGenerator {

    private DuckDBAlterTableGenerator() {
    }

    enum Action {
        ADD_COLUMN, ALTER_COLUMN, DROP_COLUMN
    }

    public static SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(table.getName());
        sb.append(" ");
        Action action = Randomly.fromOptions(Action.values());
        switch (action) {
        case ADD_COLUMN:
            sb.append("ADD COLUMN ");
            String columnName = table.getFreeColumnName();
            sb.append(columnName);
            sb.append(" ");
            sb.append(DuckDBCompositeDataType.getRandomWithoutNull().toString());
            break;
        case ALTER_COLUMN:
            sb.append("ALTER COLUMN ");
            sb.append(table.getRandomColumn().getName());
            sb.append(" SET DATA TYPE ");
            sb.append(DuckDBCompositeDataType.getRandomWithoutNull().toString());
            if (Randomly.getBoolean()) {
                sb.append(" USING ");
                DuckDBErrors.addFatalErrors(errors);
                sb.append(DuckDBToStringVisitor.asString(gen.generateExpression()));
            }
            break;
        case DROP_COLUMN:
            sb.append("DROP COLUMN ");
            sb.append(table.getRandomColumn().getName());
            break;
        default:
            throw new AssertionError(action);
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
