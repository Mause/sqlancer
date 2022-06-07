package sqlancer.duckdb;

import sqlancer.common.query.ExpectedErrors;

public final class DuckDBErrors {

    private DuckDBErrors() {
    }

    public static void addFatalErrors(ExpectedErrors errors) {
        errors.setIsAllowList(false);
        errors.add("INTERNAL");
        errors.add("differs from original result");
    }

}
