package sqlancer.duckdb.ast;

import sqlancer.Randomly;

public class DuckDBFunction {
    private String name;
    private int nrArgs;
    private boolean isVariadic;

    public DuckDBFunction(String name, int nrArgs) {
        this(name, nrArgs, false);
    }

    public DuckDBFunction(String name, int nrArgs, boolean isVariadic) {
        this.name = name;
        this.nrArgs = nrArgs;
        this.isVariadic = isVariadic;
    }

    public int getNrArgs() {
        if (isVariadic) {
            return Randomly.smallNumber() + nrArgs;
        } else {
            return nrArgs;
        }
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }
}
