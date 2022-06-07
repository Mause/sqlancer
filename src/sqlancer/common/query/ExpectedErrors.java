package sqlancer.common.query;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * This class represents the errors that executing a statement might result in. For example, an INSERT statement might
 * result in an error "UNIQUE constraint violated" when it attempts to insert a duplicate value in a column declared as
 * UNIQUE.
 */
public class ExpectedErrors {

    private final Set<String> errors = new HashSet<>();
    //! Whether or not this is an allow list or a block list
    //! In case of an allow list, errors are expected ONLY IF they occur in the errors list (default)
    //! In case of a block list, errors are expected UNLESS they occur in the errors list
    private boolean isAllowList = true;

    public ExpectedErrors add(String error) {
        if (error == null) {
            throw new IllegalArgumentException();
        }
        errors.add(error);
        return this;
    }

    public void setIsAllowList(boolean isAllowList) {
        this.isAllowList = isAllowList;
    }

    /**
     * Checks whether the error message (e.g., returned by the DBMS under test) contains any of the added error
     * messages.
     *
     * @param error
     *            the error message
     *
     * @return whether the error message contains any of the substrings specified as expected errors
     */
    public boolean errorIsExpected(String error) {
        if (error == null) {
            throw new IllegalArgumentException();
        }
        for (String s : errors) {
            if (error.contains(s)) {
                return isAllowList ? true : false;
            }
        }
        return isAllowList ? false : true;
    }

    public ExpectedErrors addAll(Collection<String> list) {
        errors.addAll(list);
        return this;
    }

    public static ExpectedErrors from(String... errors) {
        ExpectedErrors expectedErrors = new ExpectedErrors();
        for (String error : errors) {
            expectedErrors.add(error);
        }
        return expectedErrors;
    }

}
