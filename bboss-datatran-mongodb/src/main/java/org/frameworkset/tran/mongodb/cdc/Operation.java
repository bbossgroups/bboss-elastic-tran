package org.frameworkset.tran.mongodb.cdc;

/**
 * The constants for the values for the  field in the message envelope.
 */
public enum Operation {
    /**
     * The operation that read the current state of a record, most typically during snapshots.
     */
    READ("r"),
    /**
     * An operation that resulted in a new record being created in the source.
     */
    CREATE("c"),
    /**
     * An operation that resulted in an existing record being updated in the source.
     */
    UPDATE("u"),
    /**
     * An operation that resulted in an existing record being removed from or deleted in the source.
     */
    DELETE("d"),
    /**
     * An operation that resulted in an existing table being truncated in the source.
     */
    TRUNCATE("t"),
    /**
     * An operation that resulted in a generic message
     */
    MESSAGE("m");

    private final String code;

    Operation(String code) {
        this.code = code;
    }

    public static Operation forCode(String code) {
        for (Operation op : Operation.values()) {
            if (op.code().equalsIgnoreCase(code)) {
                return op;
            }
        }
        return null;
    }

    public String code() {
        return code;
    }
}
