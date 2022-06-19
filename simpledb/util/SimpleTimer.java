package simpledb.util;

public class SimpleTimer {

    private static Long instant = -1L;

    public static synchronized void setInstant(Long inst) {
        instant = inst;
    }

    public static synchronized Long getInstant() {
        instant = instant + 1;
        return instant;
    }

    public static synchronized Long reset() {
        instant = -1L;
        return instant;
    }

}
