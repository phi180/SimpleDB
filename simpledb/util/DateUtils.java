package simpledb.util;

import java.util.Arrays;
import java.util.Date;

public class DateUtils {

    public static Date min(Date... dates) {
        return Arrays.stream(dates).filter(date -> date!=null).min(Date::compareTo).orElse(null);
    }

    public static Date max(Date... dates) {
        return Arrays.stream(dates).filter(date -> date!=null).max(Date::compareTo).orElse(null);
    }

}
