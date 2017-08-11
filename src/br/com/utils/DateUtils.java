package br.com.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    public static LocalDate of(Date date) {

        if (date == null) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    public static LocalDateTime ofTime(Date date) {

        if (date == null) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    public static LocalDate of(Calendar calendar) {
        return of(calendar.getTime());
    }

    public static LocalDateTime ofTime(Calendar calendar) {
        return ofTime(calendar.getTime());
    }

    public static LocalDate minOf(Date date) {

        final LocalDate localDate = of(date);

        if (localDate == null) {
            return LocalDate.of(2000, Month.JANUARY, 1);
        } else {
            return localDate;
        }
    }

    public static LocalDateTime minOfTime(Date date) {

        final LocalDateTime localDateTime = ofTime(date);

        if (localDateTime == null) {
            return LocalDateTime.of(2000, Month.JANUARY, 1, 0, 0, 0);
        } else {
            return localDateTime;
        }
    }

    public static LocalDate maxOf(Date date) {

        final LocalDate localDate = of(date);

        if (localDate == null) {
            return LocalDate.ofYearDay(3000, 1);
        } else {
            return localDate;
        }
    }
}
