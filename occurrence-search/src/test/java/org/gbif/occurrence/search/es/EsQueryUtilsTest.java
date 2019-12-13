package org.gbif.occurrence.search.es;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EsQueryUtilsTest {

  @Test(expected = Test.None.class)
  public void dateParserTest() throws ParseException {
    EsQueryUtils.STRING_TO_DATE.apply("2019");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-01");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-15T17:17:48.191 +02:00");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-15T17:17:48.191");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-15T17:17:48.023+02:00");

    EsQueryUtils.STRING_TO_DATE.apply("2019-11-12T13:24:56.963591");
  }

  @Test
  public void dateWithYearZeroTest() {
    Date date = EsQueryUtils.STRING_TO_DATE.apply("0000");
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = EsQueryUtils.STRING_TO_DATE.apply("0000-01");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = EsQueryUtils.STRING_TO_DATE.apply("0000-01-01");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = EsQueryUtils.STRING_TO_DATE.apply("0000-01-01T00:00:01.100");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = EsQueryUtils.STRING_TO_DATE.apply("0000-01-01T17:17:48.191 +02:00");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = EsQueryUtils.STRING_TO_DATE.apply("0000-01-01T13:24:56.963591");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = EsQueryUtils.STRING_TO_DATE.apply("0000-01-01T17:17:48.023+02:00");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));
  }

  @Test
  public void lowerBoundRangeTest() {
    assertEquals(
        LocalDate.of(2019, 10, 01).atTime(LocalTime.MIN),
        EsQueryUtils.LOWER_BOUND_RANGE_PARSER.apply("2019-10"));
    assertEquals(
      LocalDate.of(2019, 01, 01).atTime(LocalTime.MIN),
      EsQueryUtils.LOWER_BOUND_RANGE_PARSER.apply("2019"));
    assertEquals(
      LocalDate.of(2019, 10, 2).atTime(LocalTime.MIN),
      EsQueryUtils.LOWER_BOUND_RANGE_PARSER.apply("2019-10-02"));
  }

  @Test
  public void upperBoundRangeTest() {
    assertEquals(
      LocalDate.of(2019, 10, 31).atTime(LocalTime.MAX),
      EsQueryUtils.UPPER_BOUND_RANGE_PARSER.apply("2019-10"));
    assertEquals(
      LocalDate.of(2019, 12, 31).atTime(LocalTime.MAX),
      EsQueryUtils.UPPER_BOUND_RANGE_PARSER.apply("2019"));
    assertEquals(
      LocalDate.of(2019, 10, 2).atTime(LocalTime.MAX),
      EsQueryUtils.UPPER_BOUND_RANGE_PARSER.apply("2019-10-02"));
  }
}
