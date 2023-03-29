package com.linagora.tmail.james.jmap.calendar

import java.io.ByteArrayInputStream
import java.time.format.DateTimeFormatter

import com.linagora.tmail.james.jmap.model.{CalendarEventByDay, CalendarEventByMonth, CalendarEventParsed, RecurrenceRule, RecurrenceRuleFrequency, RecurrenceRuleInterval}
import net.fortuna.ical4j.model.Recur.Frequency
import net.fortuna.ical4j.model.{Month, WeekDay}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{Nested, Test}

import scala.jdk.CollectionConverters._

class CalendarEventParsedTest {

  @Test
  def endFieldShouldPreferDTENDWhenPresent(): Unit = {
    val icsPayload = """BEGIN:VCALENDAR
                       |PRODID:-//Aliasource Groupe LINAGORA//OBM Calendar 3.2.1-rc2//FR
                       |CALSCALE:GREGORIAN
                       |X-OBM-TIME:1483439571
                       |VERSION:2.0
                       |METHOD:REQUEST
                       |BEGIN:VEVENT
                       |CREATED:20170103T103250Z
                       |LAST-MODIFIED:20170103T103250Z
                       |DTSTAMP:20170103T103250Z
                       |DTSTART:20170120T100000Z
                       |DTEND:20170121T100000Z
                       |DURATION:PT30M
                       |TRANSP:OPAQUE
                       |SEQUENCE:0
                       |SUMMARY:Sprint Social #3 Demo
                       |DESCRIPTION:
                       |CLASS:PUBLIC
                       |PRIORITY:5
                       |ORGANIZER;X-OBM-ID=468;CN=Attendee 1:MAILTO:attendee1@domain.tld
                       | com
                       |X-OBM-DOMAIN:domain.tld
                       |X-OBM-DOMAIN-UUID:02874f7c-d10e-102f-acda-0015176f7922
                       |LOCATION:hangout
                       |CATEGORIES:
                       |X-OBM-COLOR:
                       |UID:f1514f44bf39311568d64072ac247c17656ceafde3b4b3eba961c8c5184cdc6ee047fe
                       | b2aab16e43439a608f28671ab7c10e754c301b1e32001ad51dd20eac2fc7af20abf4093bbe
                       |ATTENDEE;CUTYPE=INDIVIDUAL;RSVP=TRUE;CN=Attendee 2;PARTSTAT=NEEDS-ACTI
                       | ON;X-OBM-ID=348:MAILTO:attendee2@domain.tld
                       |END:VEVENT
                       |END:VCALENDAR
                       |""".stripMargin

    val calendarEventParsed: CalendarEventParsed = CalendarEventParsed.from(new ByteArrayInputStream(icsPayload.getBytes()))

    assertThat(calendarEventParsed.end.get.value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")))
      .isEqualTo("2017-01-21T10:00:00Z")
  }

  @Test
  def endFieldShouldPresentWhenAbsentDTENDAndPresentDTSTARTAndDURATION(): Unit = {
    val icsPayload =
      """BEGIN:VCALENDAR
        |PRODID:-//Aliasource Groupe LINAGORA//OBM Calendar 3.2.1-rc2//FR
        |CALSCALE:GREGORIAN
        |X-OBM-TIME:1483439571
        |VERSION:2.0
        |METHOD:REQUEST
        |BEGIN:VEVENT
        |CREATED:20170103T103250Z
        |LAST-MODIFIED:20170103T103250Z
        |DTSTAMP:20170103T103250Z
        |DTSTART:20170120T100000Z
        |DURATION:PT30M
        |TRANSP:OPAQUE
        |SEQUENCE:0
        |SUMMARY:Sprint Social #3 Demo
        |DESCRIPTION:
        |CLASS:PUBLIC
        |PRIORITY:5
        |ORGANIZER;X-OBM-ID=468;CN=Attendee 1:MAILTO:attendee1@domain.tld
        | com
        |X-OBM-DOMAIN:domain.tld
        |X-OBM-DOMAIN-UUID:02874f7c-d10e-102f-acda-0015176f7922
        |LOCATION:hangout
        |CATEGORIES:
        |X-OBM-COLOR:
        |UID:ea127690-0440-404b-af98-9823c855a283
        |ATTENDEE;CUTYPE=INDIVIDUAL;RSVP=TRUE;CN=Attendee 2;PARTSTAT=NEEDS-ACTI
        | ON;X-OBM-ID=348:MAILTO:attendee2@domain.tld
        |END:VEVENT
        |END:VCALENDAR
        |""".stripMargin

    val calendarEventParsed: CalendarEventParsed = CalendarEventParsed.from(new ByteArrayInputStream(icsPayload.getBytes()))

    assertThat(calendarEventParsed.end.get.value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")))
      .isEqualTo("2017-01-20T10:30:00Z")
  }

  @Test
  def shouldParseExpectedFields(): Unit = {
    val icsPayload =
      """BEGIN:VCALENDAR
        |PRODID:-//Aliasource Groupe LINAGORA//OBM Calendar 3.2.1-rc2//FR
        |CALSCALE:GREGORIAN
        |X-OBM-TIME:1483439571
        |VERSION:2.0
        |METHOD:REQUEST
        |BEGIN:VEVENT
        |CREATED:20170103T103250Z
        |LAST-MODIFIED:20170103T103250Z
        |DTSTAMP:20170103T103250Z
        |DTSTART:20170120T100000Z
        |DTEND:20170121T100000Z
        |DURATION:PT30M
        |TRANSP:OPAQUE
        |SEQUENCE:0
        |SUMMARY:Sprint Social #3 Demo
        |DESCRIPTION:
        |CLASS:PUBLIC
        |PRIORITY:5
        |ORGANIZER;X-OBM-ID=468;CN=Attendee 1:MAILTO:attendee1@domain.tld
        | com
        |X-OBM-DOMAIN:domain.tld
        |X-OBM-DOMAIN-UUID:02874f7c-d10e-102f-acda-0015176f7922
        |LOCATION:hangout
        |CATEGORIES:
        |X-OBM-COLOR:
        |UID:ea127690-0440-404b-af98-9823c855a283
        |ATTENDEE;CUTYPE=INDIVIDUAL;RSVP=TRUE;CN=Attendee 2;PARTSTAT=NEEDS-ACTI
        | ON;X-OBM-ID=348:MAILTO:attendee2@domain.tld
        |END:VEVENT
        |END:VCALENDAR
        |""".stripMargin

    val calendarEventParsed: CalendarEventParsed = CalendarEventParsed.from(new ByteArrayInputStream(icsPayload.getBytes()))

    assertThat(calendarEventParsed.method.get.value).isEqualTo("REQUEST")
    assertThat(calendarEventParsed.sequence.get.value).isEqualTo(0)
    assertThat(calendarEventParsed.uid.get.value).isEqualTo("ea127690-0440-404b-af98-9823c855a283")
    assertThat(calendarEventParsed.priority.get.value).isEqualTo(5)
    assertThat(s"${calendarEventParsed.freeBusyStatus.get.value}").isEqualTo("busy")
    assertThat(s"${calendarEventParsed.privacy.get.value}").isEqualTo("public")
  }

  @Nested
  class RecurrenceRuleTest {
    @Test
    def parseWeeklyShouldSucceed(): Unit = {
      val calendarEventParsed: CalendarEventParsed = CalendarEventParsed.from(
        new ByteArrayInputStream(getIcsPayload("RRULE:FREQ=WEEKLY;BYDAY=MO,TU").getBytes()))

      assertThat(calendarEventParsed.recurrenceRules.value.asJava)
        .hasSize(1)

      val recurrence: RecurrenceRule = calendarEventParsed.recurrenceRules.value.head
      assertThat(recurrence)
        .isEqualTo(RecurrenceRule(frequency = RecurrenceRuleFrequency(Frequency.WEEKLY),
          byDay = Some(CalendarEventByDay(Seq(WeekDay.Day.MO, WeekDay.Day.TU)))))
    }

    @Test
    def parseThirdSundayOfAprilShouldSucceed(): Unit = {
      val calendarEventParsed: CalendarEventParsed = CalendarEventParsed.from(
        new ByteArrayInputStream(getIcsPayload("RRULE:FREQ=YEARLY;BYMONTH=4;BYDAY=SU;BYSETPOS=3").getBytes()))

      assertThat(calendarEventParsed.recurrenceRules.value.asJava)
        .hasSize(1)

      val recurrence: RecurrenceRule = calendarEventParsed.recurrenceRules.value.head
      assertThat(recurrence)
        .isEqualTo(RecurrenceRule(frequency = RecurrenceRuleFrequency(Frequency.YEARLY),
          byDay = Some(CalendarEventByDay(Seq(WeekDay.Day.SU))),
          byMonth = Some(CalendarEventByMonth(Seq(new Month(4)))),
          bySetPosition = Some(Seq(3))))
    }

    @Test
    def parseFirstAndSecondMondayOfOctoberShouldSucceed(): Unit = {
      val calendarEventParsed: CalendarEventParsed = CalendarEventParsed.from(
        new ByteArrayInputStream(getIcsPayload("RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=MO;BYSETPOS=1,2").getBytes()))

      assertThat(calendarEventParsed.recurrenceRules.value.asJava)
        .hasSize(1)

      val recurrence: RecurrenceRule = calendarEventParsed.recurrenceRules.value.head
      assertThat(recurrence)
        .isEqualTo(RecurrenceRule(frequency = RecurrenceRuleFrequency(Frequency.YEARLY),
          byMonth = Some(CalendarEventByMonth(Seq(new Month(10)))),
          bySetPosition = Some(List(1, 2)),
          byDay = Some(CalendarEventByDay(List(WeekDay.Day.MO)))))
    }

    @Test
    def parseMonthlyEvery29thOfEveryMonthShouldSucceed(): Unit = {
      val calendarEventParsed: CalendarEventParsed = CalendarEventParsed.from(
        new ByteArrayInputStream(getIcsPayload("RRULE:FREQ=MONTHLY;INTERVAL=2;BYMONTHDAY=29").getBytes()))

      assertThat(calendarEventParsed.recurrenceRules.value.asJava)
        .hasSize(1)

      val recurrence: RecurrenceRule = calendarEventParsed.recurrenceRules.value.head
      assertThat(recurrence)
        .isEqualTo(RecurrenceRule(frequency = RecurrenceRuleFrequency(Frequency.MONTHLY),
          interval = Some(RecurrenceRuleInterval.from(2)),
          byMonthDay = Some(List(29))))
    }

    @Test
    def parseMonthlyEveryLastSundayOfEvery3MonthsShouldSucceed(): Unit = {
      val calendarEventParsed: CalendarEventParsed = CalendarEventParsed.from(
        new ByteArrayInputStream(getIcsPayload("RRULE:FREQ=MONTHLY;INTERVAL=3;BYDAY=SU;BYSETPOS=-1").getBytes()))

      assertThat(calendarEventParsed.recurrenceRules.value.asJava)
        .hasSize(1)

      val recurrence: RecurrenceRule = calendarEventParsed.recurrenceRules.value.head
      assertThat(recurrence)
        .isEqualTo(RecurrenceRule(frequency = RecurrenceRuleFrequency(Frequency.MONTHLY),
          byDay = Some(CalendarEventByDay(Seq(WeekDay.Day.SU))),
          bySetPosition = Some(Seq(-1)),
          interval = Some(RecurrenceRuleInterval.from(3))))
    }

    @Test
    def parseMonthlyEveryFourthSundayOfEvery3MonthsShouldSucceed(): Unit = {
      val calendarEventParsed: CalendarEventParsed = CalendarEventParsed.from(
        new ByteArrayInputStream(getIcsPayload("RRULE:FREQ=MONTHLY;INTERVAL=3;BYDAY=SU;BYSETPOS=4").getBytes()))

      assertThat(calendarEventParsed.recurrenceRules.value.asJava)
        .hasSize(1)

      val recurrence: RecurrenceRule = calendarEventParsed.recurrenceRules.value.head
      assertThat(recurrence)
        .isEqualTo(RecurrenceRule(frequency = RecurrenceRuleFrequency(Frequency.MONTHLY),
          byDay = Some(CalendarEventByDay(Seq(WeekDay.Day.SU))),
          bySetPosition = Some(Seq(4)),
          interval = Some(RecurrenceRuleInterval.from(3))))
    }

    def getIcsPayload(rrule: String): String =
      s"""BEGIN:VCALENDAR
         |VERSION:2.0
         |PRODID:-//Sabre//Sabre VObject 4.1.3//EN
         |CALSCALE:GREGORIAN
         |METHOD:REQUEST
         |BEGIN:VTIMEZONE
         |TZID:Asia/Ho_Chi_Minh
         |BEGIN:STANDARD
         |TZOFFSETFROM:+0700
         |TZOFFSETTO:+0700
         |TZNAME:ICT
         |DTSTART:19700101T000000
         |END:STANDARD
         |END:VTIMEZONE
         |BEGIN:VTIMEZONE
         |TZID:Asia/Ho_Chi_Minh
         |BEGIN:STANDARD
         |TZOFFSETFROM:+0700
         |TZOFFSETTO:+0700
         |TZNAME:ICT
         |DTSTART:19700101T000000
         |END:STANDARD
         |END:VTIMEZONE
         |BEGIN:VTIMEZONE
         |TZID:Asia/Ho_Chi_Minh
         |BEGIN:STANDARD
         |TZOFFSETFROM:+0700
         |TZOFFSETTO:+0700
         |TZNAME:ICT
         |DTSTART:19700101T000000
         |END:STANDARD
         |END:VTIMEZONE
         |BEGIN:VEVENT
         |UID:014351ba-ca86-4b0e-bf50-77d2f20afcb3
         |TRANSP:OPAQUE
         |DTSTART;TZID=Asia/Saigon:20230328T103000
         |DTEND;TZID=Asia/Saigon:20230328T113000
         |CLASS:PUBLIC
         |X-OPENPAAS-VIDEOCONFERENCE:
         |SUMMARY:Test
         |$rrule
         |ORGANIZER;CN=Van Tung TRAN:mailto:vttran@domain.tld
         |DTSTAMP:20230328T030326Z
         |ATTENDEE;PARTSTAT=ACCEPTED;RSVP=FALSE;ROLE=CHAIR;CUTYPE=INDIVIDUAL;CN=Van T
         | ung TRAN:mailto:vttran@domain.tld
         |ATTENDEE;PARTSTAT=NEEDS-ACTION;RSVP=TRUE;ROLE=REQ-PARTICIPANT;CUTYPE=INDIVI
         | DUAL:mailto:tungtv202@domain.tld
         |SEQUENCE:0
         |END:VEVENT
         |END:VCALENDAR
         |""".stripMargin
  }

}
