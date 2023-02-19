package org.vps.web.impl;

import org.vps.web.Log;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.FieldPosition;
import java.text.ParseException;
import java.util.*;

/**
 * @author Pawel S. Veselov
 */
public class HttpDTFormat extends DateFormat {

    private static final String [] _DOW = {
        "sunday",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday"
    };

    private static final String [] _MON = {
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december"
    };

    private static final ThreadLocal parseCalendar = new ThreadLocal();
    private static final ThreadLocal formatCalendar = new ThreadLocal();


    public HttpDTFormat() {}

    public Date parse(String src, ParsePosition pos) {


        int _pos = pos.getIndex();

        if (src == null || src.length() < 1) {
            pos.setErrorIndex(0);
            return null;
        }

        pos.setErrorIndex(-1);

        // protect ourselves from creating copies (substrings) of
        // a very large string.
        // here we say that 100 characters is enough for a date.
        if (src.length() > 100) {
            src = src.substring(0, 100);
        }

        if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }

        // let's see if we have day of week.
        String aux = src.substring(pos.getIndex()).toLowerCase();

        int chunk_len = -1;
        int dow = 0;

        for (;dow<_DOW.length; dow++) {

            if (aux.startsWith(_DOW[dow])) {
                chunk_len = _DOW[dow].length();
                break;
            }

            if (aux.startsWith(_DOW[dow].substring(0,3))) {
                chunk_len = 3;
                break;
            }
        }

        if (chunk_len > 0) {
            // we found day of week, let's cut it !
            pos.setIndex(pos.getIndex() + chunk_len);
            if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }
            // do we have a comma ?
            if (src.charAt(pos.getIndex()) == ',') {
                // skip comma
                pos.setIndex(pos.getIndex()+1);
                if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }
            }
        } else {
            dow = -1;
        }

        // ok, now we either going to get a day (RFC822/RFC850)
        // or month (CTIME)

        aux = src.substring(pos.getIndex()).toLowerCase();

        int month = 0;
        for (; month<_MON.length; month++) {

            if (aux.startsWith(_MON[month])) {
                chunk_len = _MON[month].length();
                break;
            }

            if (aux.startsWith(_MON[month].substring(0, 3))) {
                chunk_len = 3;
                break;
            }
        }

        if (month > 11) { month = -1; }

        if (month >= 0) {

            // ok, this is now definetely CTIME.

            pos.setIndex(pos.getIndex() + chunk_len);
            if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }

            int day = getNumber(src, pos);
            if (day == -1) {
                pos.setErrorIndex(pos.getIndex());
                pos.setIndex(_pos); return null;
            }
            if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }

            int [] hms = getTime(src, pos);
            if (hms == null) { pos.setIndex(_pos); return null; }

            if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }

            int year = getNumber(src, pos);
            if (year == -1) {
                pos.setErrorIndex(pos.getIndex());
                pos.setIndex(_pos); return null;
            }

            year = get4Year(year);

            String tz = null;

            if (!eatSpaces(false, src, pos)) {
                StringBuffer sb = new StringBuffer();
                while (pos.getIndex() < src.length()) {
                    char c = src.charAt(pos.getIndex());
                    if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                        break;
                    }
                    sb.append(c);
                    pos.setIndex(pos.getIndex()+1);
                }
                tz = sb.toString();
            }

            return makeDate(src, pos, dow, year, month, day, hms, tz);
        }

        if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }

        // rfc822/rfc850 (rfc1123/rfc1036)
        // the only real difference is that second has dashes for date
        // and supposedly first has 4-digit year.

        // get day
        int day = getNumber(src, pos);
        if (day < 0) {
            pos.setErrorIndex(pos.getIndex());
            pos.setIndex(_pos); return null;
        }

        if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }
        if (src.charAt(pos.getIndex())=='-') { // skip dash if there.
            pos.setIndex(pos.getIndex()+1);
            if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }
        }
        aux = src.substring(pos.getIndex()).toLowerCase();
        month = 0;
        for (; month < _MON.length; month++) {
            if (aux.startsWith(_MON[month])) {
                chunk_len = _MON[month].length();
                break;
            }
            if (aux.startsWith(_MON[month].substring(0,3))) {
                chunk_len = 3;
                break;
            }
        }

        if (month > 11) {
            pos.setErrorIndex(pos.getIndex());
            pos.setIndex(_pos); return null;
        }

        pos.setIndex(pos.getIndex() + chunk_len);
        if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }
        if (src.charAt(pos.getIndex())=='-') { // skip dash if there.
            pos.setIndex(pos.getIndex()+1);
            if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }
        }

        // ok, year is all that's left.
        int year = getNumber(src, pos);
        if (year < 0) {
            pos.setErrorIndex(pos.getIndex());
            pos.setIndex(_pos); return null;
        }

        year = get4Year(year);

        if (!eatSpaces(true, src, pos)) { pos.setIndex(_pos); return null; }
        int [] hms = getTime(src, pos);
        if (hms == null) { pos.setIndex(_pos); return null; }

        String tz = null;
        if (eatSpaces(false, src, pos)) {
            StringBuffer sb = new StringBuffer();
            while (pos.getIndex() < src.length()) {
                char c = src.charAt(pos.getIndex());
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    break;
                }
                sb.append(c);
                pos.setIndex(pos.getIndex()+1);
            }
            tz = sb.toString();
        }

        return makeDate(src, pos, dow, year, month, day, hms, tz);
    }

    private int get4Year(int year) {
        if (year > 100) { return year; }
        if (year >= 75) { return year + 1900; }
        return year + 2000;
    }

    private int [] getTime(String str, ParsePosition pos) {

        // please call eatSpaces() before this method !

        int hms [] = new int[3];

        int hIndex = pos.getIndex();

        if ((hms[0] = getNumber(str, pos)) == -1) {
            pos.setErrorIndex(hIndex);
            return null;
        }

        if (!eatSpaces(true, str, pos)) { return null; }
        char c = str.charAt(pos.getIndex());
        if (c != ':') {
            pos.setErrorIndex(pos.getIndex());
            return null;
        }
        pos.setIndex(pos.getIndex()+1);
        if (!eatSpaces(true, str, pos)) { return null; }

        int mIndex = pos.getIndex();

        if ((hms[1] = getNumber(str, pos)) == -1) {
            pos.setErrorIndex(mIndex);
            return null;
        }
        if (!eatSpaces(true, str, pos)) { return null; }
        c = str.charAt(pos.getIndex());
        if (c != ':') {
            pos.setErrorIndex(pos.getIndex());
            return null;
        }
        pos.setIndex(pos.getIndex()+1);
        if (!eatSpaces(true, str, pos)) { return null; }

        int sIndex = pos.getIndex();

        if ((hms[2] = getNumber(str, pos)) == -1) {
            pos.setErrorIndex(sIndex);
            return null;
        }

        if (hms[0] > 23) {
            pos.setErrorIndex(hIndex);
            return null;
        }

        if (hms[1] > 59) {
            pos.setErrorIndex(mIndex);
            return null;
        }

        if (hms[2] > 59) {
            pos.setErrorIndex(hIndex);
            return null;
        }

        return hms;
    }

    private int getNumber(String str, ParsePosition pos) {

        // !!! buffer must be advanced to a digit
        // before this method is called.

        // no negative number accepted.

        // if number is non-parseable, -1 is returned.

        StringBuffer sb = new StringBuffer();
        boolean seenInteresting = false;
        boolean seenAnything = false;

        while (pos.getIndex() < str.length()) {

            char c = str.charAt(pos.getIndex());
            if (c == '0' && !seenInteresting) {
                seenAnything = true;
                pos.setIndex(pos.getIndex()+1);
                continue;
            }

            if (c >= '0' && c <= '9') {
                seenAnything = seenInteresting = true;
                pos.setIndex(pos.getIndex()+1);
                sb.append(c);
                continue;
            }

            break;
        }

        if (sb.length() == 0) {
            if (seenAnything) { return 0; }
            return -1;
        }

        try {
            return Integer.parseInt(sb.toString());
        } catch (Exception e) {
            // this is *VERY* strange.
            assert false;
            Log.noise("Unexpected NFE on " + sb.toString(), e);
        }

        return -1;
    }

    private boolean eatSpaces(boolean expectData, String src,
            ParsePosition pos) {

        while (pos.getIndex() < src.length()) {
            char c = src.charAt(pos.getIndex());
            if (c != ' ' && c != '\t') {
                return true;
            }
            pos.setIndex(pos.getIndex() + 1);
        }

        if (expectData) {
            pos.setErrorIndex(pos.getIndex());
        }

        return false;
    }

    private Date makeDate(String src, ParsePosition pos, int dow, int year,
            int month, int day, int [] time, String tz) {

        if (tz == null) {
            tz = "UTC";
        } else {
            tz = tz.trim().toUpperCase();
            if ("GMT".equals(tz)) {
                tz = "UTC";
            }
        }

        src = src.substring(0, pos.getIndex());

        TimeZone jtz = TimeZone.getTimeZone(tz);
        if (!jtz.getID().toUpperCase().equals(tz)) {
            Log.noise("Time zone was rewritten for datetime "+src);
        }

        Calendar calendar = (Calendar)parseCalendar.get();
        if (calendar == null) {
            calendar = Calendar.getInstance(Locale.US);
            parseCalendar.set(calendar);
        }

        calendar.clear();
        calendar.setTimeZone(jtz);
        month += Calendar.JANUARY;
        calendar.set(year, month, day, time[0], time[1], time[2]);

        if (dow >= 0) {

            dow += Calendar.SUNDAY;

            if (calendar.get(Calendar.DAY_OF_WEEK) != dow) {
                Log.noise("Invalid day of week in datetime "+src+
                        ". Specified="+dow+", JDK says="+
                        calendar.get(Calendar.DAY_OF_WEEK));
            }
        }

        return calendar.getTime();
    }


    public StringBuffer format(Date date, StringBuffer dst,
            FieldPosition pos) {


        Calendar calendar = (Calendar)formatCalendar.get();
        if (calendar == null) {
            calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"),
                    Locale.US);
            formatCalendar.set(calendar);
        }

        calendar.setTime(date);

        _FMT fmt = new _FMT(dst, pos);

        fmt.start(DateFormat.DAY_OF_WEEK_FIELD);
        int aux = calendar.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY;
        assert (aux > 0 && aux < 7);
        dst.append(Character.toUpperCase(_DOW[aux].charAt(0)));
        dst.append(_DOW[aux].substring(1,3));
        fmt.end();

        dst.append(", ");

        // Jan, <<<

        fmt.start(DateFormat.DAY_OF_WEEK_IN_MONTH_FIELD);
        aux = calendar.get(Calendar.DAY_OF_MONTH);
        if (aux < 10) {
            dst.append('0');
        }
        dst.append(Integer.toString(aux));
        fmt.end();

        dst.append(' ');

        // Jan, 01 <<<

        fmt.start(DateFormat.MONTH_FIELD);
        aux = calendar.get(Calendar.MONTH) - Calendar.JANUARY;
        dst.append(Character.toUpperCase(_MON[aux].charAt(0)));
        dst.append(_MON[aux].substring(1, 3));
        fmt.end();

        dst.append(' ');

        // Jan, 01 Nov <<<

        fmt.start(DateFormat.YEAR_FIELD);
        aux = calendar.get(Calendar.YEAR);
        String _aux = Integer.toString(aux);
        for (int i=0; i<(4-_aux.length()); i++) {
            dst.append('0');
        }
        dst.append(_aux);
        fmt.end();
        dst.append(' ');

        // Jan, 01 Nov 1980 <<<

        fmt.start(DateFormat.HOUR_OF_DAY0_FIELD);
        aux = calendar.get(Calendar.HOUR_OF_DAY);
        if (aux < 10) {
            dst.append('0');
        }
        dst.append(Integer.toString(aux));
        fmt.end();
        dst.append(':');

        // Jan, 01 Nov 1980 15:<<<

        fmt.start(DateFormat.MINUTE_FIELD);
        aux = calendar.get(Calendar.MINUTE);
        if (aux < 10) {
            dst.append('0');
        }
        dst.append(Integer.toString(aux));
        fmt.end();
        dst.append(':');

        // Jan, 01 Nov 1980 15:32:<<<

        fmt.start(DateFormat.SECOND_FIELD);
        aux = calendar.get(Calendar.SECOND);
        if (aux < 10) {
            dst.append('0');
        }
        dst.append(Integer.toString(aux));
        fmt.end();
        dst.append(" GMT");

        // Jan, 01 Nov 1980 15:32:58 GMT<<<

        return dst;

    }

    private class _FMT {
        boolean fmt;
        boolean nend; // = false;
        int fmt_code;
        StringBuffer sb;
        FieldPosition fp;

        private _FMT(StringBuffer sb, FieldPosition fp) {
            this.sb = sb;
            this.fp = fp;
            fmt = ((fmt_code = fp.getField())==-1);
        }

        private void start(int fieldId) {
            if (nend = !fmt && fmt_code == fieldId) {
                fp.setBeginIndex(sb.length());
            }
        }

        private void end() {
            if (nend) {
                nend = !(fmt = true);
                fp.setEndIndex(sb.length()+1);
            }
        }

    }

    // use for self testing
    public static void main(String a[]) throws Exception {

        Log.setLogLevel(Log.NOISE);

        HttpDTFormat fmt = new HttpDTFormat();

        testStdDate(fmt, "06 Nov 1994 08:49:37", 784111777000L);
        testStdDate(fmt, "06-Nov-94 08:49:37", 784111777000L);
        testStdDate(fmt, "Sun Nov  6 08:49:37 1994", 784111777000L);
        testStdDate(fmt, "Nov  6 08:49:37 1994", 784111777000L);
        testStdDate(fmt, "Sun Nov  6 08:49:37 1994 GMT", 784111777000L);


        int at = 20; // runs

        try {
            at = Integer.parseInt(a[0]);
        } catch (Exception e) {}

        Random r = new Random(System.currentTimeMillis());


        for (int i=0; i<at; i++) {

            long nr;

            do {
                nr = r.nextLong();
            } while (nr <= 0);

            Date d = new Date(nr);
            long ldate = (d.getTime() / 1000L) * 1000L;

            String s = fmt.format(d);
            long rdate = parseStr(fmt, s, ldate);

            if (ldate == rdate) {
                System.out.print("ok   ");
            } else {
                System.out.print("FAIL ");
                hadFails = true;
            }

            System.out.print("str : "+s);
            System.out.print("; before="+ldate);
            System.out.println("; after="+rdate);

        }

        if (hadFails) {
            System.out.println("\n!!! SOME TEST FAILED !!!");
        } else {
            System.out.println("-- all ok");
        }

    }

    private static boolean hadFails = false;

    private static long parseStr(DateFormat fmt, String s, long ldate) {
        try {
            return fmt.parse(s).getTime();
        } catch (ParseException e) {
            System.out.println("Exception parsing date "+s+"(long="+ldate+')');
            for (int j=-23; j<e.getErrorOffset(); j++) {
                System.out.print(' ');
            }
            System.out.println("^");
            System.exit(0);
            return -1L;
        } catch (Exception e) {
            System.out.println("Exception parsing date "+s);
            e.printStackTrace();
            System.exit(0);
            return -1L;
        }
    }

    private static void testStdDate(DateFormat fmt, String s, long eta) {

        long rt = parseStr(fmt, s, 0L);
        if (rt != eta) {
            System.out.println("FAIL "+s+"' Expected:"+eta+", got:"+rt);
            hadFails = true;
        } else {
            System.out.println("ok   "+s);
        }
    }

}