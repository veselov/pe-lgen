package org.vps.web;

import org.vps.web.impl.C;
import org.vps.web.impl.W;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * @author Pawel S. Veselov
 */
public class Log implements Runnable, C {

    private static String format = "%E %d/%m/%y %H:%M:%S.%i [%t] %s";
    private static String subFormat = "%E :%l %s";

    public final static int SILENT = -3;
    public final static int ERROR = -2;
    public final static int WARN = -1;
    public final static int INFO = 0;
    public final static int DEBUG = 1;
    public final static int NOISE = 2;

    // we print out everything less or equal to the log level
    private static int logLevel = 0;

    private static SVector log_msg = new SVector();

    /**
     * Sets logging format.
     * The following specials are recognized:
     * <br>
     * <pre>
     * %t   Thread name
     * %s   provided log message
     * %m   month (0 padded)
     * %d   day (0 padded)
     * %y   year (2 digits)
     * %Y   year (4 digits)
     * %h   hours (12h, 0 padded)
     * %H   hours (24h, 0 padded)
     * %M   minutes, 0 padded
     * %S   seconds, 0 padded
     * %i   milliseconds, 00 padded
     * %p   AM/PM (1 letter)
     * %P   AM/PM (2 letters)
     * %l   Line number (only valid for sub format)
     * %e   Message type (ERROR, LOG, DEBUG, etc)
     * %E   Message type (E, L, D, N, etc.)
     * %%   % character
     * </pre>
     *
     * Default format for regular lines:<b>%E %d/%m/%y %H:%M:%S.%i [%t] %s</b>
     * <br>
     * Default format for multline messages:<b>%E :%l %s</b>
     *
     * @param format Specifies format for log messages
     * @param subFormat Specifies format for multiline log messages,
     * for every line but the first line.
     */
    public static void setFormat(String format, String subFormat) {
        Log.format = format;
        Log.subFormat = subFormat;
    }

    public static String getLogLevelName() {
        return getLogLevelName(logLevel);
    }
    public static String getLogLevelName(int ll) {
        switch (ll) {
            case SILENT:
                return "SILENT";
            case ERROR:
                return "ERROR";
            case WARN:
                return "WARN";
            case INFO:
                return "INFO";
            case DEBUG:
                return "DEBUG";
            case NOISE:
                return "NOISE";
        }
        return "???";
    }

    public static boolean noise() {
        return logLevel <= NOISE;
    }
    public static boolean debug() {
        return logLevel <= DEBUG;
    }
    public static boolean info() {
        return logLevel <= INFO;
    }
    public static boolean warn() {
        return logLevel <= WARN;
    }
    public static boolean error() {
        return logLevel <= ERROR;
    }

    /**
     * Report an error message
     * @param msg message to report
     */
    public static void error(String msg) {
        error(msg, null);
    }

    /**
     * Report an error message, and an execption
     * @param msg message to report
     * @param e exception to report
     */
    public static void error(String msg, Throwable e) {
        log(ERROR, msg, e);
    }

    /**
     * Report an exception, as an error.
     * @param e exception to report
     */
    public static void error(Throwable e) {
        if (e != null) { error(e.toString(), e); }
    }

    /**
     * Report a warning message.
     * @param msg message to report
     */
    public static void warn(String msg) {
        warn(msg, null);
    }

    /**
     * Report a warning message, and an exception.
     * @param msg message to report
     * @param e exception to report
     */
    public static void warn(String msg, Throwable e) {
        log(WARN, msg, e);
    }

    /**
     * Report an exception as a warning.
     * @param e exception to report
     */
    public static void warn(Throwable e) {
        if (e != null) { warn(e.toString(), e); }
    }

    /**
     * Report an informational message
     * @param msg message to report
     */
    public static void info(String msg) {
        info(msg, null);
    }

    /**
     * Report an informational message, and an exception
     * @param msg message to report
     * @param e exception to report
     */
    public static void info(String msg, Throwable e) {
        log(INFO, msg, e);
    }

    /**
     * Report an exception as an informational message
     * @param e exception to report
     */
    public static void info(Throwable e) {
        if (e != null) { info(e.toString(), e); }
    }

    /**
     * Report a debugging message.
     * @param msg message to report.
     */
    public static void debug(String msg) {
        debug(msg, null);
    }

    /**
     * Report a debugging message and a exception.
     * @param msg message to report
     * @param e exception to report.
     */
    public static void debug(String msg, Throwable e) {
        log(DEBUG, msg, e);
    }

    /**
     * Report an exception as a debugging message.
     * @param e exception to report.
     */
    public static void debug(Throwable e) {
        if (e != null) { debug(e.toString(), e); }
    }

    /**
     * Report a noise message (extensive debugging)
     * @param msg message to report
     */
    public static void noise(String msg) {
        noise(msg, null);
    }

    /**
     * Report a noise message (extensive debugging) and an exception.
     * @param msg message to report
     * @param e exception to report
     */
    public static void noise(String msg, Throwable e) {
        log(NOISE, msg, e);
    }

    /**
     * Report an exception as a noise message (extensive debugging)
     * @param e exception to report
     */
    public static void noise(Throwable e) {
        if (e != null) { noise(e.toString(), e); }
    }

    /**
     * Sets the output log level.
     * @param ll new log level.
     * You can set this to <code>NOISE</code>, to generate all output.<br>
     * You can set this to <code>DEBUG</code>, to limit to debug messages.<br>
     * You can set this to <code>INFO</code>, so no debug or noise messages
     * are logged.<br>
     * You can set this to <code>WARN</code>, so only warning and error
     * messages are printed.<br>
     * You can set this to <code>ERROR</code>, so only error messages are
     * printed.<br>
     * You can set this to <code>SILENT</code>, so NO messages are ever printed.
     * You can't set it to any other value.
     */
    public static void setLogLevel(int ll) {
        switch (ll) {
            case NOISE:
            case DEBUG:
            case INFO:
            case WARN:
            case ERROR:
            case SILENT:
                logLevel = ll;
        }
    }

    /**
     * Report an arbitrary logging message.
     * @param pri message priority
     * @param msg message to report
     * @param e exception to report
     */
    public static void log(int pri, String msg, Throwable e) {

        if (pri > logLevel) {
            return;
        }

        long time = System.currentTimeMillis();

        if (e != null) {
            PrintWriter multi = getMulti(pri);
            multi.println(msg+" : "+e.toString());
            e.printStackTrace(multi);
            Throwable cause = e.getCause();
            if (cause != null) {
                multi.println("Cause : "+cause.toString());
                cause.printStackTrace(multi);
            }
            multi.close();
        } else if ((msg != null)&&(msg.indexOf('\n')>=0)) {
            PrintWriter multi = getMulti(pri);
            StringTokenizer st = new StringTokenizer(msg, "\n");
            while (st.hasMoreTokens()) {
                String next = st.nextToken();
                multi.println(next);
            }
            multi.close();
        } else {
            schedule(pri, msg, time, Thread.currentThread().getName());
        }
    }

    /**
     * Get a writer to report multiple lines of logging information.
     * This returns back a print writer, where data can be printed as
     * regular text, and the result will still appear as a logging message.
     * User must call <code>close()</code> on the print writer, when all
     * the data is printed. Otherwise, the data will remain in the writer
     * and will not be reported until an object is finalized. The benefit
     * of using this print writer rather then reporting multiple message is
     * that output will be printed atomically, and not interleaved with
     * any messages from any other threads.
     * @param pri priority to use
     */
    public static PrintWriter getMulti(int pri) {
        return new MultiPS(pri);
    }

    private static void schedule(int pri, String msg,
            long time, String thread) {
        new LE(pri, time, thread, msg);
    }

    private static void schedule(int pri, String[] msg,
            long time, String thread) {
        new LE(pri, time, thread, msg);
    }

    static class _WR extends Writer {
        public void write(char[] cbuf, int off, int len) {}
        public void flush() {}
        public void close() {}
    }
    private static Writer _wr = new _WR();

    static class MultiPS extends PrintWriter {

        private boolean closed = false;
        private int priority;
        private long time = 0L;
        private String threadName;

        Vector lines = new Vector();
        StringBuffer line = null;

        private Object lock = new Object();

        MultiPS(int pri) {
            super(_wr);
            priority = pri;
            threadName = Thread.currentThread().getName();
        }

        public boolean checkError() {
            return false;
        }

        protected void finalize() {
            close();
        }

        public void close() {

            if (priority > logLevel) { return; }

            synchronized (lock) {
                if (closed) { return; }
                if (line != null) {
                    lines.add(line);
                }
                closed = true;
            }

            if (lines.size() == 0) { return; }

            schedule(priority, (String[])lines.toArray(new String[0]),
                    time, threadName);
        }

        public void flush() {}

        public void print(boolean b) {
            print(String.valueOf(b));
        }
        public void print(char c) {
            print(String.valueOf(c));
        }
        public void print(char [] s) {
            if (s == null) { return; }
            for (int i=0; i<s.length; i++) {
                print(s[i]);
            }
        }
        public void print(double d) {
            print(String.valueOf(d));
        }
        public void print(float f) {
            print(String.valueOf(f));
        }
        public void print(int i) {
            print(String.valueOf(i));
        }
        public void print(long l) {
            print(String.valueOf(l));
        }
        public void print(Object obj) {
            if (obj == null) {
                print("null");
            } else {
                print(obj.toString());
            }
        }
        public void print(String s) {
            if (s == null) {
                print("null");
            } else {
                write(s);
            }
        }
        public void println() {
            print('\n');
        }
        public void println(boolean b) {
            println(String.valueOf(b));
        }
        public void println(char c) {
            println(String.valueOf(c));
        }
        public void println(char [] s) {
            if (s == null) { return; }
            for (int i=0; i<s.length; i++) {
                println(s[i]);
            }
        }
        public void println(double d) {
            println(String.valueOf(d));
        }
        public void println(float f) {
            println(String.valueOf(f));
        }
        public void println(int i) {
            println(String.valueOf(i));
        }
        public void println(long l) {
            println(String.valueOf(l));
        }
        public void println(Object obj) {
            if (obj == null) {
                println("null");
            } else {
                println(obj.toString());
            }
        }
        public void println(String s) {
            if (s == null) {
                println("null");
            } else {
                write(s+'\n');
            }
        }
        protected void setError() {}

        public void write(char [] buf) {
            write(buf, 0, buf.length);
        }

        public void write(char [] buf, int off, int len) {

            if (priority > logLevel) {
                return;
            }

            synchronized (lock) {

                if (closed) {
                    throw new RuntimeException("This writer is closed.");
                }

                if (time == 0) { time = System.currentTimeMillis(); }

                for (int i=off; i<len; i++) {
                    if (buf[i] == '\n') {
                        if (line == null) { continue; }
                        lines.add(line.toString());
                        line = null;
                    } else {
                        if (line == null) { line = new StringBuffer(); }
                        line.append(buf[i]);
                    }
                }
            }
        }

        public void write(int c) {
            write(new char[]{(char)c});
        }

        public void write(String s) {
            write(s.toCharArray());
        }

        public void write(String s, int off, int len) {
            write(s.substring(off, off+len).toCharArray());
        }
    }

    static class LE {

        long time_stamp;
        String thread_name;
        int priority;
        String [] lines;

        LE(int priority, long time_stamp, String thread_name, String message) {
            this(priority, time_stamp, thread_name, new String[]{message});
        }

        LE(int priority, long time_stamp, String thread_name, Vector messages) {
            this(priority, time_stamp, thread_name,
                    (String[])messages.toArray(new String[0]));
        }

        LE(int priority, long time_stamp, String thread_name, String [] msgs) {
            this.priority = priority;
            this.time_stamp = time_stamp;
            this.thread_name = thread_name;
            // we may want to copy the array... or not.
            this.lines = msgs;

            synchronized (log_msg) {
                log_msg.add(this);
                log_msg.notify();
            }
        }

        public void printLine(int i) {
            String out = format(i);
            System.out.println(out);
        }

        public void print() {
            for (int i=0; i<lines.length; i++) {
                printLine(i);
            }
        }

        private String format(int line_no) {

            String _form;
            if (line_no == 0) {
                _form = format;
            } else {
                _form = subFormat;
            }

            StringBuffer sb = new StringBuffer();
            boolean escape = false;

            Calendar cal = Calendar.getInstance();
            cal.setTime(new Date(time_stamp));

            for (int i=0; i<_form.length(); i++) {

                char c = _form.charAt(i);

                if (escape) {
                    escape = false;

                    switch (c) {
                    case '%':
                        sb.append('%');
                        break;
                    case 't':
                        sb.append(thread_name);
                        break;
                    case 's':
                        sb.append(lines[line_no]);
                        break;
                    case 'm':
                        W.apd_pad(sb, cal.get(Calendar.MONTH), 2);
                        break;
                    case 'd':
                        W.apd_pad(sb, cal.get(Calendar.DAY_OF_MONTH), 2);
                        break;
                    case 'y':
                        W.apd_pad(sb, cal.get(Calendar.YEAR) % 100, 2);
                        break;
                    case 'Y':
                        sb.append(cal.get(Calendar.YEAR));
                        break;
                    case 'h':
                        W.apd_pad(sb, cal.get(Calendar.HOUR), 2);
                        break;
                    case 'H':
                        W.apd_pad(sb, cal.get(Calendar.HOUR_OF_DAY), 2);
                        break;
                    case 'M':
                        W.apd_pad(sb, cal.get(Calendar.MINUTE), 2);
                        break;
                    case 'S':
                        W.apd_pad(sb, cal.get(Calendar.SECOND), 2);
                        break;
                    case 'i':
                        W.apd_pad(sb, cal.get(Calendar.MILLISECOND), 3);
                        break;
                    case 'p':
                        if (cal.get(Calendar.AM_PM) == Calendar.AM) {
                            sb.append('a');
                        } else {
                            sb.append('p');
                        }
                        break;
                    case 'P':
                        if (cal.get(Calendar.AM_PM) == Calendar.AM) {
                            sb.append("am");
                        } else {
                            sb.append("pm");
                        }
                        break;
                    case 'l':
                        sb.append(line_no);
                        break;
                    case 'e':
                        switch (priority) {
                        case ERROR:
                            sb.append("ERROR");
                            break;
                        case WARN:
                            sb.append("WARN");
                            break;
                        case INFO:
                            sb.append("INFO");
                            break;
                        case DEBUG:
                            sb.append("DEBUG");
                            break;
                        case NOISE:
                            sb.append("NOISE");
                            break;
                        default:
                            throw new RuntimeException(""+priority);
                        }
                        break;
                    case 'E':
                        switch (priority) {
                        case ERROR:
                            sb.append('E');
                            break;
                        case WARN:
                            sb.append('W');
                            break;
                        case INFO:
                            sb.append('I');
                            break;
                        case DEBUG:
                            sb.append('D');
                            break;
                        case NOISE:
                            sb.append('N');
                            break;
                        default:
                            throw new RuntimeException(""+priority);
                        }
                        break;
                    default:
                        sb.append('%');
                        sb.append(c);
                    }
                    continue;
                }

                if (c == '%') {
                    escape = true;
                    continue;
                }
                sb.append(c);
            }

            return sb.toString();

        }
    }

    /**
     * Does nothing. Can be called to warm up the loggin system when needed.
     */
    public static void warm() {}

    // this is sorta cooked version of a vector...
    static class SVector {

        Object [] array;
        int size;

        int cap_inc = 10;

        SVector() {
            size = 0;
        }

        public synchronized void add(Object element) {
            ensure_cap();
            array[size++] = element;
        }

        public synchronized Object elementAt(int idx) {
            return array[idx];
        }

        public synchronized int size() {
            return size;
        }

        public synchronized void removeThatMuch(int how_much) {

            if (how_much  <= 0) { return; }
            if (how_much > size) {  how_much = size; }

            int new_size = size - how_much;

            int new_array_len = (new_size / cap_inc);
            if ((new_size % cap_inc)>0) { new_array_len++; }
            new_array_len *= cap_inc;

            Object [] aux = new Object[new_array_len];

            System.arraycopy(array, how_much, aux, 0, new_size);

            size = new_size;
            array = aux;
        }

        private void ensure_cap() {
            // MUST BE synchronized by now
            if (array == null) {
                array = new Object[cap_inc];
                return;
            }

            if (array.length == size) {
                Object [] aux = new Object[cap_inc + array.length];
                System.arraycopy(array, 0, aux, 0, array.length);
                array = aux;
            }
        }
    }

    private Log() {

        try {
            logLevel = Integer.parseInt(System.getProperty(PROP_LL));
            if (logLevel < SILENT) {
                logLevel = SILENT;
            }
            if (logLevel > NOISE) {
                logLevel = NOISE;
            }
            info("Log level set to "+getLogLevelName()+" by system property");
        } catch (Exception xa) {
        }

        Thread thread = new Thread(this);
        thread.setDaemon(true);
        thread.setName("LOG");
        thread.start();
        debug("Logging thread started");

        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        setName("Log SH");
                        debug("Waiting for the messages to end...");
                        while (true) {
                            synchronized (log_msg) {
                                if (log_msg.size() == 0) {
                                    return;
                                }
                            }
                        }
                    }});
        } catch (Exception e) {
            warn("Failed to register a shutdown hook. Container present ?", e);
        }
    }

    public void run() {

        debug("Logging thread running...");

        while (true) {

            int know_size = 0;

            synchronized (log_msg) {
                know_size = log_msg.size();
                if (know_size == 0) {
                    try {
                        log_msg.wait();
                    } catch (Exception e) {
                        // Ignore, really !
                    }
                    continue;
                }
            }

            // the purpose of doing it that way, is not to hold the
            // lock on log_msgs for any longer than necessary, since
            // we don't want regular threads to wait while I'm writing
            // messages to stoopid stdout.

            for (int i=0; i<know_size; i++) {
                ((LE)log_msg.elementAt(i)).print();
            }

            log_msg.removeThatMuch(know_size);
        }
    }

    static {
        new Log();
    }

}