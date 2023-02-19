package org.vps.web.impl;

import org.vps.web.HException;
import org.vps.web.Log;

import java.net.URI;
import java.util.*;
import java.text.*;

/**
 * @author Pawel S. Veselov
 */
public class Cookie {

    // those are BITs
    public static final int CS_COMMENT = 0x01;
    public static final int CS_DOMAIN = 0x02;
    public static final int CS_MAX_AGE = 0x04;
    public static final int CS_PATH = 0x08;
    public static final int CS_SECURE = 0x10;
    public static final int CS_VERSION = 0x20;
    public static final int CS_COMMENT_URL = 0x40;
    public static final int CS_DISCARD = 0x80;
    public static final int CS_PORT = 0x100;

    public static final String [] attr_names = {
        "comment",
        "domain",
        "max-age",
        "path",
        "secure",
        "version",
        "commenturl",
        "discard",
        "port" };

    private Map data;           // actual cookie data
    private String comment;     // optional, 2109           // Comment
    private String domain;      // optional, 2109           // Domain
    private long max_age;       // optional, 2109           // Max-Age
    private String path;        // optional, 2109           // Path
    private int version;        // required, 2109   (1)     // Version
    private String commentURL;  // optional, 2965           // CommentURL
    private int [] port;        // optional, 2965           // Port

    private boolean completed;
    private long when;          // when this cookie is created.

    private boolean setCookie2;     // whether this came, or can be used with
                                    // set-cookie2
    private int setMask;

    public Cookie(Map rawData, boolean setCookie2) throws CookieException {
        data = new SequenceMap();
        setMask = 0;
        for (Iterator i = rawData.keySet().iterator(); i.hasNext();) {
            Object attr = i.next();
            Object value = rawData.get(attr);
            set((String)attr, (String)value);
        }

        if (data.size() == 0) {
            throw new CookieException("No actual cookie data");
        }

        this.setCookie2 = setCookie2;
        when = System.currentTimeMillis() / 1000L;
    }

    public Cookie() {
        setMask |= CS_VERSION;
        version = 1;
        data = new SequenceMap();
    }

    public boolean isExpired() {
        if ((setMask & CS_DISCARD)!=0) { return true; }
        if ((setMask & CS_MAX_AGE)==0) { return false; }
        if (((System.currentTimeMillis()/1000L)-when)>max_age) {
            return true;
        }
        return false;
    }

    public boolean appliesTo(URI uri) {
        if (!completed) { return false; }
        // domain, path and port are always set if completed.

        if (!('.' + uri.getHost()).endsWith(domain)) {
            return false;
        }

        if (!uri.getPath().startsWith(path)) {
            return false;
        }

        int rport = uri.getPort();
        if (rport == -1) { rport = 80; }

        boolean okPort = false;
        for (int i=0; i<port.length; i++) {
            if (okPort = (port[i] == rport)) {
                break;
            }
        }
        if (!okPort) return false;

        return true;
    }

    public void applyCookie(Map c1, Map c2) {
        Map map = setCookie2?c2:c1;
        for (Iterator i = data.keySet().iterator(); i.hasNext();) {
            Object key = i.next();
            String val = encode((String)data.get(key));
            map.put(key, val);
        }
    }

    public String toString() {
        return toString(true);
    }

    private String toString(boolean inclSystem) {
        boolean first = true;
        StringBuffer result = new StringBuffer();

        if (inclSystem) {
            for (int i=0; i<attr_names.length; i++) {
                String pair = getPair(attr_names[i]);
                if (pair == null) { continue; }
                if (first) {
                    first = false;
                } else {
                    result.append("; ");
                }
                result.append(pair);
            }
        }

        for (Iterator i = data.keySet().iterator(); i.hasNext(); ) {

            String key = (String)i.next();
            String val = (String)data.get(key);
            if (first) {
                first = false;
            } else {
                result.append("; ");
            }
            result.append(key);
            result.append('=');
            result.append(encode(val));
        }

        return result.toString();
    }

    public boolean hasValues(int special) {
        return (setMask & special) != 0;
    }

    // returns system attribute in form of attr=value, but only if attribute
    // is set.
    private String getPair(String name) {
        Object val = get(name);
        if (val == null) { return null; }
        if (val instanceof int[]) {
            int [] ports = (int[])val;
            StringBuffer aux = new StringBuffer();
            for (int i=0; i<ports.length; i++) {
                aux.append(ports[i]);
                if (i != (ports.length-1)) {
                    aux.append(',');
                }
            }
            val = aux;
        }
        if (val instanceof Boolean) {
            return name;
        } else {
            return name + '=' + encode(val.toString());
        }
    }

    private String encode(String str) {

        boolean pass = true;

        for (int i=0; i<str.length(); i++) {
            if (W.nonTokenCh(str.charAt(i))) {
                pass = false;
                break;
            }
        }

        if (pass) { return str; }

        StringBuffer aux = new StringBuffer();
        aux.append('"');
        for (int i=0; i<str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case '"':
                case '\\':
                    aux.append('\\');
                    break;
            }
            aux.append(c);
        }
        aux.append('"');
        return aux.toString();
    }

    /**
     * Returns the value of specified cookie attribute.
     * For special values, object will be an object, most close
     * to the type of the attribute. The custom cookie values are NEVER
     * returned. Well, there is no reason to find them, and with having
     * multiple values with the same attribute name makes it complicated
     * to return anyway (expect NULL).
     * Null is returned if the attribute is not set.
     * For the "secure" and "discard" attributes, always Boolean.TRUE
     * or NULL will be returned.
     * @param name cookie attribute
     * @return attribute value, or null if no value.
     */
    public Object get(String name) {

        name = name.toLowerCase();

        if ("comment".equals(name)) {
            if ((setMask & CS_COMMENT)!=0) {
                return comment;
            }
            return null;
        } else if ("domain".equals(name)) {
            if ((setMask & CS_DOMAIN)!=0) {
                return domain;
            }
            return null;
        } else if ("max_age".equals(name)) {
            if ((setMask & CS_MAX_AGE)!=0) {
                return new Long(max_age);
            }
            return null;
        } else if ("path".equals(name)) {
            if ((setMask & CS_PATH)!=0) {
                return path;
            }
            return null;
        } else if ("secure".equals(name)) {
            if ((setMask&CS_SECURE)==0) { return null; }
            return Boolean.TRUE;
        } else if ("discard".equals(name)) {
            if ((setMask&CS_DISCARD)==0) { return null; }
            return Boolean.TRUE;
        } else if ("version".equals(name)) {
            if ((setMask&CS_VERSION)!=0) {
                return new Integer(version);
            }
            return null;
        } else if ("commenturl".equals(name)) {
            if ((setMask&CS_COMMENT_URL)!=0) {
                return commentURL;
            }
            return null;
        } else if ("port".equals(name)) {
            if ((setMask&CS_PORT)!=0) {
                int [] aux = new int[port.length];
                System.arraycopy(port, 0, aux, 0, port.length);
                return aux;
            }
            return null;
        }

        return null;
    }

    public void complete(URI requestURI) {
        String host = requestURI.getHost();
        int port = requestURI.getPort();
        if (port == -1) {
            port = 80;
        }

        if ((setMask & CS_DOMAIN)==0) {
            if (host.indexOf((int)'.') >0) {
                domain = host.substring(host.indexOf((int)'.'));
            } else {
                domain = host;
            }
        }

        if ((setMask & CS_PORT)==0) {
            this.port = new int[]{port};
        }

        if ((setMask & CS_PATH)==0) {
            path = requestURI.getPath();
        }

        completed = true;
    }

    public void set(String attr, String value) throws CookieException {

        String la = attr.toLowerCase();

        if ("comment".equals(la)) {
            setMask |= CS_COMMENT;
            comment = value;
        } else if ("domain".equals(la)) {
            setMask |= CS_DOMAIN;
            domain = value;
        } else if ("max_age".equals(la)) {
            try {
                max_age = Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new CookieException("Invalid max age : "+value);
            }
            setMask = CS_MAX_AGE;
        } else if ("expires".equals(la)) {
            // well, expire. It's sorta the same as max-age, but absolute.
            // The damn thing about it is that we have to parse it !
            // And ! It's GMT base.
            // The only good news - it seems to be a very strict format
            // Weekday, dd-Mon-yyyy HH:MM:SS GMT
            try {
                long utc = parseExpires(value);
                setMask = CS_MAX_AGE;
                max_age = utc;
            } catch (CookieException pe) {
                Log.warn("Failed to parse expires cookie value " + value, pe);
            }
        } else if ("path".equals(la)) {
            setMask |= CS_PATH;
            path = value;
        } else if ("secure".equals(la)) {
            setMask |= CS_SECURE;
        } else if ("discard".equals(la)) {
            setMask |= CS_DISCARD;
        } else if ("version".equals(la)) {
            try {
                version = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new CookieException("Invalid cookie version : "+value);
            }
            if (version != 1) {
                throw new CookieException("Unknown cookie version : "+value);
            }
            setMask |= CS_VERSION;
        } else if ("commenturl".equals(la)) {
            setMask |= CS_COMMENT_URL;
            commentURL = value;
        } else if ("port".equals(la)) {
            StringTokenizer st = new StringTokenizer(value, ";");
            Vector aux = new Vector();
            while (st.hasMoreTokens()) {
                String port = st.nextToken().trim();
                try {
                    aux.add(new Integer(port));
                } catch (NumberFormatException e) {
                    throw new CookieException("Bad port number : "+port);
                }
            }
            setMask |= CS_PORT;
            port = new int[aux.size()];
            for (int i=0; i<port.length; i++) {
                port[i] = ((Integer)aux.elementAt(i)).intValue();
            }
        } else {
            data.remove(attr);
            data.put(attr, value);
        }
    }

    public static int getSpecialCode(String name) {
        name = name.toLowerCase();
        if ("comment".equals(name)) return CS_COMMENT;
        if ("domain".equals(name)) return CS_DOMAIN;
        if ("max-age".equals(name)) return CS_MAX_AGE;
        if ("path".equals(name)) return CS_PATH;
        if ("secure".equals(name)) return CS_SECURE;
        if ("version".equals(name)) return CS_VERSION;
        if ("commenturl".equals(name)) return CS_COMMENT_URL;
        if ("discard".equals(name)) return CS_DISCARD;
        if ("port".equals(name)) return CS_PORT;
        return 0;
    }

    public static boolean isSpecialName(String name) {
        if (getSpecialCode(name) == 0) { return false; }
        return true;
    }

    public boolean samePorts(Cookie another) {

        if ((setMask&CS_PORT) != (another.setMask&CS_PORT)) { return false; }
        // ports not set, so they are the same !
        if ((setMask&CS_PORT)==0) { return true; }
        // at this point, either port MUST NOT BE null !
        if (port.length != another.port.length) { return false; }
        for (int i=0; i<port.length; i++) {
            if (port[i] != another.port[i]) { return false; }
        }
        return true;
    }

    boolean invalidatedBy(Cookie another) {
        // we consider this cookie fully invalidated, if a new
        // cookie has all attributes as this cookie has.
        // we also only consider this one invalidated for good,
        // if it has a more NARROW scope that the other cookie.

        if (!completed || !another.completed) {
            throw new HException("Comparing incomplete cookies");
        }

        // this cookie has more narrow scope if it's domain is
        // LONGER than other cookie's domain
        if (!domain.endsWith(another.domain)) {
            return false;
        }
        // this cookie has more narrow scope if it's path is LONGER
        // than other cookie's path
        if (!path.startsWith(another.path)) {
            return false;
        }
        // this cookie has more narrow scope, if ALL of it's ports
        // fit into the port list of the other cookie.
        for (int i=0; i<port.length; i++) {

            boolean found = false;
            for (int j=0; j<port.length; j++) {
                if (port[i] == port[j]) {
                    found = true;
                }
            }
            if (!found) { return false; }
        }
        // this cookie has more narrow (or the same) scope as another
        // cookie. Let's compare the internal values.
        // We consider this cookie completely invalidated, if all of it's
        // internal values present in the another cookie.

        for (Iterator i = data.keySet().iterator(); i.hasNext(); ) {
            Object key = i.next();
            if (another.data.get(key) == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether cookie is good for this request URI.
     * @return null if cookie is good, otherwise the reason why it is not so
     * good.
     */
    public String validFor(URI requestURI) {

        String pfx = setCookie2?"[RFC 2165, 3.3.2] ":"[RFC 2109, 4.3.2] ";

        if ((setMask & CS_PATH) != 0) {
            String requestPath = requestURI.getPath();

            for(; requestPath!=null&&requestPath.length()>0&&
                    requestPath.charAt(0)=='/';
                    requestPath=requestPath.substring(1));

            String path = this.path;

            for(; path!=null&&path.length()>0&&path.charAt(0)=='/';
                    path=path.substring(1));

            if (!requestPath.startsWith(path)) {
                return pfx+
                    "Request URI doesn't start with value of path attribute";
            }
        }

        if ((setMask & CS_DOMAIN) != 0) {
            // we have domain attribute
            if (!(setCookie2 && ".local".equals(domain))) {
                int didx = domain.indexOf((int)'.', 1);
                if ((didx < 0)||(didx == (domain.length()-1))) {
                    return pfx+
                        "Domain attribute doesn't contain any embedded dots";
                }
            }
            if (!setCookie2 && (domain.indexOf((int)'.') != 0)) {
                return pfx+"Domain attribute doesn't start with a dot";
            }
            String host = '.' + requestURI.getHost();
            if (!host.endsWith(domain)) {
                return pfx+"Request host does not match the domain attribute";
            }
            String prime = host.substring(0, host.length() - domain.length());
            if (prime.indexOf((int)'.') >= 0) {
                return pfx+"HD=request host, D=domain attr. H must not contain any dots";
            }
        }

        if (setCookie2 && ((setMask & CS_PORT)!=0)) {
            int rport = requestURI.getPort();
            boolean ok = false;
            for (int i=0; i<port.length; i++) {
                if (port[i] == rport) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                return pfx+"Request port doesn't match the port list";
            }
        }

        if (setCookie2 && ((setMask & CS_VERSION)==0)) {
            return pfx+"No cookie version specified";
        }
        return null;
    }

    private long parseExpires(String exp) throws CookieException {
        return (new ExpiresParser(exp)).parse();
    }

    class ExpiresParser {

        char [] dsep = new char[]{'-', '0'};

        String str;
        int len;
        int idx;
        String where;

        ExpiresParser(String str) {
            this.str = str;
            len = str.length();
        }

        long parse() throws CookieException {
            // ok, let's just blindly skip day of the week. Who's great idea was
            // that, anyway ???
            idx++; // it can't be the first character !
            if (!rewind(',')) {
                throw new CookieException("Failed to separate Weekday (no comma)");
            }

            // we are positioned on the comma, after the Weekday.
            // skip 2 chars, to skip comma and a following space
            idx+=2;
            // okay, now the rest of the string is:
            // dd-Mon-yyyy HH:MM:SS GMT
            where = "parsing day";
            int day = getDigit()*10+getDigit();
            eat(dsep);
            where = "parsing month";
            // let's get three chars !
            idx += 3;
            forceEOF();
            String month = str.substring(idx-3, idx);
            eat(dsep);
            int year = getDigit()*1000+getDigit()*100+getDigit()*10+getDigit();
            // okay, now the time !
            where = "parsing time";
            eat(' ');
            int hours = getDigit()*10 + getDigit();
            eat(':');
            int minutes = getDigit()*10 + getDigit();
            eat(':');
            int seconds = getDigit()*10 + getDigit();
            eat(' ');
            // now, the rest must be GMT
            forceEOF();
            if (!"gmt".equalsIgnoreCase(str.substring(idx).trim())) {
                throw new CookieException("Expected GMT");
            }

            // Okay. The string is good. Now we must translate it into
            // UTC.
            String sHours;
            String sMinutes;
            String sSeconds;
            String sDay;

            if (hours >9) {
                sHours = String.valueOf(hours);
            } else {
                sHours = "0"+hours;
            }
            if (minutes > 9) {
                sMinutes = String.valueOf(minutes);
            } else {
                sMinutes = "0"+minutes;
            }
            if (seconds > 9) {
                sSeconds = String.valueOf(seconds);
            } else {
                sSeconds = "0"+seconds;
            }
            if (day > 9) {
                sDay = String.valueOf(day);
            } else {
                sDay = "0"+day;
            }

            String format = "yyyy/MMM/dd HH:mm:ss Z";

            SimpleDateFormat sdf =
                new SimpleDateFormat(format, Locale.US);

            String dateStr = String.valueOf(year)+'/'+month+'/'+sDay+' '+
                    sHours+':'+sMinutes+':'+sSeconds+" -0000";

            try {
                Date d = sdf.parse(dateStr);
                return d.getTime();
            } catch (ParseException e) {
                int epos = e.getErrorOffset();
                String errStr;
                if (epos == 0) {
                    errStr = '*' + dateStr;
                } else if (epos > 0) {
                    errStr = dateStr.substring(0, epos) + '*' + dateStr.substring(epos);
                } else {
                    errStr = dateStr;
                }

                Log.warn("Date parsing exception, date concoted : '"+errStr+"', format : '"+format+"', original : '"+str+'\'', e);
                throw new CookieException("Date parser failed");
            }
        }

        void throwCE(String why) throws CookieException {
            if (where != null) {
                throw new CookieException(why+", while "+where);
            }
            throw new CookieException(why);
        }

        void forceEOF() throws CookieException {
            if (idx >= len) {
                throwCE("Unexpected end of string");
            }
        }

        void eat(char c) throws CookieException {
            char ch = str.charAt(idx++);

            forceEOF();

            if (ch != c) {
                throwCE("Expected '"+c+"', got '"+ch+'\'');
            }
        }

        void eat(char [] c) throws CookieException {
            char ch = str.charAt(idx++);
            forceEOF();
            for (int i=0; i<c.length; i++) {
                if (c[i] == ch) { return; }
            }
            StringBuffer msg = new StringBuffer();
            msg.append("Expected one of {");
            for (int i=0;i<c.length; i++) {
                if (i>0) { msg.append(','); }
                msg.append('\'');
                msg.append(c[i]);
                msg.append('\'');
            }
            msg.append("}, got '");
            msg.append(ch);
            msg.append('\'');
        }

        int getDigit() throws CookieException {

            forceEOF();
            char c = str.charAt(idx++);
            if ((c>='0')&&(c<='9')) {
                return (int)c - (int)'0';
            }

            throwCE("Digit expected");
            // this is never reached.
            return -1;
        }

        boolean rewind(char to) {
            for (; idx<len; idx++) {
                if (str.charAt(idx)==',') {
                    return true;
                }
            }
            return false; // not found (and exhausted)
        }
    }
}