package org.vps.web.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.*;
import java.text.DateFormat;
import java.text.ParseException;

/**
 * Headers class.
 *
 * This class is NOT MT-Safe. If it needs to be, then all of it needs
 * to be synchronized. Don't forget about cookie jars.
 *
 * @author Pawel S. Veselov
 */
public class Headers {

    private Map header;
    private static final String[] _EMPTY_STRING = new String[0];

    /**
     * Create new, empty, headers object
     */
    public Headers() {
        header = Collections.synchronizedMap(new TreeMap(new ICC()));
    }

    /**
     * Create headers based on an IStream object.
     * @param src stream to read header information from.
     */
    public Headers(IStream src) throws IOException {

        this();

        while (true) {
            String headerLine = src.getHTTPLine(0, false, false);
            // end of header is determined by an empty line...
            if (headerLine.length() == 0) { break; }

            int cpos = headerLine.indexOf((int)':');
            if (cpos < 1) {
                throw new IOException("Malformed header line : "+headerLine);
            }
            String name = headerLine.substring(0, cpos).trim();
            // TODO : can I legally trim this line ?
            String val = headerLine.substring(cpos+1).trim();

            Object oldValue = header.get(name);
            if (oldValue == null) {
                header.put(name, val);
            } else if (oldValue instanceof String) {
                Vector v = new Vector();
                v.add(oldValue);
                v.add(val);
                header.put(name, v);
            } else {
                Vector v = (Vector)oldValue;
                v.add(val);
            }
        }
    }

    /**
     * Create an exact copy of this headers.
     * @return exact copy of this headers. The backing hashtable is
     * also copied, not rereferenced.
     */
    public Object clone() {

        Headers copy = new Headers();

        for (Iterator i = header.keySet().iterator(); i.hasNext(); ) {
            Object key = i.next();
            copy.header.put(key, header.get(key));
        }

        return copy;
    }

    /**
     * Sets a header field.
     * An old value for this header name is destroyed.
     * @param name header name
     * @param value header value
     */
    public void setHeader(String name, String value) {
        header.put(name, value);
    }

    /**
     * Only set header, if it doesn't exist.
     * @param name header name
     * @param value header value
     * @return true if header was set, false if old value remained.
     */
    public boolean newHeader(String name, String value) {
        if (doesExist(name)) { return false; }
        setHeader(name, value);
        return true;
    }

    /**
     * Sets a multiline header.
     * An old value for this header name is destroyed.
     * @param name header name
     * @param value header values. If the specified array is null
     * or empty, current header is deleted, but not recreated
     */
    public void setHeader(String name, String [] value) {
        if ((value == null) || (value.length == 0)) {
            removeHeader(name);
            return;
        }

        setHeader(name, value[0]);

        for (int i=1; i<value.length; i++) {
            addHeader(name, value[i]);
        }
    }

    /**
     * Removes a header line.
     * @param name header name
     */
    // TODO : may be return how many were deleted ?
    public void removeHeader(String name) {
        header.remove(name);
    }

    /**
     * Adds value to a header name.
     * If such header name doesn't yet exist, this works as
     * {@link #setHeader(String,String)}.
     * @param name header name.
     * @param value value to add.
     */
    public void addHeader(String name, String value) {

        Vector v;
        Object cur = header.get(name);
        if (cur == null) {
            setHeader(name, value);
            return;
        }

        if (cur instanceof Vector) {
            v = (Vector)cur;
        } else {
            v = new Vector();
            v.add(cur);
            header.put(name, v);
        }
        v.add(value);
    }

    public void sendHeaders(OutputStream os) throws IOException {

        for (Iterator keys = getHeaderNames().iterator();
                keys.hasNext(); ) {

            String key = (String)keys.next();

            boolean mKey = isMultiHeader(key);
            if (mKey) {
                for (Iterator i = getHeaderVector(key).iterator();
                        i.hasNext(); ) {
                    String out = key+": "+i.next();
                    W.wrtStr(os, out+"\r\n");
                    // System.err.println("HDR : "+out);
                }
            } else {
                String out = key+": "+getHeaderString(key);
                W.wrtStr(os, out+"\r\n");
                // System.err.println("HDR : "+out);
            }
        }

        W.wrtStr(os, "\r\n");
        os.flush();
    }

    public void dump(PrintStream out) {
        for (Iterator keys = getHeaderNames().iterator(); keys.hasNext(); ) {
            String key = (String)keys.next();
            String [] vals = getHeaderArray(key);
            for (int i=0; i<vals.length; i++) {
                out.println(key+": "+vals[i]);
            }
        }
    }

    public void dump(PrintWriter out) {
        for (Iterator keys = getHeaderNames().iterator(); keys.hasNext(); ) {
            String key = (String)keys.next();
            String [] vals = getHeaderArray(key);
            for (int i=0; i<vals.length; i++) {
                out.println(key+": "+vals[i]);
            }
        }
    }

    /**
     * Returns whether this is multiple or single header.
     * @return true if header is comprised of multiple values,
     * false otherwise (including when there is no header)
     */
    public boolean isMultiHeader(String name) {
        Object v = header.get(name);
        if (v == null) { return false; }
        if (v instanceof Vector) { return true; }
        return false;
    }

    public Object getHeaderObject(String name) {
        return header.get(name);
    }

    public String getHeaderString(String name) {
        return (String)header.get(name);
    }

    public long getHeaderATime(String name) {
        Object x = getHeaderObject(name);
        if (x == null) { return 0L; }
        if (x instanceof Vector && ((Vector)x).size() > 0) {
            x = ((Vector)x).elementAt(0);
        }

        if (!(x instanceof String)) {
            return 0L;
        }

        DateFormat df = new HttpDTFormat();
        try {
            return df.parse((String)x).getTime();
        } catch (ParseException e) {}

        return 0L;
    }

    public long getHeaderATimeEx(String name) throws ParseException {
        Object x = getHeaderObject(name);
        if (x == null) {
            throw new ParseException("No header "+name+" found", -1);
        }
        if (!(x instanceof String)) {
            throw new ParseException("Header +"+name+" is ambigous", -1);
        }

        DateFormat df = new HttpDTFormat();
        return df.parse((String)x).getTime();
    }

    public String [] getHeaderArray(String name) {
        Object v = header.get(name);
        if (v == null) { return _EMPTY_STRING; }
        if (v instanceof String) {
            return new String[]{(String)v};
        }
        Vector ve = (Vector)v;
        String [] out = new String[ve.size()];
        for (int i=0; i<out.length; i++) {
            out[i] = (String)ve.elementAt(i);
        }
        return out;
    }

    public boolean doesExist(String name) {
        return header.get(name) != null;
    }

    public Vector getHeaderVector(String name) {
        return (Vector)header.get(name);
    }

    public Collection getHeaderNames() {
        return header.keySet();
    }

    public String toString(String name) {
        StringBuffer sb = new StringBuffer();
        String [] a = getHeaderArray(name);
        for (int i=0; i<a.length; i++) {
            if (i>0) { sb.append('\n'); }
            sb.append(name);
            sb.append(": ");
            sb.append(a[i]);
        }
        return sb.toString();
    }

    public boolean onlyIC(String name, String value) {
        String a[] = getHeaderArray(name);
        for (int i=0; i<a.length; i++) {
            if (!value.equalsIgnoreCase(a[i])) { return false; }
        }
        return true;
    }

    public boolean equalsIC(String name, String value) {
        String a[] = getHeaderArray(name);
        for (int i=0; i<a.length; i++) {
            if (value.equalsIgnoreCase(a[i])) { return true; }
        }
        return false;
    }

    public boolean containsIC(String name, String value) {
        String a[] = getHeaderArray(name);
        for (int i=0; i<a.length; i++) {
            if ((a[i].indexOf(value)>=0)) { return true; }
        }
        return false;
    }

    public void addWSep(String name, String value, String sep) {
        if (!doesExist(name)) {
            setHeader(name, value);
            return;
        }

        String a[] = getHeaderArray(name);
        String v = a[a.length-1];
        if ((v==null)||(v.length()==0)) {
            v = value;
        } else {
            v += sep + value;
        }
        a[a.length-1] = v;

        setHeader(name, a);
    }

    public void addWComma(String name, String value) {
        addWSep(name, value, ",");
    }

    class ICC implements Comparator {

        public int compare(Object o1, Object o2) {
            return ((String)o1).compareToIgnoreCase((String)o2);
        }

        public boolean equals(Object o) {
            return false;
        }
    }
}
