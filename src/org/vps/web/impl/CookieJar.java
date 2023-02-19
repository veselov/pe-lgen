package org.vps.web.impl;

import org.vps.web.Log;

import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * @author Pawel S. Veselov
 */
public class CookieJar {

    private Vector<Cookie> cookies = new Vector<Cookie>();

    /**
     * Creates new empty cookie jar.
     */
    public CookieJar () {}

    /**
     * Finds all cookies in the specified headers.
     * The cookies are extracted out of the header, then validated
     * against specified request URI.
     * @param headers Headers as of the reply
     * @param requestURI URI used to generate the request
     */
    public void addCookies(Headers headers, URI requestURI) {
        loadUp(headers, requestURI);
    }

    private void loadUp(Headers headers, URI requestURI) {

        // let's see what kind of cookies we got here...

        Vector<Cookie> server = load(headers, "set-cookie", requestURI);
        Vector<Cookie> server2 = load(headers, "set-cookie2", requestURI);

        server.addAll(server2);

        if (server.size() == 0) { // no new cookies
            return;
        }

        // let's see if there are any cookies that got invalidated by
        // new cookies.

        Vector<Cookie> toDiscard = new Vector<Cookie>();

        for (Cookie oldCookie : cookies) {

            for (Cookie newCookie : server) {

                if (oldCookie.invalidatedBy(newCookie)) {
                    toDiscard.add(oldCookie);
                    // no need to test this old cookie anymore
                    break;
                }
            }
        }

        cookies.removeAll(toDiscard);
        cookies.addAll(server);
    }

    public void applyCookies(Headers headers, URI requestURI) {

        // throw away expired cookies.
        Vector<Cookie> expired = new Vector<Cookie>();
        for (Cookie next : cookies) {
            if (next.isExpired()) {
                expired.add(next);
                Log.debug("Discarding expired cookie " + next);
            }
        }
        cookies.removeAll(expired);

        Map forC1 = new SequenceMap();
        Map forC2 = new SequenceMap();

        for (Cookie next : cookies) {
            if (next.appliesTo(requestURI)) {
                Log.debug("ck: " + next + " applies to " + requestURI);
                next.applyCookie(forC1, forC2);
            } else {
                Log.debug("ck: " + next + " doesn't apply to " + requestURI);
            }
        }

        mapToHeader(headers, "Cookie", forC1);
        mapToHeader(headers, "Cookie2", forC2);
    }

    private void mapToHeader(Headers hdr, String hdrName, Map map) {
        hdr.removeHeader(hdrName);
        if (map.size() == 0) { return; }
        for (Object key : map.keySet()) {
            hdr.addWSep(hdrName, key + "=" + map.get(key), "; ");
        }
    }

    // this method must not return a null. Empty vector please
    private Vector<Cookie> load(Headers headers, String headerName, URI requestURI) {

        boolean is2 = ("cookie2".equalsIgnoreCase(headerName)||
                ("set-cookie2".equalsIgnoreCase(headerName)));

        Vector<Cookie> rtrn = new Vector<Cookie>();

        String vals [] = headers.getHeaderArray(headerName);
        if ((vals == null) || (vals.length == 0)) {
            return rtrn;
        }

        for (String val : vals) {

            // okay, let's get the cookies out !
            PI pi = new PI(val);
            HashMap<String, String> data = new HashMap<String, String>();

            try {
                while (true) {
                    String attr = pi.getToken(false);
                    if (attr == null) {
                        break;
                    }
                    pi.rewindTo('=');
                    String value = pi.getToken(true);
                    if (value == null) {
                        Log.warn("No cookie value for " + attr +
                                ", cookie string " + val);
                        continue;
                    }

                    if (data.get(attr) != null) {
                        Log.warn("Cookie has two values for " + attr);
                    }

                    data.put(attr, value);

                    if (!pi.rewindTo(';')) {
                        break;
                    }
                }
            } catch (ParseException pe) {
                pe.log(Log.WARN);
            }

            try {
                Cookie cookie = new Cookie(data, is2);
                String reason = cookie.validFor(requestURI);
                if (reason != null) {
                    throw new CookieException("Cookie is not valid for " +
                            requestURI + ", reason given=" + reason);
                }
                cookie.complete(requestURI);
                rtrn.add(cookie);
            } catch (CookieException e) {
                Log.warn("Bad cookie line in the header : " + val, e);
            }
        }

        return rtrn;
    }

    class ParseException extends Exception {
        PI pi;
        ParseException(PI pi, String s) {
            super(s);
            this.pi = pi;
        }

        void log(int pri) {
            PrintWriter m = Log.getMulti(pri);
            m.println("Parsing failed at position "+pi.idx);
            m.println(">>"+pi.string+"<<");
            printStackTrace(m);
            m.close();
        }
    }

    class PI {
        String string; // DON'T SET
        int idx;
        int len;
        PI(String s) {
            string = s;
            len = s.length();
        }

        private boolean eof() { return idx == len; }

        private String getToken(boolean allow_quoted) throws ParseException {

            eatSpaces();
            if (eof()) { return null; }
            StringBuilder ack = new StringBuilder();

            boolean next_escaped = false;
            boolean virgin = true;
            boolean quoted = false;

            char c = 'V';

            for (; !eof(); idx++) {

                c = string.charAt(idx);

                if (next_escaped) {
                    ack.append(c);
                    next_escaped = false;
                    continue;
                }

                if (virgin) {
                    virgin = false;
                    if (allow_quoted && (c=='"')) {
                        quoted = true;
                        continue;
                    }
                }

                if (quoted) {
                    if (c == '"') {
                        idx++;
                        break;  // well, we're done !
                    }
                    if (c == '\\') {
                        next_escaped = true;
                        continue;
                    }
                } else { // not quoted, check for special ?

                    // boolean quit = W.nonTokenCh(c);
                    // FIXME . No fixme, but, apparently, servers don't
                    // care about rfc2965/rfc2068 and won't quote tokens
                    // even if they suppose to. So let's break on ';' only
                    boolean quit = (c == ';') || (c=='=');

                    // that's okay, if we encountered any of special symbols
                    // it's the end for this token.
                    if (quit) { break; }
                }

                ack.append(c);
            }

            if (ack.length() == 0) {
                if (allow_quoted) {
                    return "";
                } else {
                    throw new ParseException(this,
                            "Unexpected token character : " +
                            c + '('+(int)c+')');
                }
            }

            if (next_escaped) {
                throw new ParseException(this, "Unterminated escape sequence");
            }

            if (quoted && (c!='"')) {
                throw new ParseException(this, "Unterminated quoted string");
            }

            return ack.toString();
        }

        /**
         * Currently rewinds works this way.
         * if an EOF is encountered before the character, false is returned
         * if character is encoutered, index is placed right after the
         * counter, and true is returned.
         * If, before the character is encountered, a symbol different from
         * #32 or #9 is encountered, exception is thrown.
         * @param c character to rewind to
         * @return <code>true</code> if successfully rewinded to
         * @throws org.vps.web.impl.CookieJar.ParseException if character other than a white space, or the specified
         * character was met before the EOF.
         */
        private boolean rewindTo(char c) throws ParseException {

            eatSpaces();
            if (eof()) { return false; }
            if (c == string.charAt(idx)) {
                idx++;
                return true;
            }

            throw new ParseException(this, "Expected character '"+c+'\'');
        }

        private void eatSpaces() {
            while (!eof()) {
                char c = string.charAt(idx);
                if ((c != ' ')&&(c != '\t')) { break; }
                idx++;
            }
        }
    }
}