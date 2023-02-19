package org.vps.web.impl;


import org.vps.web.Log;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * General utility class.
 * Has this name as to because it's very short, and all the methods are
 * static.
 * @author Pawel S. Veselov
 */
public class W {

    public static int copyIO(InputStream input,
            OutputStream output) throws IOException {
        return copyIO(input, output, 16384);
    }

    public static int copyIO(Reader input,
            Writer output) throws IOException {
        return copyIO(input, output, 16384);
    }

    public static int copyIO(Reader input, Writer output,
            int bufSize) throws IOException {

        if (bufSize <= 0) { bufSize = 16384; }

        char [] buf = new char[bufSize];
        int total = 0;

        while (true) {
            int rn = input.read(buf);
            if (rn == -1) { break; }
            output.write(buf, 0, rn);
            total += rn;
        }

        output.flush();
        return total;
    }

    public static String getSpaces(int len) {
        return new String(getSpacesC(len));
    }

    private static char [] getSpacesC(int len) {
        char [] c = new char[len];
        for (int i=0; i<len; i++) {
            c[i] = 32;
        }
        return c;
    }

    private static String getLine(char [] c) {

        int last_char = c.length-1;
        for (; (c[last_char] == 0x20)&&(last_char>=0); last_char--);
        StringBuffer out = new StringBuffer();
        for (int i=0; i<=last_char; i++) {
            out.append(c[i]);
        }
        return out.toString();
    }

    public static String prettyChar(char [] c, String encoding) {

        if (encoding == null) {
            encoding = "UTF-8";
        }

        try {
            return prettyByte((new String(c)).getBytes(encoding));
        } catch (UnsupportedEncodingException e) {
            return "Encoding "+encoding+" is not supported";
        }

    }

    public static String prettyByte(byte [] b) {

        char [] line = getSpacesC(69);
        int i;

        StringBuffer full = new StringBuffer();

        for (i=0; i<b.length; i++) {

            int j = i%16;

            if ((i>0)&&(j==0)) {
               full.append(getLine(line));
               full.append('\n');
               line = getSpacesC(69);
            }

            byte _b = b[i];
            if ((_b <= 0x20)||(_b>=0x7f)) {
                line[j] = '.';
            } else {
                line[j] = (char)_b;
            }

            line[j*3+21] = hexl(_b&0xf);
            line[j*3+20] = hexl(_b>>>4);
        }

        if (i>0) {
            full.append(getLine(line));
        }

        return full.toString();
    }

    private static char hexl(int a) {
        if (a<10) return (char)(a+'0');
        return (char)(a+'A'-10);
    }

    public static int copyIO(InputStream input, OutputStream output,
            int bufSize) throws IOException {

        if (bufSize <= 0) { bufSize = 16384; }

        byte [] buf = new byte[bufSize];
        int total = 0;

        while (true) {
            int rn = input.read(buf);
            if (rn == -1) { break; }
            output.write(buf, 0, rn);
            total += rn;
        }

        output.flush();
        return total;
    }

    private static char hex0(int x) {
        if (x<=9) { return (char)(x+'0'); }
        return (char)(x+'A'-10);
    }

    public static String encodeURL(String src) {

        StringBuffer out = new StringBuffer();

        for (int i=0; i<src.length(); i++) {

            int clen = 0;
            char c = src.charAt(i);

            for ( char z = c; z > 0; z>>>=8, clen++);

            for (int j=0; j<clen; j++) {

                int x = ((c>>(8*(clen-j-1)))&0xff);

                if ((clen>1)||(x=='+')||(x=='%')||(x<=0x20)||
                        (x>=0x7f)||(x=='?')||(x=='&')||(x=='=')) {

                    out.append('%');
                    out.append(hex0(x>>>4));
                    out.append(hex0(x&0xf));

                } else if (x == ' ') {
                    out.append('+');
                } else {
                    out.append((char)x);
                }
            }
        }
        return out.toString();
    }

    final static int [] HT_ECODE = { 503, 400, 404, 405, 301, 307};
    final static String [] HT_EMSG = {
        "Service Unavailable",
        "Bad Request",
        "Not Found", 
        "Method Not Allowed",
        "Moved Permanently",
        "Temporary Redirect"};

    public static String getHTTPReason(int rc) {

        for (int i=0; i<HT_ECODE.length; i++) {
            if (HT_ECODE[i] == rc) {
                return HT_EMSG[i];
            }
        }
        return "UNKNOWN REASON";
    }

    public static void wrtStrln(OutputStream os,
            String str) throws IOException {
        wrtStr(os, str+"\r\n");
    }

    public static void wrtStrln(OutputStream os) throws IOException {
        wrtStr(os, "\r\n");
    }

    public static void wrtStr(OutputStream os,
            String str) throws IOException {

        byte [] out = new byte[str.length()];

        // FIXME : multibyte encodings !
        for (int i=0; i<out.length; i++) {
            char c = str.charAt(i);
            if (c > 255) {
                throw new IOException("char overflow, please fix !");
            }
            out[i] = (byte)c;
        }
        os.write(out);
    }

    public static String getNodePath(Node node) {

        Node next = node;
        StringBuffer strPath = new StringBuffer();
        while ((next != null) && (!(next instanceof Document))) {
            strPath.insert(0, next.getNodeName());
            strPath.insert(0, '/');
            next = next.getParentNode();
        }
        return strPath.toString();
    }

    public static void apd_pad(StringBuffer where, int value, int no_pad) {

        int maxi = 1;
        for (int i=1; i<no_pad; i++) {
            maxi *= 10;
        }

        for (int i=1; i<no_pad; i++) {
            if (value < maxi) {
                where.append('0');
            } else break;
            maxi /= 10;
        }

        if (value > 0) {
            where.append(value);
        }
    }


    private final static String [] HV_SUP = {
        "1.0", "1.1" };

    /**     
     * Returns HTTP version in an array, zeroth element is major version no,
     * first is minor.
     */
    public static int [] getHTTPVersion(String string) throws IOException {
                    
        int version[] = new int[2];
                
        if (string.startsWith("HTTP/")) {
            String aux = string.substring(5);
            StringTokenizer st = new StringTokenizer(aux, ".");
            if (st.countTokens() == 2) {
                try { 
                    version[0] = Integer.parseInt(st.nextToken());
                    version[1] = Integer.parseInt(st.nextToken());
                    string = null;
                } catch (NumberFormatException e) {
                }
            }
        }

        if (string != null) {
            throw new IOException("Bad version string : "+string);
        }

        String mv = "" + version[0] + '.' + version[1];
        boolean fgv = false;

        for (int i = 0; i<HV_SUP.length; i++) {
            if (mv.equals(HV_SUP[i])) {
                fgv = true;
                break;
            }
        }

        if (!fgv) {
            throw new IOException("HTTP version is unsupported : "+mv);
        }   
        return version;
    }   

    public static byte [] stringToBytes(String s) {

        try {
            StringReader sr = new StringReader(s);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamWriter osw = new OutputStreamWriter(baos);
            copyIO(sr, osw);
            osw.flush();
            baos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            Log.error("IO exception (b2c,c2b?)", e);
        }
        return new byte[0];
    }

    /**
     * Checks symbol for being eligible for a token symbol.
     * @param c symbol to test.
     * @return true, if symbol can not be used in a token.
     * Refer to RFC2068, 2.2, for definition of a token.
     */
    public static boolean nonTokenCh(char c) {
        if ((c>=0)&&(c<=32)) { return true; }
        switch (c) {
            case '(':
            case ')':
            case '<':
            case '>':
            case '@':
            case ',':
            case ';':
            case ':':
            case '\\':
            case '"':
            case '/':
            case '[':
            case ']':
            case '?':
            case '=':
            case '{':
            case '}':
            case 127:
                return true;
        }
        return false;
    }

    public static String getBoundaryDelimeter() {

        Random r = new Random(System.currentTimeMillis());
        char [] c = new char[32];
        for (int i=0; i<c.length; i++) {
            do {
                int p = r.nextInt(0x7e);
                c[i] = (char)p;
            } while (!Character.isLetterOrDigit(c[i])/* &&
                c[i] != '\'' &&
                c[i] != '(' &&
                c[i] != ')' &&
                c[i] != '+' &&
                c[i] != '_' &&
                c[i] != ',' &&
                c[i] != '-' &&
                c[i] != '.' &&
                c[i] != '/' &&
                c[i] != ':' &&
                c[i] != '=' &&
                c[i] != '?' */);
        }
        return new String(c);
    }

    public static String getMimeType(String fileName) {
        /*
        if (fileName.indexOf('/')>0) {
            fileName = fileName.substring(fileName.lastIndexOf('/')+1);
        }
        */
        // TODO : implement the actual mime type resultion !
        return "application/octet-stream";
    }

    public static void xx() {

        Object a = null;
        try {
            a = new Integer("xxa");
        } catch (Exception e) {

        } finally {
            if (a != null) { }
        }

    }


}