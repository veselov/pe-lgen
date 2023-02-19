package org.vps.web.impl;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class is NOT MT Safe
 * @author Pawel S. Veselov
 */
public class IStream extends InputStream {

    private InputStream under;
    private boolean closed = false;

    private byte [] buffer;
    private int buffer_mark;

    private int threshold = 4096;

    IStream(InputStream is) {
        under = is;
        buffer = null;
        buffer_mark = 0;
    }

    public int available() throws IOException {
        checkClosed();
        return getBufferLen() + under.available();
    }

    public void close() throws IOException {
        if (closed) { return; }
        under.close();
        closed = true;
    }

    public void mark(int readLimit) {
        throw new UnsupportedOperationException("mark()");
    }

    public boolean markSupported() {
        return false;
    }

    public int read() throws IOException {
        checkClosed();
        if (getBufferLen() > 0) {
            int r = buffer[buffer_mark++];
            checkBuffer();
            return r;
        }
        return under.read();
    }

    public int read(byte [] b) throws IOException {
        return read(b, 0, b.length);
    }

    public int read(byte [] b, int offset, int len) throws IOException {

        checkClosed();

        int cbl = getBufferLen();
        if (cbl == 0) {
            return under.read(b, offset, len);
        } else if (cbl >= len) { // I have more (or exactly)
                                 // than they're asking !
            System.arraycopy(buffer, buffer_mark, b, offset, len);
            buffer_mark += len;
            checkBuffer();
            return len;
        } else { // I have less than needed.

            System.arraycopy(buffer, buffer_mark, b, offset, cbl);
            offset += cbl;
            buffer_mark += cbl;
            checkBuffer();

            int ul = under.read(b, offset, cbl);
            if (ul < 0) {
                return cbl;
            }

            return cbl + ul;
        }
    }

    private int getBufferLen() {
        if (buffer == null) { return 0; }
        return buffer.length - buffer_mark;
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("This stream is already closed");
        }
    }

    private void checkBuffer() {

        if (buffer_mark > threshold) {

            // let's shift it down...

            byte [] newBuffer = new byte[buffer.length - buffer_mark];
            System.arraycopy(buffer, buffer_mark,
                    newBuffer, 0, newBuffer.length);
            buffer_mark = 0;
            buffer = newBuffer;
        }
    }

    public void inject(byte [] data) {
        inject(data, 0, data.length);
    }

    public void inject(byte [] data, int offset, int len) {

        if (buffer == null) {
            buffer = new byte[len];
            System.arraycopy(data, offset, buffer, 0, len);
        } else {
            byte [] aux = new byte[buffer.length + len];
            System.arraycopy(buffer, 0, aux, 0, buffer.length);
            System.arraycopy(data, offset, aux, buffer.length, len);
            buffer = aux;
        }
    }

    /**
     * Returns back line, ending with CRLF.
     * @param max maximum characters allowed in the line. If 0 is specified,
     * 16384 is used.
     * @param needCRLF Controls, whether CRLF sequence, which ends the line
     * is returned in the result.
     * @param wantPartial specifies, whether upon encountering EOF before
     * CRLF, exception is thrown, or result is returned.
     */
    public String getHTTPLine(int max, boolean needCRLF,
            boolean wantPartial) throws IOException {

        StringBuffer buf = new StringBuffer();
        int last = 0;
        if (max <= 0) { max = 16384; }

        while (true) {

            int next = read();

            if (next < 0) { // EOF
                if (wantPartial) { return buf.toString(); }
                throw new IOException("Unexpected EOF. Partial buffer : "+buf);
            }

            buf.append((char)next);

            if ((last == 13)&&(next == 10)) {
                break;
            }

            if (buf.length() == max) {
                throw new IOException("Buffer overflow when waiting for line");
            }

            last = next;
        }

        if (needCRLF) {
            return buf.toString();
        }
        return buf.substring(0, buf.length()-2);
    }
}
