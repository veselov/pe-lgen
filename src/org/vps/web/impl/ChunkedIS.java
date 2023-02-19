package org.vps.web.impl;

import java.io.IOException;

import java.io.InputStream;

/**
 * @author Pawel S. Veselov
 */
public class ChunkedIS extends InputStream {

    private IStream source;
    private boolean lastChunkSeen = false;
    private boolean firstChunkSeen = false;
    private int chunkSize = 0;

    public ChunkedIS(IStream source) {
        this.source = source;
    }

    public int read() throws IOException {

        if (lastChunkSeen) { return -1; }

        if (chunkSize == 0) {
            getNextChunk();
            return read();
        }

        chunkSize--;
        return source.read();
    }

    private void getNextChunk() throws IOException {

        String chunkLine = source.getHTTPLine(0, false, false).toUpperCase();

        if (firstChunkSeen) {
            if (!"".equals(chunkLine)) {
                throw new IOException("Excpected CRLF after chunk, got : "+
                        chunkLine);
            }
            chunkLine = source.getHTTPLine(0, false, false).toUpperCase();
        } else {
            firstChunkSeen = true;
        }

        StringBuilder ckl = new StringBuilder();

        for (int i=0; i<chunkLine.length(); i++) {
            char c = chunkLine.charAt(i);
            if (((c>='0')&&(c<='9'))||((c>='A')&&(c<='F'))) {
                ckl.append(c);
                continue;
            }
            break;
        }

        String chunkSize = ckl.toString();
        if (chunkSize.length() == 0) {
            throw new IOException("Invalid chunk size line : "+chunkLine);
        }

        try {
            if ((this.chunkSize = Integer.parseInt(chunkSize, 16)) == 0) {
                lastChunkSeen = true;
            }
        } catch (Exception e) {
            throw new IOException("Error converting "+chunkSize+" to integer");
        }
    }

    public void close() throws IOException {
        if (!lastChunkSeen) {
            throw new IOException("Closing chunked stream before last chunk is read!");
        }
    }

}
