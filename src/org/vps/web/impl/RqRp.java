package org.vps.web.impl;

import org.vps.web.BrowserPiece;
import org.vps.web.HException;
import org.vps.web.Log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URI;
import java.util.zip.GZIPInputStream;

/**
 * This class represents an HTTP request reply object.
 * It holds all of the information pertaining to a single communication
 * to the server.
 * @author Pawel S. Veselov
 */
public class RqRp implements C {

    /**
     * Whether we should repost forms, if server issues 302.
     * Check the RFC2616, 10.3.3. Standard says that browsers should repost,
     * but most of the browsers won't repost.<br>
     * True by default.
     */
    public static boolean repostOn302 = true;

    private static int maxRedirects = 5;
    private static String userAgent = "cdsiscript";

    private URI requestURI;
    private URI docURI;
    private String sendURI;
    private Headers sendHeaders = new Headers();
    private Headers recvHeaders;
    private Socket commSocket;
    private int requestMethod;
    private IStream fromServer;
    private int serverCode;
    private String serverCodeReason;
    private boolean hasData;
    private InputStream dataStream;
    private boolean used;
    private byte data[];
    private byte serverData[];
    private boolean keepAlive;
    private BrowserPiece browserPiece;

    public RqRp(URI requestURI) {
        this.requestURI = requestURI;
    }

    public RqRp(URI requestURI, String referer) {
        this(requestURI, referer, null);
    }

    public RqRp(URI requestURI, RqRp referer) {
        this(requestURI, referer.docURI.toString(), referer.browserPiece);
    }

    public RqRp(URI requestURI, String referer, BrowserPiece browserPiece) {
        this(requestURI);
        if (referer != null) {
            sendHeaders.addHeader("Referer", referer);
        }
        if (browserPiece != null) {
            this.browserPiece = browserPiece;
        } else {
            this.browserPiece = new BrowserPiece() {
                public CookieJar getCookieJar() {
                    return new CookieJar();
                }

                public String getProxyHost() {
                    return null;
                }

                public int getProxyPort() {
                    return 0;
                }

                public boolean isDoProxy() {
                    return false;
                }
            };
        }
    }

    public Headers getSendHeaders() {
        return sendHeaders;
    }

    /**
     * Set the number of redirects this connection will follow.
     * @param rdr Maximum number of redirects to follow. The value
     * is only accepted if it's greater than or equal to zero.
     */
    public static void setMaxRedirects(int rdr) {
        if (rdr >= 0) {
            maxRedirects = rdr;
        }
    }

    public void setKeepAlive(boolean ka) {
        keepAlive = ka;
    }

    /**
     * Specifies the request method.
     * @param rm request method. Only defined constants can be used :
     * {@link C#RM_GET}, {@link C#RM_POST} and {@link C#RM_HEAD}.
     */
    public void setRequestMethod(int rm) {
        switch (rm) {
            case RM_GET:
            case RM_POST:
            case RM_HEAD:
                break;
            default:
                throw new HException("Unknown request method.");
        }
        requestMethod = rm;
    }

    /**
     * Recycle the connection, so it's prepared to be reused again.
     * Only valid for keep alive connections (see
     * {@link #setKeepAlive(boolean)})
     */
    public void recycle() {
    }

    /**
     * Engages the connection.
     * The requets are sent, the replies are receivied.
     */
    public void engage() {

        if (used) {
            throw new HException("This request/response has already been used");
        }

        if (requestMethod==0) {
            throw new HException("Request method not defined");
        }

        if ((sendHeaders.doesExist(TRANSFER_ENCODING) ||
                sendHeaders.doesExist(CONTENT_LENGTH))&&!hasData) {
            throw new HException("Headers indicate body must be provided, but no data specified");
        }

        if (hasData && (requestMethod == RM_GET || requestMethod == RM_HEAD)) {
            throw new HException("Data can not be present for a GET/HEAD request");
        }

        used = true;

        URI requestURI = this.requestURI;
        int tries = maxRedirects;
        int requestMethod = this.requestMethod;
        boolean hasData = this.hasData;

        try {

            while (true) {

                if (tries-- == 0) {
                    throw new HException("Too many redirects!");
                }
                // open the communication socket.
                if (browserPiece.isDoProxy()) {
                    connectViaProxy(requestURI);
                } else {
                    connectDirectly(requestURI);
                }

                sendHeaders.setHeader("Connection", "Close");
                sendHeaders.newHeader("User-Agent", userAgent);
                // sendHeaders.newHeader("Accept-Charset", "utf-8,*");
                sendHeaders.setHeader("Accept-Encoding", "gzip");
                sendHeaders.newHeader("Accept", "*/*");
                sendHeaders.newHeader("Host", requestURI.getHost());
                sendHeaders.newHeader("Accept-Charset", "ISO-8859-1,utf-8");
                sendHeaders.newHeader("Accept-Language", "en-us, en");

                fromServer = new IStream(commSocket.getInputStream());
                OutputStream toServer = commSocket.getOutputStream();
                Log.debug("Connected to "+commSocket.getRemoteSocketAddress());

                CookieJar cookieJar = browserPiece.getCookieJar();
                cookieJar.applyCookies(sendHeaders, requestURI);

                // send the request out.
                {
                    String rq;
                    switch (requestMethod) {
                        case RM_GET:
                            rq = "GET";
                            hasData = false;
                            break;
                        case RM_POST:
                            rq = "POST";
                            break;
                        case RM_HEAD:
                            rq = "HEAD";
                            break;
                        default:
                            throw new HException("Unexpected rq : "+
                                    requestMethod);
                    }
                    rq = rq + ' ' + sendURI + " HTTP/1.1";
                    Log.noise("Request line sent to " + requestURI + " : " + rq);
                    W.wrtStrln(toServer, rq);
                }

                if (hasData) {
                    loadData();
                    sendHeaders.newHeader(CONTENT_LENGTH,
                            String.valueOf(data.length));
                    sendHeaders.newHeader(CONTENT_TYPE, "text/plain");
                } else {
                    sendHeaders.removeHeader(CONTENT_LENGTH);
                    sendHeaders.removeHeader(TRANSFER_ENCODING);
                    sendHeaders.removeHeader(CONTENT_TYPE);
                }
                // TODO : may be we want to transfer-encode this thing ?

                if (Log.noise()) {
                    PrintWriter m = Log.getMulti(Log.NOISE);
                    m.println("Header sent to the "+requestURI.toString());
                    sendHeaders.dump(m);
                    m.close();
                    if (hasData) {
                        Log.noise("Outgoing data : \n"+W.prettyByte(data));
                    }
                }

                sendHeaders.sendHeaders(toServer);

                if (hasData) {
                    toServer.write(data);
                }

                getResponseLine();
                recvHeaders = new Headers(fromServer);

                if (Log.noise()) {
                    PrintWriter m = Log.getMulti(Log.NOISE);
                    m.println("Recvd server headers from "+
                            requestURI.toString());
                    recvHeaders.dump(m);
                    m.close();
                }

                cookieJar.addCookies(recvHeaders, requestURI);

                boolean retry = false;

                switch (serverCode) {

                    case 301:
                        requestURI =
                            new URI(recvHeaders.getHeaderString("location"));
                        retry = true;
                        break;
                    case 302:
                        requestURI =
                            new URI(recvHeaders.getHeaderString("location"));
                        retry = true;
                        if (!repostOn302) {
                            requestMethod = RM_GET;
                            hasData = false;
                        }
                        break;
                    case 303:
                    case 307:
                        retry = true;
                        requestURI =
                            new URI(recvHeaders.getHeaderString("location"));
                        requestMethod = RM_GET;
                        hasData = false;
                        break;
                }

                if (retry) {
                    Log.debug("Redirecting to : "+requestURI);
                    commSocket.close();
                    continue;
                }

                getServerStream(fromServer);
                break;
            }

            docURI = requestURI;

        } catch (HException e) {
            throw e;
        } catch (Exception e) {
            Log.error("Exception on engage : ", e);
            throw new HException("RqRp:engage()");
        }
    }

    public String getResponseReason() {
        return serverCodeReason;
    }

    public int getResponseCode() {
        return serverCode;
    }

    public Headers getResponseHeaders() {
        return recvHeaders;
    }

    public byte [] getResponseData() {
        return serverData;
    }

    private void getServerStream(InputStream src) throws IOException {

        /*
        if (!recvHeaders.doesExist(TRANSFER_ENCODING) &&
                !recvHeaders.doesExist(CONTENT_LENGTH)) {
            Log.debug("No data expected from server");
            return;
        }
        */

        if ((serverCode >= 100 && serverCode < 200) ||
                (serverCode == 204) || (serverCode == 304)) {
            Log.noise("No data expected from server because of status code");
            return;
        }

        if (requestMethod == RM_HEAD) {
            Log.noise("No data expected from server because it's a head request");
            return;
        }

        boolean isChunked =
            recvHeaders.equalsIC(TRANSFER_ENCODING, "chunked");
        boolean isGzipped =
            recvHeaders.equalsIC(CONTENT_ENCODING, "gzip");

        if (isChunked) {
            Log.noise("Using dechunker...");
            src = new ChunkedIS((IStream)src);
        }

        if (isGzipped) {
            Log.noise("Using gunzipper...");
            src = new GZIPInputStream(src);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream os;

        os = baos;

        W.copyIO(src, os);

        serverData = baos.toByteArray();

        if (Log.noise()) {
            Log.noise("Received data : \n" + W.prettyByte(serverData));
        }

        baos.close();
    }

    /*
    public Document createHTMLDocument() {

        if (serverData == null) {
            return null;
        }

        InputSource is = new InputSource(new ByteArrayInputStream(serverData));
        return IParser.parse(docURI, is);
    }
    */

    public URI getDocURI() {
        return docURI;
    }

    private void getResponseLine() throws IOException {
        String rlFromServer = fromServer.getHTTPLine(0, false, false);

        Log.noise("Server response : "+rlFromServer);

        String leftOver = rlFromServer;
        int sp = leftOver.indexOf((int)' ');
        if (sp <= 0) {
            throw new IOException("No distinct HTTP version in reply");
        }

        String versionString = leftOver.substring(0, sp);
        leftOver = leftOver.substring(sp+1);
        W.getHTTPVersion(versionString);

        sp = leftOver.indexOf((int)' ');
        String rcode;
        if (sp <= 0) { // no reason text.
            Log.warn("No reason text in server reply");
            rcode = leftOver;
            serverCodeReason = "";
        } else {
            rcode = leftOver.substring(0, sp);
            serverCodeReason = leftOver.substring(sp+1);
        }

        try {
            serverCode = Integer.parseInt(rcode);
        } catch (Exception e) {
            throw new IOException("Not a numeric response code in "+
                    rlFromServer);
        }
    }

    private void connectViaProxy(URI requestURI) throws IOException {
        commSocket = new Socket(browserPiece.getProxyHost(), browserPiece.getProxyPort());
        sendURI = requestURI.toString();
    }

    private void connectDirectly(URI requestURI) throws IOException {
        String host = requestURI.getHost();
        int port = requestURI.getPort();
        String scheme = requestURI.getScheme();
        if (!"http".equalsIgnoreCase(scheme)) {
            throw new HException("Scheme "+scheme+" is not supported");
        }
        if (port == -1) {
            port = 80;
        }
        commSocket = new Socket(host, port);
        sendURI = requestURI.getRawPath();
        if ("".equals(sendURI)) {
            sendURI = "/";
        }
        String aux = requestURI.getRawQuery();
        if (aux != null) {
            sendURI += '?'+aux;
        }
        aux = requestURI.getRawFragment();
        if (aux != null) {
            sendURI += '#' + aux;
        }
    }

    private void loadData() throws IOException {

        if (!hasData) { return; }
        if (dataStream == null) { return; }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        W.copyIO(dataStream, baos);
        dataStream.close();
        data = baos.toByteArray();
    }

    public void setData(InputStream is) {
        dataStream = is;
        hasData = true;
    }

    public void setData(byte [] data) {
        this.data = data;
        hasData = true;
    }

    public String getRequestURL() {
        return requestURI.toString();
    }

    public static void setUserAgent(String ua) {
        userAgent = ua;
    }

}
