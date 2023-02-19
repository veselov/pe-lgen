package org.vps.web;

import org.vps.web.impl.CookieJar;

public interface BrowserPiece {

    CookieJar getCookieJar();
    String getProxyHost();
    int getProxyPort();
    boolean isDoProxy();

}
