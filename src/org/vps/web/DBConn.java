package org.vps.web;

public class DBConn {

    public String user;
    public String password;
    public String url;

    public DBConn(String in_url) {

        int sIdx = in_url.indexOf('/');
        int aIdx = in_url.indexOf('@');
        int cIdx = in_url.indexOf(':');
        int c2Idx = in_url.lastIndexOf(':');

        if ((sIdx<0)||(aIdx<0)||(cIdx<0)||(c2Idx<0)||(cIdx==c2Idx)) {
            throw new RuntimeException("Malformed url : "+in_url);
        }

        String sid = null;
        String host = null;
        int port = 0;

        try {
            user = in_url.substring(0, sIdx);
            password = in_url.substring(sIdx+1, aIdx);
            sid = in_url.substring(aIdx+1, cIdx);
            host = in_url.substring(cIdx+1, c2Idx);
            String sPort = in_url.substring(c2Idx+1);
            port = Integer.parseInt(sPort);
        } catch (Exception e) {
            throw new RuntimeException("Malformed url : "+in_url);
        }

        if ("".equals(user)) {
            throw new RuntimeException("No user specified");
        }
        if ("".equals(password)) {
            throw new RuntimeException("No password specified");
        }
        if ("".equals(sid)) {
            throw new RuntimeException("No SID specified");
        }
        if ("".equals(host)) {
            throw new RuntimeException("No host specified");
        }

        url = "jdbc:oracle:thin:@"+host+':'+port+':'+sid;
    }

}