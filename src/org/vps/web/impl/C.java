package org.vps.web.impl;

/**
 * @author Pawel S. Veselov
 */
public interface C {

    // HTTP header constants
    String CONTENT_ENCODING = "Content-Encoding";
    String CONTENT_LENGTH = "Content-Length";
    String TRANSFER_ENCODING = "Transfer-Encoding";
    String CONTENT_TYPE = "Content-Type";

    String FORM_CT_SINGLE = "application/x-www-form-urlencoded";
    String FORM_CT_MULTI = "multipart/form-data";

    // GREP modes
    int GREP_I = 0x1;
    int GREP_W = 0x2;

    // property names (System properties)
    // logging level
    String PROP_LL = "cdsiscript.loglevel";

    /**
     * Specifies this is an HTTP GET request.
     */
    int RM_GET = 1;

    /**
     * Specifies this is an HTTP POST request.
     */
    int RM_POST = 2;

    /**
     * Specifies this is an HTTP HEAD request.
     */
    int RM_HEAD = 3;

}
