package com.fantow.remoting.netty;

import com.fantow.remoting.common.TlsMode;

public class TlsSystemConfig {

    public static final String TLS_SERVER_MODE = "tls.server.mode";




    public static TlsMode tlsMode = TlsMode.parse(System.getProperty(TLS_SERVER_MODE,
                                            "permissive"));

}
