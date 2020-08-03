package com.fantow.remoting.common;

public enum TlsMode {

    // 不支持SSL； 任何传入的SSL握手都会被拒绝，从而导致连接关闭。
    DISABLE("disable"),
    // SSL是可选的，因此，在这种模式下，服务器可以使用或不使用SSL来提供客户端连接
    PERMISSIVE("permissive"),
    // 强制执行：是必需的SSL，因此，非SSL连接也将被拒绝
    ENFORCING("enforcing");

    private String name;

    TlsMode(String name){
        this.name = name;
    }

    public static TlsMode parse(String mode){
        for(TlsMode tlsMode : TlsMode.values()){
            if(tlsMode.name.equals(mode)){
                return tlsMode;
            }
        }
        // 默认为可选类型
        return PERMISSIVE;
    }

    public String getName(){
        return this.name;
    }

}
