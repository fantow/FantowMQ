package com.fantow.remoting.protocol;

public enum SerializeType {
    JSON((byte) 0),
    FANTOWMQ((byte) 1);

    private byte code;

    SerializeType(byte code){
        this.code = code;
    }

    public byte getCode(){
        return code;
    }

    // 通过code 获取SerializeType
    public static SerializeType valueOf(byte code){
        for(SerializeType type : SerializeType.values()){
            if(type.getCode() == code){
                return type;
            }
        }
        throw new IllegalArgumentException();
    }

}
