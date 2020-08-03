package com.fantow.remoting.protocol;

import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;


/**
 * Json --> byte[]
 */
public abstract class RemotingSerializable {
    private static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    // 将本对象转为Json
    public byte[] encode(){
        String json = this.toJson(this,false);
        if(json != null){
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public static byte[] encode(Object obj){
        String json = toJson(obj,false);
        if(json != null){
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public static <T> T decode(byte[] data,Class<T> classOfT){
        String json = new String(data,CHARSET_UTF8);
        return fromJson(json,classOfT);
    }

    // Json --> Obj
    public static <T> T fromJson(String json,Class<T> classOfT){
        return JSON.parseObject(json,classOfT);
    }

    // Obj --> Json
    public static String toJson(Object obj,boolean prettyFormat){
        return JSON.toJSONString(obj,prettyFormat);
    }




}
