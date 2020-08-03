package com.fantow.remoting.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class FantowMQSerializable {
    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    // 通过FantowMQ协议对cmd的头部编码
    public static byte[] fantowMQProtocolEncode(RemotingCommand cmd){

        // remark应该是存在RemotingCommand中，用于提示的文字
        byte[] remarkBytes = null;
        int remarkLen = 0;
        if(cmd.getRemark() != null && cmd.getRemark().length() > 0){
            remarkBytes = cmd.getRemark().getBytes(CHARSET_UTF8);
            remarkLen = remarkBytes.length;
        }

        byte[] extFieldBytes = null;
        int extLen = 0;
        if(cmd.getExtFields() != null && !cmd.getExtFields().isEmpty()){
            extFieldBytes = mapSerialize(cmd.getExtFields());
            extLen = extFieldBytes.length;
        }

        int totalLen = calTotalLen(remarkLen,extLen);

        ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);

        // code
        headerBuffer.putShort((short) cmd.getCode());
        // languageCode
        headerBuffer.put(cmd.getLanguage().getCode());
        // version
        headerBuffer.putShort((short) cmd.getVersion());
        // opaque
        headerBuffer.putInt(cmd.getOpaque());
        // flag
        headerBuffer.putInt(cmd.getFlag());
        // remark
        if(remarkBytes != null){
            headerBuffer.putInt(remarkBytes.length);
            headerBuffer.put(remarkBytes);
        }else{
            headerBuffer.putInt(0);
        }
        //extFields
        if(extFieldBytes != null){
            headerBuffer.putInt(remarkBytes.length);
            headerBuffer.put(extFieldBytes);
        }else{
            headerBuffer.putInt(0);
        }
        return headerBuffer.array();
    }

    // 通过FantowMQ对cmd的头部进行解码
    public static RemotingCommand fantowMQProtocalDecode(byte[] headerArray){
        RemotingCommand remotingCommand = new RemotingCommand();
        ByteBuffer byteBuffer = ByteBuffer.wrap(headerArray);
        // code
        remotingCommand.setCode(byteBuffer.getShort());
        // languageCode ,0代表Java
        remotingCommand.setLanguage(LanguageCode.JAVA);
        // version
        remotingCommand.setVersion(byteBuffer.getShort());
        // opaque
        remotingCommand.setOpaque(byteBuffer.getInt());
        // flag
        remotingCommand.setFlag(byteBuffer.getInt());
        // remark
        int remarkLength = byteBuffer.getInt();
        if(remarkLength > 0){
            byte[] remarkContent = new byte[remarkLength];
            byteBuffer.get(remarkContent);
            remotingCommand.setRemark(new String(remarkContent,CHARSET_UTF8));
        }

        // extFields
        int extFieldsLength = byteBuffer.getInt();
        if(extFieldsLength > 0){
            byte[] extFieldsBytes = new byte[extFieldsLength];
            byteBuffer.get(extFieldsBytes);
            remotingCommand.setExtFields(mapDeserialize(extFieldsBytes));
        }
        return remotingCommand;
    }






    // 计算cmd序列化后的总长度
    private static int calTotalLen(int remark,int ext){
        // languageCode + version + opaque + flag + 4 + remark + 4 + ext
        int length = 2 + 1 + 2 + 4 + 4 + 4 + 4 + remark + 4 + ext;
        return length;
    }


    // 将extFields序列化为每一部分都是如下格式：
    // keyLen(2位) + keyBytes[] + valueLen(4位) + valueBytes[]
    private static byte[] mapSerialize(HashMap<String,String> map){

        if(map == null || map.isEmpty()){
            return null;
        }

        int totalLength = 0;
        int kvLength = 0;
        for(Map.Entry<String,String> entry : map.entrySet()){
            if(entry.getKey() != null && entry.getValue() != null){
                kvLength = 2 + entry.getKey().getBytes(CHARSET_UTF8).length
                    + 4 + entry.getValue().getBytes(CHARSET_UTF8).length;
                totalLength += kvLength;
            }
        }

        ByteBuffer content = ByteBuffer.allocate(totalLength);
        byte[] key;
        byte[] value;
        for(Map.Entry<String,String> entry : map.entrySet()){
            if(entry.getKey() != null && entry.getValue() != null){
                key = entry.getKey().getBytes(CHARSET_UTF8);
                value = entry.getValue().getBytes(CHARSET_UTF8);

                content.putShort((short) key.length);
                content.put(key);

                content.putShort((short) value.length);
                content.put(value);
            }
        }

        return content.array();
    }

    // byte[] --> HashMap<String,String>
    public static HashMap<String,String> mapDeserialize(byte[] bytes){
        if(bytes == null && bytes.length <= 0){
            return null;
        }

        HashMap<String,String> map = new HashMap<>();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        // 2位
        short keySize;
        byte[] keyContent;
        // 4位
        int valueSize;
        byte[] valueContent;
        while(byteBuffer.hasRemaining()){
            keySize = byteBuffer.getShort();
            keyContent = new byte[keySize];
            byteBuffer.get(keyContent);

            valueSize = byteBuffer.getInt();
            valueContent = new byte[valueSize];
            byteBuffer.get(valueContent);

            map.put(new String(keyContent,CHARSET_UTF8),new String(valueContent,CHARSET_UTF8));
        }
        return map;
    }


}
