package com.fantow.remoting.protocol;

import com.fantow.remoting.CommandCustomHeader;
import com.fantow.remoting.RemotingCommandType;
import com.fantow.remoting.annotation.CFNotNull;
import com.fantow.remoting.exception.RemotingCommandException;
import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RemotingCommand {

    private static final Logger logger = null;
    public static final String SERIALIZE_TYPE_PROPERTY = "serialize.type";
    public static final String SERIALIZE_TYPE_ENV = "SERIALIZE_TYPE";
    private static final int RPC_TYPE = 0;
    private static final int RPC_ONEWAY = 1;

    // 用于缓存Class对应的Fields信息，因为反射很消耗性能
    private static final Map<Class<? extends CommandCustomHeader>,Field[]> CLASS_HASH_MAP
             = new HashMap<>();
    // 用于缓存该Field是否可以为空
    private static final Map<Field,Boolean> NULLABLE_FIELD_CACHE = new HashMap<>();
    // 用于缓存类型，感觉这个的用途不大
    private static final Map<Class,String> CANONICAL_NAME_CACHE = new HashMap<>();

    private static AtomicInteger requestId = new AtomicInteger(0);

    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();


    // 配置默认的序列化方式
    static {
        // key,default value
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (protocol != null && protocol.length() > 0) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException ex) {
                ex.printStackTrace();
            }
        }
    }

    // 具体协议结构：code + languageCode + version + opaque + flag + remark + extFields
    // 请求操作码
    private int code;
    // 请求方实现的语言
    private LanguageCode language = LanguageCode.JAVA;
    // 请求方程序版本
    private int version = 0;
    // 相当于requestId，
    private int opaque = requestId.getAndIncrement();
    // 用来区分是普通RPC还是OnewayRPC
    private int flag = 0;
    // 自定义文本信息
    private String remark;
    // 自定义拓展属性(KV形式存储)
    private HashMap<String,String> extFields;
    private transient CommandCustomHeader customHeader;

    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    private transient byte[] body;



    // 传入请求码，自定义头部，返回一个RemotingCommand
    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        return cmd;
    }

    // 创建响应command
    public static RemotingCommand createResponseCommand(int code, String remark,
                                                        Class<? extends CommandCustomHeader> classHeader) {

        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);

        if (classHeader != null){
            try {
                CommandCustomHeader header = classHeader.newInstance();
                cmd.customHeader = header;
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code,String remark){
        return createResponseCommand(code,remark,null);
    }

    // 对消息解码
    public static RemotingCommand decode(ByteBuffer byteBuffer){
        // 获取ByteBuffer总长度
        int length = byteBuffer.limit();
        int oriHeaderLen = byteBuffer.getInt();
        int headerLength = getHeaderLength(oriHeaderLen);

        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        // 对消息头部解码
        RemotingCommand cmd = headerDecode(headerData,getProtocalType(oriHeaderLen));

        // 这里的4，应该是在协议中设定的，消息总长度字段会占4位
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if(bodyLength > 0){
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        cmd.setBody(bodyData);

        return cmd;
    }





    // 为什么需要使用 & 0xFFFFFF
    // https://www.cnblogs.com/puyangsky/p/6818171.html
    public static int getHeaderLength(int length){
        // 将8bit的byte型数值通过符号扩展转为32bit的int型数值
        // 该操作一般发生在将一个低等级的类型转为宽度更宽的类型时，并且不希望有符号扩展
        // 通过将高16位置0，低16位不变。
        return length & 0xFFFFFF;
    }

    // 解码header
    private static RemotingCommand headerDecode(byte[] headerData,SerializeType type){
        switch (type){
            case JSON:
                RemotingCommand resultJson = RemotingSerializable.decode(headerData,RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case FANTOWMQ:
                RemotingCommand resultFantowMQ = FantowMQSerializable.fantowMQProtocalDecode(headerData);
                resultFantowMQ.setSerializeTypeCurrentRPC(type);
                return resultFantowMQ;
            default:
                break;
        }
        return null;
    }


    public static SerializeType getProtocalType(int source){
        // 传入的source是32位，这里通过source >> 24右移24位，得到高八位数字
        return SerializeType.valueOf((byte)((source >> 24) & 0xFF));
    }


    // 添加开关用"|" ，判断开关用 "&"
    public void markResponseType(){
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public void markOnewayRPC(){
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    public static int createNewRequestId(){
        return requestId.getAndIncrement();
    }

    // -----  encode -----

    // 根据不同的编码协议，编码
    private byte[] headerEncode(){
        this.makeCustomHeaderToNet();
        if(SerializeType.FANTOWMQ == serializeTypeCurrentRPC){
            return FantowMQSerializable.fantowMQProtocolEncode(this);
        }else{
            return RemotingSerializable.encode(this);
        }
    }

    // 这个方法好像没什么用？
    public void makeCustomHeaderToNet(){
        if(this.customHeader != null){
            Field[] fields = getClazzFields(customHeader.getClass());
            if(this.extFields == null){
                this.extFields = new HashMap<>();
            }

            for(Field field : fields){
                if(!Modifier.isStatic(field.getModifiers())){
                    String name = field.getName();
                    Object value = null;
                    try{
                        field.setAccessible(true);
                        // 从customHeader中获取值
                        value = field.get(this.customHeader);
                    }catch (Exception ex){
                        ex.printStackTrace();
                    }
                    if(value != null){
                        this.extFields.put(name,value.toString());
                    }
                }
            }
        }
    }

    // 通信协议格式应该是 ： 消息总长度(4位) + 头部长度(4位) + 消息头数据 + 消息主体数据

    // encode逻辑主体
    public ByteBuffer encode(){
        // header长度
        int length = 4;

        // header数据长度
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // body数据长度
        if(this.body != null){
            length += body.length;
        }

        // 这个4应该是消息的总长度占4位
        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // 长度
        result.putInt(length);

        // header长度
        // 这里会调用markProtocolType()将headerData.length与ProtocolType合在一起
        result.put(markProtocolType(headerData.length,serializeTypeCurrentRPC));

        // header 数据
        result.put(headerData);

        // 写入body 数据
        if(this.getBody() != null){
            result.put(this.getBody());
        }

        // 翻转缓冲区
        result.flip();

        return result;
    }


    // 对消息头部进行编码
    public ByteBuffer encodeHeader(){
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    // length 表示消息体长度
    public ByteBuffer encodeHeader(int bodyLength){
        // 一开始用4位表示消息头长度
        int length = 4;

        // 将头部转为byte[]
        byte[] headerData = this.headerEncode();

        length += headerData.length;

        length += bodyLength;

        // 感觉这里是不是重复加了一个4 ?
        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // 消息总长度
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length,serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        result.flip();

        return result;
    }


    // ----- decode -----

    // 对CommandHeader进行解码
    // 这里需要传入自定义的Class来解析extField部分的内容。
    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {

        CommandCustomHeader objectHeader = null;
        try {
            objectHeader = classHeader.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        if(this.extFields != null){
            // 获取类中所有的属性
            Field[] fields = getClazzFields(classHeader);
            for(Field field : fields){
                // 找到没有被static修饰的属性
                if(!Modifier.isStatic(field.getModifiers())){
                    String fieldName = field.getName();
                    try{
                        String value = this.extFields.get(fieldName);
                        // 如果当前extFields拿不到这个值，判断这个值是否可以为空
                        if(value == null){
                            if(!isFieldNullable(field)){
                                throw new RemotingCommandException("the custom field " + fieldName + " is null");
                            }
                            continue;
                        }

                        field.setAccessible(true);
                        String type = getCanonicalName(field.getType());
                        Object valueParsed;

                        // 根据类型，将值填入field中
                        if(type.equals(STRING_CANONICAL_NAME)){
                            valueParsed = value;
                        }else if(type.equals(INTEGER_CANONICAL_NAME_1) ||
                                type.equals(INTEGER_CANONICAL_NAME_2)){
                            valueParsed = Integer.parseInt(value);
                        }else if(type.equals(LONG_CANONICAL_NAME_1) ||
                                 type.equals(LONG_CANONICAL_NAME_2)){
                            valueParsed = Long.parseLong(value);
                        }else if(type.equals(BOOLEAN_CANONICAL_NAME_1) ||
                                type.equals(BOOLEAN_CANONICAL_NAME_2)){
                            valueParsed = Boolean.parseBoolean(value);
                        }else if(type.equals(DOUBLE_CANONICAL_NAME_1) ||
                                 type.equals(DOUBLE_CANONICAL_NAME_2)){
                            valueParsed = Double.parseDouble(value);
                        }else{
                            throw new RemotingCommandException("the custom field type " + type +" is not supported");
                        }

                        field.set(objectHeader,valueParsed);
                    } catch(Throwable ex){
                        ex.printStackTrace();
                    }
                }
            }
            // 这个checkFields()需要传入的类型自己实现
            objectHeader.checkFields();
        }

        return objectHeader;
    }


    public Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader){
        Field[] fields = CLASS_HASH_MAP.get(classHeader);

        if(fields == null){
            fields = classHeader.getDeclaredFields();
            synchronized (CLASS_HASH_MAP){
                CLASS_HASH_MAP.put(classHeader,fields);
            }
        }
        return fields;
    }

    // 利用注解判断该field是否可以为空
    private boolean isFieldNullable(Field field){
        if(!NULLABLE_FIELD_CACHE.containsKey(field)){
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE){
                // @CFNotNull用来表示不能为空的属性
                NULLABLE_FIELD_CACHE.put(field,annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }


    private String getCanonicalName(Class clazz){
        String name = CANONICAL_NAME_CACHE.get(clazz);
        if(name != null){
            return name;
        }
        name = clazz.getCanonicalName();
        synchronized (CANONICAL_NAME_CACHE){
            CANONICAL_NAME_CACHE.put(clazz,name);
        }
        return name;
    }

    // 将RPC类型和header数据长度存在同一个byte[4]数组中，因为header长度为4位
    // (估计是有空位，就将RPC类型也存了进来。)
    public static byte[] markProtocolType(int source,SerializeType type){
        byte[] result = new byte[4];

        result[0] = type.getCode();
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }

    public RemotingCommandType getType(){
        if(this.isResponseType()){
            return RemotingCommandType.RESPONSE_COMMAND;
        }
        return RemotingCommandType.REQUEST_COMMAND;
    }

    // 位运算的优点
    // 利用flag 的最低位，判断是response还是request
    public boolean isResponseType(){
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    // 利用flag的第二位，判断是否为oneway(不需要返回结果)
    public boolean isOnewayRpc(){
        int bits = 1 << RPC_ONEWAY;
        return (flag & bits) == bits;
    }


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public CommandCustomHeader getCustomHeader() {
        return customHeader;
    }

    public void setCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }

    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }


}
