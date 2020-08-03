package com.fantow.remoting.netty;

public class NettySystemConfig {

    private static final String MQ_SENDBUFFER_SIZE = "com.fantow.remoting.socket.sndbuf.size";

    private static final String MQ_RECVBUFFER_SIZE = "com.fantow.remoting.socket.recvbuf.size";

    public static final String COM_FANTOWMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE =
            "com.fantowmq.remoting.clientAsyncSemaphoreValue";

    public static final String COM_FANTOWMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE =
            "com.fantowmq.remoting.clientOnewaySemaphoreValue";


    public static final int socketSendBufSize =
            Integer.parseInt(System.getProperty(MQ_SENDBUFFER_SIZE,"65535"));

    public static final int socketRecvBufSize =
            Integer.parseInt(System.getProperty(MQ_RECVBUFFER_SIZE,"65535"));

    public static final int CLIENT_ASYNC_SEMAPHORE_VALUE =
            Integer.parseInt(System.getProperty(COM_FANTOWMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE,"65535"));

    public static final int CLIENT_ONEWAY_SEMAPHORE_VALUE =
            Integer.parseInt(System.getProperty(COM_FANTOWMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE,"65535"));

}
