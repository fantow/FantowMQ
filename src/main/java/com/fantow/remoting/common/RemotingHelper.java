package com.fantow.remoting.common;


import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @Description:
 * @Author chenyang270
 * @CreateDate
 * @Version: 1.0
 */
public class RemotingHelper {

    private static final String FANTOWMQ_REMOTING = "FantowMQRemoting";
    private static final String DEFAULT_CHARSET = "UTF-8";

    // 字符串转SocketAddress
    public static SocketAddress string2SocketAddress(String addr){
        int index = addr.lastIndexOf(":");
        String host = addr.substring(0,index);
        String port = addr.substring(index + 1);
        return new InetSocketAddress(host,Integer.parseInt(port));
    }

    public static String parseChannelRemoteAddr(Channel channel){
        if(channel == null){
            return "";
        }
        SocketAddress address = channel.remoteAddress();
        String addr = address != null ? address.toString() : "";

        if(addr.length() > 0){
            int index = addr.lastIndexOf("/");
            if(index >= 0){
                return addr.substring(index + 1);
            }
            return addr;
        }
        return "";
    }

    public static String parseSocketAddressAddr(SocketAddress socketAddress){
        if (socketAddress != null) {
            String addr = socketAddress.toString();
            if(addr.length() > 0){
                return addr.substring(1);
            }
        }
        return "";
    }


}
