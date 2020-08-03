package com.fantow.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;

public class RemotingUtil {

    private static final Logger logger = LoggerFactory.getLogger(RemotingUtil.class);

    // 操作系统
    public static final String OS_NAME = System.getProperty("os.name");

    private static boolean isLinuxPlatform = true;

    static{
        if(OS_NAME != null && OS_NAME.toLowerCase().contains("linux")){
            isLinuxPlatform = true;
        }
        if(OS_NAME != null && OS_NAME.toLowerCase().contains("windows")){
            isLinuxPlatform = false;
        }
    }

    public static boolean isLinuxPlatform(){
        return isLinuxPlatform;
    }

    public static boolean isWindowsPlatform(){
        return !isLinuxPlatform;
    }

    // 与Linux原生Epoll相关
    public static Selector openSelector() throws IOException {
        Selector result = null;

        if(isLinuxPlatform()){
            try {
                Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                if(providerClazz != null){
                    try {
                        Method method = providerClazz.getMethod("provider");
                        if(method != null){
                            SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
                            if(selectorProvider != null){
                                result = selectorProvider.openSelector();
                            }
                        }
                    } catch (Exception e) {
                        logger.info("Open epoll selector in linux Platform exception: ",e);
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        if(result == null){
            result = Selector.open();
        }

        return result;
    }




    public static void closeChannel(Channel channel){
        String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("closeChannel: close the connection to remote address[{}] result:{}",addrRemote);
            }
        });
    }

}
