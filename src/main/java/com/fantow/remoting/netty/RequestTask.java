package com.fantow.remoting.netty;

import com.fantow.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

public class RequestTask implements Runnable{

    private Runnable runnable;
    private long createTimestamp = System.currentTimeMillis();
    private Channel channel;
    private RemotingCommand request;

    public RequestTask(Runnable runnable, Channel channel, RemotingCommand request) {
        this.runnable = runnable;
        this.channel = channel;
        this.request = request;
    }

    public long getCreateTimestamp(){
        return createTimestamp;
    }

    @Override
    public void run() {
        this.runnable.run();
    }

    public void returnResponse(int code,String remark){
        RemotingCommand response = RemotingCommand.createResponseCommand(code,remark);
        response.setOpaque(request.getOpaque());
        this.channel.writeAndFlush(response);
    }

}
