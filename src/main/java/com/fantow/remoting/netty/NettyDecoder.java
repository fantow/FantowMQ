package com.fantow.remoting.netty;

import com.fantow.remoting.common.RemotingUtil;
import com.fantow.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {

    private static final Logger logger = LoggerFactory.getLogger(NettyDecoder.class);

    // Frame的最大长度，超过会报错
    // 默认为16Mb
    private static final int FRAME_MAX_LENGTH =
            Integer.parseInt(System.getProperty("com.fantow.remoting.frameMaxLength","16777216"));

    public NettyDecoder(){
        super(FRAME_MAX_LENGTH,0,4,0,4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try{
            frame = (ByteBuf) super.decode(ctx, in);
            if(frame == null){
                return null;
            }

            ByteBuffer byteBuffer = frame.nioBuffer();

            return RemotingCommand.decode(byteBuffer);
        }catch (Exception ex){
            ex.printStackTrace();
            RemotingUtil.closeChannel(ctx.channel());
        }finally {
            if(frame != null){
                frame.release();
            }
        }
        return null;
    }
}
