package com.fantow.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

// 对于文件的传输，会使用Netty中自带的FileRegion，这样可以通过零拷贝传输
public class FileRegionEncoder extends MessageToByteEncoder<FileRegion> {
    @Override
    protected void encode(ChannelHandlerContext ctx, FileRegion msg, ByteBuf out) throws Exception {

        WritableByteChannel writableByteChannel = new WritableByteChannel(){

            @Override
            public int write(ByteBuffer src) throws IOException {
                out.writeBytes(src);
                return out.capacity();
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() throws IOException {

            }
        };

        long toTransfer = msg.count();

        while(true){
            long transferred = msg.transferred();
            if(toTransfer - transferred <= 0){
                break;
            }
            msg.transferTo(writableByteChannel,transferred);
        }
    }
}
