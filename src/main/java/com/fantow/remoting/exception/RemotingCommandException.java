package com.fantow.remoting.exception;


public class RemotingCommandException extends RemotingException {

    public RemotingCommandException(String message){
        super(message);
    }
    public RemotingCommandException(String message,Throwable cause){
        super(message,cause);
    }

}
