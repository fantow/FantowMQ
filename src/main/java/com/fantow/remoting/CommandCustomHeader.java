package com.fantow.remoting;

import com.fantow.remoting.exception.RemotingCommandException;

public interface CommandCustomHeader {
    void checkFields() throws RemotingCommandException;
}
