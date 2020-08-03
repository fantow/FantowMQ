package com.fantow.remoting.common;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

// semaphore是线程安全的，这个类只是为了保证一次只释放一个信号量
public class SemaphoreReleaseOnce {

    private final AtomicBoolean released = new AtomicBoolean(false);
    private final Semaphore semaphore;

    public SemaphoreReleaseOnce(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public void release(){
        if(this.semaphore != null){
            if(this.released.compareAndSet(false,true)){
                semaphore.release();
            }
        }
    }

    public Semaphore getSemaphore(){
        return this.semaphore;
    }

}
