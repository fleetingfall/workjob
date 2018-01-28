package kafka.log4j;


import org.apache.log4j.Logger;

/**
 * 发现有个问题，当topic不存在的时候可以不用手动创建，但是第一次运行会因为超时而发生错误，所以建议手动创建，或者再次运行
 */
public class LogProducer extends Thread{
    private static final Logger LOG=Logger.getLogger(LogProducer.class);
    @Override
    public void run() {
        while (true){
            LOG.info(Thread.currentThread().getName()+"：这是一条info级别的日志");
            LOG.error(Thread.currentThread().getName()+"：这是一条Error级别的日志");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws InterruptedException {
        Thread t1=new Thread(new LogProducer(),"线程一");
        Thread t2=new Thread(new LogProducer(),"线程二");
        t1.start();
        t2.start();
    }
}
