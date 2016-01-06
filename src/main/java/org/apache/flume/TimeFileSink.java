package org.apache.flume;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangzhikuan on 16/1/5.
 */
public class TimeFileSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(TimeFileSink.class);

    private static SimpleDateFormat df = new SimpleDateFormat("yyyMMddHH");

    private static final int defaultBatchSize = 100;
    private int batchSize = defaultBatchSize;

    //日志存放路径
    private String directory;
    //输出流
    private OutputStream outputStream;

    //序列化
    private String serializerType;
    private Context serializerContext;
    private EventSerializer serializer;

    //日志计数器
    private SinkCounter sinkCounter;

    //当前正在写的文件
    private File currentFile = null;


    //定时判断是否需要进行切分文件
    private ScheduledExecutorService rollService;
    private Boolean shouldRotate = false;

    public void configure(Context context) {

        //序列化
        serializerType = context.getString("sink.serializer", "TEXT");
        Preconditions.checkNotNull(serializerType, "Serializer type is undefined");
        serializerContext = new Context(context.getSubProperties("sink." + EventSerializer.CTX_PREFIX));

        //日志batch大小
        batchSize = context.getInteger("sink.batchSize", defaultBatchSize);

        //日志路径
        this.directory = context.getString("sink.directory");
        Preconditions.checkArgument(directory != null, "Directory may not be null");

        //统计信息
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }


    }

    @Override
    public void start() {
        logger.info("Starting {}...", this);
        sinkCounter.start();
        super.start();


        //定时检查是否达到了切分的标准,每秒检查一次
        this.rollService = Executors.newScheduledThreadPool(1, (new ThreadFactoryBuilder()).setNameFormat("TimeFileSink-" + Thread.currentThread().getId() + "-%d").build());
        this.rollService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                //当前小时
                String currentHour = df.format(new Date());
                //是否和文件名一直
                if (!getCurrentFile().getName().equals(currentHour)) {
                    logger.debug("Marking time to rotate file {}", getCurrentFile());
                    shouldRotate = true;
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        logger.info("TimeFileSink {} started.", getName());
    }

    public Status process() throws EventDeliveryException {

        if (this.shouldRotate) {
            //判断当前文件是否为空,如果为空,则直接跳过,创建新的文件流,老的文件流关闭
            if (outputStream != null) {
                logger.debug("Closing file {}", this.getCurrentFile());
                try {
                    serializer.flush();
                    serializer.beforeClose();
                    outputStream.close();
                    sinkCounter.incrementConnectionClosedCount();
                } catch (IOException e) {
                    sinkCounter.incrementConnectionFailedCount();
                    throw new EventDeliveryException("Unable to rotate file " + this.getCurrentFile() + " while delivering event", e);
                } finally {
                    //重新开始
                    serializer = null;
                    outputStream = null;
                    currentFile = null;
                }
            }
        }


        //如果文件流为空,则重新打开新的文件
        if (outputStream == null) {
            //打开文件
            logger.debug("Opening output stream for file {}", this.getCurrentFile());
            try {
                //打开文件流
                outputStream = new BufferedOutputStream(new FileOutputStream(this.getCurrentFile(), true));
                //序列化文件吸入
                serializer = EventSerializerFactory.getInstance(serializerType, serializerContext, outputStream);
                serializer.afterCreate();
                //增加统计信息
                sinkCounter.incrementConnectionCreatedCount();
            } catch (IOException e) {
                sinkCounter.incrementConnectionFailedCount();
                throw new EventDeliveryException("Failed to open file [" + this.getCurrentFile() + "] while delivering event", e);
            }
        }

        Channel channel = getChannel();
        //创建事务
        Transaction transaction = channel.getTransaction();

        //初始化返回值
        Status result = Status.READY;

        try {
            Event event = null;
            //开始事务
            transaction.begin();
            int eventAttemptCounter = 0;
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    //增加统计信息
                    sinkCounter.incrementEventDrainAttemptCount();
                    eventAttemptCounter++;

                    //开始写日志到缓存
                    serializer.write(event);
                } else {
                    //如果队列为空,则退出循环,返回BACKOFF状态
                    result = Status.BACKOFF;
                    break;
                }
            }
            //将缓存刷新
            serializer.flush();
            //写入到文件中
            outputStream.flush();
            //提交事务
            transaction.commit();

            //统计数量
            sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
        } catch (Exception ex) {
            //事务回滚
            transaction.rollback();
            //抛出异常
            throw new EventDeliveryException("Failed to process transaction", ex);
        } finally {
            //关闭事务
            transaction.close();
        }

        return result;
    }

    @Override
    public void stop() {
        logger.info("RollingFile sink {} stopping...", getName());
        sinkCounter.stop();
        super.stop();

        if (outputStream != null) {
            logger.debug("Closing file {}", this.getCurrentFile());

            try {
                serializer.flush();
                serializer.beforeClose();
                outputStream.close();
                sinkCounter.incrementConnectionClosedCount();
            } catch (IOException e) {
                sinkCounter.incrementConnectionFailedCount();
                logger.error("Unable to close output stream. Exception follows.", e);
            } finally {
                outputStream = null;
                serializer = null;
                currentFile = null;
            }
        }

        //关闭定时任务
        while (!rollService.isTerminated()) {
            try {
                rollService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for roll service to stop. " + "Please report this.", e);
            }
        }

        logger.info("RollingFile sink {} stopped. Event metrics: {}", getName(), sinkCounter);
    }


    private File getCurrentFile() {
        if (currentFile == null) {
            currentFile = new File(this.directory + "/" + df.format(new Date()));
        }
        return currentFile;
    }

}
