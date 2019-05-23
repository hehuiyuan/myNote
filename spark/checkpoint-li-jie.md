# checkpoint理解

## checkpoint目录结构图

checkpoint路径：hdfs://myhdfs/user/spark/checkpoint/jrcOneMinRegBak190522001

![](../.gitbook/assets/image%20%285%29.png)

/user/spark/checkpoint/jrcOneMinRegBak190522001/receivedData    目录存放的是receiver接收的数据

![](../.gitbook/assets/image%20%283%29.png)

目录存放的是rdd  
/user/spark/checkpoint/jrcOneMinRegBak190522001/8293fb52-d0de-4ba8-b6f1-8ed4a7771e1c  
  
\_partitioner中存放的是使用的分区类：如 org.apache.spark.HashPartitioner  
  
part-xxxx里面涉及到了org.apache.spark.streaming.rdd.MapWithStateRDDRecored  
org.apache.spark.streaming.util.OpenHashMapBasedStateMap

![](../.gitbook/assets/image%20%286%29.png)

![](../.gitbook/assets/image%20%288%29.png)

## checkpoint-xxxx文件的创建

```scala
 private def processEvent(event: JobGeneratorEvent) {
    logDebug("Got event " + event)
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
  }
```

当触发DoCheckpoint事件时候，会进行相应的checkpoint操作，那么什么时候触发的这个事件呢？在generateJobs方法中最后一行代码会触发DoCheckpoint事件，那么什么时候触发的generateJobs呢？上面的代码我们可以GenerateJobs\(time\)事件时候会触犯下面的方法。那么什么时候触发的GenerateJobs事件呢？代码中已经注释。其实这是spark streaming级别的一个job，根据用户设置的bathcInterval 定期执行去生成job，这里的job区别于spark core中的job。

```scala

 private def generateJobs(time: Time) {
    // Checkpoint all RDDs marked for checkpointing to ensure their lineages are
    // truncated periodically. Otherwise, we may run into stack overflows (SPARK-6847).
    ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
    Try {
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
      graph.generateJobs(time) // generate jobs using allocated block
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
        PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
    }
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }
  
  
  
  -------触发GeneratorJobs事件，定时任务
    private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")

```

```scala

def start(): Unit = synchronized {
    if (eventLoop != null) return // generator has already been started

    // Call checkpointWriter here to initialize it before eventLoop uses it to avoid a deadlock.
    // See SPARK-10125
    checkpointWriter

    eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = {
        jobScheduler.reportError("Error in job generator", e)
      }
    }
    eventLoop.start()

    if (ssc.isCheckpointPresent) {
    //checkpoint不为null的话那么从checkpoint恢复
      restart()
    } else {
      startFirstTime()
    }
  }
  
  
private def startFirstTime() {
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    //开始定时任务
    timer.start(startTime.milliseconds)
    logInfo("Started JobGenerator at " + startTime)
  }
  
  
```

上面描述了JobGenerator.start方法到docheckpoint的一个过程，那么继续回来看我们的doCheckpoint

```scala
  private def doCheckpoint(time: Time, clearCheckpointDataLater: Boolean) {
    if (shouldCheckpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
      logInfo("Checkpointing graph for time " + time)
      ssc.graph.updateCheckpointData(time)
      checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
    } else if (clearCheckpointDataLater) {
      markBatchFullyProcessed(time)
    }
  }
  
  //checkpointwriter对象
   private lazy val checkpointWriter = if (shouldCheckpoint) {
    new CheckpointWriter(this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }
  
  //checkpointwriter的writer方法，将checkpoint对象序列化然后调用一个线程，将其写到hdfs相应的文件
  //CheckpointWriteHandler是一个runnable,其run方法中会把checkpoint对象写到temp文件中，
  //然后对其重命名temp->checkpointFile
   def write(checkpoint: Checkpoint, clearCheckpointDataLater: Boolean) {
    try {
      val bytes = Checkpoint.serialize(checkpoint, conf)
      executor.execute(new CheckpointWriteHandler(
        checkpoint.checkpointTime, bytes, clearCheckpointDataLater))
      logInfo(s"Submitted checkpoint of time ${checkpoint.checkpointTime} to writer queue")
    } catch {
      case rej: RejectedExecutionException =>
        logError("Could not submit checkpoint task to the thread pool executor", rej)
    }
  }
  
  
  //checkpoint类中有两个涉及的路径，其实就是checkpoint目录下的这两个文件
  // /user/spark/checkpoint/jrcOneMinRegBak190522001/checkpoint-1558530270000.bk
  // /user/spark/checkpoint/jrcOneMinRegBak190522001/checkpoint-1558530270000
  val checkpointFile = Checkpoint.checkpointFile(checkpointDir, latestCheckpointTime)
  val backupFile = Checkpoint.checkpointBackupFile(checkpointDir, latestCheckpointTime)
  /** Get the checkpoint file for the given checkpoint time */
  def checkpointFile(checkpointDir: String, checkpointTime: Time): Path = {
    new Path(checkpointDir, PREFIX + checkpointTime.milliseconds)
  }
  /** Get the checkpoint backup file for the given checkpoint time */
  def checkpointBackupFile(checkpointDir: String, checkpointTime: Time): Path = {
    new Path(checkpointDir, PREFIX + checkpointTime.milliseconds + ".bk")
  }

  
  
```

## receiverData目录的创建这个主要是使用receiver时候

```scala
private[streaming] object WriteAheadLogBasedBlockHandler {
  def checkpointDirToLogDir(checkpointDir: String, streamId: Int): String = {
    new Path(checkpointDir, new Path("receivedData", streamId.toString)).toString
  }
}


private val writeAheadLog = WriteAheadLogUtils.createLogForReceiver(
    conf, checkpointDirToLogDir(checkpointDir, streamId), hadoopConf)


def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {    
  // Store the block in write ahead log
   val storeInWriteAheadLogFuture = Future {
      writeAheadLog.write(serializedBlock.toByteBuffer, clock.getTimeMillis())
   }
}


 //ReceiverSupervisorImpl类
 /** Store block and report it to driver */
  def pushAndReportBlock(
      receivedBlock: ReceivedBlock,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    val blockId = blockIdOption.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    //调用storeBlock这个方法
    val blockStoreResult = receivedBlockHandler.storeBlock(blockId, receivedBlock)
    logDebug(s"Pushed block $blockId in ${(System.currentTimeMillis - time)} ms")
    val numRecords = blockStoreResult.numRecords
    val blockInfo = ReceivedBlockInfo(streamId, numRecords, metadataOption, blockStoreResult)
    trackerEndpoint.askSync[Boolean](AddBlock(blockInfo))
    logDebug(s"Reported block $blockId")
  }


/** Store an iterator of received data as a data block into Spark's memory. */
  def pushIterator(
      iterator: Iterator[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(IteratorBlock(iterator), metadataOption, blockIdOption)
  }
  
  
  //recevier类调用store时候
  /** Store an iterator of received data as a data block into Spark's memory. */
  def store(dataIterator: Iterator[T]) {
    supervisor.pushIterator(dataIterator, None, None)
  }


```

## receivedBlockMetadata也是receiver时候使用

![](../.gitbook/assets/image%20%281%29.png)

这里面写入的是内容是 BlockAdditionEvent\(receivedBlockInfo\)  序列化后的内容，ReceiverTrackerEndpoint接收到AddBlock事件时候触发

```text
//ReceiverTrackerEndpoint中，ReceiverTrackerEndpoint是ReceiverTracker中定义的类
case AddBlock(receivedBlockInfo) =>
  if (WriteAheadLogUtils.isBatchingEnabled(ssc.conf, isDriver = true)) {
    walBatchingThreadPool.execute(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        if (active) {
          context.reply(addBlock(receivedBlockInfo))
        } else {
          throw new IllegalStateException("ReceiverTracker RpcEndpoint shut down.")
        }
      }
    })
  } else {
    context.reply(addBlock(receivedBlockInfo))
  }
  
  
  //ReceiverTracker中的方法
  private def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    receivedBlockTracker.addBlock(receivedBlockInfo)
  }
  
  
  
  
   //ReceivedBlockTracker中方法
   /** Add received block. This event will get written to the write ahead log (if enabled). */
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    try {
      //要往receivermetadata目录下写的内容
      val writeResult = writeToLog(BlockAdditionEvent(receivedBlockInfo))
      if (writeResult) {
        synchronized {
          getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
        }
        logDebug(s"Stream ${receivedBlockInfo.streamId} received " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId}")
      } else {
        logDebug(s"Failed to acknowledge stream ${receivedBlockInfo.streamId} receiving " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId} in the Write Ahead Log.")
      }
      writeResult
    } catch {
      case NonFatal(e) =>
        logError(s"Error adding block $receivedBlockInfo", e)
        false
    }
  }
  
  
  
  /** Write an update to the tracker to the write ahead log */
  private[streaming] def writeToLog(record: ReceivedBlockTrackerLogEvent): Boolean = {
    //wal可用时候
    if (isWriteAheadLogEnabled) {
      logTrace(s"Writing record: $record")
      try {
      //使用创建好的wal实现类去写，isWriteAheadLogEnabled使用时候初始化writeAheadLogOption
        writeAheadLogOption.get.write(ByteBuffer.wrap(Utils.serialize(record)),
          clock.getTimeMillis())
        true
      } catch {
        case NonFatal(e) =>
          logWarning(s"Exception thrown while writing record: $record to the WriteAheadLog.", e)
          false
      }
    } else {
      true
    }
  }
  
  //如何判断能否执行wal操作呢？
  private[streaming] def isWriteAheadLogEnabled: Boolean = writeAheadLogOption.nonEmpty
  private val writeAheadLogOption = createWriteAheadLog()
  private def createWriteAheadLog(): Option[WriteAheadLog] = {
    checkpointDirOption.map { checkpointDir =>
      val logDir = ReceivedBlockTracker.checkpointDirToLogDir(checkpointDirOption.get)
      //默认创建的WriteAheadLog实现类是FileBasedWriteAheadLog
      WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
    }
  }
  
  //FileBasedWriteAheadLog就会在该目录下创建相应文件例如在receiverBlockMetedata下创建如下格式文件：
  //rollingIntervalSecs wal日志文件分割间隔默认60s
  currentLogWriterStartTime = currentTime
  currentLogWriterStopTime = currentTime + (rollingIntervalSecs * 1000)
  val newLogPath = new Path(logDirectory,
        timeToLogFile(currentLogWriterStartTime, currentLogWriterStopTime))
        
  def timeToLogFile(startTime: Long, stopTime: Long): String = {
    s"log-$startTime-$stopTime"
  }

  
```

