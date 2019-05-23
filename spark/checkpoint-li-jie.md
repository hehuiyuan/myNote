# checkpoint理解

## checkpoint目录结构图

checkpoint路径：hdfs://myhdfs/user/spark/checkpoint/jrcOneMinRegBak190522001

![](../.gitbook/assets/image%20%285%29.png)

上面显示的是我们指定的checkpoint目录下的目录结构图，  
其中checkpoint-xxx存放的是metadata数据，主要针对的是Checkpoint序列化后的内容；  
receivedData /receivedBlockMetadata是receiver时候使用到的；  
8293fb52-d0de-4ba8-b6f1-8ed4a7771e1c里面存放的是rdd数据。

/user/spark/checkpoint/jrcOneMinRegBak190522001/receivedData    目录存放的是receiver接收的数据

![](../.gitbook/assets/image%20%283%29.png)

目录存放的是rdd数据  
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
      //对rdd做checkpioint，文件在checkpoin目录下一个字符串构成的目录下存放，格式是rdd-xxx
      ssc.graph.updateCheckpointData(time)
      //对metadata做checkpoint,文件是checkpoin目录下chekpoint-时间戳的文件
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

## 8293fb52-d0de-4ba8-b6f1-8ed4a7771e1c存放rdd的目录

![](../.gitbook/assets/image%20%288%29.png)

继续回到doCheckpoint方法，在写元数据之前有一个更新数据的操作

```scala
ssc.graph.updateCheckpointData(time)

def updateCheckpointData(time: Time) {
    logInfo("Updating checkpoint data for time " + time)
    this.synchronized {
      //dstreamgraph中记录了所有的dstream
      outputStreams.foreach(_.updateCheckpointData(time))
    }
    logInfo("Updated checkpoint data for time " + time)
}

  private[streaming] def updateCheckpointData(currentTime: Time) {
    logDebug(s"Updating checkpoint data for time $currentTime")
    //对rdd数据做checkpoint
    checkpointData.update(currentTime)
    //更新依赖的dstream中的rdd数据到checkpoint
    dependencies.foreach(_.upda
    teCheckpointData(currentTime))
    logDebug(s"Updated checkpoint data for time $currentTime: $checkpointData")
  }
  
  
   //DStreamCheckpointData
   def update(time: Time) {
    // Get the checkpointed RDDs from the generated RDDs
    val checkpointFiles = dstream.generatedRDDs.filter(_._2.getCheckpointFile.isDefined)
                                       .map(x => (x._1, x._2.getCheckpointFile.get))
    logDebug("Current checkpoint files:\n" + checkpointFiles.toSeq.mkString("\n"))

    // Add the checkpoint files to the data to be serialized
    if (!checkpointFiles.isEmpty) {
      currentCheckpointFiles.clear()
      currentCheckpointFiles ++= checkpointFiles
      // Add the current checkpoint files to the map of all checkpoint files
      // This will be used to delete old checkpoint files
      timeToCheckpointFile ++= currentCheckpointFiles
      // Remember the time of the oldest checkpoint RDD in current state
      timeToOldestCheckpointFileTime(time) = currentCheckpointFiles.keys.min(Time.ordering)
    }
  }
  
  
    //使用ReliableRDDCheckpointData得到这个rdd要关联的checkpoint文件
    def getCheckpointFile: Option[String] = {
    checkpointData match {
      case Some(reliable: ReliableRDDCheckpointData[T]) => reliable.getCheckpointDir
      case _ => None
    }
  }  
  
  //根据这个rdd是的状态是否是checkpointed决定返回路径的值
  def getCheckpointDir: Option[String] = RDDCheckpointData.synchronized {
    if (isCheckpointed) {
      Some(cpDir.toString)
    } else {
      None
    }
  }
  
   //这个rdd的checkpoint状态值
  private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, CheckpointingInProgress, Checkpointed = Value
}
   protected var cpState = Initialized
   def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
    //Checkpointed时候才返回相应的rdd关联路径
    cpState == Checkpointed
  }
  
  //获取这个rdd对应的保存路径
  private val cpDir: String =
    //根rdd的id获取当作这个rdd存储关联目录
    ReliableRDDCheckpointData.checkpointPath(rdd.context, rdd.id)
      .map(_.toString)
      .getOrElse { throw new SparkException("Checkpoint dir must be specified.") }
      
  //获得 rdd-rddid 构成这个rdd关联的存储目录组成的字符串
  def checkpointPath(sc: SparkContext, rddId: Int): Option[Path] = {
    sc.checkpointDir.map { dir => new Path(dir, s"rdd-$rddId") }
  }
  
  //rdd关联的checkpoint状态改变是rdd调用doCheckpoint方法时候
  private[spark] def doCheckpoint(): Unit = {
    RDDOperationScope.withScope(sc, "checkpoint", allowNesting = false, ignoreParent = true) {
      if (!doCheckpointCalled) {
        doCheckpointCalled = true
        if (checkpointData.isDefined) {
          if (checkpointAllMarkedAncestors) {
            // TODO We can collect all the RDDs that needs to be checkpointed, and then checkpoint
            // them in parallel.
            // Checkpoint parents first because our lineage will be truncated after we
            // checkpoint ourselves
            dependencies.foreach(_.rdd.doCheckpoint())
          }
          //每个rdd都有一个RDDCheckpointData对象
          checkpointData.get.checkpoint()
        } else {
          dependencies.foreach(_.rdd.doCheckpoint())
        }
      }
    }
  }
  
  //RDDCheckpointData中
  //checkpoint状态的改变是在RDDCheckpointData.checkpoint时候改变的
  //LocalRDDCheckpointData存储数据到每个executor缓存层或者本地磁盘
  //ReliableRDDCheckpointData存储数据到可靠的容错的存储系统中，比如hdfs
   final def checkpoint(): Unit = {
    // Guard against multiple threads checkpointing the same RDD by
    // atomically flipping the state of this RDDCheckpointData
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }
    //对rdd做checkpoint
    val newRDD = doCheckpoint()
    // Update our state and truncate the RDD lineage
    RDDCheckpointData.synchronized {
      cpRDD = Some(newRDD)
      cpState = Checkpointed
      rdd.markCheckpointed()
    }
  }
  
  
  protected override def doCheckpoint(): CheckpointRDD[T] = {
   //往rdd-rddid构成的目录下保存rdd
    val newRDD = ReliableCheckpointRDD.writeRDDToCheckpointDirectory(rdd, cpDir)

    // Optionally clean our checkpoint files if the reference is out of scope
    if (rdd.conf.getBoolean("spark.cleaner.referenceTracking.cleanCheckpoints", false)) {
      rdd.context.cleaner.foreach { cleaner =>
        cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)
      }
    }

    logInfo(s"Done checkpointing RDD ${rdd.id} to $cpDir, new parent is RDD ${newRDD.id}")
    newRDD
  }
  
  
 //写rdd到 rdd-rddid构成的目录下 对应的part-xxxxx文件中
 def writeRDDToCheckpointDirectory[T: ClassTag](
      originalRDD: RDD[T],
      checkpointDir: String,
      blockSize: Int = -1): ReliableCheckpointRDD[T] = {
    val checkpointStartTimeNs = System.nanoTime()

    val sc = originalRDD.sparkContext

    // Create the output path for the checkpoint
    val checkpointDirPath = new Path(checkpointDir)
    val fs = checkpointDirPath.getFileSystem(sc.hadoopConfiguration)
    if (!fs.mkdirs(checkpointDirPath)) {
      throw new SparkException(s"Failed to create checkpoint path $checkpointDirPath")
    }

    // Save to file, and reload it as an RDD
    val broadcastedConf = sc.broadcast(
      new SerializableConfiguration(sc.hadoopConfiguration))
    // TODO: This is expensive because it computes the RDD again unnecessarily (SPARK-8582)
    //写rdd到相应的checkpoint中，后面进一步详细分析
    sc.runJob(originalRDD,
      writePartitionToCheckpointFile[T](checkpointDirPath.toString, broadcastedConf) _)

    //将使用的分区类写到rdd-rddid构成的目录下的_partitioner文件中
    if (originalRDD.partitioner.nonEmpty) {
      writePartitionerToCheckpointDir(sc, originalRDD.partitioner.get, checkpointDirPath)
    }

    val checkpointDurationMs =
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - checkpointStartTimeNs)
    logInfo(s"Checkpointing took $checkpointDurationMs ms.")

    val newRDD = new ReliableCheckpointRDD[T](
      sc, checkpointDirPath.toString, originalRDD.partitioner)
    if (newRDD.partitions.length != originalRDD.partitions.length) {
      throw new SparkException(
        "Checkpoint RDD has a different number of partitions from original RDD. Original " +
          s"RDD [ID: ${originalRDD.id}, num of partitions: ${originalRDD.partitions.length}]; " +
          s"Checkpoint RDD [ID: ${newRDD.id}, num of partitions: " +
          s"${newRDD.partitions.length}].")
    }
    newRDD
  }
  
  
  //将使用的分区器写到_partitoner文件中
   private def writePartitionerToCheckpointDir(
    sc: SparkContext, partitioner: Partitioner, checkpointDirPath: Path): Unit = {
    try {
      //checkpointPartitionerFileName = "_partitioner"
      val partitionerFilePath = new Path(checkpointDirPath, checkpointPartitionerFileName)
      val bufferSize = sc.conf.getInt("spark.buffer.size", 65536)
      val fs = partitionerFilePath.getFileSystem(sc.hadoopConfiguration)
      val fileOutputStream = fs.create(partitionerFilePath, false, bufferSize)
      val serializer = SparkEnv.get.serializer.newInstance()
      val serializeStream = serializer.serializeStream(fileOutputStream)
      Utils.tryWithSafeFinally {
        //将使用的partitioner序列化后写到文件"_partitioner"中保存
        serializeStream.writeObject(partitioner)
      } {
        serializeStream.close()
      }
      logDebug(s"Written partitioner to $partitionerFilePath")
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error writing partitioner $partitioner to $checkpointDirPath")
    }
  }
  
  //进一步分析将rdd写到checkpoint中，writeRDDToCheckpointDirectory方法中关键代码如下
  sc.runJob(originalRDD,
      writePartitionToCheckpointFile[T](checkpointDirPath.toString, broadcastedConf) _)
  
  //上面的代码就是写rdd分区数据到checkpoint file中的调用地方
  /**
   * Write an RDD partition's data to a checkpoint file.
   */
  def writePartitionToCheckpointFile[T: ClassTag](
      path: String,
      broadcastedConf: Broadcast[SerializableConfiguration],
      blockSize: Int = -1)(ctx: TaskContext, iterator: Iterator[T]) {
    val env = SparkEnv.get
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(broadcastedConf.value.value)
    //finalOutputName就是part-xxxxx 分区id, 五位数字构成
    val finalOutputName = ReliableCheckpointRDD.checkpointFileName(ctx.partitionId())
    val finalOutputPath = new Path(outputDir, finalOutputName)
    //先写到临时文件，最后重命名
    val tempOutputPath =
      new Path(outputDir, s".$finalOutputName-attempt-${ctx.attemptNumber()}")

    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)

    val fileOutputStream = if (blockSize < 0) {
      val fileStream = fs.create(tempOutputPath, false, bufferSize)
      if (env.conf.get(CHECKPOINT_COMPRESS)) {
        CompressionCodec.createCodec(env.conf).compressedOutputStream(fileStream)
      } else {
        fileStream
      }
    } else {
      // This is mainly for testing purpose
      fs.create(tempOutputPath, false, bufferSize,
        fs.getDefaultReplication(fs.getWorkingDirectory), blockSize)
    }
    val serializer = env.serializer.newInstance()
    val serializeStream = serializer.serializeStream(fileOutputStream)
    Utils.tryWithSafeFinally {
      serializeStream.writeAll(iterator)
    } {
      serializeStream.close()
    }
   //重命名文件part-xxxxx-attempt-attemptNumber --->part-xxxxx
    if (!fs.rename(tempOutputPath, finalOutputPath)) {
      if (!fs.exists(finalOutputPath)) {
        logInfo(s"Deleting tempOutputPath $tempOutputPath")
        fs.delete(tempOutputPath, false)
        throw new IOException("Checkpoint failed: failed to save output of task: " +
          s"${ctx.attemptNumber()} and final output path does not exist: $finalOutputPath")
      } else {
      //重命名失败因为该文件已经存在，那么删除临时文件
        // Some other copy of this task must've finished before us and renamed it
        logInfo(s"Final output path $finalOutputPath already exists; not overwriting it")
        if (!fs.delete(tempOutputPath, false)) {
          logWarning(s"Error deleting ${tempOutputPath}")
        }
      }
    }
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

```scala
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
        
  //这个就是对应的wal写的log文件的名字的构成
  def timeToLogFile(startTime: Long, stopTime: Long): String = {
    s"log-$startTime-$stopTime"
  }

  
```

