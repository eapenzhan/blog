 ## 优点
- 自动容错
- 内容感知性调度
- 可伸缩性
## 缺点
- 粗粒度，编程模型受限
  > 但是可以适用大部分场景

## RDD主要属性
1. Partition：数据集的基本组成单位，用户可以指定其数量（默认是CPU核数），其分配的存储由BlockManager实现。
![image](https://user-images.githubusercontent.com/6768613/91516692-b26d0000-e91e-11ea-8d9e-95132f804475.png)
2. compute函数：由于RDD可以延迟计算，真正的计算可以等到Shuffle阶段。compute函数会对迭代器进行复合，不需要保存每次计算的结果。
3. RDD之间的依赖关系
  - 窄依赖:每一个parent RDD的Partition最多被子RDD的一个Partition使用
  - 宽依赖:多个子RDD的Partition会依赖同一个parentRDD的Partition
4. 分片函数(Partitioner)：只有Key-Value的RDD才有Partitioner，非Key-Value的为None。有如下两种实现
   - HashPartitioner
   - RangePartitioner
5. 存取每个Partition的优先位置列表：这个是用来感知数据位置，从而尽可能将计算任务分配到数据块的所在存储位置。

## 创建
1. 通过并行化的方式
  ```scala
val data =  Array(1,2,3)
val Data = sc.parallelize(data)
```
2. 通过读取External Datasets外部文件方式
  ```scala
val File = sc.textFile(filePath)
```

## 操作
- 转换：从现有数据集创建一个新的数据集
  - 上面也提到了RDD转换都是惰性的。只有当发生一个要求返回结果给Driver的动作时，这些转换才会真正运行。
  - 转换操作包含如下： 
    ![Transformation](https://user-images.githubusercontent.com/6768613/91533069-daba2600-e941-11ea-92b3-2dcbfa1817ab.png)

- 动作：在数据集上进行计算后，返回一个值给Driver程序
  - 动作操作包含如下：
    
    ![Action](https://user-images.githubusercontent.com/6768613/91533578-b1e66080-e942-11ea-9b1a-928bf2aa6a8a.png)

## 缓存 & 检查点
Spark速度非常快的原因之一，就是在不同操作中在内存中持久化（或缓存）一个数据集。
当持久化一个RDD后，每一个节点都将把计算的分片结果保存在内存中，并在对此数据集（或者衍生出的数据集）进行的其他动作（action）中重用。

通过缓存，Spark避免了RDD上的重复计算，能够极大地提升计算速度。但是，如果缓存丢失了，则需要重新计算。如果计算特别复杂或者计算耗时特别多，那么缓存丢失对于整个Job的影响是不容忽视的。为了避免缓存丢失重新计算带来的开销，Spark又引入了检查点（checkpoint）机制。当某个点某个executor宕了，上面缓存的RDD就会丢掉， 如果没有检查点，则需要通过依赖链重新计算出来。检查点通过把RDD保存在HDFS（或用户定义的存储级别）中，在丢失时从中复制出来，实现的高容错。

![checkpoint](https://upload-images.jianshu.io/upload_images/9193428-676ecfe7baf991b0.png?imageMogr2/auto-orient/strip|imageView2/2/w/788/format/webp)

### RDD持久化

用户可以在自己可控的RDD（有些计算过程的中间RDD，在用户程序上是无法控制的）使用`RDD#cache()`或者`RDD#persist()`函数进行缓存。前者是将RDD缓存在内存中，后者需要指定缓存存储级别。

存储级别由`StorageLevel`定义，它由五个维度来组成：
```scala
class StorageLevel private(
    private var _useDisk: Boolean,
    private var _useMemory: Boolean,
    private var _useOffHeap: Boolean,
    private var _deserialized: Boolean, // True表示以原始数据存储而非序列化后字节流
    private var _replication: Int = 1)  //
  extends Externalizable  {}
```
根据不同的组合，Spark给出了不同的存储级别:
```scala
object StorageLevel {
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(true, true, true, false, 1)
  }
```
Spark自身也会对计算结果进行缓存。在第一次动作操作计算后，RDD会自动缓存到内存中。

> 这部分源码分析待添加。

### Checkpoint处理

用户可以通过`RDD#checkpoint()`来对RDD进行检查点标记，此操作需要设置检查点的存储目录（通过`SparkContext#checkpointDir`方法）。

在Spark内部，通过如下方法可以看到，在Job运行最后会通过`doCheckpoint()`方法来执行检查点相关的操作。`runJob`会触发动作（action）操作。
```scala
/**
 * Run a function on a given set of partitions in an RDD and pass the results to the given
 * handler function. This is the main entry point for all actions in Spark.
 */
def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    resultHandler: (Int, U) => Unit): Unit = {
  ...
  dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
  progressBar.foreach(_.finishAll())
  rdd.doCheckpoint()
}
```
从如下代码可以看到，需要先检查`checkpointData`是否为空，如果不空，`RDD#doCheckpoint()`委托给`checkpointData`去做检查点的实际操作。
```scala
/**
 * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
 * has completed (therefore the RDD has been materialized and potentially stored in memory).
 * doCheckpoint() is called recursively on the parent RDDs.
 */
private[spark] def doCheckpoint(): Unit = {
  RDDOperationScope.withScope(sc, "checkpoint", allowNesting = false, ignoreParent = true) {
    if (!doCheckpointCalled) {
      doCheckpointCalled = true
      if (checkpointData.isDefined) {
        if (checkpointAllMarkedAncestors) {
          dependencies.foreach(_.rdd.doCheckpoint())
        }
        checkpointData.get.checkpoint()
      } else {
        dependencies.foreach(_.rdd.doCheckpoint())
      }
    }
  }
}
```
那checkPointData是什么呢？我们通过看他的定义
```scala
private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None
```
进一步看Ta的数据结构`RDDCheckpointData`，代码如下：
```scala
/**
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with an RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
 */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends Serializable {
  import CheckpointState._
  /** 对应RDD的checkpoint状态 */
  protected var cpState = Initialized

  /** 被 Checkpoint 的 RDD 的数据 */
  private var cpRDD: Option[CheckpointRDD[T]] = None

  /** 返回RDD是否被持久化 */
  def isCheckpointed: Boolean = RDDCheckpointData.synchronized { cpState == Checkpointed }
  
  /**
   * 计算该RDD并持久化其内容。该方法在该RDD第一次动作操作后会立即被调用
   */
  final def checkpoint(): Unit = {
    //确保在多线程调用该方法时只有一个线程执行后续操作
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }
    val newRDD = doCheckpoint()
    // Update our state and truncate the RDD lineage
    // 可以看到 cpRDD 在此处被赋值，通过 newRDD 来生成，而生成的方法是 doCheckpoint()
    RDDCheckpointData.synchronized {
      cpRDD = Some(newRDD)
      cpState = Checkpointed
      rdd.markCheckpointed()
    }
  }

  /** 这个是 Checkpoint RDD 的抽象方法， 具体操作由子类实现 */
  protected def doCheckpoint(): CheckpointRDD[T]
  
  /** 返回RDD数据，只有checkpointed状态才返回非空 */
  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }
  
  /** 测试使用 */
  def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
    cpRDD.map(_.partitions).getOrElse { Array.empty }
  }
}
```
`RDDCheckpointData`包含了RDD的checkpoint的全部信息和相关操作，控制RDD的checkpoint过程和状态变更。
下面我们通过类图来展示SPark中两种RDD的实现：
![RDD UML](https://user-images.githubusercontent.com/6768613/91720677-1830e480-ebca-11ea-8881-69158e6b6ca5.png)

Checkpoint RDD目前有两种实现
	1. `ReliableCheckpointRDD`：与之对应是`ReliableRDDCheckpointData`。Reliable 很好理解，就是把 RDD Checkpoint 到可依赖的文件系统，言下之意就是 Driver 重启的时候也可以从失败的时间点进行恢复，无需再走一次 RDD 的转换过程。其`writePartitionToCheckPointFile()`和`readCheckpointFile()`分别是从检查点写入文件和恢复的方法。
	2. `LocalCheckpointRDD`：与之对应是`LocalRDDCheckpointData`。RDD 会被保存到 Executor 本地文件系统中，以减少保存到分布式容错性文件系统的巨额开销，因此 Local 形式的 Checkpoint 是基于本地持久化来做的，没有写到外部分布式文件系统。如果本地Executor挂了，那这个RDD就丢失了。`LocalCheckpointRDD`只是相当于一个占位符，本身并没有什么方法，因为其内容已经默认Cache到本地，无需其他操作。

> RDD 在生成检查点时，其血统会被移除，即其前向依赖，这个我研究一下之后再补充这一块。

### RDD缓存读取

通过RDD的ID和当前计算的PartitionID向Storage模块的BlockManager发起查询请求，如果能够获得Block信息，会直接返回Block的信息。否则，代表该RDD是需要计算的。在计算结束后，计算结果会根据用户定义的存储级别，写入BlockManager中，以便下次直接读取该结果。

其逻辑在如下函数中实现
```scala  
     
//RDD.scala     
/**
 * Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached.
 */
private[spark] def getOrCompute(partition: Partition, context: TaskContext): Iterator[T] = {
    //由RDD Id和partitionId确认唯一的一个blockId
    //RDD 的每个 Partition 唯一对应一个 Block（BlockId 的格式为 rdd_RDD-ID_PARTITION-ID ）
    val blockId = RDDBlockId(id, partition.index)
    var readCachedBlock = true
    
    //向BlockManager查询结果，如果有缓存，直接读取，否则计算后进行持久化存储。
    SparkEnv.get.blockManager.getOrElseUpdate(blockId, storageLevel, elementClassTag, () => {
      readCachedBlock = false
      computeOrReadCheckpoint(partition, context)
    }) match {
      //持久化成功
      case Left(blockResult) =>
        //从Cache中读取成功
        if (readCachedBlock) {
          val existingMetrics = context.taskMetrics().inputMetrics
          existingMetrics.incBytesRead(blockResult.bytes)
          new InterruptibleIterator[T](context, blockResult.data.asInstanceOf[Iterator[T]]) {
            override def next(): T = {
              existingMetrics.incRecordsRead(1)
              delegate.next()
            }
          }
        } else {
          new InterruptibleIterator(context, blockResult.data.asInstanceOf[Iterator[T]])
        }
      //持久化失败
      case Right(iter) =>
        new InterruptibleIterator(context, iter.asInstanceOf[Iterator[T]])
    }
}

// BlockManager.scala
/**
 * Retrieve the given block if it exists, otherwise call the provided `makeIterator` method
 * to compute the block, persist it, and return its values.
 *
 * @return either a BlockResult if the block was successfully cached, or an iterator if the block
 *         could not be cached.
 */
def getOrElseUpdate[T](
    blockId: BlockId,
    level: StorageLevel,
    classTag: ClassTag[T],
    makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]] = {
  // Attempt to read the block from local or remote storage. If it's present, then we don't need
  // to go through the local-get-or-put path.
  get[T](blockId)(classTag) match {
    case Some(block) =>
      return Left(block)
    case _ =>
      // Need to compute the block.
  }
  // Initially we hold no locks on this block.
  doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
    case None =>
      // doPut() didn't hand work back to us, so the block already existed or was successfully
      // stored. Therefore, we now hold a read lock on the block.
      val blockResult = getLocalValues(blockId).getOrElse {
        // Since we held a read lock between the doPut() and get() calls, the block should not
        // have been evicted, so get() not returning the block indicates some internal error.
        releaseLock(blockId)
        throw new SparkException(s"get() failed for block $blockId even though we held a lock")
      }
      // We already hold a read lock on the block from the doPut() call and getLocalValues()
      // acquires the lock again, so we need to call releaseLock() here so that the net number
      // of lock acquisitions is 1 (since the caller will only call release() once).
      releaseLock(blockId)
      Left(blockResult)
    case Some(iter) =>
      // The put failed, likely because the data was too large to fit in memory and could not be
      // dropped to disk. Therefore, we need to pass the input iterator back to the caller so
      // that they can decide what to do with the values (e.g. process them without caching).
     Right(iter)
  }
}
```

## DAG生成
将RDD连接起来，表明先后执行顺序。

- 血统(Lineage)：这个概念表明RDD之间的依赖关系（RDD的Parent）。
- Stage：这个概念实际就是根据DAG的划分而来的。
  - 窄依赖：由于Partition依赖关系确定，Partition的转换可以在同一个线程完成，这些RDD被划分为同一个Stage。Stage内部每个Partition被分配一个计算任务（Task），这些Task可以并行执行。
  - 宽依赖：由于Shuffle的存在，宽依赖成为不同Stage划分的依据。Parent Stage执行全部完成后，子Stage才能执行。
  
  ![wordcountRDD转换](https://user-images.githubusercontent.com/6768613/91627052-f88d9680-e9e6-11ea-9fb3-5cb16cf5404e.jpeg)


## RDD计算

### Task

集群的计算节点Excutor会在准备好Task的运行环境后， 会用过调用`org.apache.spark.executor.Executor.TaskRunner#run`函数来执行。
运行环境信息由SparkEnv包含，在新SparkContext时创建，其中有
- **akka.actor.ActorSystem**
  - Driver上称为sparkDriver
  - Excutor上称为sparkExecutor
- **org.apache.spark.serializer.Serializer**：序列化器
- **org.apache.spark.MapOutputTracker**：保存ShuffleMapTask的输出位置信息
  - Driver上Tracer：org.apache.spark.MapOutputTrackerMaster
  - Executor上Tracer：org.apache.spark.MapOutputTrackerWorker，从Master上获取信息
- **org.apache.spark.shuffle.ShuffleManager**：内置支持HashBasedShuffle和SortBasedShuffle
  - Driver端：注册Shuffle信息
  - Executor端：上报和获取Shuffle信息
- **org.apache.spark.broadcast.BroadcastManager**：管理广播变量
- **org.apache.spark.network.BlockTransferService**：Executor读取Shuffle数据的客户端，当前支持Netty和NIO
- **org.apache.spark.storage.BlockManager**：Storage模块与其他模块的交互接口，管理Storage模块
- **org.apache.spark.SecurityManager**：管理认证和授权
- **org.apache.spark.HttpFileServer**：提供Http服务的Server类，当前用于Executor端下载依赖。
- **org.apache.spark.metrics.MetricsSystem**：搜集统计信息
- **org.apache.spark.shuffle.ShuffleMemoryManager**：管理Shuffle过程中使用的内存。
  - 管理策略：为了使得每个线程都会比较公平地获取内存资源，避免一个线程申请了大量内存后造成其他的线程需要频繁地进行spill（把内存缓冲区中的数据写入到本地磁盘）操作。对于N个线程，每个线程可以至少申请1/(2\*N)的内存，但是至多申请1/N。
  




