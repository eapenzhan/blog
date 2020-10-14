## Spark 数据流

对SerializerManager的说明：它是为各种Spark组件配置序列化，压缩和加密的组件，包括自动选择用于shuffle的Serializer。

Spark中的数据在network IO 或 local disk IO传输过程中。都需要序列化。其默认的 Serializer 是 `org.apache.spark.serializer.JavaSerializer`，在一定条件下，可以使用kryo，即`org.apache.spark.serializer.KryoSerializer`。

## Spark Serializer介绍
在*Serializer.scala*中，有4个抽象类：
- **Serializer：**用来创建`SerializerInstance`，该对象执行实际的序列化和反序列方法。
- **SerialInstance：**序列化实例。由于许多序列化库并不是线程安全的，所以只能单线程使用。
- **SerializationStream：**序列化流
- **DeserializationStream：**反序列化流

无论是Java序列化器还是Kryo序列化器都会去实现对应的类和方法。
而在大部分ShuffleWriter的写场景中，无论是`DiskBlockObjectWriter`还是其他在调用序列化器的代码，使用方法基本如下：
```scala
/**
 * 1. 创建序列化实例
 * 2. 如果开启了压缩，serializerManager将先把对象压缩，否则返回原始流
 * 3. 创建序列化流
 * 4. 流写入对象
 */
serInstance = serializer.newInstance()
bs = serializerManager.wrapStream(blockID, outputStream) 
objOut = serInstance.serializerStream(bs)
objOut.writeXXX()
```
`SerializerManager`主要用来为Spark不同的组件配置序列化及其压缩和加密（需要分别开启）。其还有一个重要的功能是自动选择，在遇到原始类型和字符串类型时，会自动选择Kryo序列化器。它也有包装的序列化接口，这些接口的主要使用场景是在RDD计算后的读写磁盘时使用。

自动选择中还有一条规则，就是`blockId`的类型不是`StreamBlockId`
```scala
def dataSeraizeStream[T: ClassTag] (
	blockId: BlockId
	outputStream: OutputStream
	values: Iterator[T]): Unit = {
	...
	val autoPick = !blockId.isInstanceOf[StreamBlockId]
	...
}
```
大家已经知道Block的概念，即Partition对应数据的实际存储单元。每个Block都有ID，而ID本身是带有Block的类型信息。从下图可以看出BlockID都有哪些类型：

![BlockId Type](https://user-images.githubusercontent.com/6768613/95954584-1fd0e380-0e2e-11eb-81e1-0502d7cbc058.jpg)

**那什么是StreamBlock呢？**
这里我们不得不提到Spark的离散流（DStream）的概念。简单来讲，RDD序列就是DSream的内部表示。数据流在进入Spark后在内部以离散的形式存储，然后在最终合并输出。

![enter image description here](http://spark.apache.org/docs/latest/img/streaming-flow.png)

Spark Streaming提供了两类内置的流媒体源。
- 基础源：如文件系统和SOCKET连接等。
- 高级源：可以通过其他实用程序类获得诸如Kafka，Kinesis等来源。

StreamBlock是在接受到外部数据流之后由BlockGenerator创建的Block单元。

## Shuffle
Shuffle的框架类图如下：

[![https://www.processon.com/view/link/5f72d8017d9c08039fc23705](https://gitlab.huawei.com/z00483239/spark-learning/uploads/b298590ce8a1eb00a5bf08695378b906/350a8065-e482-4996-afae-c681a5cdbf01.png)](https://www.processon.com/view/link/5f72d8017d9c08039fc23705)

Driver和每个Executor都会持有一个`ShuffleManager`，这个以前是ShuffleMananger可以通过配置项*spark.shuffle.manager*来配置的，不过由于一直都只使用`SortShuffleManager`，所以把这个选项删掉了。`ShuffleManager`由SparkEnv创建，Driver中的`ShuffleManager`负责书册Shuffle的元数据，比如ShuffleId、Map Task等；Executor中的ShuffleManager则负责读和写Shuffle的数据。

Shuffle Map Task通过`ShuffleWriter`将Shuffle数据写入本地。在[Spark 3.0.0](https://issues.apache.org/jira/browse/SPARK-25299)之后，把Writer部分重构成可插拔式的实现，使用了新的API。
- `ShuffleDataIO`：使用`SortShuffleManager`时插件的根。这个相当于你想要换掉读写Shuffle数据的默认的行为，就需要通过配置*spark.shuffle.sort.io.plugin.class*来替换这个根，但暂时Spark对于它只有唯一实现，即通过本地磁盘来读写Shuffle数据。
- `ShuffleExecutorComponents`；每个Executor管理Shuffle相关组件的插件子集。这个具体的实例可以通过`ShuffleDataIO`来获取。它用来实例化真正读写Shuffle数据的读写器。在`ShuffleWriter`的三个实现类里都会使用它。
- `ShuffleMapOutputWriter`：每个Map Task都会实例化一次该类，他提供了`ShufflePartitionWriter`实例用来将Map Task中的数据持久化到每个分区。

### ShuffleWriter

`ShuffleWriter`有三种实现
- `BypassMergeSortShuffleWriter`：对应过去Hash Based Shuffle的Writer。基于文件做的分区，没有Sort操作，最后分区数据被写入一个完整文件，并且有一个索引文件记录文件中每一个分区对应的FileSegment的大小。这种设计是比较朴素的，也很简单，易实现。
- `SortShuffleWriter`：该方式将溢出前数据使用数组自定义的Map或者是列表来保存，如果指定了aggerator，则使用Map结构，Map数据结构支持map端的预聚合操作，但是列表方式的不支持预聚合。
   数据每次溢出数据都进行排序，如果指定了ordering，则先按分区排序，再按每个分区内的key排序，最终数据溢出到磁盘中的临时文件中，在merge阶段，数据被SpillReader读取出来和未溢出的数据整体排序，最终数据可以整体有序的落到最终的数据文件中。
- `UnsafeShuffleWriter`：与`SortShuffleWriter`相比
	- 没有指定 aggregation 或者key排序， 因为 key 没有编码到排序指针中，所以只有 partition 级别的排序
	- 原始数据首先被序列化处理，并且再也不需要反序列，在其对应的元数据被排序后，需要Serializer支持relocation，在指定位置读取对应数据。 KryoSerializer和Spark SQL自定义的序列化器 支持这个特性。

无论是哪种方式，最终在Stage结束前将数据都写到文件系统中。

### ShuffleReader

除了需要从外部存储读取数据和RDD已经做过Cache或者Checkpoint的Task，一般Task都是从ShuffleRDD的Shuffle Read开始的。

Shuffle Read的整体架构如下图所示

![Shuffle Read](https://user-images.githubusercontent.com/6768613/95953326-3f670c80-0e2c-11eb-84a2-71e322826d19.png)
其中`HashShuffleReader`已经被替换成了`BlockStoreShuffleReader`。

`BlockStoreShuffleFetcher#fetch`会获得数据，它首先会通过`MapOutputTracker#getServerStatuses`来获得数据的meta信息，这个过程有可能需要向`MapOutputTrackerMasterActor`发送读请求，这个读请求是在`MapOutputTracker#askTracker`发出的。在获得了数据的meta信息后，它会将这些数据存入`Seq[(BlockManagerId，Seq[(BlockId， Long)])]`中，然后调用`ShuffleBlockFetcherIterator`最终发起请求。`ShuffleBlockFetcherIterator`根据数据的本地性原则进行数据获取。如果数据在本地，那么会调用`.BlockManager#getBlockData`进行本地数据块的读取。而对于Shuffle类型的数据，会调用`ShuffleManager`的`ShuffleBlockManager#getBlockData`。如果数据在其他的Executor上，若用户使用的*spark.shuffle.blockTransferService*是**netty**，那么就会通过`NettyBlockTransferService#fetchBlocks`获取；如果使用的是**nio**，那么就会通过`NioBlockTransferService#fetchBlocks`获取。

#### 读取策略

`ShuffleBlockFetcherIterator`会通过`partitionBlockByFetchMode`划分数据的读取策略：如果数据在本地，那么可以直接从`BlockManager`中获取；如果需要从其他的节点上获取，则需要通过网络。代码如下：

```scala
private[this] def partitionBlocksByFetchMode(): ArrayBuffer[FetchRequest] = {
  logDebug(s"maxBytesInFlight: $maxBytesInFlight, targetRemoteRequestSize: "
    + s"$targetRemoteRequestSize, maxBlocksInFlightPerAddress: $maxBlocksInFlightPerAddress")

  // Partition to local, host-local and remote blocks. Remote blocks are further split into
  // FetchRequests of size at most maxBytesInFlight in order to limit the amount of data in flight
  val collectedRemoteRequests = new ArrayBuffer[FetchRequest]
  var localBlockBytes = 0L
  var hostLocalBlockBytes = 0L
  var remoteBlockBytes = 0L

  for ((address, blockInfos) <- blocksByAddress) {
    if (address.executorId == blockManager.blockManagerId.executorId) {
      // 本地读取
      checkBlockSizes(blockInfos)
      val mergedBlockInfos = mergeContinuousShuffleBlockIdsIfNeeded(
        blockInfos.map(info => FetchBlockInfo(info._1, info._2, info._3)), doBatchFetch)
      numBlocksToFetch += mergedBlockInfos.size
      localBlocks ++= mergedBlockInfos.map(info => (info.blockId, info.mapIndex))
      localBlockBytes += mergedBlockInfos.map(_.size).sum
    } else if (blockManager.hostLocalDirManager.isDefined &&
      address.host == blockManager.blockManagerId.host) {
      // [SPARK 27651]本地主机读取，对于不同的BlockManager在同一主机上产生的本地存储，避免通过网络从本机获取
      checkBlockSizes(blockInfos)
      val mergedBlockInfos = mergeContinuousShuffleBlockIdsIfNeeded(
        blockInfos.map(info => FetchBlockInfo(info._1, info._2, info._3)), doBatchFetch)
      numBlocksToFetch += mergedBlockInfos.size
      val blocksForAddress =
        mergedBlockInfos.map(info => (info.blockId, info.size, info.mapIndex))
      hostLocalBlocksByExecutor += address -> blocksForAddress
      hostLocalBlocks ++= blocksForAddress.map(info => (info._1, info._3))
      hostLocalBlockBytes += mergedBlockInfos.map(_.size).sum
    } else {
      // 远程获取
      remoteBlockBytes += blockInfos.map(_._2).sum
      collectFetchRequests(address, blockInfos, collectedRemoteRequests)
    }
  }
  val numRemoteBlocks = collectedRemoteRequests.map(_.blocks.size).sum
  val totalBytes = localBlockBytes + remoteBlockBytes + hostLocalBlockBytes
  assert(numBlocksToFetch == localBlocks.size + hostLocalBlocks.size + numRemoteBlocks,
    s"The number of non-empty blocks $numBlocksToFetch doesn't equal to the number of local " +
      s"blocks ${localBlocks.size} + the number of host-local blocks ${hostLocalBlocks.size} " +
      s"+ the number of remote blocks ${numRemoteBlocks}.")
  logInfo(s"Getting $numBlocksToFetch (${Utils.bytesToString(totalBytes)}) non-empty blocks " +
    s"including ${localBlocks.size} (${Utils.bytesToString(localBlockBytes)}) local and " +
    s"${hostLocalBlocks.size} (${Utils.bytesToString(hostLocalBlockBytes)}) " +
    s"host-local and $numRemoteBlocks (${Utils.bytesToString(remoteBlockBytes)}) remote blocks")
  collectedRemoteRequests
}
```

由于Shuffle的数据量可能会很大，因此这里的网络读取分为以下几种策略：
1. 每次最多启动5个线程到最多5个节点上读取数据。
2. 每次请求的数据大小不会超过spark.reducer.maxSizeInFlight(默认值为48MB)的五分之一。这样做的原因有以下几点：
    - 避免占用目标机器过多带宽，在千兆网卡为主流的今天，带宽还是比较重要的。如果机器使用的是万兆网卡，那么可以通过设置spark.reducer.maxSizeInFlight来充分利用带宽。
    - 请求数据可以平行化，这样请求数据的时间可以大大减少。请求数据的总时间就是请求中耗时最长的。这样可以缓解一个节点出现网络拥塞时的影响。主要实现过程如下：


```scala
private val targetRemoteRequestSize = math.max(maxBytesInFlight / 5, 1L)

private def collectFetchRequests(
    address: BlockManagerId,
    blockInfos: Seq[(BlockId, Long, Int)],
    collectedRemoteRequests: ArrayBuffer[FetchRequest]): Unit = {
  val iterator = blockInfos.iterator
  var curRequestSize = 0L
  var curBlocks = Seq.empty[FetchBlockInfo]

  while (iterator.hasNext) {
    val (blockId, size, mapIndex) = iterator.next()
    assertPositiveBlockSize(blockId, size)
    curBlocks = curBlocks ++ Seq(FetchBlockInfo(blockId, size, mapIndex))
    curRequestSize += size
    // doBatchFetch：如果服务端支持，批量从同一executor上取shuffle block
    // maxBlocksInFlightPerAddress：对于给定远程端口在任意时间最大取block请求数
    val mayExceedsMaxBlocks = !doBatchFetch && curBlocks.size >= maxBlocksInFlightPerAddress
    if (curRequestSize >= targetRemoteRequestSize || mayExceedsMaxBlocks) {
      curBlocks = createFetchRequests(curBlocks, address, isLast = false,
        collectedRemoteRequests)
      curRequestSize = curBlocks.map(_.size).sum
    }
  }
  // Add in the final request
  if (curBlocks.nonEmpty) {
    curBlocks = createFetchRequests(curBlocks, address, isLast = true,
      collectedRemoteRequests)
    curRequestSize = curBlocks.map(_.size).sum
  }
}
```


真正发送请求是由`ExternalBlockStoreClient`和`NettyBlockTransferService`这两个类的`fetchBlock()`方法来执行。

> 两个类均有被BlockManager用到，这里没有深入研究。

通过异步回调Listener方法，将请求的结果放到队列（`LinkedBlockingQueue`）中，通过`ShuffleBlockFetcherIterator#next`读取。



----
参考：
1. https://cloud.tencent.com/developer/article/1491360
2. https://techmagie.wordpress.com/2017/04/22/spark-streaming-under-the-hood/
3. https://www.cnblogs.com/johnny666888/p/11291592.html
4. https://www.cnblogs.com/johnny666888/archive/2004/01/13/11285540.html
5. https://juejin.im/entry/6844903504696328205
6. 《Spark技术内幕》
