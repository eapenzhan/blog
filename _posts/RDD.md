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


