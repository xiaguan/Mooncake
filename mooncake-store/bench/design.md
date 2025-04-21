###  压测程序概览
目的，对比mooncake-store和redis的性能

### 压测程序设计

单个binary，需要用gflags区分
1. 压测模式:prefill,decode
2. 压测引擎:mooncake-store,redis
3. 压测的value size: 128B,1KB,1MB...
4. 压测操作数量：1w
5. 压测并发线程数： 1，4，8

这些是args

数据应该都是意义的，redis和mooncake-store各自实现一个KVEngine，

prefill模式下，向KVEngine种写入数据，然后发送到用redis的消息队列中
decode模式下，从redis的消息队列中读取数据，然通过KVEngine中读取数据

测量两端的总吞吐






