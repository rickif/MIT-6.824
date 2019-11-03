# review

## Part 2A

具体的实现依照raft的流转状态，划分为3个follower、candidate和leader 3个loop。节点在启动时进入follower状态，经过一个随机的超时时间（接收不到leader节点的心跳）发起选举进入candidate状态。候选人获取到绝大多数节点认可的选票之后，进入leader状态开始给各个follower节点发送心跳请求。

### tips
* AppendEntries和RequestVote请求在发起过程中，需要考虑网络失效的情况。在网络失效场景下，RPC请求返回时间可能会较长。在raft选举、心跳等时延要求较高的场景，考虑使用单独的goroutine来执行网络请求避免单个请求阻塞导致loop不满足raft协议的时延要求。同时，每个RPC调用对应一个goroutine的模型要考虑资源的限制，避免网络失效等场景，请求端短时间内新建大量goroutine导致系统资源耗尽。