# papillon

## [raft 协议实现](https://drfq7a33n6.feishu.cn/docx/Gl3zdt3d2oTE5PxPYMNc8oX0n8g)

![线程模型](picture%2Fthread.jpg)

- 主线程：
    1. 跟随者、候选人、领导者三个状态的切换以及命令的接收
    2. 需要和快照线程、状态机线程交互
    - 复制线程：
        1. 由领导者创建每个跟随者都对应一个线程，执行具体的复制逻辑
- 状态机线程：
    1. 日志提交后由主线程触发提交给状态机线程并应用到状态机中
    2. 由主线程引导应用快照到状态机中
    3. 由快照线程触发生成快照
- 快照线程：
    1. 负责定时生成快照，
    2. 由外部触发获取一个当前快照