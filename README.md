## Flink Atlas Agent (Incomplete)

### Introduce

由于 Flink 中没有暴露 Pipeline 对象，无法很好的获取到血缘信息。为了不对 Flink 代码进行修改，尝试通过 java agent 方式进行注入。

Flink Atlas Agent 拦截修改 PipelineExecutor 的子类方法，在 execute 方法后注入处理 Pipeline 对象的逻辑，获取 Flink 血缘信息，并发送到 Apache Atlas 中。

### IDEA DEBUG

配置 vm options: -javaagent:${Project Path}\target\flink-atlas-agent-1.0-SNAPSHOT.jar

### Reference

+ [\[Discussion\] Job generation / submission hooks & Atlas integration](http://apache-flink-mailing-list-archive.1008284.n3.nabble.com/Discussion-Job-generation-submission-hooks-amp-Atlas-integration-td37298.html)
+ [Flink Atlas Integration](https://docs.google.com/document/d/1wSgzPdhcwt-SlNBBqL-Zb7g8fY6bN8JwHEg7GCdsBG8/edit)