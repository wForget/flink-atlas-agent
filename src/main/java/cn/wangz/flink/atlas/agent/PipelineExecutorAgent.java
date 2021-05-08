package cn.wangz.flink.atlas.agent;

import java.lang.instrument.Instrumentation;

public class PipelineExecutorAgent {

    public static void premain(String agentArgs, Instrumentation inst) {
        inst.addTransformer(new PipelineExecutorClassFileTransformer(), true);
    }

}
