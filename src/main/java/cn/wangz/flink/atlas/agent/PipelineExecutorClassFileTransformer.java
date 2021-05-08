package cn.wangz.flink.atlas.agent;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

public class PipelineExecutorClassFileTransformer implements ClassFileTransformer {

    private static final String FLINK_PACKAGE_PREFIX = "org.apache.flink";
    private static final String PIPELINE_EXECUTOR_CLASS = "org.apache.flink.core.execution.PipelineExecutor";

    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {

        try {
            if (className == null) {
                return classfileBuffer;
            }

            String finallyClassName = className.replace("/", ".");

            if (!finallyClassName.startsWith(FLINK_PACKAGE_PREFIX) || finallyClassName.equals(PIPELINE_EXECUTOR_CLASS)) {
                return classfileBuffer;
            }

            ClassPool classPool = ClassPool.getDefault();
            CtClass subExecutorClass = classPool.get(PIPELINE_EXECUTOR_CLASS);
            CtClass finallyClass = classPool.get(finallyClassName);

            if (!finallyClass.subtypeOf(subExecutorClass)) {
                return classfileBuffer;
            }

            CtMethod ctMethod = finallyClass.getDeclaredMethod("execute");
            // TODO change to insertAfter
            ctMethod.insertBefore(PipelineAtlasHandler.class.getName() + ".handle($1);");

            return finallyClass.toBytecode();
        } catch (Throwable t) {
            return classfileBuffer;
        }
    }

}
