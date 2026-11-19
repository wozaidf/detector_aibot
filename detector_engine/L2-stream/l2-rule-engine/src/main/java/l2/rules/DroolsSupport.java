// 从 classpath 加载 DRL 规则文件并构建 Drools KieBase 的工具类。
package l2.rules;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.io.ResourceType;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.utils.KieHelper;

public final class DroolsSupport {
    private DroolsSupport() {}
    public static KieBase build(String classpathResource) {
        String artifactId = classpathResource.replaceAll("[^A-Za-z0-9._-]", "-");
        return new KieHelper()
                .setReleaseId(KieServices.Factory.get().newReleaseId("l2.rules", artifactId, "1.0.0"))
                .addResource(ResourceFactory.newClassPathResource(classpathResource), ResourceType.DRL)
                .build();
    }
}
