/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.test;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.reflections.Reflections;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

@SupportedAnnotationTypes({"com.facebook.presto.test.TestableConnector"})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class ConnectorTestProcessor extends AbstractProcessor
{
    private Map<String, Integer> testCounts = new HashMap<>();
    private Set<Class<?>> tests;
    private Set<TypeElement> connectors;

    private static final String VELOCITY_PROPERTIES = "velocity.properties";
    private final Template template;

    public ConnectorTestProcessor()
            throws IOException
    {
        Reflections reflections = new Reflections("com.facebook.presto.featuretests");

        tests = sanityChecked(reflections.getTypesAnnotatedWith(ConnectorTest.class));

        connectors = ImmutableSet.of();

        Properties properties = new Properties();
        URL url = this.getClass().getClassLoader().getResource(VELOCITY_PROPERTIES);
        try (InputStream inputStream = url.openStream()) {
            properties.load(inputStream);
        }
        VelocityEngine velocityEngine = new VelocityEngine(properties);
        template = velocityEngine.getTemplate("ConnectorTest.vt");
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment)
    {
        Set<TypeElement> newConnectors = gather(annotations, roundEnvironment, TestableConnector.class, connectors);
        addConnectors(newConnectors);

        return true;
    }

    private Set<Class<?>> sanityChecked(Set<Class<?>> tests)
    {
        Set<Class<?>> wrong = tests.stream()
                .filter(test -> test.getAnnotation(ConnectorTest.class).testName().isEmpty())
                .collect(toImmutableSet());

        if (wrong.isEmpty()) {
            return tests;
        }

        /*
         * This will allow compilation to continue as far as the build thinks
         * is reasonable, but fail at the end, possibly after finding other
         * errors. This is preferred to throwing an exception here.
         *
         * http://docs.oracle.com/javase/8/docs/api/javax/annotation/processing/Processor.html
         *
         * Filter out incorrectly annotated tests here, rather than where we
         * use the testName so that we don't generate multiple identical error
         * messages.
         */
        wrong.forEach(test -> processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, String.format("Test class %s must have a non-empty testName in it's %s annotation", test.getName(), ConnectorTest.class.getName())));

        return tests.stream()
                .filter(test -> !wrong.contains(test))
                .collect(toImmutableSet());
    }

    private Set<TypeElement> gather(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment, Class<? extends Annotation> annotation, Set<TypeElement> sink)
    {
        // Skip processing if this round isn't processing any of the annotations we claim.
        if (annotations.stream()
                .map(TypeElement::getQualifiedName)
                .noneMatch(name -> name.contentEquals(annotation.getName()))) {
            return ImmutableSet.of();
        }

        return roundEnvironment.getElementsAnnotatedWith(annotation).stream()
                .filter(element -> TypeElement.class.isAssignableFrom(element.getClass()))
                .map(TypeElement.class::cast)
                .filter(element -> !sink.contains(element))
                .collect(toImmutableSet());
    }

    private List<String> gatherUnsupportedTests(TypeElement connector, Class<?> test)
    {
        return Arrays.stream(test.getMethods())
                .filter(method -> method.getAnnotation(ConnectorTest.class) != null)
                .filter(method -> !connectorSupportsTestMethod(connector, test, method))
                .map(Method::getName)
                .collect(toImmutableList());
    }

    private void addConnectors(Set<TypeElement> newConnectors)
    {
        generate(newConnectors);

        connectors = ImmutableSet.<TypeElement>builder()
                .addAll(connectors)
                .addAll(newConnectors)
                .build();
    }

    private static Set<ConnectorFeature> supportedFeatures(TypeElement connector)
    {
        return ImmutableSet.copyOf(connector.getAnnotation(TestableConnector.class).supportedFeatures());
    }

    private static Set<ConnectorFeature> requiredFeatures(AnnotatedElement test)
    {
        return ImmutableSet.copyOf(test.getAnnotation(ConnectorTest.class).requiredFeatures());
    }

    private static boolean connectorSupportsTest(TypeElement connector, Class<?> test)
    {
        Set<ConnectorFeature> supportedFeatures = supportedFeatures(connector);
        Set<ConnectorFeature> requiredFeatures = requiredFeatures(test);

        return supportedFeatures.containsAll(requiredFeatures);
    }

    private static boolean connectorSupportsTestMethod(TypeElement connector, Class<?> test, Method testMethod)
    {
        Set<ConnectorFeature> supportedFeatures = supportedFeatures(connector);
        Set<ConnectorFeature> requiredFeatures = Sets.union(requiredFeatures(test), requiredFeatures(testMethod));

        return supportedFeatures.containsAll(requiredFeatures);
    }

    private static String getTestClassName(TypeElement connector, Class<?> test)
    {
        String connectorName = connector.getAnnotation(TestableConnector.class).connectorName();
        String testName = test.getAnnotation(ConnectorTest.class).testName();
        return "Test" + testName + connectorName;
    }

    private static String getPackageName(Element element, Class<?> test)
    {
        if (PackageElement.class.isAssignableFrom(element.getClass())) {
            List<String> components = Splitter.on(".").splitToList(test.getCanonicalName());
            return Joiner.on(".").join(((PackageElement) element).getQualifiedName().toString(), components.get(components.size() - 2), "generated");
        }

        return getPackageName(element.getEnclosingElement(), test);
    }

    private static String getFqClassName(TypeElement connector, Class<?> test)
    {
        return getPackageName(connector, test) + "." + getTestClassName(connector, test);
    }

    private void generate(Set<TypeElement> connectors)
    {
        for (TypeElement connector : connectors) {
            Set<Class<?>> supportedTestClasses = tests.stream()
                    .filter(test -> connectorSupportsTest(connector, test))
                    .collect(toImmutableSet());

            String key = connector.getQualifiedName().toString();
            int testClassCount = supportedTestClasses.size();

            if (testCounts.containsKey(key) && testClassCount != 0) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, String.format("Found additional tests for %s. This is currently unsupported.", key));
            }
            else {
                System.out.println(String.format("connector %s tests %s", key, testClassCount));
                testCounts.put(key, testClassCount);
            }

            supportedTestClasses.forEach(test -> generate(connector, test, testClassCount));
        }
    }

    private void generate(TypeElement connector, Class<?> test, int connectorTestCount)
    {
        /*
         * Can't use an ImmutableMap here. Velocity mutates the map when it does e.g. a foreach.
         */
        Map<String, Object> contextMap = new HashMap<>();

        contextMap.put("package", getPackageName(connector, test));
        contextMap.put("testBaseClass", test.getSimpleName());
        contextMap.put("fqTestBaseClass", test.getName());
        contextMap.put("fqConnectorClass", connector.getQualifiedName().toString());
        contextMap.put("testClassName", getTestClassName(connector, test));
        contextMap.put("connectorTestCount", connectorTestCount);
        contextMap.put("unsupportedTests", gatherUnsupportedTests(connector, test));

        VelocityContext context = new VelocityContext(contextMap);

        JavaFileObject targetFile;

        try {
            targetFile = processingEnv.getFiler().createSourceFile(getFqClassName(connector, test));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (Writer writer = targetFile.openWriter()) {
            template.merge(context, writer);
        }
        catch (IOException e) {
                throw new RuntimeException(e);
        }
    }
}
