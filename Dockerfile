# One image definition for every service: docker build --build-arg MODULE=order-service .
# Expects the jar from `mvn package` (module/target/app.jar).
FROM eclipse-temurin:17-jre AS extract
ARG MODULE
WORKDIR /build
COPY ${MODULE}/target/app.jar app.jar
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher --destination extracted

FROM eclipse-temurin:17-jre
RUN useradd --system --uid 10001 spring
USER 10001
WORKDIR /app
# Least-changing layers first for better image layer caching
COPY --from=extract /build/extracted/dependencies/ ./
COPY --from=extract /build/extracted/spring-boot-loader/ ./
COPY --from=extract /build/extracted/snapshot-dependencies/ ./
COPY --from=extract /build/extracted/application/ ./
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
