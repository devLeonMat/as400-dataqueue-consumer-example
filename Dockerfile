FROM gradle:9.7.1-jdk25 AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew clean bootJar --no-daemon

FROM eclipse-temurin:25-jre
RUN useradd --system --uid 10001 appuser
WORKDIR /app
COPY --from=build /workspace/build/libs/*.jar app.jar
USER 10001
EXPOSE 8080
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "/app/app.jar"]
