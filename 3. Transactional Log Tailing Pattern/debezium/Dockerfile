FROM debezium/connect:2.5

# Switch to root to download files
USER root

# Define versions
ENV GROOVY_VERSION=3.0.19

# Download Groovy core, JSR-223 engine, and JSON support
RUN curl https://repo1.maven.org/maven2/org/codehaus/groovy/groovy/${GROOVY_VERSION}/groovy-${GROOVY_VERSION}.jar -o /kafka/libs/groovy-${GROOVY_VERSION}.jar && \
    curl https://repo1.maven.org/maven2/org/codehaus/groovy/groovy-jsr223/${GROOVY_VERSION}/groovy-jsr223-${GROOVY_VERSION}.jar -o /kafka/libs/groovy-jsr223-${GROOVY_VERSION}.jar && \
    curl https://repo1.maven.org/maven2/org/codehaus/groovy/groovy-json/${GROOVY_VERSION}/groovy-json-${GROOVY_VERSION}.jar -o /kafka/libs/groovy-json-${GROOVY_VERSION}.jar

# Switch back to the default user
USER kafka