FROM apache/hive:4.0.0

USER root

# hadoop-aws + aws-sdk-bundle are already inside the image but not on Hive's classpath.
# Symlink them into hive/lib so HMS sees them.
RUN set -eux; \
    for jar in /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-*.jar \
               /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar; do \
      [ -f "$jar" ] && ln -sf "$jar" "/opt/hive/lib/$(basename "$jar")"; \
    done

# Pre-create a slot for the Postgres JDBC driver. The actual file is delivered
# by the hms-deps init container into the shared volume mounted at /aux.
RUN ln -sf /aux/postgresql.jar /opt/hive/lib/postgresql.jar

USER hive
