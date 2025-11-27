#!/bin/zsh

# 1. LOAD ALIASES (If needed for your Java setup)
setopt expand_aliases
source ~/.zshrc

# 2. CONFIGURATION
SCALA_VERSION="2.13.12"
SCALA_DIR="scala-$SCALA_VERSION"
LOCAL_SCALAC="./$SCALA_DIR/bin/scalac"

# 3. CLEANUP (Delete old jars and checkpoints to ensure fresh run)
echo ">>> [1/4] Cleaning up..."
rm -rf classes
rm -f query9bi.jar
# NOTE: We do not delete the checkpoint folder here automatically 
# to allow you to resume if needed. Manually delete 'chk_q9_bi_final_v1' if you want a fresh start.
mkdir -p classes

# 4. COMPILE
echo ">>> [2/4] Compiling Query9Bi.scala..."
SPARK_JARS="$SPARK_HOME/jars/*"

# Compile the specific Bi-Signal File
$LOCAL_SCALAC -classpath "$SPARK_JARS" -d classes Query9Bi.scala

if [ $? -ne 0 ]; then
    echo "❌ Compilation Failed."
    exit 1
fi

# 5. PACKAGE
echo ">>> [3/4] Packaging JAR..."
jar -cf query9bi.jar -C classes .

# 6. RUN
echo ">>> [4/4] Submitting Bi-Signal Query to Spark..."

# Ensure we use the alias or full path to spark-submit
# Using 'spark-submit17' based on your previous messages
spark-submit17 \
  --class Query9Bi \
  --master "local[*]" \
  --conf "spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider" \
  query9bi.jar