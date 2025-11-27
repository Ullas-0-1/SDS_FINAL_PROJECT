#!/bin/zsh

# 1. LOAD ALIASES (For spark-submit17)
setopt expand_aliases
source ~/.zshrc

# 2. COMPILER SETUP (Scala 2.13 for Spark)
SCALA_VERSION="2.13.12"
SCALA_DIR="scala-$SCALA_VERSION"
SCALA_TGZ="$SCALA_DIR.tgz"
URL="https://downloads.lightbend.com/scala/$SCALA_VERSION/$SCALA_TGZ"

if [ ! -d "$SCALA_DIR" ]; then
    echo ">>> Downloading Scala $SCALA_VERSION..."
    curl -O $URL
    tar -xzf $SCALA_TGZ
    rm $SCALA_TGZ
fi
LOCAL_SCALAC="./$SCALA_DIR/bin/scalac"

# 3. CLEAN & COMPILE
echo ">>> [1/3] Compiling Query9Batch.scala..."
rm -rf classes
rm -f query9batch.jar
mkdir -p classes

SPARK_JARS="$SPARK_HOME/jars/*"

# Compile
$LOCAL_SCALAC -classpath "$SPARK_JARS" -d classes Query9Batch.scala

if [ $? -ne 0 ]; then
    echo "❌ Compilation Failed."
    exit 1
fi

# 4. PACKAGE
echo ">>> [2/3] Packaging JAR..."
jar -cf query9batch.jar -C classes .

# 5. RUN
echo ">>> [3/3] Running Batch Job..."
spark-submit17 \
  --class Query9Batch \
  --master "local[*]" \
  query9batch.jar