#!/bin/zsh

# 1. LOAD ALIASES
setopt expand_aliases
source ~/.zshrc

# 2. SCALA COMPILER CONFIG
SCALA_VERSION="2.13.12"
SCALA_DIR="scala-$SCALA_VERSION"
SCALA_TGZ="$SCALA_DIR.tgz"
URL="https://downloads.lightbend.com/scala/$SCALA_VERSION/$SCALA_TGZ"

if [ ! -d "$SCALA_DIR" ]; then
    echo ">>> [1/5] Downloading Scala $SCALA_VERSION..."
    curl -O $URL
    tar -xzf $SCALA_TGZ
    rm $SCALA_TGZ
fi

LOCAL_SCALAC="./$SCALA_DIR/bin/scalac"

# 3. CLEANUP
echo ">>> [2/5] Cleaning up build and checkpoints..."
rm -rf classes
rm -f query9.jar
rm -rf chk_q9_mono # <--- CLEAN CHECKPOINT
mkdir -p classes

# 4. COMPILE
echo ">>> [3/5] Compiling Query9Mono.scala..."
SPARK_JARS="$SPARK_HOME/jars/*"

$LOCAL_SCALAC -classpath "$SPARK_JARS" -d classes Query9Mono.scala

if [ $? -ne 0 ]; then
    echo "❌ Compilation Failed."
    exit 1
fi

# 5. PACKAGE
echo ">>> [4/5] Packaging JAR..."
jar -cf query9.jar -C classes .

# 6. RUN
echo ">>> [5/5] Submitting to Spark..."
spark-submit17 \
  --class Query9Mono \
  --master "local[*]" \
  query9.jar