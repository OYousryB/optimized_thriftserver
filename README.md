# Clean Target  
    mvn clean

# Add barq jar 
    mvn install:install-file \
    -Dfile=BARQ_PATH/target/scala-2.11/barq_2.11-1.0.jar \
    -DgroupId=barq \
    -DartifactId=com.incorta.barq \
    -Dversion=1 \
    -Dpackaging=jar \
    -DgeneratePom=true

# Add Hermes jar 
    mvn install:install-file \
    -Dfile=HERMES_PATH/target/scala-2.11/project-hermes-assembly-1.0.jar \
    -DgroupId=hermes \
    -DartifactId=com.incorta.hermes \
    -Dversion=1 \
    -Dpackaging=jar \
    -DgeneratePom=true

﻿
# Package
     mvn -T 100  package -Dmaven.test.skip -DskipTests