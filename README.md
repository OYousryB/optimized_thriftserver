# Clean Target  
    mvn clean

# Add barq jar 
    mvn install:install-file \
    -Dfile=/mnt/disk1/barq/target/scala-2.11/barq_2.11-1.0.jar \
    -DgroupId=barq \
    -DartifactId=com.incorta.barq \
    -Dversion=1 \
    -Dpackaging=jar \
    -DgeneratePom=true
﻿
# Package
    mvn package -Dmaven.test.failure.ignore=true