# Dockerfile
FROM biocypher/base:1.2.0

# Add the Debian stretch archive repository for OpenJDK 8
RUN apt-get update &&     apt-get install -y --fix-missing software-properties-common &&     echo 'deb http://archive.debian.org/debian stretch main' > /etc/apt/sources.list.d/stretch.list &&     echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid-until &&     apt-get update &&     apt-get install -y openjdk-8-jdk &&     rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH="/bin:/home/rahul.maurya@corp.merillife.com/.local/bin:/home/rahul.maurya@corp.merillife.com/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/snap/bin"
