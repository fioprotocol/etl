# This is VERY large, and takes a long time to build.

FROM ubuntu:18.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update; apt-get -y upgrade && apt-get -y install wget gpg apt-transport-https software-properties-common

RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | apt-key add - && \
    apt-add-repository 'deb https://apt.kitware.com/ubuntu/ bionic main' && \
    add-apt-repository ppa:ubuntu-toolchain-r/test

RUN apt update && \
    apt install -y git g++-8 cmake libssl-dev libgmp-dev zlib1g-dev && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8

RUN wget https://boostorg.jfrog.io/artifactory/main/release/1.67.0/source/boost_1_67_0.tar.gz && \
    tar -xvzf boost_1_67_0.tar.gz && \
    cd boost_1_67_0 && \
    ./bootstrap.sh && \
    ./b2 install 

RUN mkdir build && \
   cd build && \
   git clone https://github.com/EOSChronicleProject/eos-chronicle.git && \
   cd eos-chronicle && \
   git submodule update --init --recursive && \
   mkdir build && \
   cd build && \
   cmake .. && \
   make -j8 && \
   make install

COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh && mkdir -p /opt/data
WORKDIR /opt/data
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

