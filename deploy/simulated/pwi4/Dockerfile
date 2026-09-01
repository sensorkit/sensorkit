# Adapted from https://github.com/sdss/lvmpwi/blob/main/container/Dockerfile

FROM ubuntu:24.04

RUN apt-get update && \
    DEBIAN_FRONTEND="noninteractive" apt-get install -y --no-install-recommends software-properties-common && \
    apt install -y gnupg ca-certificates && \
    gpg --homedir /tmp --no-default-keyring --keyring /usr/share/keyrings/mono-official-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF && \
    echo "deb [signed-by=/usr/share/keyrings/mono-official-archive-keyring.gpg] https://download.mono-project.com/repo/ubuntu stable-focal main" | tee /etc/apt/sources.list.d/mono-official-stable.list

RUN apt-get update \
    && DEBIAN_FRONTEND="noninteractive" apt-get install -y --no-install-recommends \
        wget \
        python3 \
        xfce4 xfce4-goodies \
        x11-xserver-utils \
        xfonts-base xfonts-75dpi xfonts-100dpi \
        libgtk2.0 \
        mono-devel \
        mono-basic-dbg \
        dbus-x11

ENV PWI_VERSION=4.1.6final
ENV PWI_PATH=/opt/pwi-$PWI_VERSION
ARG PWI_SHA256=1f6cfdc55a62a93c4488a5724cdf2cda70ba83f81d5cd2ad507ed782c50d7ae8

RUN mkdir -p $PWI_PATH && \
    wget https://planewave.com/files/software/PWI4/pwi-$PWI_VERSION.tar.gz && \
    echo "$PWI_SHA256  /pwi-$PWI_VERSION.tar.gz" | sha256sum -c - && \
    (cd ${PWI_PATH%/*} && tar xzf /pwi-$PWI_VERSION.tar.gz) && \
    rm /pwi-$PWI_VERSION.tar.gz

RUN apt-get update \
    && DEBIAN_FRONTEND="noninteractive" apt-get install -y --no-install-recommends \
        wmctrl \
        xdotool \
        novnc \
        xrdp \
        tigervnc-standalone-server

EXPOSE 6080/tcp
EXPOSE 8220/tcp

WORKDIR /root

RUN mkdir -p "PlaneWave Instruments/PWI4"

COPY run-pwi.sh .
COPY novnc_server .
COPY ["Settings", "PlaneWave Instruments/PWI4/Settings"]

RUN chmod +x /root/novnc_server
RUN chmod +x /root/run-pwi.sh
CMD ["/root/run-pwi.sh"]
