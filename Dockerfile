FROM amazonlinux

RUN amazon-linux-extras install python3
RUN yum -y install gcc-c++
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH=${PATH}:~/.cargo/bin
RUN pip3 install --upgrade pip
RUN pip3 install boto3 transformers s3urls

ADD scripts /opt/source_code/scripts
ADD src /opt/source_code/src
ADD Cargo.toml /opt/source_code/Cargo.toml
ADD tmp/.keep /opt/source_code/tmp/.keep
ADD entrypoint.sh /opt/source_code/entrypoint.sh

WORKDIR /opt/source_code

RUN cargo build
RUN chmod 777 entrypoint.sh

ENTRYPOINT ["/opt/source_code/entrypoint.sh"]