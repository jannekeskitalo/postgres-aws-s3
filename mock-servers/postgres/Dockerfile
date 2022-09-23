FROM postgres:14

RUN mkdir /tmp/extension
WORKDIR /tmp/extension

RUN apt-get update \ 
    && apt-get -y install python3 python3-pip postgresql-plpython3-14 make
RUN pip3 install boto3
COPY extension/* ./
RUN make install
