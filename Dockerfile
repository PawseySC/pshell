FROM alpine:3.8

RUN apk add bash
RUN apk add python3
RUN apk add py3-pip

#RUN git clone https://bitbucket.org/datapawsey/mfclient.git
RUN pip3 install boto3

copy mfclient.py /
copy pshell.py /
copy keystone.py /
copy s3client.py /

# only way I could get this file (which usually sits in ~ into the container)
copy .mf_config /root/.mf_config

ENTRYPOINT ["/bin/bash"]
#ENTRYPOINT ["python3", "pshell.py"]
