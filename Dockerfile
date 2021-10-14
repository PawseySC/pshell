FROM alpine:3.14

RUN apk add bash
RUN apk add python3
RUN apk add py3-pip

RUN pip3 install boto3
RUN pip3 install pyflakes

# won't work without a properly conifgured gcc ...
#RUN pip3 install pyinstaller
#RUN git clone https://bitbucket.org/datapawsey/mfclient.git

copy pshell.py /
copy keystone.py /
copy remote.py /
copy mfclient.py /
copy s3client.py /

# only way I could get this file (which usually sits in ~ into the container)
copy .pshell_config /root/.pshell_config

ENTRYPOINT ["/bin/bash"]
#ENTRYPOINT ["python3", "pshell.py"]
