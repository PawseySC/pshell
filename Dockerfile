FROM python:3

# won't work without a properly conifgured gcc ...
#RUN pip3 install pyinstaller
#RUN git clone https://bitbucket.org/datapawsey/mfclient.git
# TODO - add to requirements?
# lint checking
#RUN pip3 install pyflakes
# windows building
#RUN pip3 install pynsist

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pshell.py /
COPY parser.py /
COPY mfclient.py /
COPY s3client.py /

# only way I could get this file (which usually sits in ~ into the container)
COPY .pshell_config /root/.pshell_config

CMD ["python", "./pshell.py", "-v", "1"]
