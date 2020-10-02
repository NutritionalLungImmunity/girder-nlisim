FROM girder/girder:latest

RUN apt-get update; apt-get install -y libgl1
RUN mkdir /nli
COPY setup.py setup.cfg README.rst /nli/
RUN pip install --editable /nli

RUN mkdir /nli/girder_nlisim
ADD girder_nlisim /nli/girder_nlisim/
RUN girder build
ENTRYPOINT ["girder", "serve"]
