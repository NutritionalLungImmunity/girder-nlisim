FROM girder/girder:latest

RUN apt-get update; apt-get install -y libgl1
RUN pip install -U pip
RUN mkdir /nli
COPY setup.py setup.cfg README.md /nli/
RUN pip install --editable /nli

RUN mkdir /nli/girder_nlisim
ADD girder_nlisim /nli/girder_nlisim/
RUN girder build
ENTRYPOINT ["girder", "serve"]
