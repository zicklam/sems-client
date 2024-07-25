FROM python:3.12

WORKDIR /src
RUN useradd -u 1000 -m sems && chown sems:sems /src
USER sems

ADD requirements.txt /src/requirements.txt
RUN python -m pip install --user -r requirements.txt
ADD . /src/

CMD [ "/src/sems-client.py" ]
