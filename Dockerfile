FROM node:0.12

MAINTAINER Adam Magaluk <AMagaluk@apigee.com>

ADD     . /app
WORKDIR /app
RUN     npm install

CMD        []
ENTRYPOINT ["/app/worker.js"]
