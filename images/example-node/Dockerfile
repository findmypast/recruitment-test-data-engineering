FROM node:14.4

COPY package.json ./
RUN npm install

COPY example.js ./
ENTRYPOINT ./example.js
