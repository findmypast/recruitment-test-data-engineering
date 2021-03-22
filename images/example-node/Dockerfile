FROM node:15.12

WORKDIR /app

COPY package.json ./
RUN npm install

COPY example.js ./
ENTRYPOINT ./example.js
