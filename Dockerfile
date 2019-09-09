FROM node:10.16.3
WORKDIR /app
EXPOSE 8080

COPY package.json /app/package.json
COPY server.js /app/server.js

RUN npm install

ARG NODE_ENV=development
CMD ["node", "server.js"]
