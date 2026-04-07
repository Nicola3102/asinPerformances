FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --no-audit --no-fund || npm install --no-audit --no-fund

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

COPY . .
EXPOSE 5173
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["npm", "run", "dev"]
