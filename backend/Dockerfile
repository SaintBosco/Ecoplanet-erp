FROM node:20-alpine

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY . .

# Prisma client generation once schema.prisma is the live data path (Phase 1)
# RUN npx prisma generate

EXPOSE 3001
CMD ["node", "server.js"]
