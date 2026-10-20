# syntax=docker/dockerfile:1
# ---- Stage 1: install production dependencies only ----
FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

# ---- Stage 2: small runtime image (no npm cache, no tests, no dev tools) ----
FROM node:22-alpine AS runtime
ENV NODE_ENV=production \
    PORT=3002 \
    SERVICE_NAME=inventory-service
WORKDIR /app
COPY --from=deps --chown=node:node /app/node_modules ./node_modules
COPY --chown=node:node package.json ./
COPY --chown=node:node src ./src
# Run as the unprivileged "node" user that ships with the official image
USER node
EXPOSE 3002
# Liveness-style check used by Docker/Compose (Kubernetes uses its own probes)
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
  CMD node -e "fetch('http://localhost:'+process.env.PORT+'/health/live').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "src/index.js"]
