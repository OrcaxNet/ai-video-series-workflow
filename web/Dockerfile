# syntax=docker/dockerfile:1.7
FROM node:22.14.0-alpine3.21 AS dependencies
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM dependencies AS build
ARG VITE_STUDIO_MODE=local-experience
ARG VITE_API_BASE=/api/v1
ENV VITE_STUDIO_MODE=${VITE_STUDIO_MODE} \
    VITE_API_BASE=${VITE_API_BASE}
COPY . .
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27.4-alpine3.21 AS runtime
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build --chown=101:101 /app/dist /usr/share/nginx/html
USER 101:101
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:8080/health/studio || exit 1
