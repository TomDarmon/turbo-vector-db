FROM oven/bun:1 AS deps
WORKDIR /app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
ENV SKIP_ENV_VALIDATION=1
ENV BETTER_AUTH_SECRET=build-secret
ENV BETTER_AUTH_URL=http://localhost:3000
ENV DATABASE_URL=file:/tmp/turbo-vector-viz-build.db
RUN bun run build

FROM oven/bun:1 AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000

COPY --from=deps /app/package.json /app/bun.lock ./
COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/.next ./.next
COPY --from=deps /app/public ./public
COPY --from=deps /app/src ./src
COPY --from=deps /app/next.config.js ./next.config.js
COPY --from=deps /app/postcss.config.js ./postcss.config.js
COPY --from=deps /app/tsconfig.json ./tsconfig.json
COPY --from=deps /app/components.json ./components.json
COPY --from=deps /app/drizzle.config.ts ./drizzle.config.ts

RUN mkdir -p /app/data

EXPOSE 3000
CMD ["bun", "run", "start", "--", "-p", "3000", "-H", "0.0.0.0"]
