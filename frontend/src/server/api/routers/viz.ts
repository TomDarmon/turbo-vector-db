import { z } from "zod";
import { createTRPCRouter, publicProcedure } from "~/server/api/trpc";
import {
	createCollection,
	getCollectionStats,
	getQueueStatus,
	healthCheck,
	type QueryExplainResponse,
	queryExplain,
	upsertVectors,
} from "~/server/viz/backend";
import { generateVectors } from "~/server/viz/vectors";

function sleep(ms: number) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

export const vizRouter = createTRPCRouter({
	healthCheck: publicProcedure.query(async () => {
		try {
			const res = await healthCheck();
			return { ok: true, status: res.status };
		} catch {
			return { ok: false, status: "unreachable" };
		}
	}),

	collectionStats: publicProcedure
		.input(z.object({ collection: z.string().min(1) }))
		.query(async ({ input }) => {
			try {
				return await getCollectionStats(input.collection);
			} catch {
				return null;
			}
		}),

	queueStatus: publicProcedure
		.input(z.object({ collection: z.string().min(1) }))
		.query(async ({ input }) => {
			try {
				return await getQueueStatus(input.collection);
			} catch {
				return null;
			}
		}),

	runExperiment1: publicProcedure
		.input(
			z.object({
				collectionName: z.string().default("tv_lab"),
				dimension: z.number().int().default(128),
				vectorCount: z.number().int().default(100),
			}),
		)
		.mutation(async ({ input }) => {
			const timings: { step: string; ms: number }[] = [];
			const suffix = Math.random().toString(36).slice(2, 8);
			const collectionName = `${input.collectionName}_${suffix}`;
			const { dimension, vectorCount } = input;

			// Step 1: Create collection
			const t0 = Date.now();
			await createCollection(collectionName, dimension, "cosine");
			timings.push({ step: "create_collection", ms: Date.now() - t0 });

			// Step 2: Generate vectors for 2 namespaces
			const tenantACount = Math.round(vectorCount * 0.6);
			const tenantBCount = vectorCount - tenantACount;

			const t1 = Date.now();
			const tenantAVectors = generateVectors(
				tenantACount,
				dimension,
				"tenant-a",
				42,
			);
			const tenantBVectors = generateVectors(
				tenantBCount,
				dimension,
				"tenant-b",
				99,
			);
			timings.push({ step: "generate_vectors", ms: Date.now() - t1 });

			// Step 3: Upsert to both namespaces
			const t2 = Date.now();
			const upsertA = await upsertVectors(
				collectionName,
				"tenant-a",
				tenantAVectors,
			);
			const upsertB = await upsertVectors(
				collectionName,
				"tenant-b",
				tenantBVectors,
			);
			timings.push({ step: "upsert", ms: Date.now() - t2 });

			// Step 4: Poll queue until drained (max 30s)
			const t3 = Date.now();
			let queueDrained = false;
			for (let i = 0; i < 30; i++) {
				await sleep(1000);
				try {
					const q = await getQueueStatus(collectionName);
					if (q.queue_depth === 0) {
						queueDrained = true;
						break;
					}
				} catch {
					// queue endpoint may not be ready yet
				}
			}
			timings.push({ step: "queue_drain", ms: Date.now() - t3 });

			// Step 5: Get final stats
			let stats = null;
			try {
				stats = await getCollectionStats(collectionName);
			} catch {
				// stats may not be ready
			}

			const sampleVector = tenantAVectors[0]?.values ?? [];

			return {
				collectionName,
				dimension,
				tenantACounts: tenantACount,
				tenantBCounts: tenantBCount,
				operationIds: [upsertA.operation_id, upsertB.operation_id],
				queueDrained,
				timings,
				totalMs: timings.reduce((sum, t) => sum + t.ms, 0),
				stats,
				sampleVector,
			};
		}),

	runExperiment2: publicProcedure
		.input(
			z.object({
				collection: z.string(),
				vector: z.array(z.number()),
				topK: z.number().int().default(5),
			}),
		)
		.mutation(async ({ input }) => {
			const strategies = ["exact", "ann", "auto"] as const;
			const results: Record<string, QueryExplainResponse> = {};

			for (const strategy of strategies) {
				results[strategy] = await queryExplain(input.collection, {
					vector: input.vector,
					top_k: input.topK,
					search_strategy: strategy,
					namespace: "tenant-a",
					scenario_tag: `exp2-${strategy}`,
				});
			}

			return results as Record<"exact" | "ann" | "auto", QueryExplainResponse>;
		}),

	runExperiment3: publicProcedure
		.input(
			z.object({
				dimension: z.number().int().default(128),
				topK: z.number().int().default(5),
			}),
		)
		.mutation(async ({ input }) => {
			// Create a fresh collection so nothing is cached
			const suffix = Math.random().toString(36).slice(2, 8);
			const collection = `tv_hot_cold_${suffix}`;
			await createCollection(collection, input.dimension, "cosine");

			// Upsert vectors
			const vectors = generateVectors(100, input.dimension, "hc", 77);
			await upsertVectors(collection, "default", vectors);

			// Wait for indexing
			for (let i = 0; i < 30; i++) {
				await sleep(1000);
				try {
					const q = await getQueueStatus(collection);
					if (q.queue_depth === 0) break;
				} catch {
					// queue may not be ready
				}
			}

			const sampleVector = vectors[0]?.values ?? [];

			// Cold query — first time, data must be fetched from storage
			const coldStart = Date.now();
			const cold = await queryExplain(collection, {
				vector: sampleVector,
				top_k: input.topK,
				namespace: "default",
				scenario_tag: "exp3-cold",
			});
			const coldWallMs = Date.now() - coldStart;

			// Hot query — same query, data now cached
			const hotStart = Date.now();
			const hot = await queryExplain(collection, {
				vector: sampleVector,
				top_k: input.topK,
				namespace: "default",
				scenario_tag: "exp3-hot",
			});
			const hotWallMs = Date.now() - hotStart;

			// Use wall-clock time as the primary metric (more honest)
			const coldMs = Math.max(
				coldWallMs,
				(cold.explain.timings_ms.query_total as number) ?? 0,
			);
			const hotMs = Math.max(
				hotWallMs,
				(hot.explain.timings_ms.query_total as number) ?? 0,
			);
			const speedup = coldMs > 0 ? coldMs / Math.max(hotMs, 0.01) : 1;

			return { cold, hot, speedup, coldWallMs, hotWallMs };
		}),
});
