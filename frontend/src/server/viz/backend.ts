import "server-only";
import { env } from "~/env";

const BASE_URL = env.TV_API_BASE_URL ?? "http://127.0.0.1:8080";

async function tvFetch<T>(
	path: string,
	init?: RequestInit & { bodyJson?: unknown },
): Promise<T> {
	const { bodyJson, ...rest } = init ?? {};
	const res = await fetch(`${BASE_URL}${path}`, {
		...rest,
		headers: {
			"Content-Type": "application/json",
			...rest?.headers,
		},
		...(bodyJson !== undefined ? { body: JSON.stringify(bodyJson) } : {}),
	});
	if (!res.ok) {
		const text = await res.text().catch(() => "");
		throw new Error(`turbo-vector ${res.status}: ${text}`);
	}
	if (res.status === 204 || res.headers.get("content-length") === "0") {
		return undefined as T;
	}
	return res.json() as Promise<T>;
}

export async function healthCheck(): Promise<{ status: string }> {
	return tvFetch("/health");
}

export async function createCollection(
	name: string,
	dimension: number,
	metric: string,
): Promise<void> {
	await tvFetch("/v1/collections", {
		method: "POST",
		bodyJson: { name, dimension, metric },
	});
}

export type UpsertVector = {
	id: string;
	values: number[];
	metadata?: Record<string, unknown>;
};

export type UpsertResponse = {
	operation_id?: string;
};

export async function upsertVectors(
	collection: string,
	namespace: string | undefined,
	vectors: UpsertVector[],
): Promise<UpsertResponse> {
	return tvFetch(
		`/v1/collections/${encodeURIComponent(collection)}/vectors/upsert`,
		{
			method: "POST",
			bodyJson: { vectors, namespace },
		},
	);
}

export type QueryExplainRequest = {
	vector: number[];
	top_k: number;
	search_strategy?: string;
	namespace?: string;
	scenario_tag?: string;
};

export type ExplainStep = {
	id: string;
	title: string;
	service: string;
	detail: string;
	proof: Record<string, unknown>;
	duration_ms: number | null;
};

export type ExplainPayload = {
	path: string;
	temperature: string;
	scenario_tag: string | null;
	steps: ExplainStep[];
	optimizations: string[];
	fallback_reasons: string[];
	cache_summary: Record<string, unknown>;
	object_reads_summary: Record<string, unknown>;
	timings_ms: Record<string, unknown>;
};

export type QueryMatch = {
	id: string;
	score: number;
	values?: number[];
	metadata?: Record<string, unknown>;
};

export type QueryResponse = {
	matches: QueryMatch[];
	ann: Record<string, unknown>;
	distributed?: Record<string, unknown>;
};

export type QueryExplainResponse = {
	query_response: QueryResponse;
	explain: ExplainPayload;
};

export async function queryExplain(
	collection: string,
	body: QueryExplainRequest,
): Promise<QueryExplainResponse> {
	return tvFetch(
		`/v1/observability/collections/${encodeURIComponent(collection)}/query-explain`,
		{ method: "POST", bodyJson: body },
	);
}

export type CollectionStats = {
	dimension: number;
	vector_count: number;
	segments: number;
	generation: number;
};

export async function getCollectionStats(
	collection: string,
): Promise<CollectionStats> {
	return tvFetch(`/v1/collections/${encodeURIComponent(collection)}/stats`);
}

export type QueueStatus = {
	collection: string;
	queue_depth: number;
	pending_jobs: number;
	leased_jobs: number;
	oldest_job_age_ms: number | null;
	max_attempt: number;
};

export async function getQueueStatus(collection: string): Promise<QueueStatus> {
	return tvFetch(
		`/v1/observability/collections/${encodeURIComponent(collection)}/queue`,
	);
}
