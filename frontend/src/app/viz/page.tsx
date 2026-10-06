"use client";

import {
	Boxes,
	Cpu,
	Database,
	FileText,
	GitBranch,
	HardDrive,
	Search,
	Thermometer,
	Upload,
} from "lucide-react";
import { useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Progress } from "~/components/ui/progress";
import { Separator } from "~/components/ui/separator";
import type { ExperimentStatus } from "~/hooks/use-experiment";
import { api } from "~/trpc/react";
import { ArchitectureDiagram } from "./_components/architecture-diagram";
import { ExperimentShell } from "./_components/experiment-shell";
import { NamespaceExplainer } from "./_components/namespace-explainer";
import {
	PipelineDiagram,
	type PipelineStep,
} from "./_components/pipeline-diagram";
import { StatCard } from "./_components/stat-card";
import { StrategyComparison } from "./_components/strategy-comparison";
import { TemperatureBadge } from "./_components/temperature-badge";
import { TimelineSteps } from "./_components/timeline-steps";

const COLLECTION = "tv_lab";

function mutationStatus(m: {
	isPending: boolean;
	isSuccess: boolean;
	isError: boolean;
}): ExperimentStatus {
	if (m.isPending) return "running";
	if (m.isSuccess) return "complete";
	if (m.isError) return "error";
	return "idle";
}

export default function VizPage() {
	const [exp1Data, setExp1Data] = useState<{
		sampleVector: number[];
		collectionName: string;
	} | null>(null);

	// Live polling
	const health = api.viz.healthCheck.useQuery(undefined, {
		refetchInterval: 5000,
	});
	const stats = api.viz.collectionStats.useQuery(
		{ collection: exp1Data?.collectionName ?? "" },
		{ refetchInterval: 5000, enabled: !!exp1Data },
	);
	const queue = api.viz.queueStatus.useQuery(
		{ collection: exp1Data?.collectionName ?? "" },
		{ refetchInterval: 5000, enabled: !!exp1Data },
	);

	// Experiment mutations
	const exp1 = api.viz.runExperiment1.useMutation();
	const exp2 = api.viz.runExperiment2.useMutation();
	const exp3 = api.viz.runExperiment3.useMutation();

	return (
		<div className="min-h-screen bg-[linear-gradient(180deg,#f8fafc_0%,#eef2ff_100%)] dark:bg-[linear-gradient(180deg,#09090b_0%,#0f172a_100%)]">
			{/* Header */}
			<header className="sticky top-0 z-50 border-b bg-background/80 backdrop-blur-sm">
				<div className="mx-auto flex max-w-4xl items-center justify-between px-4 py-3">
					<div className="flex items-center gap-3">
						<Database className="size-5 text-primary" />
						<h1 className="font-semibold text-lg">turbo-vector Lab</h1>
					</div>
					<div className="flex items-center gap-3">
						{stats.data && (
							<Badge className="text-[10px]" variant="outline">
								{stats.data.vector_count} vectors
							</Badge>
						)}
						{queue.data && queue.data.queue_depth > 0 && (
							<Badge className="text-[10px]" variant="secondary">
								Queue: {queue.data.queue_depth}
							</Badge>
						)}
						<Badge
							className="text-[10px]"
							variant={health.data?.ok ? "default" : "destructive"}
						>
							{health.data?.ok ? "Connected" : "Disconnected"}
						</Badge>
					</div>
				</div>
			</header>

			{/* Experiments */}
			<main className="mx-auto max-w-4xl space-y-6 px-4 py-8">
				<div className="space-y-1">
					<h2 className="font-bold text-2xl">Interactive Experiments</h2>
					<p className="text-muted-foreground text-sm">
						Run real experiments against the vector store and see exactly what
						happens under the hood.
					</p>
				</div>

				<Separator />

				{/* Experiment 1 */}
				<ExperimentShell
					description="Create a collection, upsert 100 vectors across 2 namespaces, and watch the indexing pipeline process them."
					number={1}
					onReset={() => exp1.reset()}
					onRun={async () => {
						const result = await exp1.mutateAsync({
							collectionName: COLLECTION,
							dimension: 128,
							vectorCount: 100,
						});
						setExp1Data({
							sampleVector: result.sampleVector,
							collectionName: result.collectionName,
						});
					}}
					status={mutationStatus(exp1)}
					title="Your First Collection"
				>
					{exp1.isIdle && (
						<div className="py-8 text-center text-muted-foreground text-sm">
							Click <strong>Run Experiment</strong> to create a collection with
							100 vectors split across two tenant namespaces.
						</div>
					)}

					{exp1.isPending && (
						<div className="space-y-4">
							<div className="text-muted-foreground text-sm">
								Creating collection and indexing vectors...
							</div>
							<Progress className="h-1" value={null} />
							{queue.data && (
								<div className="text-muted-foreground text-xs">
									Queue depth: {queue.data.queue_depth} &middot; Pending:{" "}
									{queue.data.pending_jobs}
								</div>
							)}
						</div>
					)}

					{exp1.isSuccess && exp1.data && (
						<Experiment1Results result={exp1.data} />
					)}
				</ExperimentShell>

				{/* Experiment 2 */}
				<ExperimentShell
					description="Compare exact, ANN, and auto search strategies on the same query to see the performance and accuracy tradeoffs."
					locked={!exp1Data}
					number={2}
					onReset={() => exp2.reset()}
					onRun={() => {
						if (!exp1Data) return;
						exp2.mutate({
							collection: exp1Data.collectionName,
							vector: exp1Data.sampleVector,
							topK: 5,
						});
					}}
					status={mutationStatus(exp2)}
					title="Search Strategies"
				>
					{exp2.isIdle && (
						<div className="py-8 text-center text-muted-foreground text-sm">
							Click <strong>Run Experiment</strong> to run the same query with
							three different strategies and compare results.
						</div>
					)}

					{exp2.isPending && (
						<div className="space-y-4">
							<div className="text-muted-foreground text-sm">
								Running queries with exact, ANN, and auto strategies...
							</div>
							<Progress className="h-1" value={null} />
						</div>
					)}

					{exp2.isSuccess && exp2.data && (
						<Experiment2Results results={exp2.data} />
					)}
				</ExperimentShell>

				{/* Experiment 3 */}
				<ExperimentShell
					description="Creates a fresh collection and runs the same query twice: first cold (from S3), then hot (from cache)."
					locked={!exp2.isSuccess}
					number={3}
					onReset={() => exp3.reset()}
					onRun={() => {
						exp3.mutate({ dimension: 128, topK: 5 });
					}}
					status={mutationStatus(exp3)}
					title="Hot vs Cold"
				>
					{exp3.isIdle && (
						<div className="py-8 text-center text-muted-foreground text-sm">
							Click <strong>Run Experiment</strong> to observe the caching
							effect on query latency.
						</div>
					)}

					{exp3.isPending && (
						<div className="space-y-4">
							<div className="text-muted-foreground text-sm">
								Running cold query, then hot query...
							</div>
							<Progress className="h-1" value={null} />
						</div>
					)}

					{exp3.isSuccess && exp3.data && (
						<Experiment3Results result={exp3.data} />
					)}
				</ExperimentShell>

				{/* Experiment 4 */}
				<ExperimentShell
					description="Understand how turbo-vector uses ephemeral compute with S3 as the persistent layer."
					number={4}
					onRun={() => {}}
					status="complete"
					title="The Architecture"
				>
					<ArchitectureDiagram />
				</ExperimentShell>
			</main>
		</div>
	);
}

// --- Experiment result sub-components ---

type Exp1Result = {
	collectionName: string;
	dimension: number;
	tenantACounts: number;
	tenantBCounts: number;
	queueDrained: boolean;
	timings: { step: string; ms: number }[];
	totalMs: number;
	stats: {
		dimension: number;
		vector_count: number;
		segments: number;
		generation: number;
	} | null;
	sampleVector: number[];
};

function Experiment1Results({ result }: { result: Exp1Result }) {
	const pipelineSteps: PipelineStep[] = [
		{
			id: "create",
			label: "Create Collection",
			icon: Database,
			status: "complete",
			durationMs: result.timings.find((t) => t.step === "create_collection")
				?.ms,
		},
		{
			id: "generate",
			label: "Generate Vectors",
			icon: FileText,
			status: "complete",
			durationMs: result.timings.find((t) => t.step === "generate_vectors")?.ms,
		},
		{
			id: "upsert",
			label: "Upsert to S3",
			icon: Upload,
			status: "complete",
			durationMs: result.timings.find((t) => t.step === "upsert")?.ms,
		},
		{
			id: "queue",
			label: "Queue / Worker",
			icon: GitBranch,
			status: result.queueDrained ? "complete" : "active",
			durationMs: result.timings.find((t) => t.step === "queue_drain")?.ms,
		},
		{
			id: "index",
			label: "Index Built",
			icon: Cpu,
			status: result.queueDrained ? "complete" : "pending",
		},
	];

	return (
		<div className="space-y-6">
			<PipelineDiagram steps={pipelineSteps} />

			<div className="grid grid-cols-2 gap-3 md:grid-cols-4">
				<StatCard
					label="Total Vectors"
					value={result.tenantACounts + result.tenantBCounts}
				/>
				<StatCard label="Dimension" value={result.dimension} />
				<StatCard label="Total Time" unit="ms" value={result.totalMs} />
				<StatCard label="Namespaces" value={2} />
			</div>

			<NamespaceExplainer
				collectionName={result.collectionName}
				namespaces={[
					{
						name: "tenant-a",
						vectorCount: result.tenantACounts,
					},
					{
						name: "tenant-b",
						vectorCount: result.tenantBCounts,
					},
				]}
			/>

			<Alert>
				<HardDrive className="size-4" />
				<AlertTitle>How indexing works</AlertTitle>
				<AlertDescription className="text-xs leading-relaxed">
					When you upsert vectors, the API writes a WAL (Write-Ahead Log) record
					to S3 and enqueues a job via the broker. The worker polls the queue,
					processes the batch, builds or updates the ANN index (K-means
					centroids &rarr; binary-quantized buckets &rarr; tree hierarchy), and
					publishes a new manifest generation. The whole pipeline is
					asynchronous &mdash; the upsert returns immediately while indexing
					happens in the background.
				</AlertDescription>
			</Alert>
		</div>
	);
}

function Experiment2Results({
	results,
}: {
	results: Record<
		"exact" | "ann" | "auto",
		import("~/server/viz/backend").QueryExplainResponse
	>;
}) {
	return (
		<div className="space-y-6">
			<StrategyComparison results={results} />

			<div className="grid gap-3 md:grid-cols-3">
				<Alert>
					<Search className="size-4" />
					<AlertTitle className="text-xs">Exact</AlertTitle>
					<AlertDescription className="text-[11px] leading-relaxed">
						Loads every vector and scores all of them. Accurate but O(n). Best
						for small collections or when you need perfect recall.
					</AlertDescription>
				</Alert>
				<Alert>
					<Cpu className="size-4" />
					<AlertTitle className="text-xs">ANN</AlertTitle>
					<AlertDescription className="text-[11px] leading-relaxed">
						Binary quantization reduces each float32 dimension to 1 sign bit.
						Hamming distance (XOR + popcount) finds candidate centroids, then
						reranks top candidates at full precision. ~32x memory savings.
					</AlertDescription>
				</Alert>
				<Alert>
					<Boxes className="size-4" />
					<AlertTitle className="text-xs">Auto</AlertTitle>
					<AlertDescription className="text-[11px] leading-relaxed">
						The system checks corpus size and index availability. Uses ANN when
						the index is built (&gt;2000 vectors), falls back to exact
						otherwise.
					</AlertDescription>
				</Alert>
			</div>
		</div>
	);
}

function extractObjectReads(summary: Record<string, unknown>): number {
	const s = summary as Record<string, Record<string, number>>;
	return (
		(s.ann_bucket?.reads ?? 0) +
		(s.ann_meta?.reads ?? 0) +
		(s.rerank_segment?.reads ?? 0)
	);
}

function Experiment3Results({
	result,
}: {
	result: {
		cold: import("~/server/viz/backend").QueryExplainResponse;
		hot: import("~/server/viz/backend").QueryExplainResponse;
		speedup: number;
		coldWallMs: number;
		hotWallMs: number;
	};
}) {
	const coldMs = result.coldWallMs;
	const hotMs = result.hotWallMs;
	const maxMs = Math.max(coldMs, hotMs, 1);

	const coldReads = extractObjectReads(
		result.cold.explain.object_reads_summary,
	);
	const hotReads = extractObjectReads(result.hot.explain.object_reads_summary);

	return (
		<div className="space-y-6">
			{/* Speedup highlight */}
			<div className="rounded-lg border bg-primary/5 px-4 py-3 text-center">
				<span className="font-bold text-3xl tabular-nums">
					{result.speedup.toFixed(1)}x
				</span>
				<span className="ml-2 text-muted-foreground text-sm">
					faster on hot path
				</span>
			</div>

			{/* Side by side */}
			<div className="grid gap-4 md:grid-cols-2">
				{/* Cold */}
				<div className="space-y-3">
					<div className="flex items-center gap-2">
						<TemperatureBadge temperature="cold" />
						<span className="text-muted-foreground text-sm">First query</span>
					</div>
					<div className="space-y-2">
						<div className="flex items-center justify-between text-xs">
							<span>Latency</span>
							<span className="font-medium tabular-nums">
								{coldMs.toFixed(1)}ms
							</span>
						</div>
						<div className="h-2 overflow-hidden rounded-full bg-muted">
							<div
								className="h-full rounded-full bg-blue-500 transition-all"
								style={{ width: `${(coldMs / maxMs) * 100}%` }}
							/>
						</div>
						<div className="flex items-center justify-between text-xs">
							<span>S3 Object Reads</span>
							<span className="font-medium">{coldReads}</span>
						</div>
					</div>
					<TimelineSteps steps={result.cold.explain.steps} />
				</div>

				{/* Hot */}
				<div className="space-y-3">
					<div className="flex items-center gap-2">
						<TemperatureBadge temperature="warm" />
						<span className="text-muted-foreground text-sm">Second query</span>
					</div>
					<div className="space-y-2">
						<div className="flex items-center justify-between text-xs">
							<span>Latency</span>
							<span className="font-medium tabular-nums">
								{hotMs.toFixed(1)}ms
							</span>
						</div>
						<div className="h-2 overflow-hidden rounded-full bg-muted">
							<div
								className="h-full rounded-full bg-orange-500 transition-all"
								style={{ width: `${(hotMs / maxMs) * 100}%` }}
							/>
						</div>
						<div className="flex items-center justify-between text-xs">
							<span>S3 Object Reads</span>
							<span className="font-medium">{hotReads}</span>
						</div>
					</div>
					<TimelineSteps steps={result.hot.explain.steps} />
				</div>
			</div>

			<Alert>
				<Thermometer className="size-4" />
				<AlertTitle>Multi-tier caching</AlertTitle>
				<AlertDescription className="text-xs leading-relaxed">
					turbo-vector has three cache layers: in-memory LRU (namespace vectors
					+ ANN bucket data), local SSD rerank cache, and S3 as the source of
					truth. The first query to a pod is always cold &mdash; data is fetched
					from S3. Subsequent queries hit cached data. On Kubernetes, sticky
					sessions route queries to the same pod for consistent hot-path
					performance. Pods can restart anytime &mdash; S3 is always there.
				</AlertDescription>
			</Alert>
		</div>
	);
}
