"use client";

import { motion } from "framer-motion";
import {
	ArrowRight,
	Cloud,
	Database,
	HardDrive,
	Layers,
	Server,
	Users,
} from "lucide-react";
import { Card, CardContent } from "~/components/ui/card";

const points = [
	{
		icon: Server,
		title: "Ephemeral Pods",
		text: "Kubernetes pods can restart, scale up or down at any time. No data is lost because pods are stateless compute.",
	},
	{
		icon: Cloud,
		title: "S3 is the Source of Truth",
		text: "All manifests, segments, WAL records, and queue state are persisted to object storage. CAS-based optimistic locking replaces distributed locks.",
	},
	{
		icon: HardDrive,
		title: "Local SSD as Cache",
		text: "First query to a pod fetches from S3 (cold). Subsequent queries hit local SSD + in-memory caches (hot). Sticky sessions keep queries on the same pod.",
	},
	{
		icon: Layers,
		title: "Separation of Concerns",
		text: "API (queries), Worker (indexing), and Broker (queue) run as independent processes. Scale each role separately based on workload.",
	},
	{
		icon: Users,
		title: "Namespace Isolation",
		text: "Multi-tenant by design. Each namespace is a fully isolated partition within a collection with independent indexes and zero cross-contamination.",
	},
];

const container = {
	show: { transition: { staggerChildren: 0.12 } },
};
const item = {
	hidden: { opacity: 0, y: 12 },
	show: { opacity: 1, y: 0 },
};

export function ArchitectureDiagram() {
	return (
		<div className="space-y-6">
			{/* Visual diagram */}
			<div className="rounded-lg border bg-muted/20 p-6">
				<div className="flex flex-col items-center gap-4 md:flex-row md:justify-center md:gap-6">
					<div className="flex flex-col items-center gap-2">
						<div className="rounded-lg border bg-card px-4 py-3 text-center text-xs">
							<Server className="mx-auto mb-1 size-5 text-blue-500" />
							<div className="font-medium">API Pod</div>
							<div className="text-muted-foreground">queries</div>
						</div>
						<div className="rounded-lg border bg-card px-4 py-3 text-center text-xs">
							<Server className="mx-auto mb-1 size-5 text-orange-500" />
							<div className="font-medium">Worker Pod</div>
							<div className="text-muted-foreground">indexing</div>
						</div>
						<div className="rounded-lg border bg-card px-4 py-3 text-center text-xs">
							<Server className="mx-auto mb-1 size-5 text-purple-500" />
							<div className="font-medium">Broker Pod</div>
							<div className="text-muted-foreground">queue</div>
						</div>
					</div>

					<div className="flex flex-col items-center gap-1 text-muted-foreground">
						<ArrowRight className="size-5 md:size-6" />
						<span className="text-[10px]">read / write</span>
					</div>

					<div className="flex items-center gap-3 rounded-xl border-2 border-primary/30 border-dashed bg-primary/5 px-6 py-5">
						<Database className="size-8 text-primary" />
						<div>
							<div className="font-medium text-sm">Object Storage (S3)</div>
							<div className="text-muted-foreground text-xs">
								manifests, segments, WAL, queue
							</div>
						</div>
					</div>
				</div>
			</div>

			{/* Key points */}
			<motion.div
				animate="show"
				className="grid gap-3 md:grid-cols-2 lg:grid-cols-3"
				initial="hidden"
				variants={container}
			>
				{points.map((point) => {
					const Icon = point.icon;
					return (
						<motion.div key={point.title} variants={item}>
							<Card className="h-full" size="sm">
								<CardContent className="flex gap-3">
									<Icon className="mt-0.5 size-4 shrink-0 text-primary" />
									<div>
										<div className="font-medium text-sm">{point.title}</div>
										<p className="mt-1 text-muted-foreground text-xs leading-relaxed">
											{point.text}
										</p>
									</div>
								</CardContent>
							</Card>
						</motion.div>
					);
				})}
			</motion.div>
		</div>
	);
}
