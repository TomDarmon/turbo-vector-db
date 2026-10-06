"use client";

import { motion } from "framer-motion";
import { ChevronDown } from "lucide-react";
import { useState } from "react";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";
import {
	Collapsible,
	CollapsibleContent,
	CollapsibleTrigger,
} from "~/components/ui/collapsible";
import type { QueryExplainResponse } from "~/server/viz/backend";
import { TimelineSteps } from "./timeline-steps";

type Props = {
	results: Record<"exact" | "ann" | "auto", QueryExplainResponse>;
};

const strategyMeta: Record<
	"exact" | "ann" | "auto",
	{ label: string; variant: "default" | "secondary" | "outline" }
> = {
	exact: { label: "Exact", variant: "secondary" },
	ann: { label: "ANN", variant: "default" },
	auto: { label: "Auto", variant: "outline" },
};

const container = {
	show: { transition: { staggerChildren: 0.2 } },
};

const item = {
	hidden: { opacity: 0, y: 15 },
	show: { opacity: 1, y: 0 },
};

export function StrategyComparison({ results }: Props) {
	return (
		<motion.div
			animate="show"
			className="grid gap-4 md:grid-cols-3"
			initial="hidden"
			variants={container}
		>
			{(["exact", "ann", "auto"] as const).map((strategy) => {
				const res = results[strategy];
				const meta = strategyMeta[strategy];
				const totalMs = (res.explain.timings_ms.query_total as number) ?? 0;
				const objectReads =
					((
						res.explain.object_reads_summary as Record<
							string,
							Record<string, number>
						>
					).ann_bucket?.reads ?? 0) +
					((
						res.explain.object_reads_summary as Record<
							string,
							Record<string, number>
						>
					).ann_meta?.reads ?? 0) +
					((
						res.explain.object_reads_summary as Record<
							string,
							Record<string, number>
						>
					).rerank_segment?.reads ?? 0);

				return (
					<StrategyColumn
						key={strategy}
						label={meta.label}
						matchCount={res.query_response.matches.length}
						objectReads={objectReads}
						path={res.explain.path}
						steps={res.explain.steps}
						totalMs={totalMs}
						variant={meta.variant}
					/>
				);
			})}
		</motion.div>
	);
}

function StrategyColumn({
	label,
	variant,
	path,
	totalMs,
	matchCount,
	objectReads,
	steps,
}: {
	label: string;
	variant: "default" | "secondary" | "outline";
	path: string;
	totalMs: number;
	matchCount: number;
	objectReads: number;
	steps: QueryExplainResponse["explain"]["steps"];
}) {
	const [open, setOpen] = useState(false);

	return (
		<motion.div variants={item}>
			<Card className="h-full">
				<CardHeader>
					<div className="flex items-center gap-2">
						<Badge variant={variant}>{label}</Badge>
						<Badge className="text-[10px]" variant="outline">
							{path}
						</Badge>
					</div>
					<CardTitle className="text-lg tabular-nums">
						{totalMs.toFixed(1)}ms
					</CardTitle>
				</CardHeader>
				<CardContent className="space-y-3">
					<div className="grid grid-cols-2 gap-2 text-xs">
						<div>
							<span className="text-muted-foreground">Matches</span>
							<div className="font-medium">{matchCount}</div>
						</div>
						<div>
							<span className="text-muted-foreground">Object reads</span>
							<div className="font-medium">{objectReads}</div>
						</div>
					</div>
					<Collapsible onOpenChange={setOpen} open={open}>
						<CollapsibleTrigger className="flex w-full items-center gap-1 text-muted-foreground text-xs hover:text-foreground">
							<ChevronDown
								className={`size-3 transition-transform ${open ? "rotate-180" : ""}`}
							/>
							Execution steps
						</CollapsibleTrigger>
						<CollapsibleContent>
							<div className="mt-2">
								<TimelineSteps steps={steps} />
							</div>
						</CollapsibleContent>
					</Collapsible>
				</CardContent>
			</Card>
		</motion.div>
	);
}
