"use client";

import { motion } from "framer-motion";
import { CheckCircle2, Circle, Loader2, type LucideIcon } from "lucide-react";
import { cn } from "~/lib/utils";

export type PipelineStep = {
	id: string;
	label: string;
	icon: LucideIcon;
	status: "pending" | "active" | "complete";
	durationMs?: number;
};

type Props = {
	steps: PipelineStep[];
};

const container = {
	show: { transition: { staggerChildren: 0.4 } },
};

const item = {
	hidden: { opacity: 0, scale: 0.8 },
	show: { opacity: 1, scale: 1 },
};

function StatusIcon({ status }: { status: PipelineStep["status"] }) {
	if (status === "complete")
		return <CheckCircle2 className="size-4 text-emerald-500" />;
	if (status === "active")
		return <Loader2 className="size-4 animate-spin text-blue-500" />;
	return <Circle className="size-4 text-muted-foreground/40" />;
}

export function PipelineDiagram({ steps }: Props) {
	return (
		<motion.div
			animate="show"
			className="flex flex-wrap items-center gap-2"
			initial="hidden"
			variants={container}
		>
			{steps.map((step, i) => {
				const Icon = step.icon;
				return (
					<div className="flex items-center gap-2" key={step.id}>
						<motion.div
							className={cn(
								"flex items-center gap-2 rounded-lg border px-3 py-2 text-xs transition-colors",
								step.status === "complete" &&
									"border-emerald-500/30 bg-emerald-50 dark:bg-emerald-950/20",
								step.status === "active" &&
									"border-blue-500/30 bg-blue-50 dark:bg-blue-950/20",
								step.status === "pending" && "border-border bg-muted/30",
							)}
							variants={item}
						>
							<Icon className="size-4 shrink-0" />
							<div>
								<div className="font-medium">{step.label}</div>
								{step.durationMs != null && (
									<div className="text-[10px] text-muted-foreground">
										{step.durationMs.toFixed(0)}ms
									</div>
								)}
							</div>
							<StatusIcon status={step.status} />
						</motion.div>
						{i < steps.length - 1 && (
							<motion.div className="text-muted-foreground/40" variants={item}>
								&rarr;
							</motion.div>
						)}
					</div>
				);
			})}
		</motion.div>
	);
}
