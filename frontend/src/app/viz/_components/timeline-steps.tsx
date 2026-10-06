"use client";

import { motion } from "framer-motion";
import { Badge } from "~/components/ui/badge";
import type { ExplainStep } from "~/server/viz/backend";

const serviceColors: Record<
	string,
	"default" | "secondary" | "outline" | "destructive"
> = {
	api: "default",
	storage: "secondary",
	broker: "outline",
	worker: "destructive",
};

const container = {
	show: { transition: { staggerChildren: 0.15 } },
};

const item = {
	hidden: { opacity: 0, x: -10 },
	show: { opacity: 1, x: 0 },
};

type Props = {
	steps: ExplainStep[];
};

export function TimelineSteps({ steps }: Props) {
	return (
		<motion.div
			animate="show"
			className="relative space-y-3 pl-6"
			initial="hidden"
			variants={container}
		>
			<div className="absolute top-1 bottom-1 left-2 w-px bg-border" />
			{steps.map((step) => (
				<motion.div className="relative" key={step.id} variants={item}>
					<div className="absolute top-1.5 -left-[18px] size-2 rounded-full bg-foreground/30" />
					<div className="flex flex-wrap items-center gap-2">
						<span className="font-medium text-sm">{step.title}</span>
						<Badge variant={serviceColors[step.service] ?? "outline"}>
							{step.service}
						</Badge>
						{step.duration_ms != null && (
							<span className="text-muted-foreground text-xs">
								{step.duration_ms.toFixed(1)}ms
							</span>
						)}
					</div>
					<p className="mt-0.5 text-muted-foreground text-xs">{step.detail}</p>
				</motion.div>
			))}
		</motion.div>
	);
}
