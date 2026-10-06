"use client";

import { motion } from "framer-motion";
import {
	CheckCircle2,
	Loader2,
	Lock,
	Play,
	RotateCcw,
	XCircle,
} from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "~/components/ui/card";
import type { ExperimentStatus } from "~/hooks/use-experiment";

type Props = {
	number: number;
	title: string;
	description: string;
	locked?: boolean;
	status: ExperimentStatus;
	onRun: () => void;
	onReset?: () => void;
	children: React.ReactNode;
};

export function ExperimentShell({
	number,
	title,
	description,
	locked,
	status,
	onRun,
	onReset,
	children,
}: Props) {
	return (
		<motion.div
			animate={{ opacity: 1, y: 0 }}
			initial={{ opacity: 0, y: 20 }}
			transition={{ delay: number * 0.1 }}
		>
			<Card className={locked ? "opacity-50" : ""}>
				<CardHeader>
					<div className="flex items-center gap-3">
						<Badge variant={status === "complete" ? "default" : "outline"}>
							{number}
						</Badge>
						<div className="flex-1">
							<CardTitle>{title}</CardTitle>
							<CardDescription>{description}</CardDescription>
						</div>
						{locked ? (
							<div className="flex items-center gap-2 text-muted-foreground text-xs">
								<Lock className="size-4" />
								Complete previous experiment
							</div>
						) : (
							<div className="flex items-center gap-2">
								{status === "complete" && onReset && (
									<Button onClick={onReset} size="sm" variant="ghost">
										<RotateCcw className="mr-1 size-3" />
										Reset
									</Button>
								)}
								<Button
									disabled={status === "running"}
									onClick={onRun}
									size="sm"
								>
									{status === "running" ? (
										<>
											<Loader2 className="mr-1 size-3 animate-spin" />
											Running...
										</>
									) : status === "complete" ? (
										<>
											<CheckCircle2 className="mr-1 size-3" />
											Done
										</>
									) : status === "error" ? (
										<>
											<XCircle className="mr-1 size-3" />
											Retry
										</>
									) : (
										<>
											<Play className="mr-1 size-3" />
											Run Experiment
										</>
									)}
								</Button>
							</div>
						)}
					</div>
				</CardHeader>
				{!locked && (
					<CardContent>
						{status === "error" && (
							<div className="mb-4 rounded-md bg-destructive/10 px-3 py-2 text-destructive text-sm">
								Experiment failed. Check that the backend is running.
							</div>
						)}
						{children}
					</CardContent>
				)}
			</Card>
		</motion.div>
	);
}
