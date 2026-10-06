"use client";

import { motion, useSpring, useTransform } from "framer-motion";
import { ArrowDown, ArrowUp, Minus } from "lucide-react";
import { useEffect } from "react";
import { Card, CardContent } from "~/components/ui/card";
import { cn } from "~/lib/utils";

type Props = {
	label: string;
	value: number;
	previousValue?: number;
	unit?: string;
	animate?: boolean;
};

function AnimatedNumber({ value, unit }: { value: number; unit?: string }) {
	const spring = useSpring(0, { stiffness: 50, damping: 20 });
	const display = useTransform(spring, (v) =>
		Number.isInteger(value) ? Math.round(v).toString() : v.toFixed(2),
	);

	useEffect(() => {
		spring.set(value);
	}, [spring, value]);

	return (
		<span className="tabular-nums">
			<motion.span>{display}</motion.span>
			{unit && (
				<span className="ml-1 text-muted-foreground text-xs">{unit}</span>
			)}
		</span>
	);
}

export function StatCard({
	label,
	value,
	previousValue,
	unit,
	animate = true,
}: Props) {
	const delta =
		previousValue != null && previousValue !== 0
			? ((value - previousValue) / previousValue) * 100
			: null;

	return (
		<Card size="sm">
			<CardContent className="flex flex-col gap-1">
				<span className="text-muted-foreground text-xs">{label}</span>
				<div className="flex items-baseline gap-2">
					<span className="font-semibold text-2xl">
						{animate ? (
							<AnimatedNumber unit={unit} value={value} />
						) : (
							<>
								{Number.isInteger(value) ? value : value.toFixed(2)}
								{unit && (
									<span className="ml-1 text-muted-foreground text-xs">
										{unit}
									</span>
								)}
							</>
						)}
					</span>
					{delta != null && (
						<span
							className={cn(
								"flex items-center gap-0.5 font-medium text-xs",
								delta > 0 && "text-emerald-600",
								delta < 0 && "text-red-600",
								delta === 0 && "text-muted-foreground",
							)}
						>
							{delta > 0 ? (
								<ArrowUp className="size-3" />
							) : delta < 0 ? (
								<ArrowDown className="size-3" />
							) : (
								<Minus className="size-3" />
							)}
							{Math.abs(delta).toFixed(0)}%
						</span>
					)}
				</div>
			</CardContent>
		</Card>
	);
}
