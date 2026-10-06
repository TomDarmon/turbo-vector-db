"use client";

import { motion } from "framer-motion";
import { Flame, Snowflake } from "lucide-react";
import { cn } from "~/lib/utils";

type Props = {
	temperature: "cold" | "warm";
};

export function TemperatureBadge({ temperature }: Props) {
	const isCold = temperature === "cold";

	return (
		<motion.span
			animate={{ scale: 1, opacity: 1 }}
			className={cn(
				"inline-flex items-center gap-1.5 rounded-full px-3 py-1 font-medium text-xs",
				isCold
					? "bg-blue-100 text-blue-700 dark:bg-blue-950/40 dark:text-blue-300"
					: "bg-orange-100 text-orange-700 dark:bg-orange-950/40 dark:text-orange-300",
			)}
			initial={{ scale: 0.8, opacity: 0 }}
		>
			{isCold ? (
				<Snowflake className="size-3 animate-pulse" />
			) : (
				<Flame className="size-3 animate-pulse" />
			)}
			{isCold ? "Cold" : "Hot"}
		</motion.span>
	);
}
