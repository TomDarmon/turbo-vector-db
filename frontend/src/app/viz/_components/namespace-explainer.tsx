"use client";

import { motion } from "framer-motion";
import { Shield, Users } from "lucide-react";
import { Badge } from "~/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "~/components/ui/card";

type NamespaceInfo = {
	name: string;
	vectorCount: number;
};

type Props = {
	collectionName: string;
	namespaces: NamespaceInfo[];
};

export function NamespaceExplainer({ collectionName, namespaces }: Props) {
	return (
		<div className="space-y-3">
			<div className="rounded-lg border-2 border-border border-dashed p-4">
				<div className="mb-3 flex items-center gap-2 font-medium text-muted-foreground text-sm">
					<Shield className="size-4" />
					Collection:{" "}
					<code className="rounded bg-muted px-1.5 py-0.5">
						{collectionName}
					</code>
				</div>
				<motion.div
					animate="show"
					className="grid gap-3 md:grid-cols-2"
					initial="hidden"
					variants={{ show: { transition: { staggerChildren: 0.15 } } }}
				>
					{namespaces.map((ns) => (
						<motion.div
							key={ns.name}
							variants={{
								hidden: { opacity: 0, y: 10 },
								show: { opacity: 1, y: 0 },
							}}
						>
							<Card className="border-primary/20 bg-primary/5" size="sm">
								<CardHeader>
									<div className="flex items-center gap-2">
										<Users className="size-4 text-primary" />
										<CardTitle className="text-sm">{ns.name}</CardTitle>
										<Badge className="ml-auto text-[10px]" variant="secondary">
											{ns.vectorCount} vectors
										</Badge>
									</div>
								</CardHeader>
								<CardContent>
									<p className="text-muted-foreground text-xs">
										Isolated partition &mdash; queries here never see other
										namespaces.
									</p>
								</CardContent>
							</Card>
						</motion.div>
					))}
				</motion.div>
			</div>
			<div className="rounded-md bg-muted/50 px-4 py-3 text-muted-foreground text-xs leading-relaxed">
				<strong className="text-foreground">Namespaces</strong> are isolated
				partitions within a collection. Think multi-tenancy: each tenant&apos;s
				vectors live in their own namespace. Queries in{" "}
				<code className="rounded bg-muted px-1">tenant-a</code> never see{" "}
				<code className="rounded bg-muted px-1">tenant-b</code>&apos;s data.
				Same index infrastructure, zero cross-contamination. Each namespace
				builds its own ANN index independently.
			</div>
		</div>
	);
}
