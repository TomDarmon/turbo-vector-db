"use client";

import { useCallback, useState } from "react";

export type ExperimentStatus = "idle" | "running" | "complete" | "error";

export type ExperimentState<T> = {
	status: ExperimentStatus;
	result: T | null;
	error: string | null;
	runTimeMs: number | null;
};

export function useExperiment<TInput, TOutput>(
	mutateAsync: (input: TInput) => Promise<TOutput>,
) {
	const [state, setState] = useState<ExperimentState<TOutput>>({
		status: "idle",
		result: null,
		error: null,
		runTimeMs: null,
	});

	const run = useCallback(
		async (input: TInput) => {
			setState({
				status: "running",
				result: null,
				error: null,
				runTimeMs: null,
			});
			const start = Date.now();
			try {
				const result = await mutateAsync(input);
				setState({
					status: "complete",
					result,
					error: null,
					runTimeMs: Date.now() - start,
				});
			} catch (err) {
				setState({
					status: "error",
					result: null,
					error: err instanceof Error ? err.message : "Unknown error",
					runTimeMs: Date.now() - start,
				});
			}
		},
		[mutateAsync],
	);

	const reset = useCallback(() => {
		setState({ status: "idle", result: null, error: null, runTimeMs: null });
	}, []);

	return { state, run, reset };
}
