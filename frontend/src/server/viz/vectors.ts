import type { UpsertVector } from "./backend";

function seededRandom(seed: number): () => number {
	let s = seed;
	return () => {
		s = (s * 16807 + 0) % 2147483647;
		return (s - 1) / 2147483646;
	};
}

export function generateVectors(
	count: number,
	dimension: number,
	prefix: string,
	seed = 42,
): UpsertVector[] {
	const rng = seededRandom(seed);
	const vectors: UpsertVector[] = [];
	const categories = ["science", "history", "tech", "art", "sports"];

	for (let i = 0; i < count; i++) {
		const values: number[] = [];
		for (let d = 0; d < dimension; d++) {
			values.push(rng() * 2 - 1);
		}
		vectors.push({
			id: `${prefix}-${String(i).padStart(4, "0")}`,
			values,
			metadata: {
				category: categories[i % categories.length],
				index: i,
			},
		});
	}
	return vectors;
}
