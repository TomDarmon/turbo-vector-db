import { createEnv } from "@t3-oss/env-nextjs";
import { z } from "zod";

export const env = createEnv({
	server: {
		DATABASE_URL: z.string().url(),
		TV_VIZ_ENABLED: z.string().optional(),
		TV_API_BASE_URL: z.string().url().optional(),
		TV_BROKER_BASE_URL: z.string().url().optional(),
		NODE_ENV: z
			.enum(["development", "test", "production"])
			.default("development"),
	},
	client: {},
	runtimeEnv: {
		DATABASE_URL: process.env.DATABASE_URL,
		TV_VIZ_ENABLED: process.env.TV_VIZ_ENABLED,
		TV_API_BASE_URL: process.env.TV_API_BASE_URL,
		TV_BROKER_BASE_URL: process.env.TV_BROKER_BASE_URL,
		NODE_ENV: process.env.NODE_ENV,
	},
	skipValidation: !!process.env.SKIP_ENV_VALIDATION,
	emptyStringAsUndefined: true,
});
