import Link from "next/link";
import { api, HydrateClient } from "~/trpc/server";

export default async function Home() {
	const hello = await api.hello.hello();

	return (
		<HydrateClient>
			<div className="min-h-screen bg-[linear-gradient(180deg,#f8fafc_0%,#eef2ff_100%)] px-4 py-12">
				<h1>{hello}</h1>
				<Link
					className="mt-4 inline-block rounded-md border border-black/20 bg-white px-3 py-2 text-sm"
					href="/viz"
				>
					Open Interactive Lab
				</Link>
			</div>
		</HydrateClient>
	);
}
