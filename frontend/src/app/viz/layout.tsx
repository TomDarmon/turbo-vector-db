import type { Metadata } from "next";

export const metadata: Metadata = {
	title: "turbo-vector Lab",
	description: "Interactive demo for the turbo-vector vector store",
};

export const dynamic = "force-dynamic";

export default function VizLayout({ children }: { children: React.ReactNode }) {
	return <>{children}</>;
}
