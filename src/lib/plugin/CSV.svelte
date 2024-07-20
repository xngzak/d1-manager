<script lang="ts">
	import { t } from "svelte-i18n";
	import type { PluginData } from "./type";
	import type { Type } from "../sqlite";
	import { affinity, cast } from "../sqlite";

	export let database: string;
	export let table: string;
	export let data: PluginData;

	const cols = data.db
		.find(({ name }) => name === table)
		?.columns.sort(({ cid: a }, { cid: b }) => a - b);

	if (!cols) {
		throw new Error(`Table not found: ${table} in ${database}`);
	}

	const types: Record<string, Type> = Object.fromEntries(
		cols.map(({ name, type }) => [name, affinity(type)]),
	);

	let running = false;
	let result:
		| {
				results: Record<string, unknown>[];
				success: boolean;
				meta: {
					duration: number;
					last_row_id: number;
					changes: number;
					served_by: string;
					internal_stats: null;
				};
		  }
		| undefined;
	let error: string | undefined;

	let files: FileList | undefined;
	let keys: string[] | undefined;
	let casted: any[][] | undefined;
	async function read() {
		if (running) {
			return;
		}
		running = true;

		try {
			const { parse } = await import("csv-parse/browser/esm/sync");
			const file = files?.[0];
			if (!file) {
				return;
			}

			const text = await file.text();
			const rows: Record<string, string>[] = parse(text, {
				columns: true,
				skip_empty_lines: true,
			});

			keys = Object.keys(rows[0]);
			for (const key of keys) {
				if (!types[key]) {
					throw new Error($t("plugin.csv.invalid-column-name-key", { values: { key } }));
				}
			}

			casted = rows.map((row) => {
				return (keys || []).map((key) => {
					const value = row[key];
					const type = types[key];
					return cast(value, type);
				});
			});

			error = undefined;
		} catch (err) {
			console.error(err);
			if (err instanceof Error) {
				error = err.message;
			}
		} finally {
			running = false;
		}
	}

	async function import_csv() {
		if (running || !casted) {
			return;
		}
		running = true;

		try {
			const batchSize = 30; // 一度に送信するレコード数
			let totalDuration = 0;
			let totalChanges = 0;
			let processedRecords = 0;

			while (processedRecords < casted.length) {
				const batch = casted.slice(processedRecords, processedRecords + batchSize);
				const body = batch
					.map((row) => `(${row.map((x) => JSON.stringify(x)).join(", ")})`)
					.join(", ");
				const query = `INSERT OR REPLACE INTO ${table} (${keys?.join(", ")}) VALUES ${body}`;
				console.log(query);

				// 最初のバッチ以外は待機時間を入れる
				if (processedRecords > 0) {
					await new Promise((resolve) => setTimeout(resolve, 200)); // 200ミリ秒待機
				}

				const res = await fetch(`/api/db/${database}/all`, {
					method: "POST",
					body: JSON.stringify({ query }),
				});

				const json = await res.json();

				if (json) {
					if ("error" in json) {
						error = json?.error?.cause || json?.error?.message;
						break;
					} else {
						totalDuration += json.meta?.duration || 0;
						totalChanges += json.meta?.changes || 0;
					}
				} else {
					throw new Error($t("plugin.csv.no-result"));
				}

				processedRecords += batch.length;

				// 進捗状況の更新
				console.log(`Progress: ${((processedRecords / casted.length) * 100).toFixed(2)}%`);
			}

			if (!error) {
				result = {
					results: [],
					success: true,
					meta: {
						duration: totalDuration,
						last_row_id: 0,
						changes: totalChanges,
						served_by: "",
						internal_stats: null,
					},
				};
				files = undefined;
				keys = undefined;
				casted = undefined;
			}
		} catch (err) {
			console.error("Import error:", err);
			error = err instanceof Error ? err.message : "Unknown error occurred";
		} finally {
			running = false;
		}
	}

	async function export_csv() {
		if (running) {
			return;
		}
		running = true;

		try {
			const module = import("csv-stringify/browser/esm/sync");
			const res = await fetch(`/api/db/${database}/${table}/data`);
			const json = await res.json<any>();

			const { stringify } = await module;

			const csv = stringify(json, {
				header: true,
				columns: keys,
			});

			const a = document.createElement("a");
			a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
			a.setAttribute("download", `${table}.csv`);
			a.click();
			URL.revokeObjectURL(a.href);
			a.remove();
		} finally {
			running = false;
		}
	}
</script>

<div class="w-full rounded-lg border p-4">
	<p class="card-title">{$t("plugin.csv.import-csv")}</p>

	<div class="divider" />

	<div class="form-control w-full">
		<label class="label" for="csv">
			<span class="label-text">{$t("plugin.csv.select-a-csv-file")}</span>
		</label>
		<input
			id="csv"
			type="file"
			class="file-input-bordered file-input w-full"
			bind:files
			accept=".csv"
			on:change={read}
			disabled={running}
		/>
		<label class="label" for="csv">
			<span class="label-text-alt text-error">{error || ""}</span>
			<span class="label-text-alt">.csv</span>
		</label>
	</div>

	{#if keys && casted?.length}
		<div class="my-2 max-h-[70vh] overflow-auto">
			<table class="table-sm table w-full">
				<thead>
					<tr class="sticky top-0 z-10 bg-base-200 shadow">
						{#each keys as key}
							<th class="!relative normal-case">{key}</th>
						{/each}
					</tr>
				</thead>
				<tbody>
					{#each casted as row}
						<tr class="hover">
							{#each row as value}
								<td class="text-base-content">{value}</td>
							{/each}
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		<button class="btn-primary btn w-full" on:click={import_csv} disabled={running}>
			{$t("plugin.csv.import")}
		</button>
	{/if}
	{#if result}
		<p class="mt-2 text-sm opacity-70">
			{$t("plugin.csv.n-ms-m-changes", {
				values: {
					n: result.meta.duration.toFixed(2),
					m: result.meta.changes,
				},
			})}
		</p>
	{/if}
</div>

<div class="w-full rounded-lg border p-4">
	<p class="card-title">{$t("plugin.csv.export-csv")}</p>

	<div class="divider" />

	<button class="btn-primary btn w-full" on:click={export_csv} disabled={running}>
		{$t("plugin.csv.export")}
	</button>
</div>
