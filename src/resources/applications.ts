import { ClientValidationError, } from "../errors.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { BaseResource, } from "./base.js";

export interface AppListItem extends Record<string, unknown> {
	appId?: string;
	projectKey?: string;
	name?: string;
}

export interface AppInstanceRef extends Record<string, unknown> {
	appId?: string;
	projectKey?: string;
	jobId?: string;
}

export interface BusinessAppListItem extends Record<string, unknown> {
	id?: string;
	projectKey?: string;
	name?: string;
}

export interface BusinessAppInstanceUserPermissions extends Record<string, unknown> {
	login?: string;
	admin?: boolean;
	readProjectContent?: boolean;
	writeProjectContent?: boolean;
}

export interface AppManifestMutationResult {
	changed: boolean;
	current: Record<string, unknown>;
	manifest: Record<string, unknown>;
}

export interface AppManagedFolderExportResult extends AppManifestMutationResult {
	folderId: string;
	folder: Record<string, unknown>;
}

const HOMEPAGE_TILE_SCHEMA_HINT =
	"Use `dss app manifest get` to export an observed DSS app manifest, then `dss app manifest update --data-file PATCH.json` with explicit homepageSections JSON.";

function isRecord(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
}

function jsonEqual(left: unknown, right: unknown,): boolean {
	return JSON.stringify(left,) === JSON.stringify(right,);
}

function appInstanceManifestError(projectKey: string,): ClientValidationError {
	return new ClientValidationError(
		"Classic Dataiku App instance manifests cannot be saved through the app-manifest endpoint; only Dataiku App template project manifests can be updated.",
		"validation_failed",
		"Save against the Dataiku App template project instead; existing classic app-instance manifests are read-only through this endpoint.",
		{ projectAppType: "APP_INSTANCE", projectKey, },
	);
}

function homepageTileSchemaUnavailable(tile: string, details: Record<string, unknown>,): ClientValidationError {
	return new ClientValidationError(
		`homepage_tile_schema_unavailable: raw homepageSections schema for ${tile} is not source-verified.`,
		"homepage_tile_schema_unavailable",
		HOMEPAGE_TILE_SCHEMA_HINT,
		details,
	);
}

function manifestProjectExportManifest(manifest: Record<string, unknown>,): Record<string, unknown> {
	return isRecord(manifest.projectExportManifest) ? manifest.projectExportManifest : {};
}

export class ApplicationsResource extends BaseResource {
	/** List all Dataiku Apps. */
	async listApps(): Promise<AppListItem[]> {
		return this.client.get<AppListItem[]>("/public/api/apps/",);
	}

	/** Get the manifest for a Dataiku App template. */
	async getAppManifest(appId: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/apps/${encodeURIComponent(appId,)}/`,
		);
	}

	/** List instances created from a Dataiku App template. */
	async listInstances(appId: string,): Promise<AppInstanceRef[]> {
		return this.client.get<AppInstanceRef[]>(
			`/public/api/apps/${encodeURIComponent(appId,)}/instances/`,
		);
	}

	/** Create an instance from a Dataiku App template. */
	async createInstance(appId: string, body: Record<string, unknown>,): Promise<AppInstanceRef> {
		return this.client.post<AppInstanceRef>(
			`/public/api/apps/${encodeURIComponent(appId,)}/instances`,
			body,
		);
	}

	/** Get the manifest for an app instance project. */
	async getInstanceManifest(projectKey?: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/app-manifest`,
		);
	}

	/** Fetch an editable app template manifest, rejecting classic app-instance manifests before any PUT. */
	async getEditableInstanceManifest(projectKey?: string,): Promise<Record<string, unknown>> {
		const manifest = await this.getInstanceManifest(projectKey,);
		if (manifest.projectAppType === "APP_INSTANCE") {
			throw appInstanceManifestError(this.resolveProjectKey(projectKey,),);
		}
		return manifest;
	}

	/** Save the manifest for a Dataiku App template project (rejects classic app-instance projects). */
	async saveInstanceManifest(
		manifest: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		await this.getEditableInstanceManifest(projectKey,);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/app-manifest`,
			manifest,
		);
	}

	/** Deep-merge a manifest patch into an editable Dataiku App template manifest. */
	async updateInstanceManifest(
		patch: Record<string, unknown>,
		projectKey?: string,
	): Promise<AppManifestMutationResult> {
		const current = await this.getEditableInstanceManifest(projectKey,);
		const manifest = deepMerge(current, patch,);
		const changed = !jsonEqual(current, manifest,);
		if (changed) {
			await this.client.putVoid(
				`/public/api/projects/${this.enc(projectKey,)}/app-manifest`,
				manifest,
			);
		}
		return { changed, current, manifest, };
	}

	/** Resolve and validate a managed folder by exact id or name before writing it into an app manifest. */
	async resolveExistingManagedFolder(
		nameOrId: string,
		projectKey?: string,
	): Promise<{ folderId: string; folder: Record<string, unknown>; }> {
		const folders = await this.client.folders.list(projectKey,);
		const match = folders.find((folder,) => folder.id === nameOrId || folder.name === nameOrId);
		if (!match?.id) {
			throw new ClientValidationError(
				`Managed folder "${nameOrId}" was not found in project ${this.resolveProjectKey(projectKey,)}.`,
				"not_found",
				"Use `dss folder list --project-key KEY` to find the managed folder id or name before exporting it from the app.",
				{ folder: nameOrId, projectKey: this.resolveProjectKey(projectKey,), },
			);
		}
		const folder = await this.client.folders.get(match.id, projectKey,) as unknown as Record<string, unknown>;
		const folderId = typeof folder.id === "string" ? folder.id : match.id;
		return { folderId, folder, };
	}

	/** Export a managed folder as an app resource, idempotently adding it to the project export manifest. */
	async exportManagedFolderResource(
		nameOrId: string,
		projectKey?: string,
	): Promise<AppManagedFolderExportResult> {
		const current = await this.getEditableInstanceManifest(projectKey,);
		const { folderId, folder, } = await this.resolveExistingManagedFolder(nameOrId, projectKey,);
		const currentExport = manifestProjectExportManifest(current,);
		const currentIncluded = Array.isArray(currentExport.includedManagedFolders)
			? currentExport.includedManagedFolders
			: [];
		const alreadyIncluded = currentIncluded.some(
			(item,) => isRecord(item,) && item.id === folderId,
		);
		const includedManagedFolders = alreadyIncluded
			? currentIncluded
			: [...currentIncluded, { id: folderId, },];
		const projectExportManifest = {
			...currentExport,
			exportManagedFolders: true,
			includedManagedFolders,
		};
		const manifest = { ...current, projectExportManifest, };
		const changed = !jsonEqual(current, manifest,);
		if (changed) {
			await this.client.putVoid(
				`/public/api/projects/${this.enc(projectKey,)}/app-manifest`,
				manifest,
			);
		}
		return { changed, current, manifest, folderId, folder, };
	}

	async addProjectVariableHomepageTile(
		variable: string,
		label: string,
		buttonText: string,
		projectKey?: string,
	): Promise<never> {
		await this.getEditableInstanceManifest(projectKey,);
		throw homepageTileSchemaUnavailable("project-variable tile", {
			variable,
			label,
			buttonText,
			unsupportedReason: "No source-verified raw homepage tile schema is available for project variables.",
			closestSupportedAlternative: "dss app manifest update --data-file PATCH.json",
			projectKey: this.resolveProjectKey(projectKey,),
		});
	}

	async addScenarioHomepageTile(
		scenarioId: string,
		buttonText: string,
		projectKey?: string,
	): Promise<AppManifestMutationResult> {
		const current = await this.getEditableInstanceManifest(projectKey,);
		const tile = { type: "SCENARIO_RUN", scenarioId, prompt: buttonText, };
		const homepageSections = Array.isArray(current.homepageSections)
			? current.homepageSections
			: [];
		const alreadyPresent = homepageSections.some(
			(section,) => isRecord(section,)
				&& Array.isArray(section.tiles)
				&& section.tiles.some(
					(existingTile,) => isRecord(existingTile,)
						&& existingTile.type === tile.type
						&& existingTile.scenarioId === tile.scenarioId
						&& existingTile.prompt === tile.prompt,
				),
		);
		let manifest = current;
		if (!alreadyPresent) {
			let appended = false;
			const nextSections = homepageSections.map((section,) => {
				if (!appended && isRecord(section,) && Array.isArray(section.tiles)) {
					appended = true;
					return { ...section, tiles: [...section.tiles, tile,], };
				}
				return section;
			});
			if (!appended) {
				nextSections.push({ tiles: [tile,], },);
			}
			manifest = { ...current, homepageSections: nextSections, };
		}
		const changed = !jsonEqual(current, manifest,);
		if (changed) {
			await this.client.putVoid(
				`/public/api/projects/${this.enc(projectKey,)}/app-manifest`,
				manifest,
			);
		}
		return { changed, current, manifest, };
	}

	async addManagedFolderHomepageTile(
		folder: string,
		prompt: string,
		projectKey?: string,
	): Promise<never> {
		await this.getEditableInstanceManifest(projectKey,);
		throw homepageTileSchemaUnavailable("managed-folder tile", {
			folder,
			prompt,
			closestSupportedAlternative: "dss app manifest export-resource --managed-folder <folder>",
			projectKey: this.resolveProjectKey(projectKey,),
		});
	}

	/** Delete an app instance project. */
	async deleteInstance(projectKey?: string,): Promise<void> {
		await this.client.del(`/public/api/projects/${this.enc(projectKey,)}`,);
	}

	/** List all Business Apps. */
	async listBusinessApps(): Promise<BusinessAppListItem[]> {
		return this.client.get<BusinessAppListItem[]>("/public/api/business-apps/",);
	}

	/** Get Business App details. */
	async getBusinessApp(id: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/business-apps/${encodeURIComponent(id,)}`,
		);
	}

	/** Get Business App settings. */
	async getBusinessAppSettings(id: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/settings`,
		);
	}

	/** Save Business App settings. */
	async saveBusinessAppSettings(id: string, body: Record<string, unknown>,): Promise<void> {
		await this.client.putVoid(
			`/public/api/business-apps/${encodeURIComponent(id,)}/settings`,
			body,
		);
	}

	/** List instances of a Business App. */
	async listBusinessAppInstances(id: string,): Promise<AppInstanceRef[]> {
		return this.client.get<AppInstanceRef[]>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances`,
		);
	}

	/** Create an instance of a Business App. */
	async createBusinessAppInstance(
		id: string,
		body: Record<string, unknown>,
	): Promise<AppInstanceRef> {
		return this.client.post<AppInstanceRef>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances`,
			body,
		);
	}

	/** Upgrade a Business App instance to the latest version. */
	async upgradeBusinessAppInstance(id: string, projectKey: string,): Promise<AppInstanceRef> {
		return this.client.post<AppInstanceRef>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances/${
				encodeURIComponent(projectKey,)
			}/upgrade`,
			{},
		);
	}

	/** Get a user's effective permissions on a Business App instance. */
	async getBusinessAppInstanceUserPermissions(
		id: string,
		projectKey: string,
		user: string,
	): Promise<BusinessAppInstanceUserPermissions> {
		return this.client.get<BusinessAppInstanceUserPermissions>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances/${
				encodeURIComponent(projectKey,)
			}/permissions/${encodeURIComponent(user,)}`,
		);
	}

	/** Install or upgrade a Business App from an archive. */
	async installBusinessAppFromArchive(filePath: string,): Promise<void> {
		await this.client.upload("/public/api/business-apps/install-from-archive", filePath,);
	}
}
