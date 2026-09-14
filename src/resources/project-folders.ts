import { BaseResource, } from "./base.js";

/**
 * Project folders form the instance-wide hierarchy in which projects live.
 * These are NOT managed folders (see {@link FoldersResource} for project data
 * storage); the project-folder API is an instance-level tree of containers.
 *
 * @see https://doc.dataiku.com/dss/api/15/rest/ (Project Folders section)
 */

export interface ProjectFolder extends Record<string, unknown> {
	/** ID of the project folder. */
	id?: string;
	/** Name of the project folder (null for the root project folder). */
	name?: string | null;
	/** ID of the parent project folder (null for the root project folder). */
	parentId?: string | null;
	/** IDs of this project folder's children. */
	childrenIds?: string[];
	/** Project keys inside this project folder. */
	projectKeys?: string[];
}

export interface ProjectFolderSettings extends Record<string, unknown> {
	/** Name of the project folder. */
	name?: string;
	/** Login of the owner of the project folder. */
	owner?: string;
	/** Permissions of the project folder. */
	permissions?: ProjectFolderPermission[] | null;
}

export interface ProjectFolderPermission extends Record<string, unknown> {
	group?: string;
	read?: boolean;
	writeContents?: boolean;
	admin?: boolean;
}

function folderPath(folderId: string,): string {
	return `/public/api/project-folders/${encodeURIComponent(folderId,)}`;
}

export class ProjectFoldersResource extends BaseResource {
	/**
	 * Get the definition of the root project folder. Only items on which the
	 * API key has READ privilege for project folders (children) and READ_CONF
	 * for projects are listed.
	 */
	async root(): Promise<ProjectFolder> {
		return this.client.get<ProjectFolder>("/public/api/project-folders/",);
	}

	/** Get the definition of a project folder. Requires READ on the folder. */
	async get(folderId: string,): Promise<ProjectFolder> {
		return this.client.get<ProjectFolder>(folderPath(folderId,),);
	}

	/** Get the settings of a project folder. Requires ADMIN on the folder. */
	async getSettings(folderId: string,): Promise<ProjectFolderSettings> {
		return this.client.get<ProjectFolderSettings>(`${folderPath(folderId,)}/settings`,);
	}

	/** Replace the settings of a project folder. Requires ADMIN on the folder. */
	async updateSettings(
		folderId: string,
		settings: ProjectFolderSettings,
	): Promise<void> {
		await this.client.putVoid(`${folderPath(folderId,)}/settings`, settings,);
	}

	/**
	 * Move a project folder into another project folder (change its parent)
	 * along with its content. A folder cannot be moved into one of its own
	 * sub-folders. Requires ADMIN on the folder and WRITE_CONTENTS on the
	 * destination.
	 */
	async move(folderId: string, destinationId: string,): Promise<void> {
		await this.client.post(
			`${folderPath(folderId,)}/move?destination=${encodeURIComponent(destinationId,)}`,
		);
	}

	/**
	 * Permanently delete a project folder. It must be empty: no sub-folders
	 * and no projects inside. Requires ADMIN on the folder.
	 */
	async delete(folderId: string,): Promise<void> {
		await this.client.del(folderPath(folderId,),);
	}

	/**
	 * Create a sub-project folder within the given project folder. Requires
	 * WRITE_CONTENTS on the parent.
	 */
	async createChild(
		folderId: string,
		name: string,
	): Promise<ProjectFolder> {
		return this.client.post<ProjectFolder>(
			`${folderPath(folderId,)}/children?name=${encodeURIComponent(name,)}`,
		);
	}

	/**
	 * Move a project from the project folder it lives in to another project
	 * folder. Requires WRITE_CONTENTS on the destination folder and admin on
	 * the project.
	 */
	async moveProject(
		folderId: string,
		projectKey: string,
		destinationId: string,
	): Promise<void> {
		await this.client.post(
			`${folderPath(folderId,)}/projects/${encodeURIComponent(projectKey,)}/move?destination=${
				encodeURIComponent(destinationId,)
			}`,
		);
	}
}
