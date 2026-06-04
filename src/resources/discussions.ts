import { BaseResource, } from "./base.js";

export interface DiscussionReply {
	text: string;
	author?: string;
	time?: number;
	editedOn?: number;
	[key: string]: unknown;
}

export interface Discussion {
	id: string;
	topic?: string;
	replies?: DiscussionReply[];
	[key: string]: unknown;
}

export class DiscussionsResource extends BaseResource {
	/** List discussions attached to a project object. */
	async list(objectType: string, objectId: string, projectKey?: string,): Promise<Discussion[]> {
		return this.client.get<Discussion[]>(
			this.objectDiscussionsPath(objectType, objectId, projectKey,),
		);
	}

	/** Get one discussion attached to a project object, including replies. */
	async get(
		objectType: string,
		objectId: string,
		discussionId: string,
		projectKey?: string,
	): Promise<Discussion> {
		return this.client.get<Discussion>(
			this.discussionPath(objectType, objectId, discussionId, projectKey,),
		);
	}

	/** Create a discussion with its first reply. */
	async create(
		objectType: string,
		objectId: string,
		topic: string,
		reply: string,
		projectKey?: string,
	): Promise<Discussion> {
		return this.client.post<Discussion>(
			this.objectDiscussionsPath(objectType, objectId, projectKey,),
			{ topic, reply, },
		);
	}

	/** Add a reply to an existing discussion. */
	async reply(
		objectType: string,
		objectId: string,
		discussionId: string,
		text: string,
		projectKey?: string,
	): Promise<Discussion> {
		return this.client.post<Discussion>(
			`${this.discussionPath(objectType, objectId, discussionId, projectKey,)}/replies/`,
			{ reply: text, },
		);
	}

	private objectDiscussionsPath(objectType: string, objectId: string, projectKey?: string,): string {
		const pk = this.enc(projectKey,);
		const encodedType = encodeURIComponent(objectType,);
		const encodedId = encodeURIComponent(objectId,);
		return `/public/api/projects/${pk}/discussions/${encodedType}/${encodedId}/`;
	}

	private discussionPath(
		objectType: string,
		objectId: string,
		discussionId: string,
		projectKey?: string,
	): string {
		const collectionPath = this.objectDiscussionsPath(objectType, objectId, projectKey,);
		return `${collectionPath}${encodeURIComponent(discussionId,)}`;
	}
}
