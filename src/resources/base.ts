import type { DataikuClient, } from "../client.js";

export abstract class BaseResource {
	constructor(protected readonly client: DataikuClient,) {}

	protected resolveProjectKey(pk?: string,): string {
		return this.client.resolveProjectKey(pk,);
	}

	protected enc(pk?: string,): string {
		return encodeURIComponent(this.resolveProjectKey(pk,),);
	}
}
