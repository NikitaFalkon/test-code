import { computed, effect, inject, Injectable, signal, Signal } from "@angular/core";
import { rxResource } from "@angular/core/rxjs-interop";
import { Observable } from "rxjs";
import { compressToUTF16, decompressFromUTF16 } from "lz-string";
import { LoggerService } from "./logger.service";
import {
	Profession,
	ProfessionProficiency
} from "@interfaces/profession.interface";
import { Skill } from "@interfaces/skill.interface";
import { HttpClient } from "@angular/common/http";
import { environment } from "../../environments/environment";
import { NamedItemInterface } from "@interfaces/named-item.interface";
import { User } from "@interfaces/auth/user.interface";

interface CachedResource<T> {
	value: Signal<T[]>;
	load: () => Promise<T[]>;
}

@Injectable({
	providedIn: "root"
})
export class CommonService {
	private readonly http = inject(HttpClient);
	private readonly logger = inject(LoggerService);

	private readonly apiUrl = `${environment.apiHost}/api`;
	private readonly skillsStorageKey = "cached_skills";

	private readonly professionsResource = this.createCachedResource(() =>
			this.getProfessions()
	);
	private readonly professionProficienciesResource = this.createCachedResource(
			() => this.getProfessionProficiencies()
	);
	private readonly allUsersResource = this.createCachedResource(() =>
			this.getAllUsers()
	);
	private readonly languagesResource = this.createCachedResource(() =>
			this.getLanguages()
	);
	private readonly languageLevelsResource = this.createCachedResource(() =>
			this.getLanguageLevels()
	);
	private readonly skillsResource = this.createCachedResource(
			() => this.getSkills(),
			{
				seed: this.readCachedSkills(),
				onLoaded: (skills) => this.writeCachedSkills(skills),
				lazy: true
			}
	);

	readonly professions = this.professionsResource.value;
	readonly professionProficiencies = this.professionProficienciesResource.value;
	readonly allUsers = this.allUsersResource.value;
	readonly languages = this.languagesResource.value;
	readonly languageLevels = this.languageLevelsResource.value;
	readonly skills = this.skillsResource.value;

	loadProfessions = (): Promise<Profession[]> =>
			this.professionsResource.load();
	loadProfessionProficiencies = (): Promise<ProfessionProficiency[]> =>
			this.professionProficienciesResource.load();
	loadAllUsers = (): Promise<User[]> => this.allUsersResource.load();
	loadLanguages = (): Promise<NamedItemInterface[]> =>
			this.languagesResource.load();
	loadLanguageLevels = (): Promise<NamedItemInterface[]> =>
			this.languageLevelsResource.load();
	loadSkills = (): Promise<Skill[]> => this.skillsResource.load();

	private getProfessions(): Observable<Profession[]> {
		return this.http.get<Profession[]>(`${this.apiUrl}/v1/professions`);
	}

	private getProfessionProficiencies(): Observable<ProfessionProficiency[]> {
		return this.http.get<ProfessionProficiency[]>(
				`${this.apiUrl}/v1/profession_proficiencies`
		);
	}

	private getAllUsers(): Observable<User[]> {
		return this.http.get<User[]>(`${this.apiUrl}/v1/users`);
	}

	private getSkills(): Observable<Skill[]> {
		return this.http.get<Skill[]>(`${this.apiUrl}/v1/skills`);
	}

	private getLanguages(): Observable<NamedItemInterface[]> {
		return this.http.get<NamedItemInterface[]>(`${this.apiUrl}/v1/languages`);
	}

	private getLanguageLevels(): Observable<NamedItemInterface[]> {
		return this.http.get<NamedItemInterface[]>(`${this.apiUrl}/v1/levels`);
	}

	private readCachedSkills(): Skill[] {
		try {
			const cached = sessionStorage.getItem(this.skillsStorageKey);
			if (!cached) {
				return [];
			}
			const decompressed = decompressFromUTF16(cached);
			return decompressed ? (JSON.parse(decompressed) as Skill[]) : [];
		} catch (err) {
			this.logger.error(err);
			return [];
		}
	}

	private writeCachedSkills(skills: Skill[]): void {
		try {
			sessionStorage.setItem(
					this.skillsStorageKey,
					compressToUTF16(JSON.stringify(skills))
			);
		} catch (err) {
			this.logger.error(err);
		}
	}

	private createCachedResource<T>(
			fetcher: () => Observable<T[]>,
			options?: {
				seed?: T[];
				onLoaded?: (items: T[]) => void;
				lazy?: boolean;
			}
	): CachedResource<T> {
		const fallback = options?.seed ?? [];
		const hasSeed = fallback.length > 0;
		const requested = signal(!options?.lazy);
		let pendingResolvers: ((items: T[]) => void)[] = [];

		const resource = rxResource({
			params: () => (requested() ? true : undefined),
			stream: () => fetcher(),
			defaultValue: fallback
		});

		const value = computed(() =>
				resource.hasValue() ? resource.value() : fallback
		);

		effect(() => {
			const status = resource.status();
			if (status !== "resolved" && status !== "error") {
				return;
			}

			let result: T[];
			if (status === "error") {
				this.logger.error(resource.error());
				result = fallback;
			} else {
				result = resource.value();
				options?.onLoaded?.(result);
			}

			const resolvers = pendingResolvers;
			pendingResolvers = [];
			resolvers.forEach((resolve) => resolve(result));
		});

		const load = (): Promise<T[]> => {
			if (!requested()) {
				requested.set(true);
			} else if (resource.status() === "error") {
				resource.reload();
			}

			if (hasSeed) {
				return Promise.resolve(fallback);
			}

			if (resource.status() === "resolved") {
				return Promise.resolve(resource.value());
			}
			return new Promise<T[]>((resolve) => pendingResolvers.push(resolve));
		};

		return { value, load };
	}
}