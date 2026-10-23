import { computed, inject, Injectable, Signal, signal } from "@angular/core";
import {
	HttpClient,
	httpResource,
	HttpResourceRef,
	HttpResponse
} from "@angular/common/http";
import { environment } from "../../environments/environment";
import {
	ApproveCvImprove,
	CvImproveWsData,
	FeedbackCandidate,
	ImproveCandidate,
	ImproveCandidateWsMessage,
	isConversionMessage,
	isCvImproveMessage
} from "@interfaces/feedback-candidate.interface";
import { defer, Observable, repeat, retry, Subject, Subscription, timer } from "rxjs";
import { webSocket } from "rxjs/webSocket";
import { CredentialsStoreService } from "./credentials-store.service";
import { LoggerService } from "./logger.service";
import { TaskStatus } from "@interfaces/candidate.interface";
import { IConvMessage } from "@interfaces/messages.interface";

@Injectable({
	providedIn: "root"
})
export class MatchFeedbackService {
	private readonly apiUrl = `${environment.apiHost}/api`;
	private readonly wsUrl = `${environment.wsHost}/api`;
	private readonly http = inject(HttpClient);
	private readonly credentialsStore = inject(CredentialsStoreService);
	private readonly logger = inject(LoggerService);

	private readonly _currentImprovements = signal<
			Map<string, { cvId: number; vacancyId: number; personId: number }>
	>(new Map());

	private readonly _pendingApprovals = signal<Map<string, CvImproveWsData>>(
			new Map()
	);

	private readonly _approvalSubmittedKeys = signal<Set<string>>(new Set());

	private readonly _manuallyCancelledKeys = signal<Set<string>>(new Set());
	readonly manuallyCancelledKeys = this._manuallyCancelledKeys.asReadonly();

	private readonly downloadedPublications = new Set<string>();

	readonly isGloballyImproving = computed(
			() => this._currentImprovements().size > 0
	);

	private readonly _pipeline$ = new Subject<ImproveCandidateWsMessage>();
	readonly pipeline$ = this._pipeline$.asObservable();

	private wsSubscription: Subscription | null = null;

	private readonly triggerDownload = (response: HttpResponse<Blob>): void => {
		const blob = response.body!;
		const disposition = response.headers.get("content-disposition") ?? "";
		const filenameMatch = disposition.match(
				/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/
		);
		const filename = filenameMatch
				? filenameMatch[1].replace(/['"]/g, "")
				: "improved-cv";
		const url = URL.createObjectURL(blob);
		const a = document.createElement("a");
		a.href = url;
		a.download = filename;
		document.body.appendChild(a);
		a.click();
		document.body.removeChild(a);
		setTimeout(() => URL.revokeObjectURL(url));
	};

	connectWs(): void {
		if (this.wsSubscription) return;
		this.wsSubscription = defer(() => this.improvePipelineWebSocket())
				.pipe(
						retry({
							delay: (_err, retryCount) =>
									timer(MatchFeedbackService.retryDelayMs(retryCount)),
							resetOnSuccess: true
						}),
						repeat({ delay: 2000 })
				)
				.subscribe((msg) => {
					if (isCvImproveMessage(msg)) {
						this.handleImpMessage(msg.data);
					} else if (isConversionMessage(msg)) {
						this.handleConvMessage(msg.data);
					}

					this._pipeline$.next(msg);
				});
	}

	private handleImpMessage(data: CvImproveWsData): void {
		const { task_status, cv_id, vacancy_id, person_id } = data;
		const k = MatchFeedbackService.key(cv_id, vacancy_id);
		if (
				task_status === TaskStatus.Failed ||
				task_status === TaskStatus.Revoked
		) {
			this.clearImprovement(cv_id, vacancy_id);
		} else if (!this._manuallyCancelledKeys().has(k)) {
			this.startGlobalImproving(cv_id, vacancy_id, person_id);
			if (task_status === TaskStatus.PendingApproval) {
				this._pendingApprovals.update((m) => {
					const existing = m.get(k);
					if (
							existing &&
							existing.publication_id === data.publication_id &&
							existing.filename === data.filename
					) {
						return m;
					}
					const nm = new Map(m);
					nm.set(k, data);
					return nm;
				});
			} else {
				this._pendingApprovals.update((m) => {
					if (!m.has(k)) return m;
					const nm = new Map(m);
					nm.delete(k);
					return nm;
				});
				this._approvalSubmittedKeys.update((s) => {
					if (!s.has(k)) return s;
					const ns = new Set(s);
					ns.delete(k);
					return ns;
				});
			}
		}
	}

	private handleConvMessage(data: IConvMessage): void {
		const { task_status, publication_id, cv_id, vacancy_id } = data;
		const isTerminal =
				task_status === TaskStatus.Succeeded ||
				task_status === TaskStatus.Failed ||
				task_status === TaskStatus.Revoked;
		if (cv_id != null && vacancy_id != null && isTerminal) {
			this.clearImprovement(cv_id, vacancy_id);
		}
		if (
				task_status === TaskStatus.Succeeded &&
				publication_id &&
				!this.downloadedPublications.has(publication_id)
		) {
			this.downloadedPublications.add(publication_id);
			this.downloadConversion(publication_id).subscribe({
				next: (response) => this.triggerDownload(response),
				error: (err) => {
					this.downloadedPublications.delete(publication_id);
					this.logger.error(err);
				}
			});
		}
	}

	disconnectWs(): void {
		this.wsSubscription?.unsubscribe();
		this.wsSubscription = null;
	}

	private static key(cvId: number, vacancyId: number): string {
		return `${cvId}:${vacancyId}`;
	}

	private static retryDelayMs(retryCount: number): number {
		return Math.min(retryCount * 2000, 30000);
	}

	startGlobalImproving(
			cvId: number,
			vacancyId: number,
			personId: number
	): void {
		const k = MatchFeedbackService.key(cvId, vacancyId);
		this._manuallyCancelledKeys.update((s) => {
			if (!s.has(k)) return s;
			const ns = new Set(s);
			ns.delete(k);
			return ns;
		});
		this._currentImprovements.update((m) => {
			const nm = new Map(m);
			nm.set(k, { cvId, vacancyId, personId });
			return nm;
		});
	}

	manuallyCancel(cvId: number, vacancyId: number): void {
		const k = MatchFeedbackService.key(cvId, vacancyId);
		this._manuallyCancelledKeys.update((s) => {
			const ns = new Set(s);
			ns.add(k);
			return ns;
		});
		this.clearImprovement(cvId, vacancyId);
	}

	markApprovalSubmitted(cvId: number, vacancyId: number): void {
		const k = MatchFeedbackService.key(cvId, vacancyId);
		this._pendingApprovals.update((m) => {
			if (!m.has(k)) return m;
			const nm = new Map(m);
			nm.delete(k);
			return nm;
		});
		this._approvalSubmittedKeys.update((s) => {
			const ns = new Set(s);
			ns.add(k);
			return ns;
		});
	}

	clearImprovement(cvId: number, vacancyId: number): void {
		const k = MatchFeedbackService.key(cvId, vacancyId);
		this._currentImprovements.update((m) => {
			if (!m.has(k)) return m;
			const nm = new Map(m);
			nm.delete(k);
			return nm;
		});
		this._pendingApprovals.update((m) => {
			if (!m.has(k)) return m;
			const nm = new Map(m);
			nm.delete(k);
			return nm;
		});
		this._approvalSubmittedKeys.update((s) => {
			if (!s.has(k)) return s;
			const ns = new Set(s);
			ns.delete(k);
			return ns;
		});
	}

	forgetDownload(publicationId: string): void {
		this.downloadedPublications.delete(publicationId);
	}

	hasImprovement(cvId: number, vacancyId: number): boolean {
		return this._currentImprovements().has(
				MatchFeedbackService.key(cvId, vacancyId)
		);
	}

	getPendingApproval(
			cvId: number,
			vacancyId: number
	): CvImproveWsData | undefined {
		return this._pendingApprovals().get(
				MatchFeedbackService.key(cvId, vacancyId)
		);
	}

	isApprovalSubmittedFor(cvId: number, vacancyId: number): boolean {
		return this._approvalSubmittedKeys().has(
				MatchFeedbackService.key(cvId, vacancyId)
		);
	}

	isManuallyCancelledFor(cvId: number, vacancyId: number): boolean {
		return this._manuallyCancelledKeys().has(
				MatchFeedbackService.key(cvId, vacancyId)
		);
	}

	getAllFeedbackForMatchResource(
			matchId: Signal<number>
	): HttpResourceRef<FeedbackCandidate[]> {
		return httpResource<FeedbackCandidate[]>(
				() => {
					const id = matchId();
					if (!id) return undefined;

					return {
						url: `${this.apiUrl}/v1/vacancy_cv_match/${id}/feedbacks`,
						headers: { Accept: "application/json" }
					};
				},
				{
					defaultValue: []
				}
		);
	}

	editFeedbackComment(
			feedbackCandidate: FeedbackCandidate
	): Observable<FeedbackCandidate> {
		const payload = {
			content: feedbackCandidate.content
		};

		return feedbackCandidate.id
				? this.http.patch<FeedbackCandidate>(
						`${this.apiUrl}/v1/vacancy_cv_match/${feedbackCandidate.match_id}/feedbacks/${feedbackCandidate.id}`,
						payload
				)
				: this.http.post<FeedbackCandidate>(
						`${this.apiUrl}/v1/vacancy_cv_match/${feedbackCandidate.match_id}/feedbacks`,
						payload
				);
	}

	deleteFeedbackComment(
			feedbackCandidate: FeedbackCandidate
	): Observable<FeedbackCandidate> {
		return this.http.delete<FeedbackCandidate>(
				`${this.apiUrl}/v1/vacancy_cv_match/${feedbackCandidate.match_id}/feedbacks/${feedbackCandidate.id}`
		);
	}

	getQuestionAnswerPairs(vacancyId: number, cvId: number) {
		return this.http.get<[]>(
				`${this.apiUrl}/v1/llm/vacancy/${vacancyId}/cv/${cvId}/qa_pairs`
		);
	}

	improveCandidate(payload: ImproveCandidate): Observable<void> {
		return this.http.post<void>(`${this.apiUrl}/v1/llm/cv/improve`, payload);
	}

	approveCvImprove(payload: ApproveCvImprove): Observable<void> {
		return this.http.post<void>(
				`${this.apiUrl}/v1/llm/cv/improve/approve`,
				payload
		);
	}

	private improvePipelineWebSocket(): Observable<ImproveCandidateWsMessage> {
		return webSocket<ImproveCandidateWsMessage>(
				`${this.wsUrl}/v1/llm/cv/improve/pipeline/ws${this.getWsTokenParam()}`
		);
	}

	private getWsTokenParam(): string {
		const token = this.credentialsStore.accessToken;
		return token ? `?token=${token}` : "";
	}

	downloadConversion(publicationId: string): Observable<HttpResponse<Blob>> {
		return this.http.get(`${this.apiUrl}/v1/conversions`, {
			params: { publication_id: publicationId },
			responseType: "blob",
			observe: "response"
		});
	}
}
