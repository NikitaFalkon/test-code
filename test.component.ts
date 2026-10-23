import {
	ChangeDetectionStrategy,
	Component,
	computed,
	DestroyRef,
	effect,
	inject,
	input,
	OnInit,
	signal
} from "@angular/core";
import { takeUntilDestroyed } from "@angular/core/rxjs-interop";
import { filter, switchMap, tap } from "rxjs";
import { CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { MatProgressSpinner } from "@angular/material/progress-spinner";
import { MatIcon } from "@angular/material/icon";
import { MatTooltip } from "@angular/material/tooltip";
import { RouterLink } from "@angular/router";
import { MatDialog } from "@angular/material/dialog";
import { MatchFeedbackService } from "../../../../services/match-feedback.service";
import { AuthService } from "../../../../services/auth.service";
import { LoggerService } from "../../../../services/logger.service";
import { ApproveInstructionsDialogComponent } from "../../../vacancies/vacancy-details-container/approve-instructions-dialog/approve-instructions-dialog.component";
import { TaskStatus } from "@interfaces/candidate.interface";
import {
	CvImproveWsData,
	ImproveCandidateWsMessage,
	isConversionMessage,
	isCvImproveMessage
} from "@interfaces/feedback-candidate.interface";

type ImproveStage =
		| "improving"
		| "pending_approval"
		| "converting"
		| "done"
		| "failed"
		| "revoked";

interface ImproveDisplayState {
	stage: ImproveStage;
	filename: string;
	firstName?: string;
	lastName?: string;
	cvId?: number;
	vacancyId?: number;
	personId?: number;
	publicationId?: string;
	pendingData?: CvImproveWsData;
	errorMessage?: string;
}

function toStage(msg: ImproveCandidateWsMessage): ImproveStage {
	const status = msg.data.task_status;

	if (isCvImproveMessage(msg)) {
		switch (status) {
			case TaskStatus.PendingApproval:
				return "pending_approval";
			case TaskStatus.Failed:
				return "failed";
			case TaskStatus.Revoked:
				return "revoked";
			default:
				return "improving";
		}
	}
	switch (status) {
		case TaskStatus.Succeeded:
			return "done";
		case TaskStatus.Failed:
		case TaskStatus.Revoked:
			return "failed";
		default:
			return "converting";
	}
}

@Component({
	selector: "app-cv-improve-processing-panel",
	imports: [
		CdkDrag,
		CdkDragHandle,
		MatProgressSpinner,
		MatIcon,
		MatTooltip,
		RouterLink
	],
	templateUrl: "./cv-improve-processing-panel.component.html",
	styleUrl: "./cv-improve-processing-panel.component.scss",
	changeDetection: ChangeDetectionStrategy.OnPush
})
export class CvImproveProcessingPanelComponent implements OnInit {
	private readonly matchFeedbackService = inject(MatchFeedbackService);
	private readonly destroyRef = inject(DestroyRef);
	private readonly authService = inject(AuthService);
	private readonly dialog = inject(MatDialog);
	private readonly logger = inject(LoggerService);

	dragBoundary = input<string>("app-cv-finder-container");

	displayState = signal<Map<string, ImproveDisplayState>>(new Map());
	displayStateEntries = computed(() =>
			Array.from(this.displayState().entries())
	);
	isHidden = signal(false);

	isApprovalSubmittedFor = (cvId: number, vacancyId: number): boolean =>
			this.matchFeedbackService.isApprovalSubmittedFor(cvId, vacancyId);

	private static stateKey(cvId: number, vacancyId: number): string {
		return `${cvId}:${vacancyId}`;
	}

	private readonly cancelEffect = effect(() => {
		const cancelled = this.matchFeedbackService.manuallyCancelledKeys();
		if (cancelled.size === 0) return;
		this.displayState.update((m) => {
			let changed = false;
			const nm = new Map(m);
			for (const [k, s] of nm) {
				if (s.stage === "revoked" || s.cvId == null || s.vacancyId == null)
					continue;
				if (
						cancelled.has(
								CvImproveProcessingPanelComponent.stateKey(s.cvId, s.vacancyId)
						)
				) {
					nm.set(k, { ...s, stage: "revoked" });
					changed = true;
				}
			}
			return changed ? nm : m;
		});
	});

	ngOnInit() {
		this.matchFeedbackService.pipeline$
				.pipe(takeUntilDestroyed(this.destroyRef))
				.subscribe((msg) => this.handlePipelineMessage(msg));
	}

	closeEntry(key: string): void {
		const s = this.displayState().get(key);
		if (s) this.releaseResources(s);
		this.displayState.update((m) => {
			if (!m.has(key)) return m;
			const nm = new Map(m);
			nm.delete(key);
			return nm;
		});
	}

	toggleHidden() {
		this.isHidden.update((v) => !v);
	}

	openApproveDialog(state: ImproveDisplayState): void {
		const pending = state.pendingData;
		if (!pending) return;

		const cvId = pending.cv_id;
		const vacancyId = pending.vacancy_id;
		const dialogId = `approve-instructions-${cvId}-${vacancyId}`;
		if (this.dialog.getDialogById(dialogId)) return;
		const isEng = this.authService.language() === "EN";
		this.dialog
				.open(ApproveInstructionsDialogComponent, {
					id: dialogId,
					width: "600px",
					panelClass: "moved-dialog-pane",
					disableClose: true,
					data: {
						instructions: isEng
								? (pending.instructions_en ?? "")
								: (pending.instructions_ru ?? ""),
						filename: pending.filename ?? "",
						isEng
					}
				})
				.afterClosed()
				.pipe(
						tap((result: string | null | false) => {
							if (result === false)
								this.matchFeedbackService.clearImprovement(cvId, vacancyId);
						}),
						filter((result): result is string => typeof result === "string"),
						tap(() =>
								this.matchFeedbackService.markApprovalSubmitted(cvId, vacancyId)
						),
						switchMap((instructions) =>
								this.matchFeedbackService.approveCvImprove({
									cv_id: cvId,
									vacancy_id: vacancyId,
									publication_id: pending.publication_id!,
									filename: pending.filename!,
									instructions
								})
						)
				)
				.subscribe({
					error: (err) => {
						this.matchFeedbackService.clearImprovement(cvId, vacancyId);
						this.logger.error(err);
					}
				});
	}

	closeAll() {
		for (const s of this.displayState().values()) {
			this.releaseResources(s);
		}
		this.displayState.set(new Map());
	}

	private handlePipelineMessage(msg: ImproveCandidateWsMessage): void {
		if (!isCvImproveMessage(msg) && !isConversionMessage(msg)) return;
		const stage = toStage(msg);

		const {
			filename,
			first_name: firstName,
			last_name: lastName,
			error_message
		} = msg.data;

		const cvId = msg.data.cv_id;
		const vacancyId = msg.data.vacancy_id;
		const personId = msg.data.person_id;
		const publicationId = msg.data.publication_id;
		if (cvId == null || vacancyId == null) return;

		const isInProgress =
				stage === "improving" ||
				stage === "pending_approval" ||
				stage === "converting";
		if (
				isInProgress &&
				this.matchFeedbackService.isManuallyCancelledFor(cvId, vacancyId)
		) {
			return;
		}

		const k = CvImproveProcessingPanelComponent.stateKey(cvId, vacancyId);
		this.displayState.update((m) => {
			const nm = new Map(m);
			const prev = nm.get(k);
			nm.set(k, {
				stage,
				filename: filename ?? prev?.filename ?? "",
				firstName: firstName ?? prev?.firstName,
				lastName: lastName ?? prev?.lastName,
				cvId,
				vacancyId,
				personId: personId ?? prev?.personId,
				publicationId: publicationId ?? prev?.publicationId,
				pendingData:
						isCvImproveMessage(msg) && stage === "pending_approval"
								? msg.data
								: undefined,
				errorMessage: error_message
			});
			return nm;
		});
		this.isHidden.set(false);
	}

	private releaseResources(s: ImproveDisplayState): void {
		if (s.cvId != null && s.vacancyId != null) {
			this.matchFeedbackService.clearImprovement(s.cvId, s.vacancyId);
		}
		if (s.publicationId) {
			this.matchFeedbackService.forgetDownload(s.publicationId);
		}
	}
}