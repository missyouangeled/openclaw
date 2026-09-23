import type { TranscriptMessageAppendResult } from "./session-accessor.sqlite-contract.js";
import type {
  appendTranscriptEventSnapshotSync,
  TranscriptEventAppendResult,
  TranscriptWriteSnapshot,
} from "./session-accessor.sqlite-transcript-write.js";

export function isTranscriptMessageAppendCurrentTail(
  snapshot: TranscriptWriteSnapshot<TranscriptMessageAppendResult<unknown> | undefined>,
): boolean {
  const anchor = snapshot.result?.anchor;
  return (
    anchor !== undefined &&
    anchor.generation === snapshot.after.generation &&
    anchor.rawSeq === snapshot.after.rawSeq
  );
}

export function requireTranscriptEventAppendSnapshot(
  result: ReturnType<typeof appendTranscriptEventSnapshotSync>,
  message: string,
): TranscriptWriteSnapshot<Extract<TranscriptEventAppendResult, { appended: true }>> {
  if (result.ok && result.value.result.appended) {
    return { ...result.value, result: result.value.result };
  }
  const cause = result.ok ? { code: "transcript-event-not-appended" as const } : result.error;
  throw new Error(`${message}: ${cause.code}`, { cause });
}
