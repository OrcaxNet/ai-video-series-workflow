import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiProblem, type ApprovalInput, type GateId } from "../domain";
import { gates } from "../mock-data";
import { HttpControlPlaneApi, MockControlPlaneApi } from "./control-plane";

const input = (
  gateId: GateId,
  expectedRevision: number,
  idempotencyKey: string,
  decision: "APPROVED" | "RETURNED" = "APPROVED",
): ApprovalInput => ({
  gateId,
  expectedRevision,
  idempotencyKey,
  decision,
  explanation: "reviewed",
  bindings: gates[gateId].bindings,
});

describe("MockControlPlaneApi", () => {
  it("creates a project operation from creator inputs without requiring a provider key", async () => {
    const api = new MockControlPlaneApi();
    await expect(
      api.createSeries(
        {
          title: "潮汐失语者 PoC",
          sourceText: "一段有权改编的原作内容",
          aspectRatio: "16:9",
          targetDuration: "03:20",
          targetAudience: "悬疑剧观众",
          visualStyle: "沿海现实主义",
        },
        "22222222-2222-4222-8222-222222222222",
      ),
    ).resolves.toMatchObject({ state: "ACCEPTED" });
  });

  it("keeps all provider capabilities explicitly mock-only without live credentials", async () => {
    const api = new MockControlPlaneApi();
    const status = await api.getProviderStatus();

    expect(status).toHaveLength(4);
    expect(status.every((item) => !item.liveConfigured && !item.liveCallsEnabled)).toBe(true);
    expect(status.every((item) => item.mockEvidence === "mock_only")).toBe(true);
  });

  it("enforces the gate order and unlocks G2 only after G1 approval", async () => {
    const api = new MockControlPlaneApi();

    await expect(api.createApproval(input("G2", 9, "g2-too-early"))).rejects.toMatchObject({
      status: 422,
      errorCode: "GATE_REQUIRED",
    });

    await expect(api.createApproval(input("G1", 7, "g1-approve"))).resolves.toMatchObject({
      gate: "G1",
      decision: "APPROVED",
    });
    await expect(api.createApproval(input("G2", 9, "g2-approve"))).resolves.toMatchObject({
      gate: "G2",
      decision: "APPROVED",
    });
  });

  it("replays the same idempotent decision and rejects key reuse with a changed body", async () => {
    const api = new MockControlPlaneApi();
    const first = await api.createApproval(input("G1", 7, "same-key"));
    const replay = await api.createApproval(input("G1", 7, "same-key"));

    expect(replay).toEqual(first);
    await expect(api.createApproval(input("G1", 7, "same-key", "RETURNED"))).rejects.toMatchObject({
      status: 409,
      errorCode: "conflict",
    });
  });

  it("rejects a second approval for the same terminal revision", async () => {
    const api = new MockControlPlaneApi();
    await api.createApproval(input("G1", 7, "first-approval"));

    await expect(api.createApproval(input("G1", 8, "second-approval"))).rejects.toMatchObject({
      status: 409,
      errorCode: "RUN_TERMINAL",
    });
  });

  it("protects concurrent updates with ETag and never overwrites the newer revision", async () => {
    const api = new MockControlPlaneApi();
    await api.simulateConcurrentUpdate("G1");

    await expect(api.createApproval(input("G1", 7, "stale-client"))).rejects.toMatchObject({
      status: 409,
      errorCode: "REVISION_CONFLICT",
    });
  });

  it("returns the frozen affectedObjects.currentRevision truth for safe conflict recovery", async () => {
    const api = new MockControlPlaneApi();
    await api.simulateConcurrentUpdate("G1");

    const conflict = await api.createApproval(input("G1", 7, "stale-client")).catch((error: unknown) => error);
    expect(conflict).toBeInstanceOf(ApiProblem);
    expect((conflict as ApiProblem).affectedObjects).toEqual([
      { objectType: "GATE", objectId: "G1", currentRevision: 8 },
    ]);
    await expect(api.createApproval(input("G1", 8, "fresh-client"))).resolves.toMatchObject({
      gate: "G1",
      decision: "APPROVED",
      expectedRevision: 9,
    });
  });

  it("creates a new revision for regeneration instead of mutating the reviewed one", async () => {
    const api = new MockControlPlaneApi();
    await api.createApproval(input("G1", 7, "g1-return", "RETURNED"));
    const regenerated = await api.regenerateGate("G1", 8);

    expect(regenerated).toMatchObject({
      gate: "G1",
      revision: 8,
      revisionId: "gate-g1-r8",
      etag: 9,
    });
  });

  it("requires a new G2 revision after a regenerated G1 snapshot", async () => {
    const api = new MockControlPlaneApi();
    await api.createApproval(input("G1", 7, "g1-first"));
    await api.createApproval(input("G2", 9, "g2-first"));
    await api.regenerateGate("G1", 8);
    await api.createApproval(input("G1", 9, "g1-new"));

    await expect(api.createApproval(input("G2", 10, "g2-stale"))).rejects.toMatchObject({
      status: 422,
      errorCode: "STALE_DEPENDENCY",
    });

    await api.regenerateGate("G2", 10);
    await expect(api.createApproval(input("G2", 11, "g2-new"))).resolves.toMatchObject({
      gate: "G2",
      decision: "APPROVED",
    });
  });
});

describe("HttpControlPlaneApi", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("uses the frozen conflict payload and never calls an invented gate read route", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          type: "https://errors.example/revision-conflict",
          title: "Revision conflict",
          status: 409,
          detail: "G1 changed",
          errorCode: "REVISION_CONFLICT",
          retryable: false,
          traceId: "trc-live-conflict",
          suggestedAction: "Reconcile from affected objects",
          affectedObjects: [{ objectType: "GATE", objectId: "G1", currentRevision: 12 }],
        }),
        { status: 409, headers: { "Content-Type": "application/problem+json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const api = new HttpControlPlaneApi("/api/v1");

    const conflict = await api.createApproval(input("G1", 7, "live-stale")).catch((error: unknown) => error);

    expect(conflict).toBeInstanceOf(ApiProblem);
    expect((conflict as ApiProblem).affectedObjects[0]?.currentRevision).toBe(12);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/api/v1/approvals");
  });
});
