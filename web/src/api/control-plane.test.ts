import { describe, expect, it } from "vitest";
import type { ApprovalInput, GateId } from "../domain";
import { gates } from "../mock-data";
import { MockControlPlaneApi } from "./control-plane";

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

  it("lets a stale client synchronize the current ETag and safely resubmit", async () => {
    const api = new MockControlPlaneApi();
    await api.simulateConcurrentUpdate("G1");

    const synchronized = await api.synchronizeGate("G1");
    expect(synchronized).toMatchObject({ id: "G1", revision: 7, etag: 8 });
    await expect(api.createApproval(input("G1", synchronized.etag, "fresh-client"))).resolves.toMatchObject({
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
});
