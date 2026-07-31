import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiProblem, type ApprovalInput, type CreateJobAttemptInput, type GateId } from "../domain";
import { gates, jobs } from "../mock-data";
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

const jobAttemptInput = (
  state: "FAILED" | "CANCELLED" = "FAILED",
  idempotencyKey = "attempt-key",
): CreateJobAttemptInput => ({
  sourceJob: { ...jobs[1], state },
  nextAttempt: 2,
  generationAttemptId: "22222222-2222-4222-8222-222222222202",
  idempotencyKey,
});

const projectInput = {
  title: "潮汐失语者 PoC",
  sourceText: "一段有权改编的原作内容",
  aspectRatio: "16:9",
  targetDuration: "03:20",
  targetAudience: "悬疑剧观众",
  visualStyle: "沿海现实主义",
};

const approveUpstreamGates = async (api: MockControlPlaneApi) => {
  await api.createApproval(input("G1", 7, "g1-before-g3"));
  await api.createApproval(input("G2", 9, "g2-before-g3"));
};

describe("MockControlPlaneApi", () => {
  it("creates a project operation from creator inputs without requiring a provider key", async () => {
    const api = new MockControlPlaneApi();
    await expect(
      api.createSeries(projectInput, "22222222-2222-4222-8222-222222222222"),
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

  it("rejects direct G3 approval while current attempts are incomplete", async () => {
    const api = new MockControlPlaneApi();
    await approveUpstreamGates(api);

    await expect(api.createApproval(input("G3", 2, "g3-incomplete"))).rejects.toMatchObject({
      status: 422,
      errorCode: "G3_RUNS_INCOMPLETE",
    });
  });

  it.each(["FAILED", "CANCELLED"] as const)(
    "rejects direct G3 approval while a current attempt is %s",
    async (state) => {
      const jobSeed = jobs.map((job) => ({
        ...job,
        state: (job.id === "job-v-032" ? state : "SUCCEEDED") as typeof job.state,
      }));
      const api = new MockControlPlaneApi(gates, jobSeed);
      await approveUpstreamGates(api);

      await expect(
        api.createApproval(input("G3", 2, `g3-${state.toLowerCase()}`)),
      ).rejects.toMatchObject({
        status: 422,
        errorCode: "G3_TERMINAL_ATTEMPT_BLOCKED",
      });
    },
  );

  it.each(["EPISODE_REVISION", "QC_REPORT", "MANIFEST", "ARTIFACT"] as const)(
    "rejects direct G3 approval without the exact %s binding",
    async (missingType) => {
      const jobSeed = jobs.map((job) => ({ ...job, state: "SUCCEEDED" as const }));
      const api = new MockControlPlaneApi(gates, jobSeed);
      await approveUpstreamGates(api);
      const g3Input = input("G3", 2, `g3-missing-${missingType.toLowerCase()}`);
      g3Input.bindings = g3Input.bindings.filter((binding) => binding.objectType !== missingType);

      await expect(api.createApproval(g3Input)).rejects.toMatchObject({
        status: 422,
        errorCode: "G3_BINDING_REQUIRED",
      });
    },
  );

  it("rejects G3 when an immutable evidence revision differs from control-plane truth", async () => {
    const jobSeed = jobs.map((job) => ({ ...job, state: "SUCCEEDED" as const }));
    const api = new MockControlPlaneApi(gates, jobSeed);
    await approveUpstreamGates(api);
    const g3Input = input("G3", 2, "g3-evidence-conflict");
    g3Input.bindings = g3Input.bindings.map((binding) =>
      binding.objectType === "MANIFEST"
        ? { ...binding, revisionId: "manifest-e03-stale" }
        : binding,
    );

    await expect(api.createApproval(g3Input)).rejects.toMatchObject({
      status: 409,
      errorCode: "G3_BINDING_CONFLICT",
    });
  });

  it("approves G3 only after the Mock control plane records successful attempts and exact evidence", async () => {
    const api = new MockControlPlaneApi();
    await approveUpstreamGates(api);
    await api.completeMockRun();

    await expect(api.createApproval(input("G3", 2, "g3-complete"))).resolves.toMatchObject({
      gate: "G3",
      decision: "APPROVED",
    });
  });

  it.each(["FAILED", "CANCELLED"] as const)(
    "creates a distinct current Job for a %s creative redo without mutating the source",
    async (state) => {
      const api = new MockControlPlaneApi();
      const input = jobAttemptInput(state, `new-${state.toLowerCase()}-attempt`);
      const result = await api.createJobAttempt(input);

      expect(result).toMatchObject({
        providerJobId: "job-v-032-a2",
        generationAttemptId: input.generationAttemptId,
        state: "QUEUED",
      });
      expect(result.providerJobId).not.toBe(input.sourceJob.id);
      expect(input.sourceJob).toMatchObject({
        id: "job-v-032",
        state,
        attempt: 1,
        isCurrentAttempt: true,
      });
    },
  );

  it("keeps creative-attempt submission idempotent and rejects a changed payload", async () => {
    const api = new MockControlPlaneApi();
    const firstInput = jobAttemptInput("FAILED", "same-attempt-key");
    const first = await api.createJobAttempt(firstInput);

    await expect(api.createJobAttempt(firstInput)).resolves.toEqual(first);
    await expect(
      api.createJobAttempt({
        ...firstInput,
        nextAttempt: 3,
      }),
    ).rejects.toMatchObject({ status: 409, errorCode: "conflict" });
  });

  it("rejects a different-key replay of an already persisted source attempt", async () => {
    const api = new MockControlPlaneApi();
    const firstInput = jobAttemptInput("FAILED", "original-attempt-key");
    const first = await api.createJobAttempt(firstInput);

    await expect(
      api.createJobAttempt({
        ...firstInput,
        idempotencyKey: "different-attempt-key",
        generationAttemptId: "44444444-4444-4444-8444-444444444404",
      }),
    ).rejects.toMatchObject({
      status: 409,
      errorCode: "ATTEMPT_ALREADY_EXISTS",
    });
    await expect(api.createJobAttempt(firstInput)).resolves.toEqual(first);
  });

  it("advances server-side current attempt truth from attempt 2 to attempt 3", async () => {
    const api = new MockControlPlaneApi();
    const firstInput = jobAttemptInput("FAILED", "attempt-2-key");
    const attempt2 = await api.createJobAttempt(firstInput);
    const attempt3 = await api.createJobAttempt({
      sourceJob: {
        ...firstInput.sourceJob,
        id: attempt2.providerJobId,
        state: "FAILED",
        attempt: 2,
        isCurrentAttempt: true,
      },
      nextAttempt: 3,
      generationAttemptId: "55555555-5555-4555-8555-555555555505",
      idempotencyKey: "attempt-3-key",
    });

    expect(attempt3).toMatchObject({
      providerJobId: "job-v-032-a3",
      generationAttemptId: "55555555-5555-4555-8555-555555555505",
    });
    await expect(
      api.createJobAttempt({
        ...firstInput,
        idempotencyKey: "stale-source-key",
        generationAttemptId: "66666666-6666-4666-8666-666666666606",
      }),
    ).rejects.toMatchObject({
      status: 409,
      errorCode: "ATTEMPT_ALREADY_EXISTS",
    });
    await expect(
      api.createJobAttempt({
        ...firstInput,
        nextAttempt: 3,
        idempotencyKey: "superseded-source-key",
        generationAttemptId: "77777777-7777-4777-8777-777777777707",
      }),
    ).rejects.toMatchObject({
      status: 409,
      errorCode: "ATTEMPT_SOURCE_SUPERSEDED",
    });
  });
});

describe("HttpControlPlaneApi", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("fails closed before fetch when a Mock-derived project is submitted in live mode", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const api = new HttpControlPlaneApi("/api/v1");

    await expect(api.createSeries(projectInput, "live-project")).rejects.toMatchObject({
      status: 501,
      errorCode: "LIVE_PROJECTION_BINDINGS_REQUIRED",
    });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it.each(["G1", "G2", "G3"] as const)(
    "fails closed before fetch when Mock-derived %s approval is submitted in live mode",
    async (gateId) => {
      const fetchMock = vi.fn();
      vi.stubGlobal("fetch", fetchMock);
      const api = new HttpControlPlaneApi("/api/v1");

      await expect(
        api.createApproval(input(gateId, gates[gateId].etag, `live-${gateId}`)),
      ).rejects.toMatchObject({
        status: 501,
        errorCode: "LIVE_PROJECTION_BINDINGS_REQUIRED",
      });
      expect(fetchMock).not.toHaveBeenCalled();
    },
  );

  it("fails closed before fetch when a Mock-derived Job attempt is submitted in live mode", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const api = new HttpControlPlaneApi("/api/v1");

    await expect(
      api.createJobAttempt(jobAttemptInput("FAILED", "live-attempt")),
    ).rejects.toMatchObject({
      status: 501,
      errorCode: "LIVE_PROJECTION_BINDINGS_REQUIRED",
    });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("fails closed before fetch for every remaining Mock-only control mutation", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const api = new HttpControlPlaneApi("/api/v1");

    await expect(api.completeMockRun()).rejects.toMatchObject({
      errorCode: "LIVE_PROJECTION_BINDINGS_REQUIRED",
    });
    await expect(api.regenerateGate("G1", 7)).rejects.toMatchObject({
      errorCode: "LIVE_PROJECTION_BINDINGS_REQUIRED",
    });
    await expect(api.simulateConcurrentUpdate("G1")).rejects.toMatchObject({
      errorCode: "LIVE_PROJECTION_BINDINGS_REQUIRED",
    });
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
