import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { MockControlPlaneApi, type ControlPlaneApi } from "./api/control-plane";
import { App } from "./app";
import type { CreateJobAttemptInput, CreateJobAttemptResult } from "./domain";
import { createInitialState } from "./mock-data";
import { StudioProvider, useStudio } from "./studio-store";

const renderStudio = (api: ControlPlaneApi = new MockControlPlaneApi()) =>
  render(
    <StudioProvider api={api}>
      <App />
    </StudioProvider>,
  );

const failedJobsState = () => {
  const state = createInitialState();
  state.view = "jobs";
  state.jobs[1] = { ...state.jobs[1], state: "FAILED" };
  return state;
};

class DeferredAttemptApi extends MockControlPlaneApi {
  readonly attemptInputs: CreateJobAttemptInput[] = [];
  private resolveAttempt?: (result: CreateJobAttemptResult) => void;

  override async createJobAttempt(input: CreateJobAttemptInput): Promise<CreateJobAttemptResult> {
    this.attemptInputs.push(structuredClone(input));
    return new Promise((resolve) => {
      this.resolveAttempt = resolve;
    });
  }

  completeAttempt() {
    const input = this.attemptInputs[0];
    if (!input || !this.resolveAttempt) throw new Error("No pending attempt");
    this.resolveAttempt({
      providerJobId: "job-v-032-a2",
      generationAttemptId: input.generationAttemptId,
      state: "QUEUED",
      traceId: "trc-deferred-attempt-2",
      createdAt: "2026-07-30T06:50:00Z",
    });
  }
}

class LostFirstResponseApi extends MockControlPlaneApi {
  readonly attemptInputs: CreateJobAttemptInput[] = [];
  private loseResponse = true;

  override async createJobAttempt(input: CreateJobAttemptInput): Promise<CreateJobAttemptResult> {
    this.attemptInputs.push(structuredClone(input));
    const result = await super.createJobAttempt(input);
    if (this.loseResponse) {
      this.loseResponse = false;
      throw new Error("simulated accepted request with lost response");
    }
    return result;
  }
}

function SynchronousDuplicateAttemptProbe() {
  const { actions } = useStudio();
  return (
    <button
      type="button"
      onClick={() => {
        void actions.createJobAttempt("job-v-032");
        void actions.createJobAttempt("job-v-032");
      }}
    >
      同步重复创建
    </button>
  );
}

describe("creator studio", () => {
  it("makes the mock/live evidence boundary visible", () => {
    renderStudio();

    expect(screen.getByText("Mock only", { selector: ".mode-pill strong" })).toBeInTheDocument();
    expect(screen.getByText("live pending_key")).toBeInTheDocument();
    expect(screen.getByText(/不能把结果当作真实生成质量/)).toBeInTheDocument();
  });

  it("does not expose downstream gate review before G1 is approved", async () => {
    renderStudio();

    expect(screen.getAllByRole("button", { name: "等待上游" })).toHaveLength(2);
    fireEvent.click(screen.getByRole("button", { name: "开始审核" }));
    fireEvent.change(screen.getByLabelText("审核意见"), { target: { value: "资产与权利快照已核对" } });
    fireEvent.click(screen.getByRole("button", { name: "批准并锁定 r7" }));

    await waitFor(() => expect(screen.getByText("G1 已批准并锁定")).toBeInTheDocument());
    expect(screen.getAllByRole("button", { name: "开始审核" })).toHaveLength(1);
    expect(screen.getAllByRole("button", { name: "等待上游" })).toHaveLength(1);
  });

  it("sends one Provider request when the same attempt intent is activated synchronously twice", async () => {
    const api = new DeferredAttemptApi();
    render(
      <StudioProvider api={api} initialState={failedJobsState()}>
        <SynchronousDuplicateAttemptProbe />
        <App />
      </StudioProvider>,
    );

    fireEvent.click(screen.getByRole("button", { name: "同步重复创建" }));

    await waitFor(() => expect(api.attemptInputs).toHaveLength(1));
    expect(screen.getByText(/本次重复激活不会发送第二个 Provider Job 请求/)).toBeInTheDocument();
    await act(async () => {
      api.completeAttempt();
      await Promise.resolve();
    });
    await waitFor(() => expect(screen.getByText("S03-02 attempt 2 已创建")).toBeInTheDocument());
  });

  it("reuses the original intent IDs after an accepted request loses its response", async () => {
    const api = new LostFirstResponseApi();
    render(
      <StudioProvider api={api} initialState={failedJobsState()}>
        <App />
      </StudioProvider>,
    );
    const createAttempt = screen.getByRole("button", { name: "为 S03-02 创建新 attempt" });

    fireEvent.click(createAttempt);
    await waitFor(() => expect(screen.getByText("500 · INTERNAL_ERROR")).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: "为 S03-02 创建新 attempt" }));

    await waitFor(() => expect(api.attemptInputs).toHaveLength(2));
    expect(api.attemptInputs[1]?.idempotencyKey).toBe(api.attemptInputs[0]?.idempotencyKey);
    expect(api.attemptInputs[1]?.generationAttemptId).toBe(api.attemptInputs[0]?.generationAttemptId);
    expect(api.attemptInputs[0]?.idempotencyKey).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    await waitFor(() => expect(screen.getByText("S03-02 attempt 2 已创建")).toBeInTheDocument());
    expect(screen.getByText("job-v-032-a2", { exact: true })).toBeInTheDocument();
  });
});
