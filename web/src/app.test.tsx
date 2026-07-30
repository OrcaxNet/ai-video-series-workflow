import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { MockControlPlaneApi } from "./api/control-plane";
import { App } from "./app";
import { StudioProvider } from "./studio-store";

const renderStudio = () =>
  render(
    <StudioProvider api={new MockControlPlaneApi()}>
      <App />
    </StudioProvider>,
  );

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
});
