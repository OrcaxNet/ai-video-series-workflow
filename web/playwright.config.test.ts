import { describe, expect, it } from "vitest";
import config from "./playwright.config";

describe("Playwright server isolation", () => {
  it("never reuses the OrbStack Studio bound to port 4173", () => {
    expect(config.use).toMatchObject({ baseURL: "http://127.0.0.1:4174" });
    expect(config.webServer).toMatchObject({
      command: expect.stringContaining("--port 4174 --strictPort"),
      url: "http://127.0.0.1:4174",
      reuseExistingServer: false,
    });
  });
});
