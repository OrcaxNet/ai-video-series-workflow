import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./app";
import { StudioProvider } from "./studio-store";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <StudioProvider>
      <App />
    </StudioProvider>
  </StrictMode>,
);
