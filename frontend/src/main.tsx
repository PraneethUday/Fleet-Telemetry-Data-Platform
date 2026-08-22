import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import App from "./App";
import "./styles.css";

const container = document.getElementById("root");
// Failing loudly beats React's "container is null" further down the stack: if
// index.html ever loses the mount node, this says exactly what is wrong.
if (!container) {
  throw new Error('Mount node #root is missing from index.html');
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
