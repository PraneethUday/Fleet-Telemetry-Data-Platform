import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dashboard is served as static files by whatever fronts it (a dev server
// locally, Azure Static Web Apps / a CDN in the cloud) and talks to the API
// cross-origin. There is deliberately no dev proxy: proxying would let a
// same-origin assumption creep into the code that then breaks in Azure, where
// the API really is on another host. CORS is handled by the backend instead.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
